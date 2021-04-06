package frontend

import (
	"context"
	"sync"
	"time"

	"go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/mvcc/mvccpb"

	"github.com/khenidak/london/pkg/types"

	klogv2 "k8s.io/klog/v2"
)

type watcher struct {
	watcherId      int64
	closerFn       func(string)
	key            string
	watcherContext context.Context
	waitGroup      *sync.WaitGroup
	watchServer    etcdserverpb.Watch_WatchServer
}

// Note: there might be multiple calls to Watch(...)
// Each server may have multiple watch requests. When wiring
// context wire the instance of ws that created the stream
// otherwise you will be crossing context streams
// the root of this call is grpc itself, we don't maintain
// a reference to ws on FE, we do that per call to this func
func (fe *frontend) Watch(ws etcdserverpb.Watch_WatchServer) error {
	// this map is maintained by each watch server instance
	watchersForThisServer := map[int64]*watcher{}
	// watch server root context
	watchServerDone := ws.Context().Done()
	// wait group for all watchers out of this server
	watchServerWg := sync.WaitGroup{}
	watchServerLock := sync.Mutex{}

	// creates a watcher close func that:
	// 1- removes it from list of tracked watcher
	// 2- closes the context for the loop to stop
	// 3- sends the close message
	createWatcher := func(key string) *watcher {
		watchServerLock.Lock()
		defer watchServerLock.Unlock()

		// create new Id for this watch
		newWatchId := fe.watchCount + 1
		watcherCtx, cancel := context.WithCancel(context.Background())
		w := &watcher{
			key:            key,
			watcherId:      newWatchId,
			watcherContext: watcherCtx,
			waitGroup:      &watchServerWg,
			watchServer:    ws,
			closerFn: func(reason string) {
				watchServerLock.Lock()
				defer watchServerLock.Unlock()
				if _, ok := watchersForThisServer[newWatchId]; !ok {
					// already closed
					return
				}

				// remove it from tracked watchers
				delete(watchersForThisServer, newWatchId)
				// cancel context for watcher loop
				cancel()

				// notify watch server that we are closing this watcher
				closeResponse := &etcdserverpb.WatchResponse{
					Header:       &etcdserverpb.ResponseHeader{},
					Canceled:     true,
					CancelReason: reason,
					WatchId:      newWatchId,
					Created:      false,
				}
				// transport errors, context cancelation (racy between
				// server closing and watcher close).. all ignored
				_ = ws.Send(closeResponse)
			},
		}

		watchersForThisServer[newWatchId] = w
		return w
	}

	// this loop is for watch server itself
	// to create and destroy watchers
	for {
		select {
		case <-watchServerDone:
			klogv2.Infof("WATCH CLOSE ALL")
			currentWatchers := func() []func(string) {
				watchServerLock.Lock()
				defer watchServerLock.Unlock()
				all := make([]func(string), 0, len(watchersForThisServer))
				for _, watcher := range watchersForThisServer {
					all = append(all, watcher.closerFn)
				}
				return all
			}()

			// close
			for _, closer := range currentWatchers {
				// we are primarly interested in stopping the loop
				// not sending the close message. So we will attempt
				// to send the close message but we will ignore the error
				// that may result out of the op
				closer("watch server is closing")
			}
			watchServerWg.Wait() // wait for all to finish
			return nil

		default:
			msg, err := ws.Recv()
			if err != nil {
				return err
			}

			if createRequest := msg.GetCreateRequest(); createRequest != nil {
				// run its loop
				w := createWatcher(string(createRequest.Key))
				watchServerWg.Add(1)
				go fe.watcherLoop(w, createRequest)
				continue
			}

			if cancelRequest := msg.GetCancelRequest(); cancelRequest != nil {
				watchServerLock.Lock()
				w := watchersForThisServer[cancelRequest.WatchId]
				watchServerLock.Unlock()
				w.closerFn("close requested by watch server")
				continue
			}

			//TODO: progress request?
			klogv2.Infof("WATCH +UNSUPPORTED+ unknown watch request (PROGRESS?):%+v", msg)
		}
	}
}

func (fe *frontend) watcherLoop(w *watcher, r *etcdserverpb.WatchCreateRequest) {
	defer w.waitGroup.Done()
	keyWithSuffix := suffixedKey(string(r.Key))

	klogv2.Infof("WATCHRUNNING:%v-%v", w.watcherId, keyWithSuffix)
	first := true
	done := w.watcherContext.Done()
	lastRevision := r.StartRevision + 1 // azure greater than op is a bit problamtic
	createSend := false
	for {

		select {
		case <-done:
			klogv2.Infof("WATCH %v:%v is done", w.watcherId, w.key)
			return
		default:
			if !first {
				//We scale it down to once every 250 ms
				// except first run
				time.Sleep(250 * time.Millisecond)
			}
			first = false

			records, err := fe.be.ListForWatch(keyWithSuffix, lastRevision)
			if err != nil {
				klogv2.Infof("WATCH CLOSERR (ListForWatch) :%v", err)
				w.closerFn(err.Error())
				return
			}

			if len(records) == 0 {
				continue
			}
			// update last rev

			allEvents := make([]*mvccpb.Event, 0, len(records))
			for _, record := range records {
				lastRevision = record.ModRevision()
				// convert this record to an event
				e, err := fe.recordToEvent(record)
				if err != nil {
					klogv2.Infof("WATCH err (recordToEvent) :%v", err)
					w.closerFn(err.Error())
					return
				}
				// add it to events
				allEvents = append(allEvents, e)
			}
			// set revision to + 1
			lastRevision = lastRevision + 1

			// TODO: We need to figure out a way to send create
			// if the watch didn't produce data at all
			if !createSend {
				created := &etcdserverpb.WatchResponse{
					Header:  &etcdserverpb.ResponseHeader{},
					Created: true,
					WatchId: w.watcherId,
					Events:  []*mvccpb.Event{},
				}

				klogv2.Infof("WATCH sending created:%v-%v", w.watcherId, keyWithSuffix)
				if err := w.watchServer.Send(created); err != nil {
					klogv2.Infof("WATCHSENDERR 1st send err %v:%v %v", w.watcherId, keyWithSuffix, err)
					// don't close watcher here, since the error is from underlying
					// grpc stream and context would be probably already closed
					return
				}

				createSend = true
			}

			klogv2.Infof("WATCHSEND: %v events to :%v", len(allEvents), keyWithSuffix)
			// prep a response
			response := &etcdserverpb.WatchResponse{
				Header:          createResponseHeader(lastRevision),
				WatchId:         w.watcherId,
				CompactRevision: r.StartRevision,
				Events:          allEvents,
			}

			// send it
			if err := w.watchServer.Send(response); err != nil {
				klogv2.Infof("WATCHCLOSEERR err (sending response) :%v", err)
				// don't close watcher here, since the error is from underlying
				// grpc stream and context would be probably already closed
				return
			}

			klogv2.Infof("WATCHSENDOK %v", keyWithSuffix)
		}
	}
}

// given record (of type event) convert it to a etcd
// watch Event
func (fe *frontend) recordToEvent(record types.Record) (*mvccpb.Event, error) {
	e := &mvccpb.Event{}
	e.Kv = types.RecordToKV(record)

	if !record.IsEventRecord() {
		panic("must be event record") // should never happen. Left here during early release and must be removed later on
	}
	if record.IsCreateEvent() {
		e.Type = mvccpb.PUT
		return e, nil
	}

	if record.IsDeleteEvent() {
		e.Type = mvccpb.DELETE
		e.PrevKv = types.RecordToKV(record)
		e.Kv.Value = nil
		e.PrevKv.ModRevision = record.PrevRevision()
		return e, nil
	}

	// this is an update, get old key to do old-new
	e.Type = mvccpb.PUT
	oldRecord, _, err := fe.be.Get(string(record.Key()), record.PrevRevision())
	if err != nil {
		return nil, err
	}

	// this will happen if old has been compacted out of the db
	if oldRecord == nil {
		return e, nil
	}

	e.PrevKv = types.RecordToKV(oldRecord)
	return e, nil
}
