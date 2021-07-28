package frontend

import (
	"context"
	"fmt"

	"go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/mvcc/mvccpb"

	klogv2 "k8s.io/klog/v2"

	storageerrors "github.com/khenidak/london/pkg/backend/storageerrors"

	"github.com/khenidak/london/pkg/types"
)

func (fe *frontend) Put(ctx context.Context, r *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	if r.IgnoreLease || r.IgnoreValue || r.PrevKv {
		return nil, createUnsupportedError("put update")
	}
	// this is an insert operation
	insertedRecord, err := fe.be.Insert(string(r.Key), r.Value, r.Lease)
	if err != nil {
		return nil, err
	}

	putResponse := &etcdserverpb.PutResponse{}
	putResponse.Header = createResponseHeader(insertedRecord.ModRevision())
	putResponse.PrevKv = nil

	fe.lm.notifyInserted(insertedRecord)
	return putResponse, nil
}

func (fe *frontend) Range(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	if err := isValidRangeRequest(r); err != nil {
		klogv2.V(4).Infof("invalid RangeRequest:%+v with err:%v", r, err)
		return nil, err
	}

	if len(r.RangeEnd) == 0 {
		// treat as Get
		return fe.rangeGet(ctx, r)
	}

	return fe.rangeList(ctx, r)
}
func (fe *frontend) rangeList(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	rangeEnd := r.RangeEnd[len(r.RangeEnd)-1]
	// TODO: confirm that this is the correct approach
	if rangeEnd == byte(0) {
		return nil, createUnsupportedError("key >= key range requests are not supported")
	}

	createResponse := func(rev int64, records []types.Record) *etcdserverpb.RangeResponse {
		response := &etcdserverpb.RangeResponse{
			Header: createResponseHeader(rev),
			Count:  int64(len(records)),
		}

		if !r.CountOnly {
			kvs := types.RecordsToKVs(records)
			response.Kvs = kvs
		}

		return response
	}

	if string(r.RangeEnd) == string(byte(0)) &&
		string(r.Key) == string(byte(0)) {
		if r.GetRevision() != 0 {
			return nil, createUnsupportedError("get all keys with revision is not supported")
		}
		// list all
		klogv2.V(4).Infof("FE: EXPENSIVE CALL - ListAllCurrent() ")
		rev, records, err := fe.be.ListAllCurrent()
		if err != nil {
			return nil, err
		}
		return createResponse(rev, records), nil
	}

	keyWithSuffix := suffixedKey(string(r.Key))
	rev, _ := fe.be.CurrentRevision()
	records, err := fe.lm.list(keyWithSuffix)
	if err != nil {
		return nil, err
	}

	return createResponse(rev, records), nil
}

func (fe *frontend) rangeGet(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	if r.Limit != 0 {
		return nil, createUnsupportedError("range/get with limit !=0")
	}

	createResponse := func(record types.Record) *etcdserverpb.RangeResponse {
		response := &etcdserverpb.RangeResponse{}
		if record == nil {
			// non existent results
			rev, _ := fe.be.CurrentRevision()
			response.Header = createResponseHeader(rev)
			response.Count = 0
			return response
		}
		response.Header = createResponseHeader(record.ModRevision())
		response.Count = 1
		response.Kvs = []*mvccpb.KeyValue{types.RecordToKV(record)}

		return response
	}

	// try to find in cache
	if cachedRecord := fe.lm.getRecord(string(r.Key)); cachedRecord != nil {
		return createResponse(cachedRecord), nil
	}

	// get from from store
	record, _, err := fe.be.Get(string(r.Key), r.Revision)
	if err != nil { // TODO: errCompact if revision is used
		return nil, err
	}

	return createResponse(record), nil
}

func (fe *frontend) DeleteRange(ctx context.Context, r *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	return nil, fmt.Errorf("delete is not supported")
}

// Txn is the entry point for update, upsert and delete
func (fe *frontend) Txn(ctx context.Context, r *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	// insert
	if shouldInsert(r) {
		return fe.txnInsert(ctx, r.Success[0].GetRequestPut())
	}

	if shouldDelete(r) {
		rev, key := getDeleteDetails(r)
		return fe.txnDelete(ctx, key, rev)
	}

	// update
	if shouldUpdate(r) {
		rev, key, val, lease := getUpdateDetails(r)
		return fe.txnUpdate(ctx, rev, key, val, lease)
	}

	klogv2.V(2).Infof("Warning: Got unsupported range request:%+v", r)
	return nil, createUnsupportedError("unsupported range request")
}

func (fe *frontend) txnUpdate(ctx context.Context, rev int64, key string, val []byte, lease int64) (*etcdserverpb.TxnResponse, error) {
	if rev == 0 {
		// create mode
		insertedRecord, err := fe.be.Insert(key, val, lease)
		if err != nil {
			return nil, err
		}
		resp := &etcdserverpb.TxnResponse{
			Header:    createResponseHeader(insertedRecord.ModRevision()),
			Succeeded: true,

			Responses: []*etcdserverpb.ResponseOp{
				{
					Response: &etcdserverpb.ResponseOp_ResponsePut{
						ResponsePut: &etcdserverpb.PutResponse{
							Header: createResponseHeader(insertedRecord.ModRevision()),
						},
					},
				},
			},
		}
		fe.lm.notifyInserted(insertedRecord)
		return resp, nil
	}

	// update mode
	oldRecord, record, err := fe.be.Update(key, val, rev, lease)
	if err == nil {
		// success: record was updated
		// notify list manager
		fe.lm.notifyUpdated(oldRecord, record)

		resp := &etcdserverpb.TxnResponse{
			Header:    createResponseHeader(record.ModRevision()),
			Succeeded: true,

			Responses: []*etcdserverpb.ResponseOp{
				{
					Response: &etcdserverpb.ResponseOp_ResponsePut{
						ResponsePut: &etcdserverpb.PutResponse{
							Header: createResponseHeader(record.ModRevision()),
						},
					},
				},
			},
		}

		return resp, nil
	}

	// error cases -- update itself was not performed
	// we should be able to ignore errors here
	currentRev, _ := fe.be.CurrentRevision()

	// record is not found
	if storageerrors.IsNotFoundError(err) {
		// this can happen if:
		// 0. the client is really making a mistake.
		// 1. api-server cache is not update yet. it will perform another get and retry
		// 2. our own cache is yet to be updated.
		// -- in order to avoid an unneeded retry for (#1+#2): inform listManage
		// that this has been deleted.
		// we have no rev here, so we have to resort to use key only
		fe.lm.notifyDeletedKey(key)

		klogv2.V(4).Infof("Warning: Txn-Update: NOT-FOUND: %v:%v", key, rev)
		resp := &etcdserverpb.TxnResponse{
			Header:    createResponseHeader(currentRev),
			Succeeded: false,
			Responses: []*etcdserverpb.ResponseOp{
				{
					Response: &etcdserverpb.ResponseOp_ResponseRange{
						ResponseRange: &etcdserverpb.RangeResponse{
							Header: createResponseHeader(currentRev),
							Kvs:    []*mvccpb.KeyValue{types.RecordToKV(oldRecord)},
						},
					},
				},
			},
		}

		return resp, nil
	}

	// record exist but not at the expected revision
	if storageerrors.IsConflictError(err) {
		// similar to NotFound error. The catch here is we don't have old's old record
		// so we use the insert notification
		fe.lm.notifyInserted(oldRecord)

		klogv2.V(8).Infof("Warning: Txn-Update: Conflict: KEY(%v):REV(%v) - STORED:%v", key, rev, oldRecord.ModRevision())
		resp := &etcdserverpb.TxnResponse{
			Header:    createResponseHeader(currentRev),
			Succeeded: false,

			Responses: []*etcdserverpb.ResponseOp{
				{
					Response: &etcdserverpb.ResponseOp_ResponseRange{
						ResponseRange: &etcdserverpb.RangeResponse{
							Header: createResponseHeader(currentRev),
							Kvs:    []*mvccpb.KeyValue{types.RecordToKV(oldRecord)},
						},
					},
				},
			},
		}
		return resp, nil
	}

	// unknown error. return as is
	return nil, err
}
func (fe *frontend) txnDelete(ctx context.Context, key string, rev int64) (*etcdserverpb.TxnResponse, error) {
	record, err := fe.be.Delete(key, rev)

	if err != nil {
		// similar to failed update cases
		if storageerrors.IsNotFoundError(err) {
			fe.lm.notifyDeletedKey(key)
		}
		klogv2.V(2).Infof("Warning: error deleting %v:%v %v", rev, key, err)
		return nil, err // TODO: should it be an error response?
	}

	// notify
	fe.lm.notifyDeleted(record)
	// prep and send return
	return &etcdserverpb.TxnResponse{
		Header: createResponseHeader(record.ModRevision()),

		Responses: []*etcdserverpb.ResponseOp{
			{
				Response: &etcdserverpb.ResponseOp_ResponseRange{
					ResponseRange: &etcdserverpb.RangeResponse{
						Header: createResponseHeader(record.ModRevision()),
						Kvs:    []*mvccpb.KeyValue{types.RecordToKV(record)},
					},
				},
			},
		},
		Succeeded: true,
	}, nil
}

func (fe *frontend) txnInsert(ctx context.Context, putRequest *etcdserverpb.PutRequest) (*etcdserverpb.TxnResponse, error) {
	insertedRecord, err := fe.be.Insert(string(putRequest.Key), putRequest.Value, putRequest.Lease)
	// error occured
	if err != nil {
		// key existed
		if storageerrors.IsEntityAlreadyExists(err) {
			rev, _ := fe.be.CurrentRevision() // we ignore error here.
			return &etcdserverpb.TxnResponse{
				Succeeded: false,
				Header:    createResponseHeader(rev),
			}, nil
		}
		// unknown error
		return nil, err
	}
	// success txn-insert
	response := &etcdserverpb.TxnResponse{
		Succeeded: true,
		Header:    createResponseHeader(insertedRecord.ModRevision()),
		Responses: []*etcdserverpb.ResponseOp{
			{
				Response: &etcdserverpb.ResponseOp_ResponsePut{
					ResponsePut: &etcdserverpb.PutResponse{
						Header: createResponseHeader(insertedRecord.ModRevision()),
					},
				},
			},
		},
	}

	fe.lm.notifyInserted(insertedRecord)
	return response, nil
}

func (fe *frontend) Compact(ctx context.Context, r *etcdserverpb.CompactionRequest) (*etcdserverpb.CompactionResponse, error) {
	rev, err := fe.be.Compact(r.Revision)
	if err != nil {
		return nil, err
	}

	// notify list manager
	fe.lm.notifyCompact(r.Revision)

	return &etcdserverpb.CompactionResponse{
		Header: createResponseHeader(rev),
	}, nil
}

// thanks for k3s-io/kine for doing the work to figure out the format of a tx
// for each type of action
func shouldInsert(txn *etcdserverpb.TxnRequest) bool {
	// should have one compare with `=`
	// and only put request in success (and no actions on failures)
	if len(txn.Compare) != 1 {
		return false
	}

	if len(txn.Failure) != 0 {
		return false
	}

	if len(txn.Success) != 1 {
		return false
	}

	compare0 := txn.Compare[0]
	if compare0.Target != etcdserverpb.Compare_MOD {
		return false
	}

	if compare0.Result != etcdserverpb.Compare_EQUAL {
		return false
	}

	if compare0.GetModRevision() != 0 {
		return false
	}

	if compare0.GetModRevision() != 0 {
		return false
	}

	return txn.Success[0].GetRequestPut() != nil
}

// test a txn for Delete. Note: delete is
// if mod == (!zero) DELETE else GET
// OR
// just delete current
func shouldDelete(txn *etcdserverpb.TxnRequest) bool {
	if len(txn.Compare) == 0 {
		if len(txn.Failure) != 0 {
			return false
		}

		if len(txn.Success) != 2 {
			return false
		}

		if txn.Success[0].GetRequestRange() == nil {
			return false
		}

		if txn.Success[1].GetRequestDeleteRange() == nil {
			return false
		}

		return true
	}

	if len(txn.Compare) == 1 {
		if txn.Compare[0].Target != etcdserverpb.Compare_MOD {
			return false
		}

		if len(txn.Failure) != 1 {
			return false
		}

		if len(txn.Success) != 1 {
			return false
		}

		if txn.Compare[0].Result != etcdserverpb.Compare_EQUAL {
			return false
		}

		if txn.Failure[0].GetRequestRange() == nil {
			return false
		}

		if txn.Success[0].GetRequestDeleteRange() == nil {
			return false
		}
		return true
	}
	return false
}

// given a txn which has been already tested for Delete, get the delete details
func getDeleteDetails(txn *etcdserverpb.TxnRequest) (int64, string) {
	if len(txn.Compare) == 0 {
		return 0, string(txn.Success[1].GetRequestDeleteRange().Key)
	}

	return txn.Compare[0].GetModRevision(), string(txn.Success[0].GetRequestDeleteRange().Key)
}

// tests a txn request for update
func shouldUpdate(txn *etcdserverpb.TxnRequest) bool {
	if len(txn.Compare) != 1 {
		return false
	}

	if len(txn.Success) != 1 {
		return false
	}

	if len(txn.Failure) != 1 {
		return false
	}

	if txn.Compare[0].Target != etcdserverpb.Compare_MOD {
		return false
	}

	if txn.Compare[0].Result != etcdserverpb.Compare_EQUAL {
		return false
	}

	if txn.Success[0].GetRequestPut() == nil {
		return false
	}

	if txn.Failure[0].GetRequestRange() == nil {
		return false
	}

	return true
}

// given a txn (that has been tested for Update) return update details
func getUpdateDetails(txn *etcdserverpb.TxnRequest) (rev int64, key string, val []byte, lease int64) {
	return txn.Compare[0].GetModRevision(), string(txn.Compare[0].Key),
		txn.Success[0].GetRequestPut().Value, txn.Success[0].GetRequestPut().Lease
}

// validates range requests for supportability.
func isValidRangeRequest(r *etcdserverpb.RangeRequest) error {
	// https://etcd.io/docs/v3.3.12/dev-guide/api_reference_v3/ (range request)
	if r.Serializable {
		return createUnsupportedError("RangeRequest.Serializable is not supported")
	}

	if r.KeysOnly {
		return createUnsupportedError("keysOnly")
	}

	if r.MaxCreateRevision != 0 {
		return createUnsupportedError("maxCreateRevision")
	}

	if r.SortOrder != 0 {
		return createUnsupportedError("sortOrder")
	}

	if r.SortTarget != 0 {
		return createUnsupportedError("sortTarget")
	}

	if r.Serializable {
		return createUnsupportedError("serializable")
	}

	if r.MinModRevision != 0 {
		return createUnsupportedError("minModRevision")
	}

	if r.MinCreateRevision != 0 {
		return createUnsupportedError("minCreateRevision")
	}

	if r.MaxCreateRevision != 0 {
		return createUnsupportedError("maxCreateRevision")
	}

	if r.MaxModRevision != 0 {
		return createUnsupportedError("maxModRevision")
	}

	return nil
}
