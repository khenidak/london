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

	klogv2.Infof("TXN PUT: %v", string(r.Key))

	if r.IgnoreLease || r.IgnoreValue || r.PrevKv {
		return nil, createUnsupportedError("put update")
	}
	// this is an insert operation
	rev, err := fe.be.Insert(string(r.Key), r.Value, r.Lease)
	if err != nil {
		return nil, err
	}

	putResponse := &etcdserverpb.PutResponse{}
	putResponse.Header = createResponseHeader(rev)
	putResponse.PrevKv = nil

	return putResponse, nil
}

func (fe *frontend) Range(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	if err := isValidRangeRequest(r); err != nil {
		klogv2.Infof("invalid RangeRequest:%+v with err:%v", r, err)
		return nil, err
	}

	if len(r.RangeEnd) == 0 {
		// treat as Get
		klogv2.Infof("RANGE-GET: %v", string(r.Key))
		return fe.rangeGet(ctx, r)
	}

	klogv2.Infof("RANGE-LIST: %v", string(r.Key))
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
		rev, records, err := fe.be.ListAllCurrent()
		if err != nil {
			return nil, err
		}
		return createResponse(rev, records), nil
	}

	keyWithSuffix := suffixedKey(string(r.Key))
	rev, records, err := fe.be.ListForPrefix(keyWithSuffix)
	if err != nil {
		return nil, err
	}

	return createResponse(rev, records), nil
}

func (fe *frontend) rangeGet(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	if r.Limit != 0 {
		return nil, createUnsupportedError("range/get with limit !=0")
	}
	record, rev, err := fe.be.Get(string(r.Key), r.Revision)
	if err != nil { // TODO: errCompact if revision is used
		return nil, err
	}

	response := &etcdserverpb.RangeResponse{}
	if record == nil {
		// non existent results
		response.Header = createResponseHeader(0)
		response.Count = 0
		return response, nil
	}
	/*
	   should be ok, since latest only gets current
	   	if record.IsDeleted() && r.Revision == 0 {
	   		// looking for current which does not exist
	   		response.Header = createResponseHeader(0)
	   		response.Count = 0
	   		return response, nil
	   	}
	*/
	response.Header = createResponseHeader(rev)
	response.Count = 1
	response.Kvs = []*mvccpb.KeyValue{types.RecordToKV(record)}

	return response, nil
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
		return fe.txtUpdate(ctx, rev, key, val, lease)
	}

	return nil, createUnsupportedError("unsupported range request")
}

func (fe *frontend) txtUpdate(ctx context.Context, rev int64, key string, val []byte, lease int64) (*etcdserverpb.TxnResponse, error) {

	if rev == 0 {
		klogv2.Infof("TXN-UPDATE-INSERT: %v %v", rev, key)
		// create mode
		createRev, err := fe.be.Insert(key, val, lease)
		if err != nil {
			return nil, err
		}
		resp := &etcdserverpb.TxnResponse{
			Header:    createResponseHeader(createRev),
			Succeeded: true,

			Responses: []*etcdserverpb.ResponseOp{
				{
					Response: &etcdserverpb.ResponseOp_ResponsePut{
						ResponsePut: &etcdserverpb.PutResponse{
							Header: createResponseHeader(createRev),
						},
					},
				},
			},
		}
		return resp, nil
	}

	// update mode
	klogv2.Infof("TXN-UPDATE-UPDATE: %v %v %v", rev, key, string(val))
	record, err := fe.be.Update(key, val, rev, lease)
	if err == nil {
		klogv2.Infof("TXN-UPDATE:OK")
		// success: record was updated
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
	} else {

		// we should be able to ignore errors here
		currentRev, _ := fe.be.CurrentRevision()
		if storageerrors.IsNotFoundError(err) {
			klogv2.Infof("TXN-UPDATE:NOT-FOUND")
			// record is not found
			// TODO set current
			resp := &etcdserverpb.TxnResponse{
				Header:    createResponseHeader(currentRev),
				Succeeded: false,
				Responses: []*etcdserverpb.ResponseOp{},
			}

			return resp, nil
		}

		if storageerrors.IsConflictError(err) {
			klogv2.Infof("TXN-UPDATE:CONFLICT")
			// record exist but not at the expected revision
			resp := &etcdserverpb.TxnResponse{
				Header:    createResponseHeader(currentRev),
				Succeeded: false,

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
			}
			return resp, nil
		}
	}

	return nil, err
}
func (fe *frontend) txnDelete(ctx context.Context, key string, rev int64) (*etcdserverpb.TxnResponse, error) {
	record, err := fe.be.Delete(key, rev)

	if err != nil {
		return nil, err // TODO: should it be an error response?
	}

	return &etcdserverpb.TxnResponse{
		Header: createResponseHeader(rev),

		Responses: []*etcdserverpb.ResponseOp{
			{
				Response: &etcdserverpb.ResponseOp_ResponseRange{
					ResponseRange: &etcdserverpb.RangeResponse{
						Header: createResponseHeader(rev),
						Kvs:    []*mvccpb.KeyValue{types.RecordToKV(record)},
					},
				},
			},
		},
		Succeeded: true,
	}, nil
}

func (fe *frontend) txnInsert(ctx context.Context, putRequest *etcdserverpb.PutRequest) (*etcdserverpb.TxnResponse, error) {
	// TODO: validate the put request for case/values etc..
	klogv2.Infof("TXN-INSERT: %v", string(putRequest.Key))
	rev, err := fe.be.Insert(string(putRequest.Key), putRequest.Value, putRequest.Lease)
	// error occured
	if err != nil {
		// key existed
		if storageerrors.IsEntityAlreadyExists(err) {
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
		Header:    createResponseHeader(rev),
		Responses: []*etcdserverpb.ResponseOp{
			{
				Response: &etcdserverpb.ResponseOp_ResponsePut{
					ResponsePut: &etcdserverpb.PutResponse{
						Header: createResponseHeader(rev),
					},
				},
			},
		},
	}

	return response, nil
}

func (fe *frontend) Compact(ctx context.Context, r *etcdserverpb.CompactionRequest) (*etcdserverpb.CompactionResponse, error) {
	rev, err := fe.be.DeleteAllBeforeRev(r.Revision)
	if err != nil {
		return nil, err
	}

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
