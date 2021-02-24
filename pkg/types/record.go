package types

import (
	"go.etcd.io/etcd/mvcc/mvccpb"
)

type Record interface {
	Key() []byte
	Value() []byte
	ModRevision() int64
	CreateRevision() int64
	PrevRevision() int64
	Lease() int64
	IsEventRecord() bool
	IsDeleteEvent() bool
	IsUpdateEvent() bool
	IsCreateEvent() bool
}

func RecordToKV(record Record) *mvccpb.KeyValue {
	return &mvccpb.KeyValue{
		CreateRevision: record.CreateRevision(),
		ModRevision:    record.ModRevision(),
		Key:            record.Key(),
		Value:          record.Value(),
		Lease:          record.Lease(),
	}
}

func RecordsToKVs(records []Record) []*mvccpb.KeyValue {
	all := make([]*mvccpb.KeyValue, len(records), len(records))

	for i, v := range records {
		r := RecordToKV(v)
		all[i] = r
	}

	return all
}
