package backend

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"

	"github.com/khenidak/london/pkg/backend/consts"
	storageerrors "github.com/khenidak/london/pkg/backend/storageerrors"
	"github.com/khenidak/london/pkg/backend/storerecord"
	"github.com/khenidak/london/pkg/backend/utils"
	"github.com/khenidak/london/pkg/config"

	basictestutils "github.com/khenidak/london/test/utils/basic"
)

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyz0123456789")

func randStringRunes(n int) string {
	rand.Seed(time.Now().UnixNano())

	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func GetEntitiesForKeyRev(t *testing.T, c *config.Config, key string, rev int64) []*storage.Entity {
	t.Helper()
	validKey := storerecord.CreateValidKey(key)
	validRev := storerecord.RevToString(rev)

	o := &storage.QueryOptions{
		Filter: fmt.Sprintf("%s eq '%s' and %s eq '%s' and %s ne '%s'",
			consts.PartitionKeyFieldName,
			validKey,
			consts.RevisionFieldName,
			validRev,
			consts.EntityTypeFieldName,
			consts.EntityTypeEvent),
	}

	t.Logf("filter: %v", o.Filter)

	table := c.Runtime.StorageTable
	res, err := utils.SafeExecuteQuery(table, consts.DefaultTimeout, storage.MinimalMetadata, o)
	if err != nil {
		t.Fatalf("failed to query table with err:%v", err)
	}

	return res.Entities
}

func TestInsert(t *testing.T) {
	c := basictestutils.MakeTestConfig(t, false)

	be, err := NewBackend(c)
	if err != nil {
		t.Fatalf("failed to create backend with err:%v", err)
	}

	keyFormat := "/%s/%s/%s"
	testCases := []struct {
		name        string
		key         string
		value       []byte
		lease       int64
		expectError bool

		entityCount int
	}{
		{
			name:        "insert-small",
			value:       []byte("k"),
			key:         fmt.Sprintf(keyFormat, randStringRunes(8), randStringRunes(8), randStringRunes(8)),
			lease:       1,
			expectError: false,
			entityCount: 1, // goes into rowEntity
		},
		{
			name:        "split-entity-row",
			value:       []byte(randStringRunes(1024 * 1024))[:consts.DataFieldMaxSize*(consts.DataFieldsPerRow-1)],
			key:         fmt.Sprintf(keyFormat, randStringRunes(8), randStringRunes(8), randStringRunes(8)),
			lease:       1,
			expectError: false,
			entityCount: 1,
		},
		{
			name:        "half-one-extra",
			value:       []byte(randStringRunes(1024 * 1024))[:consts.DataFieldMaxSize*(consts.DataFieldsPerRow+1)],
			key:         fmt.Sprintf(keyFormat, randStringRunes(8), randStringRunes(8), randStringRunes(8)),
			lease:       1,
			expectError: false,
			entityCount: 2,
		},
		{
			name:        "multi-data-records",
			value:       []byte(randStringRunes(1024 * 1024 * 10))[:consts.DataRowMaxSize*3],
			key:         fmt.Sprintf(keyFormat, randStringRunes(8), randStringRunes(8), randStringRunes(8)),
			lease:       1,
			expectError: false,
			entityCount: 3,
		},

		// TODO insert large records test cases
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Logf("testName:%s", testCase.name)
			insertedRecord, err := be.Insert(testCase.key, testCase.value, testCase.lease)
			if err != nil {
				if !testCase.expectError {
					t.Fatalf("unexpected error during insert:%v", err)
				}
				return
			}
			if err == nil {
				if testCase.expectError {
					t.Fatalf("expected an error got none")
				}
			}

			entities := GetEntitiesForKeyRev(t, c, testCase.key, insertedRecord.ModRevision())
			if len(entities) != testCase.entityCount {
				t.Fatalf("expected %v entities got %v", testCase.entityCount, len(entities))
			}

			record, err := storerecord.NewFromEntities(entities, false)
			if err != nil {
				t.Fatalf("unexpected error trying to convert entites to record:%v", err)
			}
			if record.CreateRevision() != record.ModRevision() {
				t.Fatalf("expected createRev == modRev got %v != %v", record.CreateRevision(), record.ModRevision())
			}

			if string(record.Value()) != string(testCase.value) {
				t.Fatalf("inserted value does not match original value")
			}
		})
	}

	t.Run("repeat-insert", func(t *testing.T) {
		// double insert should yeild into entity alraedy exist
		randKey := fmt.Sprintf(keyFormat, randStringRunes(8), randStringRunes(8), randStringRunes(8))
		_, err = be.Insert(randKey, []byte("hello, World!"), 1)
		if err != nil {
			t.Fatalf("failed to do initial insert for duplicate record test")
		}

		_, err = be.Insert(randKey, []byte("hello, Universe!"), 1)
		if err == nil {
			t.Fatalf("should have gotten error dor duplicate insertion")
		}
		if !storageerrors.IsEntityAlreadyExists(err) {
			t.Fatalf("expected entity alrady exist error but got %v", err)
		}
	})
}
