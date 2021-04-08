package utils

import (
	"strings"

	"github.com/Azure/azure-sdk-for-go/storage"

	"github.com/khenidak/london/pkg/backend/retryable"
)

// these options are used for retrying any call to
// to external store. For now we are only interested
// in error filtering. we are ok with other defaults
var retryableOptions []*retryable.Option = []*retryable.Option{
	retryable.WithRetryableErrorFilter(func(e error) bool {
		if strings.Contains(e.Error(), "connection reset by peer") {
			return true
		}
		// the SDK takes care of the rest of errors
		return false
	}),
}

// executes a table batch with retry
func SafeExecuteBatch(b *storage.TableBatch) error {
	return retryable.RetryWithOpts(b.ExecuteBatch, retryableOptions...)
}

// executes a table query with retry
func SafeExecuteQuery(t *storage.Table,
	timeout uint,
	ml storage.MetadataLevel,
	options *storage.QueryOptions) (*storage.EntityQueryResult, error) {

	var res *storage.EntityQueryResult

	exec := func() error {
		var e error
		res, e = t.QueryEntities(timeout, ml, options)
		return e
	}

	err := retryable.RetryWithOpts(exec, retryableOptions...)
	return res, err
}

// executes NextResult() with retry
func SafeExecuteNextResult(lastResult *storage.EntityQueryResult, options *storage.TableOptions) (*storage.EntityQueryResult, error) {
	var res *storage.EntityQueryResult
	exec := func() error {
		var e error
		res, e = lastResult.NextResults(options)
		return e
	}

	err := retryable.RetryWithOpts(exec, retryableOptions...)
	return res, err
}

// executes Entity.Get() with retry
func SafeExecuteEntityGet(entity *storage.Entity, timeout uint, ml storage.MetadataLevel, options *storage.GetEntityOptions) error {
	exec := func() error {
		return entity.Get(timeout, ml, options)
	}

	return retryable.RetryWithOpts(exec, retryableOptions...)
}
