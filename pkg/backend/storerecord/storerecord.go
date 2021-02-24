package storerecord

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/Azure/azure-sdk-for-go/storage"

	"github.com/khenidak/london/pkg/backend/consts"
	"github.com/khenidak/london/pkg/types"
)

// azure storage partitionkey and row key fields are string. in order
// for < > operation to work revisions needs to be padded
func RevToString(rev int64) string {
	return fmt.Sprintf("%019d", rev)
}

// azure storage table partition key has to be valid
// e.g. no /
func CreateValidKey(key string) string {
	return strings.ReplaceAll(key, "/", "|")
}
func ValidKeyToKey(key string) string {
	return strings.ReplaceAll(key, "|", "/")
}

type storeRecord struct {
	rowEntity    *storage.Entity
	dataEntities []*storage.Entity
}

type StoreRecord interface {
	types.Record
	RowEntity() *storage.Entity
	DataEntities() []*storage.Entity
}

func (sr *storeRecord) RowEntity() *storage.Entity {
	return sr.rowEntity
}
func (sr *storeRecord) DataEntities() []*storage.Entity {
	return sr.dataEntities
}

// create a new record for Insert
func NewRecord(key string, rev int64, lease int64, value []byte) (StoreRecord, error) {
	if len(value) > consts.MaxValueSize {
		return nil, fmt.Errorf("value > 2MB is not allowed. Max(%v) got(%v)", consts.MaxValueSize, len(value))
	}

	newRecord := &storeRecord{}

	parts := split(value, consts.DataFieldMaxSize)

	validKey := CreateValidKey(key)
	rowRev := RevToString(rev)

	rowEntity := &storage.Entity{}
	rowEntity.PartitionKey = validKey
	rowEntity.RowKey = consts.CurrentFlag
	rowEntity.Properties = map[string]interface{}{
		consts.RevisionFieldName:       rowRev,
		consts.CreateRevisionFieldName: rowRev,
		consts.LeaseFieldName:          lease,
		consts.FlagsFieldName:          consts.CurrentFlag,
	}

	// insert the first 10 parts into the row entity
	max := consts.DataFieldsPerRow
	if len(parts) < max {
		max = len(parts)
	}

	for i := 0; i < max; i++ {
		rowEntity.Properties[consts.DataFieldName[i]] = parts[i]
	}

	setEntityAsRow(rowEntity)
	newRecord.rowEntity = rowEntity

	// the count does not include row entity data parts
	newRecord.dataEntities = dataEntitesFromParts(validKey, rowRev, parts)
	rowEntity.Properties[consts.DataPartsCountFieldName] = fmt.Sprintf("%v", len(newRecord.dataEntities))
	return newRecord, nil
}

func NewFromEntities(entities []*storage.Entity, ignoreDataRows bool) (StoreRecord, error) {
	if len(entities) < 1 {
		return nil, fmt.Errorf("expecetd minimum # of entities == 1 got %v", len(entities))
	}

	existingRecord := &storeRecord{}
	existingRecord.dataEntities = make([]*storage.Entity, 0, len(entities)-1)
	for _, e := range entities {
		if IsRowEntity(e) || IsEventEntity(e) {
			if existingRecord.rowEntity != nil {
				return nil, fmt.Errorf("duplicate row entity found")
			}
			existingRecord.rowEntity = e
			continue
		}

		existingRecord.dataEntities = append(existingRecord.dataEntities, e)
	}

	if existingRecord.rowEntity == nil {
		return nil, fmt.Errorf("failed: found no row entity in entities")
	}

	externalEntityCountAsString := existingRecord.rowEntity.Properties[consts.DataPartsCountFieldName].(string)
	externalEntityCount, _ := strconv.ParseInt(externalEntityCountAsString, 10, 32)

	if !ignoreDataRows && len(existingRecord.dataEntities) != int(externalEntityCount) {
		return nil, fmt.Errorf("failed: Expected data rows count: %v got %v", externalEntityCount, len(existingRecord.dataEntities))
	}
	return existingRecord, nil
}

func copyDataProperties(src *storage.Entity, target *storage.Entity) {
	for _, dataFieldName := range consts.DataFieldName {
		if value, ok := src.Properties[dataFieldName]; ok {
			target.Properties[dataFieldName] = value
			continue
		}
		break
	}
}

// creates a new record from a row and data entities
func NewFromRowAndDataEntities(rowEntity *storage.Entity, dataEntities []*storage.Entity) (StoreRecord, error) {
	allEntities := make([]*storage.Entity, 0, len(dataEntities)+1)
	allEntities = append(allEntities, rowEntity)
	allEntities = append(allEntities, dataEntities...)

	return NewFromEntities(allEntities, false)
}

func CreateEventEntityFromRecord(record StoreRecord) *storage.Entity {
	rowEntity := record.RowEntity()

	eventType := rowEntity.Properties[consts.FlagsFieldName].(string)
	revision := rowEntity.Properties[consts.RevisionFieldName].(string)
	createRevision := rowEntity.Properties[consts.CreateRevisionFieldName].(string)
	prevRevision := ""
	if prev, ok := rowEntity.Properties[consts.PrevRevisionFieldName]; ok {
		prevRevision = prev.(string)
	}
	countParts := rowEntity.Properties[consts.DataPartsCountFieldName].(string)

	e := &storage.Entity{
		Properties: make(map[string]interface{}),
	}
	e.PartitionKey = record.RowEntity().PartitionKey
	e.RowKey = fmt.Sprintf(consts.EventEntityRowKeyFormat, eventType, revision)
	e.Properties[consts.RevisionFieldName] = revision
	e.Properties[consts.FlagsFieldName] = eventType
	e.Properties[consts.CreateRevisionFieldName] = createRevision
	e.Properties[consts.PrevRevisionFieldName] = prevRevision
	e.Properties[consts.DataPartsCountFieldName] = countParts
	copyDataProperties(rowEntity, e)
	setEntityAsEvent(e)

	return e
}

// create a deleted record based on existing record
func NewForDeleted(rev int64, old StoreRecord) (StoreRecord, error) {
	record := &storeRecord{}
	validRev := RevToString(rev)
	// ceate row entity
	rowEntity := &storage.Entity{}
	rowEntity.OdataEtag = old.RowEntity().OdataEtag
	rowEntity.PartitionKey = old.RowEntity().PartitionKey
	rowEntity.RowKey = validRev
	rowEntity.Properties = map[string]interface{}{
		consts.RevisionFieldName:       validRev,
		consts.FlagsFieldName:          consts.DeletedFlag,
		consts.PrevRevisionFieldName:   old.RowEntity().Properties[consts.RevisionFieldName],
		consts.CreateRevisionFieldName: old.RowEntity().Properties[consts.CreateRevisionFieldName],
	}

	copyDataProperties(old.RowEntity(), rowEntity)
	setEntityAsRow(rowEntity)
	record.rowEntity = rowEntity
	// now copy old data into new record
	dataEntities := make([]*storage.Entity, 0, len(old.DataEntities()))
	for _, part := range old.DataEntities() {
		thisDataEntity := &storage.Entity{
			Properties: make(map[string]interface{}),
		}
		thisDataEntity.PartitionKey = rowEntity.PartitionKey
		rowKeyParts := strings.Split(part.RowKey, "-")

		thisDataEntity.RowKey = revForDataRow(validRev, rowKeyParts[1])
		thisDataEntity.Properties[consts.RevisionFieldName] = validRev

		copyDataProperties(part, thisDataEntity)
		setEntityAsData(thisDataEntity)

		dataEntities = append(dataEntities, thisDataEntity)
	}

	record.dataEntities = dataEntities
	record.rowEntity.Properties[consts.DataPartsCountFieldName] = fmt.Sprintf("%v", len(dataEntities))
	return record, nil
}

// creates a new record for update and modify old record accordingly
func NewForUpdate(rev int64, newVal []byte, old StoreRecord, lease int64) (StoreRecord, error) {
	// create the updated record
	newRecord, err := NewRecord(string(old.Key()), rev, lease, newVal)
	if err != nil {
		return nil, err
	}
	// But this record needs to modified for update
	// 1- set prev revision
	newRecord.RowEntity().Properties[consts.PrevRevisionFieldName] =
		old.RowEntity().Properties[consts.RevisionFieldName]

		//2- set create revision to point to old record's create rev
	newRecord.RowEntity().Properties[consts.CreateRevisionFieldName] =
		old.RowEntity().Properties[consts.CreateRevisionFieldName]

		// 3- copy odata so we don't mess up the transaction as we commit to table
	newRecord.RowEntity().OdataEtag = old.RowEntity().OdataEtag

	// 4- set as an updated record
	newRecord.RowEntity().Properties[consts.FlagsFieldName] = consts.UpdatedFlag

	// now modify the row entity old for
	oldsr := old.(*storeRecord)
	entity := &storage.Entity{}
	entity.PartitionKey = newRecord.RowEntity().PartitionKey
	entity.RowKey = old.RowEntity().Properties[consts.RevisionFieldName].(string)
	entity.Properties = map[string]interface{}{
		consts.FlagsFieldName:          consts.UpdatedFlag,
		consts.DataPartsCountFieldName: old.RowEntity().Properties[consts.DataPartsCountFieldName],
		consts.RevisionFieldName:       old.RowEntity().Properties[consts.RevisionFieldName],
		consts.CreateRevisionFieldName: old.RowEntity().Properties[consts.CreateRevisionFieldName],
		consts.PrevRevisionFieldName:   old.RowEntity().Properties[consts.PrevRevisionFieldName],
	}
	setEntityAsRow(entity)

	oldsr.rowEntity = entity
	return newRecord, nil
}

// takes parts and create entities for them
// starts with consts.DataFieldsPerRow .. end of parts
func dataEntitesFromParts(validKey string, validRev string, parts [][]byte) []*storage.Entity {
	capacity := (len(parts) - consts.DataFieldsPerRow) / consts.DataFieldsPerRow
	if capacity == 0 {
		capacity = 1
	}

	dataEntities := make([]*storage.Entity, 0, capacity)

	createEntity := func(idx int) *storage.Entity {
		e := &storage.Entity{
			Properties: make(map[string]interface{}),
		}
		e.PartitionKey = validKey
		e.RowKey = revForDataRow(validRev, fmt.Sprintf("%v", idx))
		e.Properties[consts.RevisionFieldName] = validRev

		setEntityAsData(e)
		return e
	}

	prop := 0
	added := 0
	thisDataEntity := createEntity(added)
	for i := consts.DataFieldsPerRow; i < len(parts); i++ {
		thisDataEntity.Properties[consts.DataFieldName[prop]] = parts[i]
		if prop == consts.DataFieldsPerRow-1 || i == len(parts)-1 {
			added++
			prop = 0
			dataEntities = append(dataEntities, thisDataEntity)
			thisDataEntity = createEntity(added)
		} else {
			prop++
		}
	}

	return dataEntities
}

// splits a value to parts so it can go into multiple data records
func split(value []byte, chunkSize int) [][]byte {

	splited := make([][]byte, 0, len(value)/chunkSize)

	for low := 0; low < len(value); low += chunkSize {
		high := low + chunkSize

		if high > len(value) {
			high = len(value)
		}

		splited = append(splited, value[low:high])
	}

	return splited
}
func IsRowEntity(e *storage.Entity) bool {
	propVal, ok := e.Properties[consts.EntityTypeFieldName]
	if !ok {
		return false
	}
	propValString := propVal.(string)
	return propValString == consts.EntityTypeRow
}

func IsDataEntity(e *storage.Entity) bool {
	propVal, ok := e.Properties[consts.EntityTypeFieldName]
	if !ok {
		return false
	}
	propValString := propVal.(string)
	return propValString == consts.EntityTypeData
}

func IsEventEntity(e *storage.Entity) bool {
	propVal, ok := e.Properties[consts.EntityTypeFieldName]
	if !ok {
		return false
	}
	propValString := propVal.(string)
	return propValString == consts.EntityTypeEvent
}

func setEntityAsEvent(e *storage.Entity) {
	e.Properties[consts.EntityTypeFieldName] = consts.EntityTypeEvent
}

func setEntityAsRow(e *storage.Entity) {
	e.Properties[consts.EntityTypeFieldName] = consts.EntityTypeRow
}

func setEntityAsData(e *storage.Entity) {
	e.Properties[consts.EntityTypeFieldName] = consts.EntityTypeData
}

func revForDataRow(validRev string, dataRowIdx string) string {
	return fmt.Sprintf(consts.DataEntityRowKeyFormat, dataRowIdx, validRev)
}

func (sr *storeRecord) Key() []byte {
	return []byte(ValidKeyToKey(sr.rowEntity.PartitionKey))
}
func (sr *storeRecord) Value() []byte {
	value := make([]byte, 0, 0) // could be better TODO: pre allocate

	addDataFromEntity := func(e *storage.Entity) bool {
		for _, dataFieldName := range consts.DataFieldName {
			val, ok := e.Properties[dataFieldName]
			if !ok {
				return false
			}
			value = append(value, val.([]byte)...)
		}
		return true
	}

	// get data from row entity
	if !addDataFromEntity(sr.rowEntity) {
		return value
	}

	// make sure that data entities are ordered
	ordered := make([]*storage.Entity, len(sr.dataEntities), len(sr.dataEntities))
	for _, e := range sr.dataEntities {
		rev := e.RowKey
		qualifierParts := strings.Split(rev, "-")

		idx, err := strconv.ParseInt(qualifierParts[1], 10, 32)
		if err != nil {
			panic(err) // should never happen
		}
		ordered[idx] = e

	}

	// get data from other entities
	for _, de := range ordered {
		if !addDataFromEntity(de) {
			return value
		}
	}

	return value
}

func (sr *storeRecord) CreateRevision() int64 {
	revString := sr.rowEntity.Properties[consts.CreateRevisionFieldName].(string)
	n, _ := strconv.ParseInt(revString, 10, 64)
	return n
}
func (sr *storeRecord) ModRevision() int64 {
	revString := sr.rowEntity.Properties[consts.RevisionFieldName].(string)

	// this could be buggy. We depend on insert/update/delete
	// all doing the right thing when setting revs
	n, _ := strconv.ParseInt(revString, 10, 64)
	return n

}
func (sr *storeRecord) Lease() int64 {
	if val, ok := sr.rowEntity.Properties[consts.LeaseFieldName]; ok {
		return val.(int64)
	}
	return 0
}

func (sr *storeRecord) PrevRevision() int64 {
	val, ok := sr.rowEntity.Properties[consts.PrevRevisionFieldName]
	if ok {
		revString := val.(string)
		if revString == "" {
			return 0
		}
		n, _ := strconv.ParseInt(revString, 10, 64)
		return n
	}

	return 0
}

func (sr *storeRecord) IsEventRecord() bool {
	return IsEventEntity(sr.rowEntity)
}
func (sr *storeRecord) IsDeleteEvent() bool {
	if !sr.IsEventRecord() {
		return false
	}

	val, ok := sr.rowEntity.Properties[consts.FlagsFieldName]
	if ok {
		return val.(string) == consts.DeletedFlag
	}

	return false
}

func (sr *storeRecord) IsUpdateEvent() bool {
	if !sr.IsEventRecord() {
		return false
	}

	if val, ok := sr.rowEntity.Properties[consts.FlagsFieldName]; ok {
		return val.(string) == consts.UpdatedFlag
	}

	return false
}

func (sr *storeRecord) IsCreateEvent() bool {
	if !sr.IsEventRecord() {
		return false
	}

	if val, ok := sr.rowEntity.Properties[consts.FlagsFieldName]; ok {
		return val.(string) == consts.CurrentFlag
	}

	return false
}
