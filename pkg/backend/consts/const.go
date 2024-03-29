package consts

const (
	// Note: to minimize data on the wire we use appreviated field names
	// where possible

	// Field names
	PartitionKeyFieldName   = "PartitionKey"
	RowKeyFieldName         = "RowKey"
	RevisionFieldName       = "mr" // ModRevision
	CreateRevisionFieldName = "cr"
	PrevRevisionFieldName   = "pr"
	FlagsFieldName          = "f"
	LeaseFieldName          = "l"
	DataPartsCountFieldName = "c"
	EntityTypeFieldName     = "et"

	// Data field names
	DataField0 = "df0"
	DataField1 = "df1"
	DataField2 = "df2"
	DataField3 = "df3"
	DataField4 = "df4"
	DataField5 = "df5"
	DataField6 = "df6"
	DataField7 = "df7"
	DataField8 = "df8"
	DataField9 = "df9"

	// Flags
	CurrentFlag = "C"
	UpdatedFlag = "U"
	DeletedFlag = "D"

	// Entity Types
	EntityTypeRow            = "RR"
	EntityTypeData           = "DD"
	EntityTypeEvent          = "EE"
	EntityTypeLeaderElection = "LE"
	EntityTypeLease          = "LS"

	EventEntityRowKeyFormat = "ev-%s"
	DataEntityRowKeyFormat  = "data-%v-%s"

	WriteTesterPartitionKey = "__SYS__WRITE_ACCESS_CHECK__"
	WriteTestRowKey         = "__SYS__WRITE_ACCESS_CHECK__"

	RevisionerPartitionKey = "__SYS_R__"
	RevisionerRowKey       = "__SYS_R__"
	RevisionerProperty     = "__SYS_R__"
	DefaultTimeout         = 1

	// for every row including # of data elements, the purpose is to
	// 1 - ensure that row entity has big enough data not to go round triping to azure storage
	// 2 - minimize the overall data entities
	// -- the below means every row will carry 640k
	DataFieldsPerRow = 10
	DataFieldMaxSize = 64 * 1024                           // 64K
	DataRowMaxSize   = DataFieldMaxSize * DataFieldsPerRow // 640k
	MaxValueSize     = DataRowMaxSize * 16 * 2             // 2MB
)

var DataFieldName []string

func init() {
	DataFieldName = []string{
		DataField0,
		DataField1,
		DataField2,
		DataField3,
		DataField4,
		DataField5,
		DataField6,
		DataField7,
		DataField8,
		DataField9,
	}
}

// leader election
const (
	LeaderElectPartitionName = "__LEADER_ELECT__"
	// fields
	LeaderElectOwnerNameFieldName = "ow"
	LeaderElectExpiresOnFieldName = "eo"
)

// lease
const (
	LeasePartitionName       = "__LEASE__"
	LeaseGrantedTTLFieldName = "lgttl"
	LeaseExpiresOnFieldName  = "lex"
	LeaseStatusFieldName     = "lstu"
)

// low water mark (compact
const (
	CompactPartitionName = "__COMPACT__"
	CompactRowKey        = "__COMPACT__"
	CompactRevFieldName  = "lowwatermark"
)
