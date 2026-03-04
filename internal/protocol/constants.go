package protocol

const (
	ApiKeyFetch                   = 1
	ApiKeyVersions                = 18
	ApiKeyDescribeTopicPartitions = 75

	ErrNone                    = 0
	ErrUnknownTopicOrPartition = 3
	ErrUnsupportedVersion      = 35
	ErrUnknownTopicId          = 100
)
