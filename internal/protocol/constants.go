package protocol

const (
	ApiKeyProduce                 = 0
	ApiKeyFetch                   = 1
	ApiKeyVersions                = 18
	ApiKeyCreateTopics            = 19
	ApiKeyDescribeTopicPartitions = 75

	ErrNone                    = 0
	ErrUnknownTopicOrPartition = 3
	ErrUnsupportedVersion      = 35
	ErrTopicAlreadyExists      = 36
	ErrUnknownTopicId          = 100
	ErrUnknownServerError      = -1
)
