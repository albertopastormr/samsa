package client

import (
	"github.com/albertopastormr/samsa/internal/network"
	"github.com/albertopastormr/samsa/internal/protocol"
)

type KafkaClient struct {
	networkClient *network.Client
	correlationID int32
}

func NewKafkaClient(addr string) (*KafkaClient, error) {
	c, err := network.Connect(addr)
	if err != nil {
		return nil, err
	}
	return &KafkaClient{
		networkClient: c,
		correlationID: 0,
	}, nil
}

func (c *KafkaClient) Close() error {
	return c.networkClient.Close()
}

func (c *KafkaClient) nextCorrelationID() int32 {
	c.correlationID++
	return c.correlationID
}

func (c *KafkaClient) ApiVersions() (*protocol.ApiVersionsResponse, error) {
	cid := c.nextCorrelationID()
	req := &protocol.ApiVersionsRequest{}
	
	err := c.networkClient.SendRequest(protocol.ApiKeyVersions, 4, cid, nil, req)
	if err != nil {
		return nil, err
	}

	reader, err := c.networkClient.ReceiveResponse()
	if err != nil {
		return nil, err
	}

	resp := protocol.DecodeApiVersionsResponse(reader)
	return &resp, nil
}

func (c *KafkaClient) DescribeTopicPartitions(topics []string) (*protocol.DescribeTopicPartitionsResponse, error) {
	cid := c.nextCorrelationID()
	req := &protocol.DescribeTopicPartitionsRequest{
		Topics:                 topics,
		ResponsePartitionLimit: 100,
		Cursor:                 -1,
	}

	err := c.networkClient.SendRequest(protocol.ApiKeyDescribeTopicPartitions, 0, cid, nil, req)
	if err != nil {
		return nil, err
	}

	reader, err := c.networkClient.ReceiveResponse()
	if err != nil {
		return nil, err
	}

	resp := protocol.DecodeDescribeTopicPartitionsResponse(reader)
	return &resp, nil
}

func (c *KafkaClient) Produce(topicName string, partition int32, records []byte) (*protocol.ProduceResponse, error) {
	cid := c.nextCorrelationID()
	req := &protocol.ProduceRequest{
		Acks:      1,
		TimeoutMs: 1000,
		Topics: []protocol.ProduceRequestTopic{
			{
				Name: topicName,
				Partitions: []protocol.ProduceRequestPartition{
					{
						Index:   partition,
						Records: records,
					},
				},
			},
		},
	}

	err := c.networkClient.SendRequest(protocol.ApiKeyProduce, 11, cid, nil, req)
	if err != nil {
		return nil, err
	}

	reader, err := c.networkClient.ReceiveResponse()
	if err != nil {
		return nil, err
	}

	resp := protocol.DecodeProduceResponse(reader)
	return &resp, nil
}
