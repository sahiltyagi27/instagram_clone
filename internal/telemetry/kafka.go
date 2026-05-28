package telemetry

import "github.com/segmentio/kafka-go"

// KafkaHeaderCarrier adapts a kafka.Message header slice to OTel's
// TextMapCarrier interface so trace context can be injected into outgoing
// Kafka messages and extracted from incoming ones.
type KafkaHeaderCarrier []kafka.Header

func (c KafkaHeaderCarrier) Get(key string) string {
	for _, h := range c {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c *KafkaHeaderCarrier) Set(key, value string) {
	*c = append(*c, kafka.Header{Key: key, Value: []byte(value)})
}

func (c KafkaHeaderCarrier) Keys() []string {
	keys := make([]string, len(c))
	for i, h := range c {
		keys[i] = h.Key
	}
	return keys
}
