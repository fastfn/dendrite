package dentrite

// ZeroMQ Transport implementation
type ZMQTransport struct{}

func InitZMQTransport() Transport {
	return &ZMQTransport{}
}
