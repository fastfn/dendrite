package dendrite

import (
	"time"
)

// ZeroMQ Transport implementation
type ZMQTransport struct{}

func InitZMQTransport(hostname string, timeout time.Duration) (Transport, error) {
	return &ZMQTransport{}, nil
}
