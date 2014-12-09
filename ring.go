package dendrite

type Ring struct {
}

func CreateRing(config *Config, transport Transport) (*Ring, error) {
	return &Ring{}, nil
}

func JoinRing(config *Config, transport Transport, existing string) (*Ring, error) {
	return &Ring{}, nil
}
func (ring *Ring) Len() int {
	return 0
}
