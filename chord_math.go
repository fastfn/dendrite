package dendrite

import (
	"math/rand"
	"time"
)

func min(a, b int) int {
	if a <= b {
		return a
	} else {
		return b
	}
}

// Generates a random stabilization time
func randStabilize(conf *Config) time.Duration {
	min := conf.StabilizeMin
	max := conf.StabilizeMax
	rand.Seed(time.Now().UnixNano())
	r := rand.Float64()
	return time.Duration((r * float64(max-min)) + float64(min))
}
