package dendrite

import (
	"math/big"
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

// checks if target is between start and end, optionally check if equal to end
func between(start, end, target []byte, rincl bool) bool {
	var a, b, c big.Int
	(&a).SetBytes(start)
	(&b).SetBytes(end)
	(&c).SetBytes(target)
	ab_dist := distance(&a, &b)
	ac_dist := distance(&a, &c)
	cmp := ab_dist.Cmp(ac_dist)
	switch cmp {
	case -1:
		return false
	case 0:
		if rincl {
			return true
		} else {
			return false
		}
	default:
		return true
	}
}

// calculates distance in ring between a and b
func distance(a, b *big.Int) *big.Int {
	cmp := a.Cmp(b)
	dist := big.NewInt(0)
	switch cmp {
	case 0:
		// a == b
		return dist
	case -1:
		// a < b
		return dist.Sub(b, a)
	default:
		// a > b, ring loop
		ring_len := big.NewInt(0)
		ring_len.Exp(big.NewInt(2), big.NewInt(160), nil)
		dist.Sub(ring_len, a)
		return dist.Add(dist, b)
	}
}
