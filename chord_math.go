package dendrite

import (
	"bytes"
	//"log"
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

// Checks if a key is STRICTLY between two ID's exclusively
func between(id1, id2, key []byte, rincl bool) bool {
	// Check for ring wrap around
	if bytes.Compare(id1, id2) == 1 {
		if rincl {
			return bytes.Compare(id1, key) == -1 ||
				bytes.Compare(id2, key) >= 0
		}
		return bytes.Compare(id1, key) == -1 ||
			bytes.Compare(id2, key) == 1
	}

	// Handle the normal case
	if rincl {
		return bytes.Compare(id1, key) == -1 &&
			bytes.Compare(id2, key) >= 0
	}
	return bytes.Compare(id1, key) == -1 &&
		bytes.Compare(id2, key) == 1
}

// Returns the vnode nearest a key
func nearestVnodeToKey(vnodes []*Vnode, key []byte) *Vnode {
	for i := len(vnodes) - 1; i >= 0; i-- {
		if bytes.Compare(vnodes[i].Id, key) == -1 {
			return vnodes[i]
		}
	}
	// Return the last vnode
	return vnodes[len(vnodes)-1]
}

// Computes the offset by (n + 2^exp) % (2^mod)
func powerOffset(id []byte, exp int, mod int) []byte {
	// Copy the existing slice
	off := make([]byte, len(id))
	copy(off, id)

	// Convert the ID to a bigint
	idInt := big.Int{}
	idInt.SetBytes(id)

	// Get the offset
	two := big.NewInt(2)
	offset := big.Int{}
	offset.Exp(two, big.NewInt(int64(exp)), nil)

	// Sum
	sum := big.Int{}
	sum.Add(&idInt, &offset)

	// Get the ceiling
	ceil := big.Int{}
	ceil.Exp(two, big.NewInt(int64(mod)), nil)

	// Apply the mod
	idInt.Mod(&sum, &ceil)

	// Add together
	return idInt.Bytes()
}
