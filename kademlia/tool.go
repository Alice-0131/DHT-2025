package kademlia

import (
	"crypto/sha1"
	"math/big"
)

var power [m + 5]*big.Int

func init() {
	k := big.NewInt(2)
	power[0] = big.NewInt(1)
	for i := 1; i <= m; i++ {
		power[i] = new(big.Int)
		power[i].Mul(power[i-1], k)
	}
}

func id(s string) *big.Int {
	hash := sha1.New()
	hash.Write([]byte(s))
	return (&big.Int{}).SetBytes(hash.Sum(nil))
}

func Xor(a, b *big.Int) *big.Int {
	return new(big.Int).Xor(a, b)
}

func no(a *big.Int) int {
	if a.Cmp(big.NewInt(0)) == 0 {
		return 0
	}
	for i := 0; i < m; i++ {
		if a.Cmp(power[i]) >= 0 {
			return i
		}
	}
	return m
}
