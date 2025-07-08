package chord

import (
	"crypto/sha1"
	"math/big"
)

func id(s string) *big.Int {
	hash := sha1.New()
	hash.Write([]byte(s))
	return (&big.Int{}).SetBytes(hash.Sum(nil))
}

// flag == true:(); flag == false:(]
func in_range(l, r, k *big.Int, flag bool) bool {
	if l.Cmp(r) > 0 {
		if k.Cmp(l) > 0 || k.Cmp(r) < 0 || !flag && k.Cmp(r) == 0 {
			return true
		}
	} else {
		if k.Cmp(l) > 0 && (flag && k.Cmp(r) < 0 || !flag && k.Cmp(r) <= 0) {
			return true
		}
	}
	return false
}
