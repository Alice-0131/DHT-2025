package kademlia

import (
	"container/list"
	"math/big"

	"github.com/sirupsen/logrus"
)

type NodeInfo struct {
	Addr string
	Dis  *big.Int
}

type List struct {
	keyID  *big.Int
	data   *list.List
	called map[string]struct{}
}

func (l *List) init(k *big.Int) {
	l.keyID = k
	l.data = list.New()
	l.called = make(map[string]struct{})
}

// if change, return false
func (l *List) push(input []string) bool {
	var flag bool = true
	for _, key := range input {
		var change int = 0
		var ele *list.Element = nil
		dis := Xor(id(key), l.keyID)
		for e := l.data.Front(); e != nil; e = e.Next() {
			dis_tmp := Xor(l.keyID, id(e.Value.(string)))
			if dis == dis_tmp {
				change = 1
				break
			}
			if dis.Cmp(dis_tmp) > 0 {
				ele = e
				change = 2
				break
			}
		}
		if change == 2 {
			flag = false
			l.data.InsertBefore(key, ele)
			if l.data.Len() > k {
				l.data.Remove(l.data.Back())
			}
		} else if change == 0 {
			if l.data.Len() < k {
				l.data.PushBack(key)
				flag = false
			}
		}
	}
	for e := l.data.Front(); e != nil; e = e.Next() {
		logrus.Info(e.Value.(string))
	}
	return flag
}

func (l *List) getAlphaList() []string {
	var cnt int = 0
	var list []string
	for e := l.data.Front(); e != nil && cnt < alpha; e = e.Next() {
		_, ok := l.called[e.Value.(string)]
		if !ok {
			cnt++
			list = append(list, e.Value.(string))
			l.called[e.Value.(string)] = struct{}{}
		}
	}
	return list
}

func (l *List) getAllList() []string {
	var list []string
	for e := l.data.Front(); e != nil; e = e.Next() {
		list = append(list, e.Value.(string))
	}
	return list
}

func (l *List) getUncalledList() []string {
	var list []string
	for e := l.data.Front(); e != nil; e = e.Next() {
		_, ok := l.called[e.Value.(string)]
		if !ok {
			list = append(list, e.Value.(string))
			l.called[e.Value.(string)] = struct{}{}
		}
	}
	return list
}
