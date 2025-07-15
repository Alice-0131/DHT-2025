package kademlia

import (
	"sync"
	//"time"
)

type DataBase struct {
	data map[string]string
	//republishTime map[string]time.Time
	//expireTime    map[string]time.Time
	dataLock sync.RWMutex
}

func (dataBase *DataBase) init() {
	dataBase.dataLock.Lock()
	dataBase.data = make(map[string]string)
	//dataBase.republishTime = make(map[string]time.Time)
	//dataBase.expireTime = make(map[string]time.Time)
	dataBase.dataLock.Unlock()
}

func (dataBase *DataBase) get(key string) (bool, string) {
	dataBase.dataLock.RLock()
	value, ok := dataBase.data[key]
	dataBase.dataLock.RUnlock()
	return ok, value
}

func (dataBase *DataBase) put(pair Pair) {
	dataBase.dataLock.Lock()
	dataBase.data[pair.Key] = pair.Value
	dataBase.dataLock.Unlock()
}

func (dataBase *DataBase) republishList() []Pair {
	var list []Pair
	dataBase.dataLock.Lock()
	for k, v := range dataBase.data {
		list = append(list, Pair{k, v})
	}
	dataBase.dataLock.Unlock()
	return list
}

//func (dataBase *DataBase) expire() {}
