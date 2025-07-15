package kademlia

import (
	"container/list"
	"sync"
)

type RoutingTable struct {
	node      *Node
	buckets   [m]*list.List
	tableLock sync.RWMutex
}

func (routingTable *RoutingTable) init(node *Node) {
	routingTable.node = node
	routingTable.tableLock.Lock()
	for i := range routingTable.buckets {
		routingTable.buckets[i] = list.New()
	}
	routingTable.tableLock.Unlock()
}

func (routingTable *RoutingTable) find_node(i int) []string {
	var size int = 0
	var li []string
	var remove []*list.Element
	routingTable.tableLock.Lock()
	for e := routingTable.buckets[i].Front(); e != nil && size < k; e = e.Next() {
		var online bool
		if err := routingTable.node.RemoteCall(e.Value.(string), "Node.Ping", "", &online); err != nil {
			online = false
		}
		if online {
			li = append(li, e.Value.(string))
			size++
		} else {
			remove = append(remove, e)
		}
	}
	for _, e := range remove {
		routingTable.buckets[i].Remove(e)
	}
	routingTable.tableLock.Unlock()
	for j := (i + 1) % m; j != i && size < k; j = (j + 1) % m {
		var remove []*list.Element
		routingTable.tableLock.Lock()
		for e := routingTable.buckets[j].Front(); e != nil && size < k; e = e.Next() {
			var online bool
			if err := routingTable.node.RemoteCall(e.Value.(string), "Node.Ping", "", &online); err != nil {
				online = false
			}
			if online {
				li = append(li, e.Value.(string))
				size++
			} else {
				remove = append(remove, e)
			}
		}
		for _, e := range remove {
			routingTable.buckets[j].Remove(e)
		}
		routingTable.tableLock.Unlock()
	}
	return li
}

func (routingTable *RoutingTable) update(addr string) {
	//logrus.Infof("%s update routingtable %s", routingTable.node.Addr, addr)
	d := Xor(id(routingTable.node.Addr), id(addr))
	i := no(d)
	var flag bool = false
	routingTable.tableLock.Lock()
	for e := routingTable.buckets[i].Front(); e != nil; e = e.Next() {
		if e.Value.(string) == addr {
			routingTable.buckets[i].MoveToBack(e)
			flag = true
			break
		}
	}
	routingTable.tableLock.Unlock()
	if !flag { // addr is not in bucket
		routingTable.tableLock.Lock()
		if routingTable.buckets[i].Len() < k {
			routingTable.buckets[i].PushBack(addr)
		} else {
			front_list := routingTable.buckets[i].Front()
			var online bool
			if err := routingTable.node.RemoteCall(front_list.Value.(string), "Node.Ping", "", &online); err != nil {
				online = false
			}
			if !online {
				routingTable.buckets[i].Remove(front_list)
				routingTable.buckets[i].PushBack(addr)
			} else {
				routingTable.buckets[i].MoveToBack(front_list)
			}
		}
		routingTable.tableLock.Unlock()
	}
}
