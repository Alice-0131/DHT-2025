package kademlia

import (
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// Note: The init() function will be executed when this package is imported.
// See https://golang.org/doc/effective_go.html#init for more details.
func init() {
	// You can use the logrus package to print pretty logs.
	// Here we set the log output to a file.
	f, _ := os.Create("dht-test.log")
	logrus.SetOutput(f)
}

const (
	m     = 32
	k     = 8
	alpha = 2

	//republishTime time.Duration = 1 * time.Hour
	refreshTime time.Duration = 30 * time.Second
)

type Node struct {
	Addr     string // address and port number of the node, e.g., "localhost:1234"
	online   bool
	listener net.Listener
	server   *rpc.Server

	data         DataBase
	routingTable RoutingTable
}

// Pair is used to store a key-value pair.
// Note: It must be exported (i.e., Capitalized) so that it can be
// used as the argument type of RPC methods.
type Pair struct {
	Key   string
	Value string
}

type Ret struct {
	List  []string
	Flag  bool
	Value string
}

// Initialize a node.
// Addr is the address and port number of the node, e.g., "localhost:1234".
func (node *Node) Init(addr string) {
	node.Addr = addr
	node.data.init()
}

func (node *Node) RunRPCServer(wg *sync.WaitGroup) {
	node.server = rpc.NewServer()
	node.server.Register(node)
	var err error
	node.listener, err = net.Listen("tcp", node.Addr)
	wg.Done()
	if err != nil {
		logrus.Fatal("listen error: ", err)
	}
	for node.online {
		conn, err := node.listener.Accept()
		if err != nil {
			logrus.Error("accept error: ", err)
			return
		}
		go node.server.ServeConn(conn)
	}
}

func (node *Node) StopRPCServer() {
	node.online = false
	node.listener.Close()
}

// RemoteCall calls the RPC method at addr.
//
// Note: An empty interface can hold values of any type. (https://tour.golang.org/methods/14)
// Re-connect to the client every time can be slow. You can use connection pool to improve the performance.
func (node *Node) RemoteCall(addr string, method string, args interface{}, reply interface{}) error {
	if method != "Node.Ping" {
		logrus.Infof("[%s] RemoteCall %s %s %v", node.Addr, addr, method, args)
	}
	// Note: Here we use DialTimeout to set a timeout of 10 seconds.
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		logrus.Error("dialing: ", err)
		return err
	}
	client := rpc.NewClient(conn)
	defer client.Close()
	err = client.Call(method, args, reply)
	if err != nil {
		logrus.Error("RemoteCall error: ", err)
		return err
	}
	return nil
}

//
// Periodic Function
//

func (node *Node) maintain() {
	go func() {
		for node.online {
			node.refresh()
			time.Sleep(refreshTime)
		}
	}()
}

func (node *Node) republish() {
	list := node.data.republishList()
	var wg sync.WaitGroup
	for _, p := range list {
		wg.Add(1)
		pair := p
		go func() {
			defer wg.Done()
			node.publish(pair)
		}()
	}
	wg.Wait()
}

func (node *Node) refresh() {
}

//
// RPC Methods
//

func (node *Node) Ping(_ string, reply *bool) error {
	*reply = node.online
	return nil
}

func (node *Node) Store(pair Pair, _ *struct{}) error {
	node.data.put(pair)
	return nil
}

func (node *Node) FindNode(key string, reply *[]string) error {
	d := Xor(id(key), id(node.Addr))
	i := no(d)
	*reply = node.routingTable.find_node(i)
	logrus.Infof("%s findnode %s: %v", node.Addr, key, *reply)
	return nil
}

func (node *Node) FindValue(key string, reply *Ret) error {
	ok, v := node.data.get(key)
	if ok {
		*reply = Ret{[]string{}, true, v}
		return nil
	}
	d := Xor(id(key), id(node.Addr))
	i := no(d)
	*reply = Ret{node.routingTable.find_node(i), false, ""}
	return nil
}

func (node *Node) Update(addr string, _ *struct{}) error {
	node.routingTable.update(addr)
	return nil
}

//
// Private methods
//

func (node *Node) nodeLookup(key string) []string {
	var list []string
	var res List
	var wg sync.WaitGroup
	res.init(id(key))
	node.FindNode(key, &list)
	res.push(list)
	for {
		var flag bool = true
		var flagLock sync.RWMutex
		list = res.getAlphaList()
		for i := 0; i < len(list); i++ { // try to call alpha uncalled node
			wg.Add(1)
			l := list[i]
			go func() {
				defer wg.Done()
				var tmp []string
				if err := node.RemoteCall(l, "Node.FindNode", key, &tmp); err != nil {
					logrus.Error("find_node in node_lookup error: ", err)
					return
				}
				node.routingTable.update(l)
				if err := node.RemoteCall(l, "Node.Update", node.Addr, nil); err != nil {
					logrus.Error("update in node_lookup error: ", err)
					return
				}
				flagLock.Lock()
				flag = res.push(tmp) && flag
				flagLock.Unlock()
			}()
		}
		wg.Wait()
		if flag { // try to call all uncalled node
			list = res.getUncalledList()
			for i := 0; i < len(list); i++ {
				wg.Add(1)
				l := list[i]
				go func() {
					defer wg.Done()
					var tmp []string
					if err := node.RemoteCall(l, "Node.FindNode", key, &tmp); err != nil {
						logrus.Error("find_node in node_lookup error: ", err)
						return
					}
					node.routingTable.update(l)
					if err := node.RemoteCall(l, "Node.Update", node.Addr, nil); err != nil {
						logrus.Error("update in node_lookup error: ", err)
						return
					}
					flagLock.Lock()
					flag = res.push(tmp) && flag
					flagLock.Unlock()
				}()
			}
			wg.Wait()
			if flag {
				break
			}
		}
	}
	li := res.getAllList()
	logrus.Infof("%s nodelookup %s: %v", node.Addr, key, li)
	return li
}

func (node *Node) valueLookup(key string) (bool, string) {
	var ret Ret
	var res List
	res.init(id(key))
	node.FindValue(key, &ret)
	if ret.Flag {
		return true, ret.Value
	}
	res.push(ret.List)
	for {
		var flag bool = true
		ret.List = res.getAlphaList() // try to call alpha uncalled node
		for i := 0; i < len(ret.List); i++ {
			var tmp Ret
			if err := node.RemoteCall(ret.List[i], "Node.FindValue", key, &tmp); err != nil {
				logrus.Error("find_node in node_lookup error: ", err)
				return false, ""
			}
			node.routingTable.update(ret.List[i])
			if err := node.RemoteCall(ret.List[i], "Node.Update", node.Addr, nil); err != nil {
				logrus.Error("update in value_lookup error: ", err)
				return false, ""
			}
			if tmp.Flag {
				return true, tmp.Value
			}
			flag = res.push(tmp.List) && flag
		}
		if flag {
			ret.List = res.getUncalledList() // try to call all uncalled node
			for i := 0; i < len(ret.List); i++ {
				var tmp Ret
				if err := node.RemoteCall(ret.List[i], "Node.FindValue", key, &tmp); err != nil {
					logrus.Error("find_node in node_lookup error: ", err)
					return false, ""
				}
				node.routingTable.update(ret.List[i])
				if err := node.RemoteCall(ret.List[i], "Node.Update", node.Addr, nil); err != nil {
					logrus.Error("update in value_lookup error: ", err)
					return false, ""
				}
				if tmp.Flag {
					return true, tmp.Value
				}
				flag = res.push(tmp.List) && flag
			}
			if flag {
				return false, ""
			}
		}
	}
}

func (node *Node) publish(pair Pair) {
	list := node.nodeLookup(pair.Key)
	var wg sync.WaitGroup
	for i := 0; i < len(list); i++ {
		wg.Add(1)
		l := list[i]
		go func() {
			defer wg.Done()
			if err := node.RemoteCall(l, "Node.Store", pair, nil); err != nil {
				logrus.Error("store in publish error: ", err)
				return
			}
			node.routingTable.update(l)
			if err := node.RemoteCall(l, "Node.Update", node.Addr, nil); err != nil {
				logrus.Error("update in publish error: ", err)
				return
			}
		}()
	}
	wg.Wait()
}

//
// DHT methods
//

func (node *Node) Run(wg *sync.WaitGroup) {
	node.online = true
	go node.RunRPCServer(wg)
}

func (node *Node) Create() {
	logrus.Info("Create")
	node.Init(node.Addr)
	node.routingTable.init(node)
	node.maintain()
}

func (node *Node) Join(addr string) bool {
	logrus.Infof("Join %s with %s", node.Addr, addr)
	node.Init(node.Addr)
	node.routingTable.init(node)
	node.routingTable.update(addr)
	if err := node.RemoteCall(addr, "Node.Update", node.Addr, nil); err != nil {
		logrus.Error("update in value_lookup error: ", err)
		return false
	}
	node.nodeLookup(node.Addr)
	node.maintain()
	return true
}

func (node *Node) Put(key string, value string) bool {
	logrus.Infof("Put %s %s", key, value)
	node.publish(Pair{key, value})
	return true
}

func (node *Node) Get(key string) (bool, string) {
	logrus.Infof("Get %s", key)
	ok, value := node.valueLookup(key)
	return ok, value
}

func (node *Node) Delete(key string) bool {
	return true
}

func (node *Node) Quit() {
	logrus.Infof("Quit %s", node.Addr)
	if !node.online {
		return
	}
	node.republish()
	node.online = false
	node.StopRPCServer()
}

func (node *Node) ForceQuit() {
	logrus.Infof("ForceQuit %s", node.Addr)
	if !node.online {
		return
	}
	node.online = false
	node.StopRPCServer()
}
