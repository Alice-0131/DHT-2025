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

type Node struct {
	Addr   string // address and port number of the node, e.g., "localhost:1234"
	online bool

	listener  net.Listener
	server    *rpc.Server
	data      map[string]string
	dataLock  sync.RWMutex
	peers     map[string]struct{} // we use map as a set
	peersLock sync.RWMutex
}

// Pair is used to store a key-value pair.
// Note: It must be exported (i.e., Capitalized) so that it can be
// used as the argument type of RPC methods.
type Pair struct {
	Key   string
	Value string
}

// Initialize a node.
// Addr is the address and port number of the node, e.g., "localhost:1234".
func (node *Node) Init(addr string) {
	node.Addr = addr
	node.data = make(map[string]string)
	node.peers = make(map[string]struct{})
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
// RPC Methods
//

//
// DHT methods
//

func (node *Node) Run(wg *sync.WaitGroup) {
	node.online = true
	go node.RunRPCServer(wg)
}

func (node *Node) Create() {
	logrus.Info("Create")
}

func (node *Node) Join(addr string) bool {
	logrus.Infof("Join %s", addr)
	return true
}

func (node *Node) Put(key string, value string) bool {
	logrus.Infof("Put %s %s", key, value)
	return true
}

func (node *Node) Get(key string) (bool, string) {
	logrus.Infof("Get %s", key)
	return true, ""
}

func (node *Node) Delete(key string) bool {
	logrus.Infof("Delete %s", key)
	return true
}

func (node *Node) Quit() {
	logrus.Infof("Quit %s", node.Addr)
	node.StopRPCServer()
}

func (node *Node) ForceQuit() {
	logrus.Info("ForceQuit")
	node.StopRPCServer()
}
