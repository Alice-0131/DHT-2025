package chord

import (
	"fmt"
	"math/big"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	size = 3
	m    = 160
)

var power [m + 5]*big.Int

// Note: The init() function will be executed when this package is imported.
// See https://golang.org/doc/effective_go.html#init for more details.
func init() {
	// You can use the logrus package to print pretty logs.
	// Here we set the log output to a file.
	f, _ := os.Create("dht-test.log")
	logrus.SetOutput(f)
	k := big.NewInt(2)
	power[0] = big.NewInt(1)
	for i := 1; i <= m; i++ {
		power[i] = new(big.Int)
		power[i].Mul(power[i-1], k)
	}
}

type Node struct {
	Addr     string // address and port number of the node, e.g., "localhost:1234"
	online   bool
	listener net.Listener
	server   *rpc.Server

	data       map[string]string
	dataLock   sync.RWMutex
	backup     map[string]string
	backupLock sync.RWMutex

	successors      [size]string
	successorsLock  sync.RWMutex
	predecessor     string
	predecessorLock sync.RWMutex
	fingerStrat     [m + 5]*big.Int // n + 2^(i)
	fingerNode      [m + 5]string   // successor(fingerStart[i])
	fingerLock      sync.RWMutex
	quitLock        sync.RWMutex
	quitflag        bool
}

// Pair is used to store a key-value pair.
// Note: It must be exported (i.e., Capitalized) so that it can be
// used as the argument type of RPC methods.
type Pair struct {
	Key   string
	Value string
}

type Reply struct {
	Flag  bool
	Value string
}

type In struct { //used for debugging
	K *big.Int
	S string
}

// Initialize a node.
// Addr is the address and port number of the node, e.g., "localhost:1234".
func (node *Node) Init(addr string) {
	node.Addr = addr
	node.dataLock.Lock()
	node.data = make(map[string]string)
	node.dataLock.Unlock()
	node.backupLock.Lock()
	node.backup = make(map[string]string)
	node.backupLock.Unlock()
	node.fingerLock.Lock()
	for i := 0; i < m; i++ {
		node.fingerStrat[i] = new(big.Int).Add(power[i], id(node.Addr))
		if node.fingerStrat[i].Cmp(power[m]) >= 0 {
			node.fingerStrat[i].Sub(node.fingerStrat[i], power[m])
		}
		node.fingerNode[i] = node.Addr
	}
	node.fingerLock.Unlock()
}

func (node *Node) refresh() {
	node.successorsLock.Lock()
	for i := 0; i < size; i++ {
		node.successors[i] = node.Addr
	}
	node.successorsLock.Unlock()
	node.predecessorLock.Lock()
	node.predecessor = ""
	node.predecessorLock.Unlock()
	node.dataLock.Lock()
	node.data = make(map[string]string)
	node.dataLock.Unlock()
	node.backupLock.Lock()
	node.backup = make(map[string]string)
	node.backupLock.Unlock()
	node.fingerLock.Lock()
	for i := 0; i < m; i++ {
		node.fingerNode[i] = node.Addr
	}
	node.fingerLock.Unlock()
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
	node.quitflag = false
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
	// Note: Here we use DialTimeout to set a timeout of 1 seconds.
	conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
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
		for node.quitflag {
			node.quitLock.Lock()
			node.stabilize()
			node.quitLock.Unlock()
			time.Sleep(200 * time.Millisecond)
		}
	}()
	go func() {
		for node.quitflag {
			node.fix_fingers()
			time.Sleep(200 * time.Millisecond)
		}
	}()
}

func (node *Node) stabilize() {
	logrus.Infof("[%s] stabilize", node.Addr)
	if err := node.UpdateSuccessors("", nil); err != nil {
		logrus.Error("update_successors in stabilize error: ", err)
		return
	}
	if err := node.UpdatePredecessor("", nil); err != nil {
		logrus.Error("update_predecessor in stabilize error: ", err)
		return
	}
	var sucpre string
	node.successorsLock.RLock()
	suc := node.successors[0]
	node.successorsLock.RUnlock()
	if err := node.RemoteCall(suc, "Node.Predecessor", "", &sucpre); err != nil {
		logrus.Error("RemoteCall predecessor in stabillize error: ", err)
		return
	}
	this_id := id(node.Addr)
	sucpre_id := id(sucpre)
	if sucpre != "" && (suc == node.Addr || in_range(this_id, id(suc), sucpre_id, true)) {
		node.successorsLock.Lock()
		for j := 1; j < size; j++ {
			node.successors[j] = node.successors[j-1]
		}
		node.successors[0] = sucpre
		node.successorsLock.Unlock()
		node.fingerLock.Lock()
		node.fingerNode[0] = sucpre
		node.fingerLock.Unlock()
		var list []Pair
		var new_data []Pair
		var delete_data []string
		var move_backup []Pair
		var delete_backup []string
		if err := node.RemoteCall(suc, "Node.Data", "", &list); err != nil {
			logrus.Error("RemoteCall data in stabilize error: ", err)
			return
		}
		for _, p := range list {
			if in_range(this_id, sucpre_id, id(p.Key), false) {
				new_data = append(new_data, p)
				delete_data = append(delete_data, p.Key)
			}
		}
		node.dataLock.RLock()
		for key, value := range node.data {
			move_backup = append(move_backup, Pair{key, value})
		}

		node.dataLock.RUnlock()
		for _, p := range move_backup {
			delete_backup = append(delete_backup, p.Key)
		}
		if err := node.RemoteCall(suc, "Node.DeleteBackup", delete_backup, nil); err != nil {
			logrus.Error("RemoteCall delete_backup in stabilize error: ", err)
			return
		}
		if err := node.RemoteCall(sucpre, "Node.PutBackup", move_backup, nil); err != nil {
			logrus.Error("RemoteCall put_backup in stabilize error: ", err)
			return
		}
		if err := node.RemoteCall(suc, "Node.DeleteData", delete_data, nil); err != nil {
			logrus.Error("RemoteCall delete_data in stabilize error: ", err)
			return
		}
		if err := node.RemoteCall(suc, "Node.PutBackup", new_data, nil); err != nil {
			logrus.Error("RemoteCall put_backup in stabilize error: ", err)
			return
		}
		var sucsuc string
		if err := node.RemoteCall(suc, "Node.Successor", "", &sucsuc); err != nil {
			logrus.Error("RemoteCall successor in stabilize error: ", err)
			return
		}
		if err := node.RemoteCall(sucsuc, "Node.DeleteBackup", delete_data, nil); err != nil {
			logrus.Error("RemoteCall delete_backup in stabilize error: ", err)
			return
		}
		if err := node.RemoteCall(sucpre, "Node.PutData", new_data, nil); err != nil {
			logrus.Error("RemoteCall put_data in stabilize error: ", err)
			return
		}
	}
	node.successorsLock.RLock()
	suc = node.successors[0]
	node.successorsLock.RUnlock()
	if err := node.RemoteCall(suc, "Node.Notify", node.Addr, nil); err != nil {
		logrus.Error("RemoteCall notify in stabilize error: ", err)
	}
}

func (node *Node) fix_fingers() {
	//logrus.Infof("[%s] fix_fingers", node.Addr)
	rand.Seed(time.Now().UnixNano())
	i := rand.Intn(159) + 1
	var suc string
	var start *big.Int
	node.fingerLock.Lock()
	start = node.fingerStrat[i]
	node.fingerLock.Unlock()
	if err := node.FindSuccessor(In{start, "finger"}, &suc); err != nil {
		logrus.Error("find_successor in fix_finger error: ", err)
	}
	node.fingerLock.Lock()
	node.fingerNode[i] = suc
	node.fingerLock.Unlock()
}

//
// RPC Methods
//

// Note: The methods used for RPC must be exported (i.e., Capitalized),
// and must have two arguments, both exported (or builtin) types.
// The second argument must be a pointer.
// The return type must be error.
// In short, the signature of the method must be:
//   func (t *T) MethodName(argType T1, replyType *T2) error
// See https://golang.org/pkg/net/rpc/ for more details.

// Here we use "_" to ignore the arguments we don't need.
// The empty struct "{}" is used to represent "void" in Go.

// node.successor
func (node *Node) Successor(_ string, reply *string) error {
	if err := node.UpdateSuccessors("", nil); err != nil {
		logrus.Error("update_successors in successor error: ", err)
	}
	node.successorsLock.RLock()
	*reply = node.successors[0]
	node.successorsLock.RUnlock()
	logrus.Infof("[%s] successor: %s", node.Addr, *reply)
	return nil
}

// node.predecessor
func (node *Node) Predecessor(_ string, reply *string) error {
	if err := node.UpdatePredecessor("", nil); err != nil {
		logrus.Error("update_predecessor in predecessor error: ", err)
	}
	node.predecessorLock.RLock()
	*reply = node.predecessor
	node.predecessorLock.RUnlock()
	logrus.Infof("[%s] predecessor: %s", node.Addr, *reply)
	return nil
}

// node.successors
func (node *Node) Successors(_ string, reply *[]string) error {
	node.successorsLock.RLock()
	*reply = node.successors[:]
	node.successorsLock.RUnlock()
	logrus.Infof("[%s] successors: %s", node.Addr, *reply)
	return nil
}

// node.data
func (node *Node) Data(_ string, reply *[]Pair) error {
	node.dataLock.RLock()
	for k, v := range node.data {
		*reply = append(*reply, Pair{k, v})
	}
	node.dataLock.RUnlock()
	return nil
}

// node.backup
func (node *Node) Backup(_ string, reply *[]Pair) error {
	node.backupLock.RLock()
	for k, v := range node.backup {
		*reply = append(*reply, Pair{k, v})
	}
	node.backupLock.RUnlock()
	return nil
}

func (node *Node) UpdateSuccessors(_ string, _ *struct{}) error { //and update data and backup
	logrus.Infof("[%s] update_successors", node.Addr)
	var is_online bool
	var i int
	var successor string
	for { // find first online node
		node.successorsLock.RLock()
		successor = node.successors[i]
		node.successorsLock.RUnlock()
		if err := node.RemoteCall(successor, "Node.Ping", "", &is_online); err != nil {
			is_online = false
		}
		if is_online || i == size-1 {
			break
		}
		i++
	}
	var list []string
	if err := node.RemoteCall(successor, "Node.Successors", "", &list); err != nil {
		logrus.Error("RemoteCall successors in update_successors error: ", err)
		return err
	}
	node.successorsLock.Lock()
	node.successors[0] = successor
	for j := 1; j < size; j++ {
		node.successors[j] = list[j-1]
	}
	nextsuccessor := node.successors[1]
	node.successorsLock.Unlock()
	node.fingerLock.Lock()
	node.fingerNode[0] = successor
	node.fingerLock.Unlock()
	if i != 1 { // no need to update data and backup
		return nil
	}
	// move successor's backup to nextsuccessor's backup
	var backup []Pair
	if err := node.RemoteCall(successor, "Node.Backup", "", &backup); err != nil {
		logrus.Error("RemoteCall backup in update_successors error: ", err)
		return err
	}
	var delete_backup []string
	for _, b := range backup {
		delete_backup = append(delete_backup, b.Key)
	}
	if err := node.RemoteCall(successor, "Node.DeleteBackup", delete_backup, nil); err != nil {
		logrus.Error("RemoteCall delete_backup in update_successors error: ", err)
		return err
	}
	if err := node.RemoteCall(nextsuccessor, "Node.PutBackup", backup, nil); err != nil {
		logrus.Error("RemoteCall put_backup in update_successors error: ", err)
		return err
	}
	// copy successor's backup to successor's data
	if err := node.RemoteCall(successor, "Node.PutData", backup, nil); err != nil {
		logrus.Error("RemoteCall put_data in update_successors error: ", err)
		return err
	}
	// copy node's data to successor's backup
	var data []Pair
	node.dataLock.Lock()
	for k, v := range node.data {
		data = append(data, Pair{k, v})
	}
	node.dataLock.Unlock()
	if err := node.RemoteCall(successor, "Node.PutBackup", data, nil); err != nil {
		logrus.Error("RemoteCall put_backup in update_successors error: ", err)
		return err
	}
	return nil
}

func (node *Node) UpdatePredecessor(_ string, _ *struct{}) error {
	logrus.Infof("[%s] update_predecessor", node.Addr)
	node.predecessorLock.RLock()
	pre := node.predecessor
	node.predecessorLock.RUnlock()
	if pre == "" {
		return nil
	}
	var online bool
	if err := node.RemoteCall(pre, "Node.Ping", "", &online); err != nil {
		online = false
	}
	if !online {
		node.predecessorLock.Lock()
		node.predecessor = ""
		node.predecessorLock.Unlock()
	}
	return nil
}

func (node *Node) SetPredecessor(addr string, _ *struct{}) error {
	node.predecessorLock.Lock()
	node.predecessor = addr
	node.predecessorLock.Unlock()
	return nil
}

// node.find_successor(k)
func (node *Node) FindSuccessor(tmp In, reply *string) error {
	if err := node.FindPredecessor(tmp, reply); err != nil {
		logrus.Error("find_predecessor in find_successor error: ", err)
		return err
	}
	if err := node.RemoteCall(*reply, "Node.Successor", "", reply); err != nil {
		logrus.Error("RemoteCall successor in find_successor error: ", err)
		return err
	}
	logrus.Infof("[%s] find_successor(%s): %s", node.Addr, tmp.S, *reply)
	return nil
}

// node.find_predecessor(k)
func (node *Node) FindPredecessor(tmp In, reply *string) error {
	*reply = node.Addr
	var successor string
	if err := node.UpdateSuccessors("", nil); err != nil {
		logrus.Error("update_successors in find_predecessor error: ", err)
	}
	node.successorsLock.RLock()
	successor = node.successors[0]
	node.successorsLock.RUnlock()
	for !(successor == node.Addr || in_range(id(*reply), id(successor), tmp.K, false)) {
		if err := node.RemoteCall(*reply, "Node.ClosestPrecedingFinger", tmp, reply); err != nil {
			logrus.Error("RemoteCall closest_preceding_finger in findpredecessor error: ", err)
			return err
		}
		if err := node.RemoteCall(*reply, "Node.Successor", "", &successor); err != nil {
			logrus.Error("RemoteCall successor in findpredecessor error: ", err)
			return err
		}
	}
	logrus.Infof("[%s] find_predecessor(%s): %s", node.Addr, tmp.S, *reply)
	return nil
}

// node.closest_preceding_finger(k)
func (node *Node) ClosestPrecedingFinger(tmp In, reply *string) error {
	n_id := id(node.Addr)
	for i := m - 1; i >= 0; i-- {
		node.fingerLock.RLock()
		node_fingerNode_i := node.fingerNode[i]
		node.fingerLock.RUnlock()
		var online bool
		if err := node.RemoteCall(node_fingerNode_i, "Node.Ping", "", &online); err != nil {
			online = false
		}
		if online && in_range(n_id, tmp.K, id(node_fingerNode_i), true) {
			*reply = node_fingerNode_i
			return nil
		}
	}
	*reply = node.Addr
	logrus.Infof("[%s] closest_preceding_finger(%s): %s", node.Addr, tmp.S, *reply)
	return nil
}

// check if online
func (node *Node) Ping(_ string, reply *bool) error {
	*reply = node.quitflag
	return nil
}

func (node *Node) Notify(n string, _ *struct{}) error {
	//logrus.Infof("[%s] notify(%s)", node.Addr, n)
	node.predecessorLock.RLock()
	pre := node.predecessor
	node.predecessorLock.RUnlock()
	if pre == "" || in_range(id(pre), id(node.Addr), id(n), true) || pre == node.Addr {
		node.predecessorLock.Lock()
		node.predecessor = n
		node.predecessorLock.Unlock()
	}
	return nil
}

func (node *Node) PutData(pair []Pair, _ *struct{}) error {
	logrus.Infof("[%s] put_data(%s)", node.Addr, pair)
	node.dataLock.Lock()
	for _, p := range pair {
		node.data[p.Key] = p.Value
	}
	node.dataLock.Unlock()
	return nil
}

func (node *Node) PutBackup(pair []Pair, _ *struct{}) error {
	logrus.Infof("[%s] put_backup(%s)", node.Addr, pair)
	node.backupLock.Lock()
	for _, p := range pair {
		node.backup[p.Key] = p.Value
	}
	node.backupLock.Unlock()
	return nil
}

func (node *Node) GetData(key string, reply *Reply) error {
	node.dataLock.RLock()
	reply.Value, reply.Flag = node.data[key]
	node.dataLock.RUnlock()
	return nil
}

func (node *Node) DeleteData(key []string, reply *bool) error {
	logrus.Infof("[%s] delete_data(%s)", node.Addr, key)
	for _, k := range key {
		node.dataLock.RLock()
		_, ok := node.data[k]
		node.dataLock.RUnlock()
		if !ok {
			*reply = false
			return nil
		}
		node.dataLock.Lock()
		delete(node.data, k)
		node.dataLock.Unlock()
	}
	*reply = true
	return nil
}

func (node *Node) DeleteBackup(key []string, reply *bool) error {
	logrus.Infof("[%s] delete_backup(%s)", node.Addr, key)
	for _, k := range key {
		node.backupLock.RLock()
		_, ok := node.backup[k]
		node.backupLock.RUnlock()
		if !ok {
			*reply = false
			return nil
		}
		node.backupLock.Lock()
		delete(node.backup, k)
		node.backupLock.Unlock()
	}

	*reply = true
	return nil
}

//
// DHT methods
//

func (node *Node) Run(wg *sync.WaitGroup) {
	logrus.Infof("Run %s", node.Addr)
	node.online = true
	node.quitflag = true
	go node.RunRPCServer(wg)
}

func (node *Node) Create() {
	logrus.Info("Create")
	node.refresh()
	node.maintain()
}

func (node *Node) Join(addr string) bool {
	logrus.Infof("[%s] Join (%s)", node.Addr, addr)
	node.refresh()
	var successor string
	if err := node.RemoteCall(addr, "Node.FindSuccessor", In{id(node.Addr), node.Addr}, &successor); err != nil {
		logrus.Error("RemoteCall find_successor in join error: ", err)
		return false
	}
	node.successorsLock.Lock()
	node.successors[0] = successor
	node.successorsLock.Unlock()
	node.maintain()
	return true
}

func (node *Node) Put(key string, value string) bool {
	logrus.Infof("Put %s %s", key, value)
	var successor string
	var pair []Pair
	pair = append(pair, Pair{key, value})
	if err := node.FindSuccessor(In{id(key), key}, &successor); err != nil {
		logrus.Error("find_successor in put error: ", err)
		return false
	}
	if err := node.RemoteCall(successor, "Node.PutData", pair, nil); err != nil {
		logrus.Error("RemoteCall put_data in put error: ", err)
		return false
	}
	if err := node.RemoteCall(successor, "Node.Successor", "", &successor); err != nil {
		logrus.Error("RemoteCall successor in put error: ", err)
		return false
	}
	if err := node.RemoteCall(successor, "Node.PutBackup", pair, nil); err != nil {
		logrus.Error("RemoteCall put_backup in put error: ", err)
		return false
	}
	return true
}

func (node *Node) Get(key string) (bool, string) {
	logrus.Infof("Get %s", key)
	var successor string
	var reply Reply
	if err := node.FindSuccessor(In{id(key), key}, &successor); err != nil {
		logrus.Error("find_successor in get error: ", err)
		return false, ""
	}
	if err := node.RemoteCall(successor, "Node.GetData", key, &reply); err != nil {
		logrus.Error("RemoteCall get_data in get error: ", err)
		return false, ""
	}
	logrus.Infof("Get %s %v: %s", key, reply.Flag, reply.Value)
	return reply.Flag, reply.Value
}

func (node *Node) Delete(key string) bool {
	logrus.Infof("Delete %s", key)
	var successor string
	var reply bool
	if err := node.FindSuccessor(In{id(key), key}, &successor); err != nil {
		logrus.Error("find_successor in delete error: ", err)
		return false
	}
	var k []string
	k = append(k, key)
	if err := node.RemoteCall(successor, "Node.DeleteData", k, &reply); err != nil {
		logrus.Error("RemoteCall delete_data in delete error: ", err)
		return false
	}
	if !reply {
		logrus.Infof("Delete %s false", key)
		return false
	}
	if err := node.RemoteCall(successor, "Node.Successor", "", &successor); err != nil {
		logrus.Error("RemoteCall find_successor in delete error: ", err)
		return false
	}
	if err := node.RemoteCall(successor, "Node.DeleteBackup", k, &reply); err != nil {
		logrus.Error("RemoteCall delete_backup in delete error: ", err)
		return false
	}
	logrus.Infof("Delete %s %v", key, reply)
	return reply
}

func (node *Node) Quit() {
	if !node.quitflag {
		return
	}
	node.quitLock.Lock()
	logrus.Infof("Quit %s", node.Addr)
	node.quitflag = false
	var suc string
	var pre string
	if err := node.Successor("", &suc); err != nil {
		logrus.Error("successor in quit error: ", err)
		return
	}
	if err := node.Predecessor("", &pre); err != nil {
		logrus.Error("predecessor in quit error: ", err)
		return
	}
	logrus.Infof("Quit %s,1", node.Addr)
	if err := node.RemoteCall(suc, "Node.SetPredecessor", pre, nil); err != nil {
		logrus.Error("RemoteCall set_predecessor in quit error: ", err)
		return
	}
	logrus.Infof("Quit %s,2", node.Addr)
	if pre != "" {
		if err := node.RemoteCall(pre, "Node.UpdateSuccessors", "", nil); err != nil {
			logrus.Error("RemoteCall update_successors in quit error: ", err)
			return
		}
		if err := node.RemoteCall(pre, "Node.Predecessor", "", &pre); err != nil {
			logrus.Error("RemoteCall predecessor in quit error: ", err)
			return
		}
		if pre != "" {
			if err := node.RemoteCall(pre, "Node.UpdateSuccessors", "", nil); err != nil {
				logrus.Error("RemoteCall update_successors in quit error: ", err)
				return
			}
			if err := node.RemoteCall(pre, "Node.Predecessor", "", &pre); err != nil {
				logrus.Error("RemoteCall predecessor in quit error: ", err)
				return
			}
			if pre != "" {
				if err := node.RemoteCall(pre, "Node.UpdateSuccessors", "", nil); err != nil {
					logrus.Error("RemoteCall update_successors in quit error: ", err)
					return
				}
			}
		}
	}
	logrus.Infof("Quit %s,3", node.Addr)
	node.quitLock.Unlock()
	logrus.Infof("Quit %s over", node.Addr)
	node.StopRPCServer()
}

func (node *Node) ForceQuit() {
	if !node.quitflag {
		return
	}
	logrus.Infof("ForceQuit %s", node.Addr)
	node.StopRPCServer()
}

// debug function
func (node *Node) Traverse(start string, _ *struct{}) error {
	if node.Addr == start {
		return nil
	}
	if start == "" {
		start = node.Addr
	}
	node.predecessorLock.RLock()
	pre := node.predecessor
	node.predecessorLock.RUnlock()
	node.successorsLock.RLock()
	suc := node.successors
	node.successorsLock.RUnlock()
	fmt.Printf("traverse: %s\npredecessor: %s\nsucessors: %s\n", node.Addr, pre, suc)
	node.RemoteCall(suc[0], "Node.Traverse", start, nil)
	return nil
}
