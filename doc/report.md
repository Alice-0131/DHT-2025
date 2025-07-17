# Report

## Intro
CS(Client-Server) structure consists of server and client. This structure is serviced by a central server, which introduce a problem: if the central server fails, then the whole structure will fail. P2P(Peer to Peer) networking can solve this problem, in which each node is both a client and a server. 

A naive implementation of P2P networking is flooding search, which is stable but too slow. In this project, I implement two DHT(Distributed Hash Table) protocols: Chord and Kademlia, which are both stable and quick.

## Chord

### Structure
```go
type Node struct {
	Addr     string
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
	fingerStrat     [m + 5]*big.Int
	fingerNode      [m + 5]string
	fingerLock      sync.RWMutex
	quitLock        sync.RWMutex
	quitflag        bool
}
```
### Function
```go
//
// Periodic Function
//
func (node *Node) maintain()
func (node *Node) stabilize() 
func (node *Node) fix_fingers() 

//
// RPC Methods
//
func (node *Node) Successor(_ string, reply *string) error 
func (node *Node) Predecessor(_ string, reply *string) error 
func (node *Node) Successors(_ string, reply *[]string) error
func (node *Node) Data(_ string, reply *[]Pair) error 
func (node *Node) Backup(_ string, reply *[]Pair) error 
func (node *Node) UpdateSuccessors(_ string, _ *struct{}) error 
func (node *Node) UpdatePredecessor(_ string, _ *struct{}) error 
func (node *Node) SetPredecessor(addr string, _ *struct{}) error 
func (node *Node) FindSuccessor(tmp In, reply *string) error 
func (node *Node) FindPredecessor(tmp In, reply *string) error 
func (node *Node) ClosestPrecedingFinger(tmp In, reply *string) error 
func (node *Node) Ping(_ string, reply *bool) error 
func (node *Node) Notify(n string, _ *struct{}) error 
func (node *Node) PutData(pair []Pair, _ *struct{}) error 
func (node *Node) PutBackup(pair []Pair, _ *struct{}) error 
func (node *Node) GetData(key string, reply *Reply) error 
func (node *Node) DeleteData(key []string, reply *bool) error 
func (node *Node) DeleteBackup(key []string, reply *bool) error 

//
// DHT methods
//
func (node *Node) Run(wg *sync.WaitGroup) 
func (node *Node) Create() 
func (node *Node) Join(addr string) bool 
func (node *Node) Put(key string, value string) bool 
func (node *Node) Get(key string) (bool, string) 
func (node *Node) Delete(key string) bool 
func (node *Node) Quit() 
func (node *Node) ForceQuit() 
```

### Interpretation

In this protocol, the system can automatically update predecessor and successor by running `stabilize` periodically as long as the inserted node's successor is correct. To ensure this, I should constantly update every node's successor.

`fingerTable` can enhance the efficiency of finding nodes, which allows the time comlexity of DHT methods is $O(log n)$. `fix_fingers` will fix the finger table periodically to ensure its correctness.

Moreover, if a node suddenly fails, the ring will break. To prevent this horrible situation, every node records its successor list. Then if a node's successor fails, it can instantly know its next successor.

### Problems when Debugging

- `ClosestPrecedingFinger` calls recursive function, but if the system only has one node, it will be trapped in an endless loop. In the meantime, the log information is confusing, so it took me a long time to find the problem. I used special judgement to solve this problem.

- In `UpdateSuccessors`, I made some small mistakes so that some nodes update their successors and back up data wrongly.

- When quiting a node, I first let $online = false$, so that it couldn't remotecall RPC methods successfully. So I used `quitflag` to mark if the node is quiting.

## Kademlia

### Structure

```go
type Node struct {
	Addr     string
	online   bool
	listener net.Listener
	server   *rpc.Server

	data         DataBase
	routingTable RoutingTable
}
```
### Function

```go
//
// RPC Methods
//
func (node *Node) Ping(_ string, reply *bool) error 
func (node *Node) Store(pair Pair, _ *struct{}) error 
func (node *Node) FindNode(key string, reply *[]string) error
func (node *Node) FindValue(key string, reply *Ret) error
func (node *Node) Update(addr string, _ *struct{}) error

//
// Private methods
//
func (node *Node) nodeLookup(key string) []string 
func (node *Node) valueLookup(key string) (bool, string) 
func (node *Node) publish(pair Pair) 
func (node *Node) republish()

//
// DHT methods
//
func (node *Node) Run(wg *sync.WaitGroup) 
func (node *Node) Create() 
func (node *Node) Join(addr string) bool 
func (node *Node) Put(key string, value string) bool 
func (node *Node) Get(key string) (bool, string) 
func (node *Node) Delete(key string) bool 
func (node *Node) Quit() 
func (node *Node) ForceQuit() 
```

### Interpretation
Kademlia defines the distance between two nodes with XOR, so its topological structure is undirectional. Kademlia uses `routingTable` to accelerate finding, so it should update its buckets periodically.

The system will store data in k closest nodes, and it will republish and expire data periodically. However, in this project, the test is naive and takes a little time, so I didn't implement these two periodical function.

### Problems when Debugging
- In goroutine, loop variable is captured by func literal, causing concurrency to be unsafe.

- In many cases, I forgot to update `routingTable` so that some nodes couldn't find the closest k nodes correctly.

- The choice of k matters a lot. If $k = 10$, then the advance test may fail with high error rate. If $k = 20$, then the `quit` function runs too slow. At last I choose $k = 15$ to ensure accuracy and proficiency.