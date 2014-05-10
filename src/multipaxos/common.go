package multipaxos

// Errors

type Err struct {
	Msg string
}

const (
	NotLeader = "ErrNotLeader"
)

// Presence

type Presence string

const (
	Alive = "Alive"
	Missing = "Missing"
	Dead = "Dead"
)

// -----------------

// Convenience types

type ServerName string

// -- Shared Map : built-in concurrency support --

func MakeSharedMap() *SharedMap {
	return &SharedMap{Map: make(map[interface{}]interface{})}
}

type SharedMap struct {
	Map map[interface{}]interface{}
	Mu sync.Mutex
}

func (m *SharedMap) SafeGet(key interface{}) (interface{}, bool) {
	m.Mu.Lock()
	value, exists := m.Map[key]
	m.Mu.Unlock()
	return value, exists
}

func (m *SharedMap) SafePut(key interface{}, value interface{}) {
	m.Mu.Lock()
	m.Map[key] = value
	m.Mu.Unlock()
}

/*
func (m *SharedMap) Len() int {
	m._mu.Lock()
	length := len(m._map)
	m._mu.Unlock()
	return length
}
*/

func MakeSharedResponses() *SharedMap {
	return &SharedMap(_map: make(map[int]PrepareReply))
}

// -- Shared Counter : built-in concurrency support --

func MakeSharedCounter() *SharedCounter {
	return &SharedCounter{_n: 0}
}

type SharedCounter struct {
	_n int
	_mu int
}

func (c *SharedCounter) count() int {
	c._mu.Lock()
	count := c._n
	c._mu.Unlock()
	return count
}

func (c *SharedCounter) incr() {
	c._mu.Lock()
	c._n += 1
	c._mu.Unlock()
}

// -----

// Paxos

type Proposer struct {
	//TODO: define this
	Mu sync.Mutex
	V_prime interface{}
}

type Acceptor struct {
	Mu sync.Mutex
	N_p int
	N_a int
	V_a interface{}
}

type Learner struct {
	Mu sync.Mutex
	Decided bool
	V interface{}
}

// --------

// RPC args & replies

type PrepareEpochArgs struct {
  //TODO: define this
}

type PrepareEpochReply struct {
  //TODO: include piggy-back if necessary
	map[int]PrepareReply
}

type PrepareReply struct {
	N_a int
	V_a interface{}
	OK bool
	N_p int // the round number that may have caused a reject
	//TODO: include piggy-back if necessary
}

type AcceptArgs struct {
  Seq int
  N int
  V interface{}
  //TODO: include piggy-back if necessary
}

type AcceptReply struct {
  OK bool
  //TODO: do we need to give any information on our highest seen proposal number? (as in 3a)
  //TODO: include piggy-back if necessary
}

type DecideArgs struct {
  Seq int
  V interface{}
  //TODO: include piggy-back if necessary
}

type DecideReply struct {
  // Empty
}

type PingArgs struct {
	// Empty
}

type PingReply struct {
	PiggyBack PiggyBack
}

type PiggyBack struct {
  //TODO: define this & include in relevant args/replies
  Me int
  LocalMin int
  MaxKnownMin int
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
  c, err := rpc.Dial("unix", srv)
  if err != nil {
    err1 := err.(*net.OpError)
    if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
      fmt.Printf("paxos Dial() failed: %v\n", err1)
    }
    return false
  }
  defer c.Close()

  err = c.Call(name, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}
