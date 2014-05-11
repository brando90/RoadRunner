package multipaxos

// Errors

type Err struct {
	Msg string
}

const (
	NotLeader = "ErrNotLeader"
)

// Life State (Alive/Missing/Dead)

type LifeState string

const (
	Alive = "Alive"
	Missing = "Missing"
	Dead = "Dead"
)

// -----------------

// Convenience types

type ServerID int
type ServerName string

// -- SharedInt : built-in concurrency support --

func MakeSharedInt() *SharedInt {
	return &SharedInt{}
}

type SharedInt struct {
	Int int
	Mu sync.Mutex
}


// -- Shared Map : built-in concurrency support --

func MakeSharedMap() *SharedMap {
	return &SharedMap{Map: make(map[interface{}]interface{})}
}

type SharedMap struct {
	Map map[interface{}]interface{}
	Mu sync.Mutex
}

/*
func (m *SharedMap) SafeReset() {
	m.Mu.Lock()
	m.Map = make(map[interface{}]interface{})
	m.Mu.Unlock()
}
*/

//TODO: is this method ever called?
func (m *SharedMap) SafeGet(key interface{}) (interface{}, bool) {
	m.Mu.Lock()
	value, exists := m.Map[key]
	m.Mu.Unlock()
	return value, exists
}

//TODO: is this method ever called?
func (m *SharedMap) SafePut(key interface{}, value interface{}) {
	m.Mu.Lock()
	m.Map[key] = value
	m.Mu.Unlock()
}

//TODOL shpuld this be a helper method of mpx?
func (m *SharedMap) aggregate(epochReplies map[int]PrepareReply) {
	m.Mu.Lock()
	for seq, prepareReply := range epochReplies {
		prepareReplies := m.Map[seq].([]PrepareReply)
		prepareReplies = append(prepareReplies, prepareReply)
		m.Map[seq] = prepareReplies
	}
	m.Mu.Unlock()
}

// -- Shared Counter : built-in concurrency support --

func MakeSharedCounter() *SharedCounter {
	return &SharedCounter{_n: 0}
}

type SharedCounter struct {
	_n int
	_mu int
}

/*
func (c *SharedCounter) SafeReset() {
	c._mu.Lock()
	c._n = 0
	c._mu.Unlock()
}
*/

func (c *SharedCounter) SafeCount() int {
	c._mu.Lock()
	count := c._n
	c._mu.Unlock()
	return count
}

func (c *SharedCounter) SafeIncr() {
	c._mu.Lock()
	c._n += 1
	c._mu.Unlock()
}

// -- Shared Slice --

func MakeSharedSlice() *SharedSlice {
	return &SharedSlice{Slice: []interface{}{}}
}

type SharedSlice struct {
	Mu sync.Mutex
	Slice []interface{}
}

func (s *SharedSlice) SafeFill(length int, content interface{}) {
	s.Mu.Lock()
	for i := 0; i < length; i++ {
		s = append(s, content)
	}
	s.Mu.Unlock()
}

/*
func (s *SharedSlice) SafeReset() {
	s.Mu.Lock()
	s.Slice = []interface{}{}
	s.Mu.Unlock()
}
*/

// -----

// Paxos

type Proposer struct {
	Mu sync.Mutex
	N_prime int
	V_prime DeepCopyable
}

/*
Processes prepare replies for this proposer's sequence number
*/
func (proposer *Proposer) SafeProcess(prepareReplies []PrepareReply) (bool, bool) {
	proposer.Mu.Lock()
	prepareOKs := 0
	for _, prepareReply := range prepareReplies {
		if prepareReply.OK {
			prepareOKs += 1
			if prepareReply.N_a > proposer.N_prime && prepareReply.V_a != nil { // received higher (n_a,v_a) from prepareOK
				proposer.N_prime = prepareReply.N_a
				proposer.V_prime = prepareReply.V_a
			}
		}else {
			witnessedReject = true
		}
		mpx.considerEpoch(reply.N_p) // keeping track of maxKnownEpoch
	}
	proposer.Mu.Unlock()
	return witnessedReject, mpx.isMajority(prepareOKs)
}

type Acceptor DeepCopyable {
	Mu sync.Mutex
	N_p int
	N_a int
	V_a DeepCopyable
}

func (acceptor Acceptor) DeepCopy() Acceptor {
	acceptor.Mu.Lock()
	copy := Acceptor{
		N_p: acceptor.N_p,
		N_a: acceptor.N_a,
		V_a: acceptor.V_a.DeepCopy()
	}
	acceptor.Mu.Unlock()
	return copy
}

type Learner struct {
	Mu sync.Mutex
	Decided bool
	V DeepCopyable
}

// --------

// RPC args & replies

//OPTIMIZATION: piggyback in rpc replies

type PrepareEpochArgs struct {
	N int
	Seq int
	PiggyBack PiggyBack
}

type PrepareEpochReply struct {
	EpochReplies map[int]PrepareReply
	//OPTIMIZATION: PiggyBack PiggyBack
}

type PrepareReply struct {
	N_a int
	V_a DeepCopyable
	OK bool
	N_p int // the round number that may have caused a reject
}

type AcceptArgs struct {
  Seq int
  N int
  V DeepCopyable
	PiggyBack PiggyBack
}

type AcceptReply struct {
  OK bool
	N_p int
	//OPTIMIZATION: PiggyBack PiggyBack
}

type DecideArgs struct {
  Seq int
  V DeepCopyable
	PiggyBack PiggyBack
}

type DecideReply struct {
  // Empty
	//OPTIMIZATION: PiggyBack PiggyBack
}

type PingArgs struct {
	// Empty
	PiggyBack PiggyBack
}

type PingReply struct {
	//OPTIMIZATION: PiggyBack PiggyBack
}

type PiggyBack struct {
  Me int
  LocalMin int
  MaxKnownMin int
	MaxKnownEpoch int
}

// Disk

func MakeDisk() *Disk {
	return &Disk{
		dead: false,
		crashed: false,
		Acceptors: make(map[int]Acceptors),
		LocalMin: 0
	}
}

type Disk struct {
	dead bool
	crashed bool
	Mu sync.Mutex
	Acceptors map[int]*Acceptors
	//OPTIMIZATION: keep track of learners... helps KV in common case
	LocalMin int
}

func (d *Disk) SafeDead() {
	d.Mu.Lock()
	d.dead = true
	d.Mu.Unlock()
}

func (d *Disk) SafeErase() {
	d.Mu.Lock()
	d.crashed = true
	d.Acceptors = make(map[int]*Acceptors)
	d.LocalMin = 0
	d.Mu.Unlock()
}

func (d *Disk) SafeAlive() {
	d.Mu.Lock()
	d.dead = false
	d.Mu.Unlock()
}

func (d *Disk) SafeCrashed() bool {
	d.Mu.Lock()
	crashed := d.crashed
	d.Mu.Unlock()
	return crashed
}

func (d *Disk) SafeWriteAcceptor(seq int, acceptor *Acceptor) {
	d.Mu.Lock()
	if (!d.dead) {
		d.Acceptors[seq] = &(acceptor.DeepCopy())
	}
	d.Mu.Unlock()
}

func (d *Disk) SafeWriteLocalMin(localMin int) {
	d.Mu.Lock()
	if (!d.dead) {
		d.LocalMin = localMin
	}
	d.Mu.Unlock()
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
