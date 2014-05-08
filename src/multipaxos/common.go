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
  //TODO: define this
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
