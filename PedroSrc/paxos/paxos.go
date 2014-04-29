package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
  "net"
  "net/rpc"
  "log"
  "os"
  "syscall"
  "sync"
  "fmt"
  "math/rand"
  "math"
)

const Debug=0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf("\t"+format, a...)
  }
  return
}


type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]

  // Your data here.
  maxSeq int
  minSeq int
  //TODO: convert to map[int]*Proposer
  proposers map[int]Proposer
  //TODO: convert to map[int]*Acceptor
  acceptors map[int]Acceptor
  //TODO: convert to map[int]*Learner
  learners map[int]Learner
  mins map[int]int
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


//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
  if seq >= px.minSeq {
    go px.propose(seq, v)
  }

  px.mu.Lock()
  defer px.mu.Unlock()
  if seq > px.maxSeq {
    px.maxSeq = seq
  }
}

func (px *Paxos) isMajority(x int) bool {
  return x >= int(math.Floor(float64(len(px.peers))/float64(2.0))) + 1
}

func (px *Paxos) nextProposalNum(seq int) int {
  px.mu.Lock()
  defer px.mu.Unlock()

  proposer := px.proposers[seq]
  maxN := proposer.MaxN
  l := len(px.peers)
  // generate unique n higher than any n we have seen so far
  nextN := ((maxN/l + 1) * l) + px.me
  return nextN
}

func (px *Paxos) logProposalNum(seq int, n int) {
  px.mu.Lock()
  defer px.mu.Unlock()

  proposer := px.proposers[seq]
  defer func(){px.proposers[seq] = proposer}()
  if n > proposer.MaxN {
    proposer.MaxN = n
  }
}

func (px *Paxos) assertProposer(seq int) {
  px.mu.Lock()
  defer px.mu.Unlock()
  _, exists := px.proposers[seq]
  if !exists {
    proposer := Proposer{MaxN: 0}
    px.proposers[seq] = proposer
  }
}

func (px *Paxos) propose(seq int, v interface{}) {
  px.assertProposer(seq)

  n := px.me

  undecided:
  for !px.dead {
    prepareMajority, v_prime := px.prepareMajority(seq, n)
    if v_prime == nil {
      v_prime = v
    }

    if prepareMajority {
      acceptMajority := px.acceptMajority(seq, n, v_prime)

      if acceptMajority {
        px.decideAll(seq, v_prime)
        break undecided
      }
    }
    px.logProposalNum(seq, n)
    n = px.nextProposalNum(seq)
  }
}

func (px *Paxos) sendPrepare(pxnode string, prepareArgs *PrepareArgs, prepareReply *PrepareReply) bool {
  if pxnode == px.peers[px.me] {
    px.PrepareHandler(prepareArgs, prepareReply)
    return true
  }else {
    return call(pxnode, "Paxos.PrepareHandler", prepareArgs, prepareReply)
  }
}

func (px *Paxos) prepareMajority(seq int, n int) (bool, interface{}) {
    prepareOKs := 0
    n_a := 0
    var v_a interface{}
    for _, pxnode := range px.peers {
      prepareArgs := PrepareArgs{Seq: seq, N: n}
      // piggyback local min seqs in agreement messages
      prepareArgs.PBDone = PBDone{Me: px.me, MinSeq: px.minSeq}
      prepareReply := PrepareReply{}
      replyReceived := px.sendPrepare(pxnode, &prepareArgs, &prepareReply)
      if replyReceived {
        if prepareReply.OK {
          prepareOKs += 1
          if prepareReply.N_A > n_a && prepareReply.V_A != nil { // received higher (n_a,v_a) from prepareOK
              n_a = prepareReply.N_A
              v_a = prepareReply.V_A
          }
        }
        px.logProposalNum(seq, prepareReply.MaxN)
      }
    }
    return px.isMajority(prepareOKs), v_a
}

func (px *Paxos) sendAccept(pxnode string, acceptArgs *AcceptArgs, acceptReply *AcceptReply) bool {
  if pxnode == px.peers[px.me] {
    px.AcceptHandler(acceptArgs, acceptReply)
    return true
  }else {
    return call(pxnode, "Paxos.AcceptHandler", acceptArgs, acceptReply)
  }
}

func (px *Paxos) acceptMajority(seq int, n int, v interface{}) bool {
  acceptOKs := 0
  for _, pxnode := range px.peers {
    acceptArgs := AcceptArgs{Seq: seq, N: n, V: v}
    // piggyback local min seqs in agreement messages
    acceptArgs.PBDone = PBDone{Me: px.me, MinSeq: px.minSeq}
    acceptReply := AcceptReply{}
    replyReceived := px.sendAccept(pxnode, &acceptArgs, &acceptReply)
    if replyReceived {
      if acceptReply.OK {
        acceptOKs += 1
      }
      px.logProposalNum(seq, acceptReply.MaxN)
    }
  }
  return px.isMajority(acceptOKs)
}

func (px *Paxos) sendDecide(pxnode string, decideArgs *DecideArgs, decideReply *DecideReply) bool {
  if pxnode == px.peers[px.me] {
    px.DecideHandler(decideArgs, decideReply)
    return true
  }else {
    return call(pxnode, "Paxos.DecideHandler", decideArgs, decideReply)
  }
}

func (px *Paxos) decideAll(seq int, v interface{}) {
  for _, pxnode := range px.peers {
    decideArgs := DecideArgs{Seq: seq, V: v}
    decideArgs.PBDone = PBDone{Me: px.me, MinSeq: px.minSeq}
    decideReply := DecideReply{}
    px.sendDecide(pxnode, &decideArgs, &decideReply)
  } 
}

func (px *Paxos) PrepareHandler(args *PrepareArgs, reply *PrepareReply) error {
  // unpack args
  seq, n, pbdone := args.Seq, args.N, args.PBDone
  px.processPBDone(pbdone)

  px.mu.Lock()
  defer px.mu.Unlock()
  if seq > px.maxSeq {
    px.maxSeq = seq
  }

  acceptor, exists := px.acceptors[seq]
  if !exists {
    acceptor = Acceptor{N_P: 0, N_A: 0}
  }
  defer func(){px.acceptors[seq] = acceptor}()

  if n > acceptor.N_P {
    acceptor.N_P = n
    reply.N_A = acceptor.N_A
    reply.V_A = acceptor.V_A
    reply.OK = true
  }else {
    reply.OK = false
  }
  reply.MaxN = acceptor.N_P
  return nil
}

func (px *Paxos) AcceptHandler(args *AcceptArgs, reply *AcceptReply) error {
  // unpack args
  seq, n, v, pbdone := args.Seq, args.N, args.V, args.PBDone
  px.processPBDone(pbdone)

  px.mu.Lock()
  defer px.mu.Unlock()
  if seq > px.maxSeq {
    px.maxSeq = seq
  }

  acceptor, exists := px.acceptors[seq]
  if !exists {
    acceptor = Acceptor{N_P: 0, N_A: 0}
  }
  defer func(){px.acceptors[seq] = acceptor}()

  if n >= acceptor.N_P {
    acceptor.N_P = n
    acceptor.N_A = n
    acceptor.V_A = v
    reply.OK = true
  }else {
    reply.OK = false
  }
  reply.MaxN = acceptor.N_P
  return nil
}

func (px *Paxos) DecideHandler(args *DecideArgs, reply *DecideReply) error {
  // unpack args
  seq, v, pbdone := args.Seq, args.V, args.PBDone
  px.processPBDone(pbdone)

  px.mu.Lock()
  defer px.mu.Unlock()
  if seq > px.maxSeq {
    px.maxSeq = seq
  }

  learner, exists := px.learners[seq]
  if !exists {
    learner = Learner{}
  }
  defer func(){px.learners[seq] = learner}()

  learner.Decided = true
  learner.V = v
  return nil
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  px.mu.Lock()
  defer px.mu.Unlock()

  if seq > px.minSeq {
    px.minSeq = seq
  }

  for s, _ := range px.proposers {
    if s <= seq {
      delete(px.proposers, s)
    }
  }

  for s, _ := range px.learners {
    if s <= seq {
      delete(px.learners, s)
    }
  }
}

func (px *Paxos) processPBDone(pbdone PBDone) {
  px.mu.Lock()
  defer px.mu.Unlock()
  px.mins[pbdone.Me] = pbdone.MinSeq

  min := px.Min()

  for s, _ := range px.acceptors {
    if s < min {
      delete(px.acceptors, s)
    }
  }
}

/* piggyback done data in agreement messages
func (px *Paxos) sendDone(pxnode string, doneArgs *DoneArgs, doneReply *DoneReply) bool {
  if pxnode == px.peers[px.me] {
    px.DoneHandler(doneArgs, doneReply)
    return true
  }else {
    return call(pxnode, "Paxos.DoneHandler", doneArgs, doneReply)
  }
}

func (px *Paxos) DoneHandler(args *DoneArgs, reply *DoneReply) error {
  reply.Done = px.minSeq
  return nil
}
*/

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  return px.maxSeq
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefore cannot forget these
// instances.
// 
func (px *Paxos) Min() int {
  min := -2
  for _, localMin := range px.mins {
    if min == -2 {
      min = localMin
    }else if localMin < min {
      min = localMin
    }
  }
  return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  px.mu.Lock()
  defer px.mu.Unlock()

  if seq < px.Min() {
    return false, nil
  }
  learner, exists := px.learners[seq]
  if !exists {
    learner = Learner{}
  }
  defer func(){px.learners[seq] = learner}()

  return learner.Decided, learner.V
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  px := &Paxos{}
  px.peers = peers
  px.me = me


  // Your initialization code here.
  px.minSeq = -1
  px.maxSeq = -1
  px.proposers = make(map[int]Proposer)
  px.acceptors = make(map[int]Acceptor)
  px.learners = make(map[int]Learner)
  px.mins = make(map[int]int)
  for pxindex, _ := range px.peers {
    px.mins[pxindex] = -1
  }

  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l
    
    // please do not change any of the following code,
    // or do anything to subvert it.
    
    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }


  return px
}
