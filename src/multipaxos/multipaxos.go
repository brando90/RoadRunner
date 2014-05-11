package multipaxos

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

type MultiPaxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int

  localMin int // one more than the lowest sequence number that we have called Done() on
  localMax int // highest sequence number this server knows of //TODO: update this where ever necessary
  //TODO: OPTIMIZATION :: maxKnownMin int // needed to to know up to watch seq to process in the log.

  proposers SharedMap // internally: map[int]*Proposer
  acceptors SharedMap // internally: map[int]*Acceptor
  learners  SharedMap // internally: map[int]*Learner

  mins SharedSlice // internally: []int for calculating GlobalMin()

  lifeStates SharedSlice // internally: []LifeState keeps track of Alive/Missing/Dead for each peer
  actingAsLeader bool

  epoch int
  maxKnownEpoch int // for generating a higher, unique epoch number

  disk Disk // simulates disk in software for persistence plan
}

// ---

// API

/*
Tries to send accept
If this server considers itself a leader, send accepts
Otherwise, DO NOT send accepts. Relay potential leader information to client instead
*/
func (mpx *MultiPaxos) Push(seq int, v interface{}) Err {
  if actingAsLeader {
    go mpx.leaderPropose(seq, v)
    return nil
  }else {
    return Err{Msg: NotLeader}
  }
}

/*TODO: write a better comment
  The application on this machine is done with
  all instances <= seq.
  If this machine were to die, then it wouldn't need to know about operations commited at or bellow seq
  (since it has already applied all the Ops to its state for <=seq).
*/
func (mpx *MultiPaxos) Done(seq int) {
  mpx.mu.Lock()
  if seq >= mpx.localMin { // done up to or beyond our local min
    mpx.localMin = seq + 1 // update local min
  }
  mpx.mu.Unlock() //OPTIMIZATION: fine-grain locking for localMin
  //forgets until seq (inclusive)
  mpx.safeForgetUntil(mpx.proposers, seq)
  mpx.safeForgetUntil(mpx.learners, seq)
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (mpx *MultiPaxos) Max() int {
  //TODO: update localMax in other methods where ever relevant
  return mpx.localMax
}

/*
Returns the lowest known min.
*/
func (mpx *MultiPaxos) GlobalMin() int {
  mpx.mu.Lock()
  globalMin := mpx.localMin
  mpx.mu.Unlock() //OPTIMIZATION: fine-grain locking for localMin
  mpx.mins.Mu.Lock()
  for _, min := range mpx.mins.Slice {
    if min < globalMin {
      globalMin = min
    }
  }
  mpx.mins.Mu.Unlock()
  mpx.safeForgetUntil(mpx.acceptors, globalMin)
  return globalMin
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (mpx *MultiPaxos) Status(seq int) (bool, interface{}) {
  if seq < mpx.GlobalMin() {
    panic("Cannot remember decision at sequence %d after Done(%d) was called for this instance", seq, mpx.localMin-1)
  }
  learner := mpx.summonLearner(seq)
  learner.Mu.Lock()
  defer learner.Mu.Unlock()
  return learner.Decided, learner.Value
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (mpx *MultiPaxos) Kill() {
  mpx.dead = true
  if mpx.l != nil {
    mpx.l.Close()
  }
}

// -----

// RPC's

// -- Prepare Phase --

/*
Sends prepare epoch for sequences >= seq to all acceptors
*/
func (mpx *MultiPaxos) prepareEpochPhase(seq int) {
  for !mpx.dead {
    responses := MakeSharedMap()
    rollcall := MakeSharedCounter()
    done := make(chan bool)
    for _, peer := range mpx.peers {
      go sendPrepareEpoch(peerID, seq, responses, rollcall, done)
    }
    <- done
    // Process aggregated replies
    witnessedReject, witnessedMajority := mpx.SafeProcessAggregated(responses)
    if witnessedReject {
      mpx.refreshEpoch()
      continue // try again
    }else if witnessedMajority {
      return
    }else {
      continue // try again
    }
  }
}

/*
Processes the aggregated responses for a prepare epoch phase
*/
func (mpx *MultiPaxos) SafeProcessAggregated(responses *SharedMap) (bool, bool) {
  witnessedReject := false
  witnessedMajority := false
  mpx.SafeProcessAggregated(responses)
  responses.Mu.Lock()
  for k, v := range responses.Map {
    sn := k.(int)
    prepareReplies := v.([]PrepareReply) // accumulated replies for sequence number = s
    proposer := summonProposer(sn)
    seqReject, seqMajority := proposer.SafeProcess(prepareReplies)
    if seqReject {
      witnessedReject = true
    }
    if seqMajority {
      witnessedMajority = true
    }
  }
  responses.Mu.Unlock()
  return witnessedReject, witnessedMajority
}

/*
Sends prepare epoch for sequence >= seq to one server, processes reply, and increments rollcall
*/
func (mpx *MultiPaxos) prepareEpoch(peerID ServerID, seq int, responses *SharedMap, rollcall *SharedCounter, done chan bool) {
  args := PrepareEpochArgs{Epoch: mpx.epoch, Seq: seq}
  args.PiggyBack = PiggyBack{Me: mpx.me, LocalMin: mpx.localMin, MaxKnownMin: mpx.maxKnownMin}
  reply := PrepareEpochReply{}
  replyReceived := sendPrepareEpoch(peerID, &args, &reply)
  if replyReceived {
    responses.aggregate(reply.EpochReplies)
  }
  rollcall.SafeIncr()
  if rollcall.SafeCount() == len(mpx.peers) {
    done <- true
  }
}

func (mpx *MultiPaxos) sendPrepareEpoch(peerID serverID, args *PrepareEpochArgs, reply *PrepareEpochArgs) bool {
  if peerID == mpx.me {
    mpx.PrepareEpochHandler(args, reply)
    return true
  }else {
    return call(mpx.peers[peerID], "MultiPaxos.PrepareEpochHandler", args, reply)
  }
}

// -- Accept Phase --

/*
Sends accept with round number = epoch to all acceptors at sequence = seq
Returns true if a majority accepted; false otherwise
*/
func (mpx *MultiPaxos) acceptPhase(seq int, v interface{}) (bool, bool) {
  acceptOKs := 0
  witnessedReject = false
  for _, peer := range mpx.peers {
    args := AcceptArgs{Seq: seq, N: mpx.epoch, V: v}
    args.PiggyBack = PiggyBack{Me: mpx.me, LocalMin: mpx.localMin, MaxKnownMin: mpx.maxKnownMin}
    reply := AcceptReply{}
    replyReceived := mpx.sendAccept(peer, &args, &reply)
    if replyReceived {
      if reply.OK {
        acceptOKs += 1
      }else {
        witnessedReject  = true
      }
      mpx.considerEpoch(reply.N_p) // keeping track of knownMaxEpoch
    }
  }
  return witnessedReject, mpx.isMajority(acceptOKs)
}

func (mpx *MultiPaxos) sendAccept(peerID ServerID, args *AcceptArgs, reply *AcceptReply) bool {
  if peerID == mpx.me {
    mpx.AcceptHandler(args, reply)
    return true
  }else {
    return call(mpx.peers[peerID], "MultiPaxos.AcceptHandler", args, reply)
  }
}

// -- Decide Phase --

func (mpx *MultiPaxos) decidePhase(seq int, v interface{}) {
  for _, peer := range mpx.peers {
    args := DecideArgs{Seq: seq, V: v}
    args.PiggyBack = PiggyBack{Me: mpx.me, LocalMin: mpx.localMin, MaxKnownMin: mpx.maxKnownMin}
    reply := DecideReply{}
    mpx.sendDecide(peer, &args, &reply)
  }
}

func (mpx *MultiPaxos) sendDecide(peerID ServerID, args *DecideArgs, reply *DecideReply) {
  if peerID == mpx.me {
    mpx.DecideHandler(args, reply)
    return true
  }else {
    return call(mpx.peers[peerID], "MultiPaxos.DecideHandler", args, reply)
  }
}

func (mpx *MultiPaxos) ping(peerID ServerID, rollcall *SharedCounter, done chan bool) {
  if peerID != mpx.me {
    args := PingArgs{}
    reply := PingReply{}
    replyReceived := call(mpx.peers[peerID], "MultiPaxos.PingHandler", &args, &reply)
    if replyReceived {
      mpx.processPiggyBack(reply.PiggyBack)
      mpx.mu.Lock()
      mpx.presence[peerID] = Alive
      mpx.mu.Unlock() //OPTIMIZATION: array lock for presence
    }else {
      mpx.mu.Lock()
      if mpx.presence[peerID] == Alive {
        mpx.presence[peerID] = Missing
      }else if mpx.presence[peerID] == Missing {
        mpx.presence[peerID] = Dead
      }
      mpx.mu.Unlock() //OPTIMIZATION: array lock for presence
    }
  }
  // account for pinged server
  rollcall.SafeIncr()
  if rollcall.SafeCount() == len(mpx.peers) { // accounted for all servers
    done <- true
  }
}

// ------------

// RPC handlers

func (mpx *MultiPaxos) PrepareEpochHandler(args *PrepareEpochArgs, reply *PrepareEpochReply) error {
  mpx.processPiggyBack(args.PiggyBack)
  epochReplies := make(map[int]PrepareReply)
  /*TODO: do this here? or abstract to helper method??
  if args.Epoch > mpx.maxKnownEpoch {
    mpx.maxKnownEpoch = args.Epoch
    mpx.refreshEpoch()
  }
  */
  for seq := args.Seq; seq <= mpx.localMax; seq++ {
    acceptor := mpx.summonAcceptor(seq)
    epochReplies[seq] = acceptor.SafePrepare(args.Epoch)
  }
  reply.EpochReplies = epochReplies
  return nil
}

func (mpx *MultiPaxos) DecideHandler(args *DecideArgs, reply *DecideReply) error {
  mpx.processPiggyBack(args.PiggyBack)
  learner := mpx.summonLearner(args.Seq)
  learner.Mu.Lock()
  learner.Decided = true
  learner.V = args.V
  learner.Mu.Unlock()
  return nil
}

func (mpx *MultiPaxos) PingHandler(args *PingArgs, reply *PingReply) error {
  mpx.processPiggyBack(args.PiggyBack)
  mpx.mu.Lock()
  defer mpx.mu.Unlock() //OPTIMIZATION: fine-grain locking for localMin, maxKnownMin
  reply.PiggyBack = PiggyBack{Me: mpx.me, LocalMin: mpx.localMin, MaxKnownMin: mpx.maxKnownMin}
  return nil
}

// ----------------

// Internal methods

func (mpx *MultiPaxos) isMajority(x int) bool {
  return x >= (len(mpx.peers)/2) + 1 // integer division == math.Floor()
}

func (mpx *MultiPaxos) leaderPropose(seq int, v interface{}) {
  undecided:
  for !mpx.dead {
    proposer := mpx.summonProposer(seq)
    proposer.Lock()
    v_prime := proposer.V_prime
    proposer.Unlock()
    witnessedReject, acceptMajority := mpx.acceptPhase(seq, v_prime)
    if witnessedReject {
      mpx.refreshEpoch()
      mpx.prepareEpochPhase(seq)
      continue
    }else if acceptMajority {
      mpx.decidePhase(seq, v_prime)
      break undecided
    }else {
      continue
    }
  }
}

func (mpx *MultiPaxos) considerEpoch(e int) {
  mpx.mu.Lock()
  defer mpx.mu.Unlock() //OPTIMIZATION: fine-grain locking for maxKnownEpoch and maxKnownEpoch
  if e > mpx.maxKnownEpoch {
    mpx.maxKnownEpoch = e
  }
}

func (mpx *MultiPaxos) refreshEpoch() {
  l := len(mpx.peers)
  // generate unique epoch higher than any epoch we have seen so far
  mpx.mu.Lock()
  mpx.epoch = ((mpx.maxKnownEpoch/l + 1) * l) + mpx.me
  mpx.maxKnownEpoch = mpx.epoch // not strictly necessary, but maintains the implied invariant of maxKnownEpoch
  mpx.mu.Unlock() //OPTIMIZATION: fine-grain locking for epoch and maxKnownEpoch
}

/*
Periodic tick function
Ping all servers
Once we have accounted for all servers, run the leader election protocol
If this server considers itself a leader, start acting as a leader
*/
func (mpx *MultiPaxos) tick() {
  rollcall := MakeSharedCounter()
  done := make(chan bool)
  for _, peer := range mpx.peers {
    go ping(peer, rollcall, done)
  }
  <- done
  // leader decision & action
  leaderID := mpx.leaderElection()
  if leaderID == mpx.me {
    if !mpx.actingAsLeader {
      mpx.actAsLeader()
    }
  }else {
    if mpx.actingAsLeader {
      mpx.relinquishLeadership()
    }
  }
}

/*
Leader Election Protocol
Consider server dead if it has not responded in 2*ping_interval
(Missing if not responded in 1*ping_interval)
Pick highest ID of servers considered living to be the leader
*/
func (mpx *MultiPaxos) leaderElection() int {
  highestLivingID := mpx.me
  mpx.lifeStates.Mu.Lock()
  for k, v := range mpx.lifeStates.Slice {
    peerID := k.(int)
    lifeState := v.(LifeState)
    if lifeState == Alive || lifeState == Missing {
      if peerID > highestLivingID {
        highestLivingID = peerID
      }
    }
  }
  mpx.lifeStates.Mu.Unlock()
  return highestLivingID
}

/*
Called when this server starts considering itself a leader
*/
func (mpx *MultiPaxos) actAsLeader() {
  mpx.mu.Lock()
  mpx.actingAsLeader = true
  seq := mpx.localMax
  mpx.mu.Unlock()
  //OPTIMIZATION: fine-grain locking for actingAsLeader and localMin...?
  mpx.prepareEpochPhase(seq)
}

/*
Called when this server no longer considers itself a leader
*/
func (mpx *MultiPaxos) relinquishLeadership() {
  mpx.mu.Lock()
  mpx.actingAsLeader = false
  //TODO: handle/respond to in-progress requests correctly
  mpx.mu.Unlock() //OPTIMIZATION: fine-grain lock for actingAsLeader...?
}

// -- Summoners (lazy instantiators) --

func (mpx *MultiPaxos) summonProposer(seq int) *Proposer {
  mpx.proposers.Mu.Lock()
  proposer, exists := mpx.proposers.Map[seq]
  if !exists {
    proposer = &Proposer{}
    mpx.proposers[seq] = proposer
  }
  mpx.proposers.Mu.Unlock()
  return proposer.(*Proposer)
}

func (mpx *MultiPaxos) summonAcceptor(seq int) *Acceptor {
  mpx.acceptors.Mu.Lock()
  acceptor, exists := mpx.acceptors.Map[seq]
  if !exists {
    acceptor = &Acceptor{}
    acceptor.SafePrepare(mpx.maxKnownEpoch)
    //TODO: Lock for maxKnownEpoch
    //TODO: might need to initialize from disk
    mpx.acceptors = acceptor
  }
  mpx.acceptors.Mu.Unlock()
  return acceptor.(*Acceptor)
}

func (mpx *MultiPaxos) summonLearner(seq int) *Learner {
  mpx.learners.Mu.Lock()
  learner, exists := mpx.learners.Map[seq]
  if !exists {
    learner = &Learner{Decided: false}
    mpx.learners[seq] = learner
  }
  mpx.learners.Mu.Unlock()
  return learner.(*Learner)
}

/*
Processes the current state of the PB information.
*/
func (mpx *MultiPaxos) processPiggyBack(piggyBack PiggyBack){
  mpx.mins.Mu.Lock()
  if piggyBack.LocalMin > mpx.mins.Slice[piggyBack.Me] {
    mpx.mins.Slice[piggyBack.Me] = piggyBack.LocalMin
  }
  mpx.mins.Mu.Unlock()
  mpx.mu.Lock()
  if piggyBack.MaxKnownMin > mpx.maxKnownMin {
    mpx.maxKnownMin = mpx.MaxKnownMin
  }
  min := mpx.GlobalMin()
  mpx.mu.Unlock() //OPTIMIZATION: fine-grain locking for maxKnownMins
}

// -- Garbage collection --
// Deletes anything within the paxos map (e.g. proposers, acceptors, learners)
// from a sequence <= the threshold
// No server will need this information in the future
func (mpx *MultiPaxos) forgetUntil(pxRole *SharedMap, threshold int) {
  pxRole.Mu.Lock()
  for s, _ := range pxRole.Map {
    if s <= threshold {
      delete(pxRole.Map, s)
    }
  }
  pxRole.Mu.Unlock()
}

// -----------

// Constructor

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *MultiPaxos {
  mpx := &MultiPaxos{}
  mpx.peers = peers
  mpx.me = me

  // Initialization
  //TODO: initialize
  mpx.localMin = 0
  mpx.localMax = 0
  mpx.proposers = MakeSharedMap()
  mpx.acceptors = MakeSharedMap()
  mpx.learners = MakeSharedMap()
  mpx.mins = MakeSharedSlice()
  mpx.lifeStates = MakeSharedSlice()
  mpx.actingAsLeader = false
  mpx.epoch = 0
  mpx.maxKnownEpoch = 0
  mpx.disk = Disk{}

  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(mpx)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(mpx)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    mpx.l = l

    // please do not change any of the following code,
    // or do anything to subvert it.

    // create a thread to accept RPC connections
    go func() {
      for mpx.dead == false {
        conn, err := mpx.l.Accept()
        if err == nil && mpx.dead == false {
          if mpx.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if mpx.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            mpx.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            mpx.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && mpx.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }
  return mpx
}
