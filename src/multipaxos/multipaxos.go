package multipaxos
//TODO: check seq vs. round number
import (
  "net"
  "net/rpc"
  "log"
  "os"
  "syscall"
  "sync"
  "fmt"
  "time"
  //"math"
  "math/rand"
)

/*
:: TABLE OF CONTENTS ::
-----------------------
Section 0 : MultiPaxos Object
Section 1 : API
Section 2 : RPC's & RPC Helpers
Section 3 : RPC Handlers & RPC Handler Helpers
Section 4 : Internal Methods
*/

/* Section 0 : MultiPaxos Object
--------------------------------
-- SubSection 0 : Declaration --
-- SubSection 1 : Constructors --
*/

// -- SubSection 0 : Declaration --

/*
MultiPaxos object definition
*/
type MultiPaxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int

  // Important sequence numbers
  localMin int // one more than the lowest sequence number that we have called Done() on
  localMax int // highest sequence number for which this server knows a paxos instance has been started
  maxKnownMin int // needed to to know up to watch seq to process in the log.
  mins []int // for calculating GlobalMin()
  // Leader election state
  lifeStates []LifeState // keeps track of Alive/Missing/Dead for each peer
  actingAsLeader bool
  // Important round numbers
  epoch int
  highestPrepareEpoch int
  maxKnownEpoch int // for generating a higher, unique epoch number
  // Paxos Instances
  proposers map[int]*Proposer
  proposersMu sync.Mutex
  acceptors map[int]*Acceptor
  acceptorsMu sync.Mutex
  learners  map[int]*Learner
  learnersMu sync.Mutex
  // Persistence
  disk *Disk // simulates disk in software for persistence plan
}

// -- SubSection 1 : Constructors --

func Make(peers []string, me int, rpcs *rpc.Server) *MultiPaxos {
  return makeWithDisk(peers, me, rpcs, MakeDisk())
}

/*
the application wants to create a multipaxos peer.
the ports of all the multipaxos peers (including this one)
are in peers[]. this servers port is peers[me].
*/
func makeWithDisk(peers []string, me int, rpcs *rpc.Server, disk *Disk) *MultiPaxos {
  mpx := &MultiPaxos{}
  mpx.peers = peers
  mpx.me = me

  // Initialization
  mpx.localMin = 0
  mpx.localMax = 0
  mpx.maxKnownMin = 0
  mpx.mins = make([]int, len(mpx.peers))
  mpx.lifeStates = make([]LifeState, len(mpx.peers))
  mpx.actingAsLeader = false
  mpx.epoch = 0
  mpx.highestPrepareEpoch = 0
  mpx.maxKnownEpoch = 0
  mpx.proposers = make(map[int]*Proposer)
  mpx.acceptors = make(map[int]*Acceptor)
  mpx.learners = make(map[int]*Learner)
  mpx.disk = disk

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
  go func() {
    for mpx.dead == false {
      mpx.tick()
      time.Sleep(250 * time.Millisecond) //Tune: set ping interval
    }
  }()
  return mpx
}

/* Section 1 : API
------------------
-- SubSection 0 : KV Interface --
-- SubSection 1 : Failure Simulation Interface --
*/

// -- SubSection 0 : KV Interface --

/*
Paxos values need to be isolated from externalities
We want to avoid any changes to a decided value through references/pointers/etc
Thus, we need something that can be deep-copied
(deep copies are isolated from externalites)
*/
type DeepCopyable interface {
  DeepCopy() DeepCopyable
}

/*
Tries to send a propose
If this server considers itself a leader, send accepts
Otherwise, DO NOT send accepts
*/
func (mpx *MultiPaxos) Push(seq int, v DeepCopyable) Err {
  if mpx.actingAsLeader {
    go mpx.leaderPropose(seq, v.DeepCopy())
    return Err{Nil: true}
  }else {
    return Err{Msg: NotLeader, Nil: false}
  }
}

/*
  The application on this machine is done with
  all instances <= seq.
  If this machine were to die, then it wouldn't need to know about operations commited at or bellow seq
  (since it has already applied all the Ops to its state for sequences <=seq).
*/
func (mpx *MultiPaxos) Done(seq int) {
  mpx.mu.Lock()
  defer mpx.mu.Unlock()
  if seq >= mpx.localMin { // done up to or beyond our local min
    mpx.localMin = seq + 1 // update local min
  }
  mpx.disk.WriteLocalMin(mpx.localMin)
  //forgets until seq (inclusive)
  mpx.forgetProposersUntil(seq)
  mpx.forgetLearnersUntil(seq)
}

/*
the application wants to know the
highest instance sequence known to
this peer.
*/
func (mpx *MultiPaxos) Max() int {
  mpx.mu.Lock()
  defer mpx.mu.Unlock() //Q: is locking necessary here?
  return mpx.localMax
}

/*
Returns the lowest known min.
*/
func (mpx *MultiPaxos) GlobalMin() int {
  mpx.mu.Lock()
  defer mpx.mu.Unlock()
  globalMin := mpx.localMin
  for _, min := range mpx.mins {
    if min < globalMin {
      globalMin = min
    }
  }
  mpx.DPrintf("global min calculated: %d", globalMin)
  return globalMin
}

/*
the application wants to know whether this
peer thinks an instance has been decided,
and if so what the agreed value is. Status()
should just inspect the local peer state;
it should not contact other Paxos peers.
*/
func (mpx *MultiPaxos) Status(seq int) (bool, interface{}) {
  fmt.Println("STATUS call")
  if seq < mpx.GlobalMin() {
    panic(fmt.Sprintf("Cannot remember decision at sequence %d after Done(%d) was called for this instance", seq, mpx.localMin-1))
  }
  learner := mpx.summonLearner(seq)
  learner.Lock()
  defer learner.Unlock()
  return learner.Decided, learner.V
}

// -- SubSection 1 : Failure Simulation Interface --

/*
tell the peer to shut itself down.
for testing. (without persistence)
please do not change this function.
NB: assumes fail-stop
*/
func (mpx *MultiPaxos) Kill() {
  mpx.dead = true
  if mpx.l != nil {
    mpx.l.Close()
  }
}

/*
Simulate server crash
Specifiy whether simulated disk loss should occur
*/
func (mpx *MultiPaxos) Crash(loseDisk bool) {
  //RPC answer: on piazza -> https://piazza.com/class/hpo4va6kbsh4be?cid=636
  mpx.mu.Lock()
  defer mpx.mu.Unlock()
  mpx.dead = true
  if mpx.l != nil {
    mpx.l.Close()
  }
  //TODO: need to pass dead as argument
  if loseDisk {
    mpx = makeWithDisk(mpx.peers, mpx.me, nil, nil)
  }else {
    mpx = makeWithDisk(mpx.peers, mpx.me, nil, mpx.disk)
  }
}

/*
Signal server to undergo recovery protocol
*/
func (mpx *MultiPaxos) Reboot() {
  if mpx.disk == nil { // disk loss
    mpx.disk = MakeDisk() // initialize replacement disk
    mpx.recoverFromPeers()
  }else { // disk intact
    mpx.recoverFromDisk()
  }
  mpx.dead = false
}

/* Section 2 : RPC's and RPC Helpers
------------------------------------
-- SubSection 0 : Ping --
-- SubSection 1 : Prepare Epoch Phase --
-- SubSection 2 : Prepare Epoch Helpers --
-- SubSection 3 : Accept Phase --
-- subSection 4 : Accept Helpers --
-- SubSection 5 : Decide Phase --
-- SubSection 6 : Decide Helpers --
*/

// -- SubSection 0 : Ping --

func (mpx *MultiPaxos) ping(peerID int, rollcall *SharedCounter, done chan bool) {
  mpx.DPrintf("Sending ping to server id:%d", peerID)
  if peerID != mpx.me {
    args := PingArgs{}
    reply := PingReply{}
    replyReceived := call(mpx.peers[peerID], "MultiPaxos.PingHandler", &args, &reply)
    mpx.mu.Lock() // -> lifeStates should not be concurrently accessed
    if replyReceived {
      mpx.lifeStates[peerID] = Alive
    }else {
      if mpx.lifeStates[peerID] == Alive {
        mpx.lifeStates[peerID] = Missing
      }else if mpx.lifeStates[peerID] == Missing {
        mpx.lifeStates[peerID] = Dead
      }
    }
    mpx.mu.Unlock()
  }
  // account for pinged server
  rollcall.SafeIncr()
  if rollcall.SafeCount() == len(mpx.peers) { // accounted for all servers
    done <- true
  }
}

// -- SubSection 1 : Prepare Phase --

/*
Sends prepare epoch for sequences >= seq to all acceptors
*/
func (mpx *MultiPaxos) prepareEpochPhase(seq int) {
  for !mpx.dead {
    responses := MakeSharedResponses()
    rollcall := MakeSharedCounter()
    done := make(chan bool)
    for peerID, _ := range mpx.peers {
      go mpx.prepareEpoch(peerID, seq, responses, rollcall, done)
    }
    <- done
    // Process aggregated replies
    witnessedReject, witnessedMajority := mpx.processAggregated(responses)
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

// -- SubSection 2 : Prepare Epoch Helpers

/*
Processes the aggregated responses for a prepare epoch phase
*/
func (mpx *MultiPaxos) processAggregated(responses *SharedResponses) (bool, bool) {
  responses.Lock()
  defer responses.Unlock()
  witnessedReject := false
  witnessedMajority := false
  for seq, prepareReplies := range responses.Aggregated {
    seqReject, seqMajority := mpx.process(seq, prepareReplies)
    if seqReject {
      witnessedReject = true
    }
    if seqMajority {
      witnessedMajority = true
    }
  }
  return witnessedReject, witnessedMajority
}

func (mpx *MultiPaxos) process(seq int, prepareReplies []PrepareReply) (bool, bool) {
  witnessedReject := false
  proposer := mpx.summonProposer(seq)
  proposer.Lock()
  defer proposer.Unlock()
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
    mpx.refreshMaxKnownEpoch(prepareReply.N_p) // keeping track of maxKnownEpoch
  }
  return witnessedReject, mpx.isMajority(prepareOKs)
}

/*
Sends prepare epoch for sequence >= seq to one server, processes reply, and increments rollcall
*/
func (mpx *MultiPaxos) prepareEpoch(peerID int, seq int, responses *SharedResponses, rollcall *SharedCounter, done chan bool) {
  args := PrepareEpochArgs{N: mpx.epoch, Seq: seq}
  args.PiggyBack = PiggyBack{
    Me: mpx.me,
    LocalMin: mpx.localMin,
    MaxKnownMin: mpx.maxKnownMin,
    MaxKnownEpoch: mpx.maxKnownEpoch, //GO: for some reason the go compiler needs a comma here
  }
  reply := PrepareEpochReply{}
  replyReceived := mpx.sendPrepareEpoch(peerID, &args, &reply)
  if replyReceived {
    responses.Lock()
    aggregate(responses, reply.EpochReplies)
    responses.Unlock()
  }
  rollcall.SafeIncr()
  if rollcall.SafeCount() == len(mpx.peers) {
    done <- true
  }
}

func (mpx *MultiPaxos) sendPrepareEpoch(peerID int, args *PrepareEpochArgs, reply *PrepareEpochReply) bool {
  if peerID == mpx.me {
    mpx.PrepareEpochHandler(args, reply)
    return true
  }else {
    return call(mpx.peers[peerID], "MultiPaxos.PrepareEpochHandler", args, reply)
  }
}

// -- SubSection 3 : Accept Phase --

/*
Sends accept with round number = epoch to all acceptors at sequence = seq
Returns true if a majority accepted; false otherwise
*/
func (mpx *MultiPaxos) acceptPhase(seq int, v DeepCopyable) (bool, bool) {
  acceptOKs := 0
  witnessedReject := false
  for peerID, _ := range mpx.peers {
    args := AcceptArgs{Seq: seq, N: mpx.epoch, V: v}
    args.PiggyBack = PiggyBack{
      Me: mpx.me,
      LocalMin: mpx.localMin,
      MaxKnownMin: mpx.maxKnownMin,
      MaxKnownEpoch: mpx.maxKnownEpoch, //GO: for some reason the go compiler needs a comma here
    }
    reply := AcceptReply{}
    replyReceived := mpx.sendAccept(peerID, &args, &reply)
    if replyReceived {
      if reply.OK {
        acceptOKs += 1
      }else {
        witnessedReject  = true
      }
      mpx.refreshMaxKnownEpoch(reply.N_p) // keeping track of knownMaxEpoch
    }
  }
  return witnessedReject, mpx.isMajority(acceptOKs)
}

// -- SubSection 4 : Accept Helpers --

func (mpx *MultiPaxos) sendAccept(peerID int, args *AcceptArgs, reply *AcceptReply) bool {
  if peerID == mpx.me {
    mpx.AcceptHandler(args, reply)
    return true
  }else {
    return call(mpx.peers[peerID], "MultiPaxos.AcceptHandler", args, reply)
  }
}

// -- SubSection 5 : Decide Phase --

func (mpx *MultiPaxos) decidePhase(seq int, v DeepCopyable) {
  for peerID, _ := range mpx.peers {
    args := DecideArgs{Seq: seq, V: v}
    args.PiggyBack = PiggyBack{
      Me: mpx.me,
      LocalMin: mpx.localMin,
      MaxKnownMin: mpx.maxKnownMin,
      MaxKnownEpoch: mpx.maxKnownEpoch, //GO: for some reason the go compiler needs a comma here
    }
    reply := DecideReply{}
    mpx.sendDecide(peerID, &args, &reply)
  }
}

// -- SubSection 6 : Decide Helpers --

func (mpx *MultiPaxos) sendDecide(peerID int, args *DecideArgs, reply *DecideReply) {
  if peerID == mpx.me {
    mpx.DecideHandler(args, reply)
  }else {
    call(mpx.peers[peerID], "MultiPaxos.DecideHandler", args, reply)
  }
}

/* Section 3 : RPC Handlers & RPC Handler Helpers
-------------------------------------------------
-- SubSection 0 : Ping Handler --
-- SubSection 1 : Prepare Handler --
-- SubSection 2 : Prepare Handler Helper --
-- SubSection 3 : Accept Handler --
-- SubSection 4 : Decide Handler --
*/

// -- SubSection 0 : Ping Handler --

func (mpx *MultiPaxos) PingHandler(args *PingArgs, reply *PingReply) error {
  mpx.DPrintf("entered pinghandler")
  mpx.processPiggyBack(args.PiggyBack)
  mpx.DPrintf("exited ping handler")
  return nil
}

// -- SubSection 1 : Prepare Handler --

func (mpx *MultiPaxos) PrepareEpochHandler(args *PrepareEpochArgs, reply *PrepareEpochReply) error {
  mpx.DPrintf("Prepare epoch handler")
  mpx.refreshHighestPrepareEpoch(args.N)
  mpx.refreshLocalMax(args.Seq)
  mpx.processPiggyBack(args.PiggyBack)
  epochReplies := make(map[int]PrepareReply)
  //OPTIMIZATION: concurrently apply prepares to acceptors
  for seq := args.Seq; seq <= mpx.localMax; seq++ {
    mpx.DPrintf("summoning acceptor at seq:%d", seq)
    acceptor := mpx.summonAcceptor(seq)
    if acceptor == nil {
      mpx.DPrintf("about to prepare a nil acceptor")
    }else {
      mpx.DPrintf("about to prepare non-nil acceptor")
    }
    epochReplies[seq] = mpx.prepareAcceptor(seq, acceptor, args.N)
    //OPTIMIZATION: batch acceptor disk writes
  }
  reply.EpochReplies = epochReplies
  return nil
}

// -- SubSection 2 : Prepare Handler Helper --

func (mpx *MultiPaxos) prepareAcceptor(seq int, acceptor *Acceptor, n int) PrepareReply {
  acceptor.Lock()
  defer acceptor.Unlock()
  prepareReply := PrepareReply{}
  if n > acceptor.N_p {
    acceptor.N_p = n
    prepareReply.N_a = acceptor.N_a
    prepareReply.V_a = acceptor.V_a
    prepareReply.OK = true
  }else {
    prepareReply.OK = false
  }
  prepareReply.N_p = acceptor.N_p
  if acceptor == nil {
    mpx.DPrintf("about to write nil as acceptor")
  }
  mpx.disk.WriteAcceptor(seq, acceptor)
  return prepareReply
}

// -- SubSection 3 : Accept Handler --

func (mpx *MultiPaxos) AcceptHandler(args *AcceptArgs, reply *AcceptReply) error {
  acceptor := mpx.summonAcceptor(args.Seq)
  acceptor.Lock()
  defer acceptor.Unlock()
  mpx.refreshLocalMax(args.Seq)
  mpx.processPiggyBack(args.PiggyBack)
  if args.N >= acceptor.N_p {
    acceptor.N_p = args.N
    acceptor.N_a = args.N
    acceptor.V_a = args.V
    reply.OK = true
  }else {
    reply.OK = false
  }
  reply.N_p = acceptor.N_p
  mpx.disk.WriteAcceptor(args.Seq, acceptor) // write will deep-copy acceptor internally for safety
  return nil
}

// -- SubSection 4 : Decide Handler --

func (mpx *MultiPaxos) DecideHandler(args *DecideArgs, reply *DecideReply) error {
  learner := mpx.summonLearner(args.Seq)
  learner.Lock()
  defer learner.Unlock()
  mpx.refreshLocalMax(args.Seq)
  mpx.processPiggyBack(args.PiggyBack)
  learner.Decided = true
  learner.V = args.V
  return nil
}

/* Section 4 : Internal Methods
-------------------------------
-- SubSection 0 : Paxos --
-- SubSection 1 : MultiPaxos --
-- SubSection 2 : Tick --
-- SubSection 3 : Garbage Collection --
-- SubSection 4 : Persistence --
*/

// -- SubSection 0 : Paxos --

func (mpx *MultiPaxos) isMajority(x int) bool {
  return x >= (len(mpx.peers)/2) + 1 // integer division == math.Floor()
}

func (mpx *MultiPaxos) refreshLocalMax(seq int) {
  mpx.mu.Lock()
  if seq > mpx.localMax {
    mpx.localMax = seq
  }
  mpx.mu.Unlock()
}

/*
Lazy instantiator for proposers
*/
func (mpx *MultiPaxos) summonProposer(seq int) *Proposer {
  mpx.proposersMu.Lock()
  defer mpx.proposersMu.Unlock()
  proposer, exists := mpx.proposers[seq]
  if !exists {
    proposer = &Proposer{}
    mpx.proposers[seq] = proposer
  }
  return proposer
}

/*
Lazy instantiator for acceptors
Prepares acceptors immediately upon creation if server
has received prepare epoch
*/
func (mpx *MultiPaxos) summonAcceptor(seq int) *Acceptor {
  mpx.DPrintf("summoning acceptor")
  mpx.acceptorsMu.Lock()
  mpx.DPrintf("summon acceptor lock")
  acceptor, exists := mpx.acceptors[seq]
  if !exists {
    acceptor = &Acceptor{}
    if acceptor == nil {
      mpx.DPrintf("lazily instantiated nil acceptor!!")
    }
    mpx.prepareAcceptor(seq, acceptor, mpx.highestPrepareEpoch)
    mpx.acceptors[seq] = acceptor
  }
  mpx.DPrintf("summon acceptor done & unlock")
  mpx.acceptorsMu.Unlock()
  return acceptor
}

/*
Lazy instantiator for learners
*/
func (mpx *MultiPaxos) summonLearner(seq int) *Learner {
  mpx.learnersMu.Lock()
  defer mpx.learnersMu.Unlock()
  learner, exists := mpx.learners[seq]
  if !exists {
    learner = &Learner{Decided: false}
    mpx.learners[seq] = learner
  }
  return learner
}

/*
Processes the current state of the PB information.
*/
func (mpx *MultiPaxos) processPiggyBack(piggyBack PiggyBack) {
  mpx.mu.Lock()
  if piggyBack.LocalMin > mpx.mins[piggyBack.Me] {
    mpx.mins[piggyBack.Me] = piggyBack.LocalMin
  }
  if piggyBack.MaxKnownMin > mpx.maxKnownMin {
    mpx.maxKnownMin = piggyBack.MaxKnownMin
  }
  if piggyBack.MaxKnownEpoch > mpx.maxKnownEpoch {
    mpx.maxKnownEpoch = piggyBack.MaxKnownEpoch
  }
  mpx.mu.Unlock()
  mpx.DPrintf("processed piggyback...forgetting relevant acceptors")
  // potential mins entry update may have increased GlobalMin
  globalMin := mpx.GlobalMin()
  mpx.forgetAcceptorsUntil(globalMin)
}

// -- SubSection 1 : MultiPaxos --

/*
Leader Election Protocol
Consider server dead if it has not responded in 2*ping_interval
(Missing if not responded in 1*ping_interval)
Pick highest ID of servers considered living to be the leader
*/
func (mpx *MultiPaxos) leaderElection() int {
  highestLivingID := mpx.me
  mpx.mu.Lock()
  for peerID, lifeState := range mpx.lifeStates {
    if lifeState == Alive || lifeState == Missing {
      if peerID > highestLivingID {
        highestLivingID = peerID
      }
    }
  }
  mpx.mu.Unlock()
  mpx.DPrintf("leader should be server id:%d", highestLivingID)
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
  mpx.DPrintf("ACTING AS LEADER")
  mpx.prepareEpochPhase(seq)
}

/*
Called when this server no longer considers itself a leader
*/
func (mpx *MultiPaxos) relinquishLeadership() {
  mpx.mu.Lock()
  defer mpx.mu.Unlock()
  mpx.actingAsLeader = false
}

/*
Leader propose tries to propose a value with the current epoch round number for the current leader.
If it receives a majority of accepts, the a decision happened!
If not but it received a reject, it will try to do another prepare phase and refresh its epoch number (and increase it).
If no majority formed but not rejection was received, keep trying to propose value.
*/
func (mpx *MultiPaxos) leaderPropose(seq int, v DeepCopyable) {
  undecided:
  for !mpx.dead {
    proposer := mpx.summonProposer(seq)
    proposer.Lock()
    v_prime := proposer.V_prime
    if v_prime == nil {
      v_prime = v
    }
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

func (mpx *MultiPaxos) refreshMaxKnownEpoch(e int) {
  mpx.mu.Lock()
  defer mpx.mu.Unlock()
  if e > mpx.maxKnownEpoch {
    mpx.maxKnownEpoch = e
  }
}

func (mpx *MultiPaxos) refreshHighestPrepareEpoch(e int) {
  mpx.mu.Lock()
  defer mpx.mu.Unlock()
  if e > mpx.highestPrepareEpoch {
    mpx.highestPrepareEpoch = e
  }
}

func (mpx *MultiPaxos) refreshEpoch() {
  mpx.mu.Lock()
  defer mpx.mu.Unlock()
  l := len(mpx.peers)
  // generate unique epoch higher than any epoch we have seen so far
  mpx.epoch = ((mpx.maxKnownEpoch/l + 1) * l) + mpx.me
  mpx.maxKnownEpoch = mpx.epoch // not strictly necessary, but maintains the implied invariant of maxKnownEpoch
}

// -- SubSection 2 : Tick --

/*
Periodic tick function
Ping all servers
Once we have accounted for all servers, run the leader election protocol
If this server considers itself a leader, start acting as a leader
*/
func (mpx *MultiPaxos) tick() {
  mpx.DPrintf("TICK")
  rollcall := MakeSharedCounter()
  done := make(chan bool)
  for peerID, _ := range mpx.peers {
    go mpx.ping(peerID, rollcall, done)
  }
  <- done
  mpx.DPrintf("PAST TICK DONE")
  // leader decision & action
  leaderID := mpx.leaderElection()
  if leaderID == mpx.me {
    if !mpx.actingAsLeader { //Q: actingAsLeader access issues? Are boolean read/writes atomic?
      mpx.actAsLeader()
    }
  }else {
    if mpx.actingAsLeader {
      mpx.relinquishLeadership()
    }
  }
}

// -- SubSection 3 : Garbage Collection --

/*
Deletes proposers from a sequence <= the threshold
No server will need this information in the future
*/
func (mpx *MultiPaxos) forgetProposersUntil(threshold int) {
  mpx.proposersMu.Lock()
  defer mpx.proposersMu.Unlock()
  for s, _ := range mpx.proposers {
    if s <= threshold {
      delete(mpx.proposers, s)
    }
  }
}

func (mpx *MultiPaxos) forgetAcceptorsUntil(threshold int) {
  mpx.DPrintf("starting acceptor forgetting")
  mpx.acceptorsMu.Lock()
  mpx.DPrintf("acceptor forget locked")
  for s, _ := range mpx.acceptors {
    if s <= threshold {
      delete(mpx.acceptors, s)
    }
  }
  mpx.DPrintf("done acceptor forgetting")
  mpx.acceptorsMu.Unlock()
  mpx.DPrintf("acceptor forget unlock")
}

func (mpx *MultiPaxos) forgetLearnersUntil(threshold int) {
  mpx.learnersMu.Lock()
  defer mpx.learnersMu.Unlock()
  for s, _ := range mpx.learners {
    if s <= threshold {
      delete(mpx.learners, s)
    }
  }
}

// -- SubSection 4 : Persistence --

func (mpx *MultiPaxos) recoverFromDisk() {
  mpx.disk.Lock()
  mpx.localMin = mpx.disk.ReadLocalMin()
  mpx.localMax = mpx.localMin
  mpx.maxKnownMin = mpx.localMin
  mpx.mins = make([]int, len(mpx.peers)) // mins filled in after acceptors
  mpx.lifeStates = make([]LifeState, len(mpx.peers))
  for i, _ := range mpx.lifeStates { // fill in lifeStates with Dead
    mpx.lifeStates[i] = Dead
  }
  mpx.actingAsLeader = false
  mpx.epoch = 0
  mpx.highestPrepareEpoch = 0
  mpx.maxKnownEpoch = 0
  mpx.proposers = make(map[int]*Proposer)
  mpx.acceptors = make(map[int]*Acceptor)
  globalMin := mpx.localMin
  for seq, acceptor := range mpx.disk.ReadAcceptors() {
    acceptorCopy := acceptor.DeepCopy()
    mpx.acceptors[seq] = &acceptorCopy
    if seq < globalMin {
      globalMin = seq
    }
  }
  mpx.learners = make(map[int]*Learner) //OPTIMIZATION: get directly from disk
  for i, _ := range mpx.mins { // fill in mins with globalMin
    mpx.mins[i] = globalMin
  }
  mpx.disk.Unlock()
}

func (mpx *MultiPaxos) recoverFromPeers() {
  /*TODO:
  Step 1:
  Ask each server for their localMax
  if we receive at least a majority of replies (w/o rpc failure):
    mark = max(localMax) from majority

  Step 2:
  Ask each server for their localMin
  if any server has localMin > mark:
    copy necessary state from server
    begin operating normally
  else:
    try Step 2 again
  */
}

// DEBUGGING

const Debug = true

func (mpx *MultiPaxos) DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug {
    prefix := fmt.Sprintf("Server ID:%d :: ", mpx.me)
    log.Printf(prefix+format, a...)
  }
  return
}
