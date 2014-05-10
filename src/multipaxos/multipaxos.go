package multipaxos
//TODO: PANIC! when updating epoch number, take care to choose a UNIQUE epoch/round number
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

  proposers map[int]*Proposer
  acceptors map[int]*Acceptor
  learners  map[int]*Learner

  mins []int // for calculating GlobalMin()

  presence []Presence
  actingAsLeader bool

  epoch int
  maxKnownEpoch int // for generating a higher, unique epoch number
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
  mpx.forgetUntil(mpx.proposers, seq)
  mpx.forgetUntil(mpx.learners, seq)
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
  defer mpx.mu.Unlock() //OPTIMIZATION: array lock for mins
  globalMin := mpx.localMin
  for _, min := range mpx.mins {
    if min < globalMin {
      globalMin = min
    }
  }
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
  /*TODO: reevaluate this commented code. pretty sure its not needed (this case should never happen in non-byzantine settings... i think)
  if seq < mpx.GlobalMin() {
    return false, nil
  }
  */
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
    //TODO: refactor below into helper methods
    witnessedReject := false
    witnessedMajority := false
    responses.Mu.Lock()
    for k, v := range responses.Map {
      sn := k.(int)
      prepareReplies := v.([]PrepareReply) // accumulated replies for sequence number = s
      proposer := summonProposer(sn)
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
        mpx.considerEpoch(reply.N_p) // keeping track of knownMaxEpoch
      }
      if mpx.isMajority(prepareOKs) {
        witnessedMajority = true
      }
      proposer.Mu.Unlock()
    }
    responses.Mu.Unlock()
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
  //TODO: process responses into proposers (write to V_prime field if necessary)
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
    acceptor.Mu.Lock()
    //TODO: abstract below to helper method (prepareHandler)
    prepareReply := PrepareReply{}
    if args.Epoch > acceptor.N_p {
      acceptor.N_p = args.Epoch
      prepareReply.N_a = acceptor.N_a
      prepareReply.V_a = acceptor.V_a
      prepareReply.OK = true
    }else {
      prepareReply.OK = false
    }
    acceptor.Mu.Unlock()
    prepareReply.N_p = acceptor.N_p
    epochReplies[seq] = prepareReply
  }
  reply.EpochReplies = epochReplies
  return nil
}

func (mpx *MultiPaxos) AcceptHandler(args *AcceptArgs, reply *AcceptReply) error {
  mpx.processPiggyBack(args.PiggyBack)
  acceptor := mpx.summonAcceptor(args.Seq)
  acceptor.Lock()
  if args.N >= acceptor.N_p {
    acceptor.N_p = n
    acceptor.N_a = n
    acceptor.V_a = v
    reply.OK = true
  }else {
    reply.OK = false
  }
  acceptor.Unlock()
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

func (mpx *MultiPaxos) considerEpoch(epoch int) {
  mpx.mu.Lock()
  defer mpx.mu.Unlock() //OPTIMIZATION: fine-grain locking for maxKnownEpoch and maxKnownEpoch
  if epoch > mpx.maxKnownEpoch {
    mpx.maxKnownEpoch = epoch
  }
}

func (mpx *MultiPaxos) refreshEpoch() {
  mpx.mu.Lock()
  defer mpx.mu.Unlock() //OPTIMIZATION: fine-grain locking for epoch and maxKnownEpoch
  l := len(mpx.peers)
  // generate unique epoch higher than any epoch we have seen so far
  mpx.epoch := ((mpx.maxKnownEpoch/l + 1) * l) + mpx.me
  mpx.maxKnownEpoch = mpx.epoch // not strictly necessary, but maintains the implied invariant of maxKnownEpoch
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
  mpx.mu.Lock()
  defer mpx.mu.Unlock() //OPTIMIZATION: lock for presence array instead of global lock
  highestLivingID := mpx.me
  for peerID, presence := range mpx.presence {
    if presence == Alive || presence == Missing {
      if peerID > highestLivingID {
        highestLivingID = peerID
      }
    }
  }
  return highestLivingID
}

/*
Called when this server starts considering itself a leader
*/
func (mpx *MultiPaxos) actAsLeader() {
  mpx.mu.Lock()
  mpx.actingAsLeader = true
  seq := mpx.localMin //TODO: what sequence number should we prepareEpoch? localMin? localMax?
  mpx.mu.Unlock()
  //OPTIMIZATION: fine-grain locking for actingAsLeader and localMin...?
  mpx.prepareEpochPhase(seq)
}

/*
Called when this server no longer considers itself a leader
*/
func (mpx *MultiPaxos) relinquishLeadership() {
  mpx.mu.Lock()
  defer mpx.mu.Unlock()
  //OPTIMIZATION: fine-grain lock for actingAsLeader...?
  mpx.actingAsLeader = false
  //TODO: handle/respond to in-progress requests correctly
}

// -- Summoners (lazy instantiators) --

func (mpx *MultiPaxos) summonProposer(seq int) *Proposer {
  mpx.mu.Lock()
  defer mpx.mu.Unlock()
  //OPTIMIZATION: lock for proposers map instead of global lock
  proposer, exists := mpx.proposers[seq]
  if !exists {
    proposer = &Proposer{}
    mpx.proposers[seq] = proposer
  }
  return proposer.(*Proposer)
}

func (mpx *MultiPaxos) summonAcceptor(seq int) *Acceptor {
  mpx.mu.Lock()
  defer mpx.mu.Unlock()
  //OPTIMIZATION: lock for acceptors map instead of global lock
  acceptor, exists := mpx.acceptors[seq]
  if !exists {
    acceptor = &Acceptor{}
    //TODO: initialize properly (prepare with epoch as round number if seq > promised epoch sequence start)
    //TODO: might need to initialize from disk
    mpx.acceptors = acceptor
  }
  return acceptor.(*Acceptor)
}

func (mpx *MultiPaxos) summonLearner(seq int) *Learner {
  mpx.mu.Lock()
  defer mpx.mu.Unlock()
  //OPTIMIZATION: lock for learners map instead of global lock
  learner, exists := mpx.learners[seq]
  if !exists {
    learner = &Learner{Decided: false}
    mpx.learners[seq] = learner
  }
  return learner.(*Learner)
}

/*
Processes the current state of the PB information.
*/
func (mpx *MultiPaxos) processPiggyBack(piggyBack PiggyBack){
  mpx.mu.Lock()
  defer mpx.mu.Unlock() //OPTIMIZATION: fine-grain locking for maxKnownMins and map lock for mins
  if piggyBack.LocalMin > mpx.mins[piggyBack.Me] {
    mpx.mins[piggyBack.Me] = piggyBack.LocalMin
  }
  if piggyBack.MaxKnownMin > mpx.maxKnownMin {
    mpx.maxKnownMin = mpx.MaxKnownMin
  }
  min := mpx.GlobalMin()
  mpx.forgetUntil(mpx.acceptors, min)
}

// -- Garbage collection --
// Deletes anything within the paxos map (e.g. proposers, acceptors, learners)
// from a sequence <= the threshold
// No server will need this information in the future
func (mpx *MultiPaxos) forgetUntil(pxMap map[int]interface{}, threshold int) {
  mpx.mu.Lock()
  defer mpx.mu.Unlock() //OPTIMIZATION: map lock for pxMap
  for s, _ := range pxMap {
    if s <= threshold {
      delete(pxMap, s)
    }
  }
}

// -----------

// Constructor

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  //TODO: implement this
}
