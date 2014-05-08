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
  //TODO: mutexes
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int

  localMin int //one more than the lowest sequence number that we have called Done() on
  knownMax int //TODO: what is this for?
  maxKnownMin int //needed to to know up to watch seq to process in the log.

  proposers map[int]*Proposer
  acceptors map[int]*Acceptor
  learners  map[int]*Learner

  mins []int // for calculating GlobalMin()

  presence []Presence
  actingAsLeader bool
  epoch int
}

// ---

// API

/*
Tries to send accept
If this server considers itself a leader, send accepts
Otherwise, DO NOT send accepts. Relay potential leader information to client instead
*/
func (mpx *MultiPaxos) Push(seq int, v interface{}) (Err, ServerName) {
  if actingAsLeader {
    //TODO: send accept
    //TODO: sending accepts should be concurrent
    go mpx.preparedPropose()
    return nil, ""
  }else {
    return Err{Msg: NotLeader}, //TODO: return servername of who we think is the leader
  }
}

/*TODO: write a better comment
  The application on this machine is done with
  all instances <= seq.
  If this machine were to die, then it wouldn't need to know about operations commited at or bellow seq
  (since it has already applied all the Ops to its state for <=seq).
*/
func (mpx *MultiPaxos) Done(seq int) {
  //TODO: locking
  if seq >= mpx.localMin { // done up to or beyond our local min
    mpx.localMin = seq + 1 // update local min
  }
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
  //TODO: update knownMax in other methods where ever relevant
  return mpx.knownMax
}

/*
Returns the lowest known min.
*/
func (mpx *MultiPaxos) GlobalMin() int {
  //TODO: locking
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
  //TODO: locking
  /*TODO: reevaluate this commented code. pretty sure its not needed (this case should never happen in non-byzantine settings... i think)
  if seq < mpx.GlobalMin() {
    return false, nil
  }
  */
  learner := mpx.summonLearner(seq)
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

func (mpx *MultiPaxos) makeAcceptorsPersistent(){
  //TODO
}

func (mpx *MultiPaxos) makeLearnersPersistent(){
  //TODO
}

// -----

// RPC's
//TODO: piggy-back values in Paxos RPC's

// -- Prepare Phase --

/*
Sends prepare epoch for sequences >= seq to all acceptors
*/
func (mpx *MultiPaxos) prepareEpochAll(seq int) {
  //TODO: implement this
  //if we get any rejects from acceptors who have accepted for round number E > e this leader's epoch
  // update this leader's epoch to E+1
}

/*
Sends prepare epoch for sequence >= seq to one server
*/
func (mpx *MultiPaxos) sendPrepareEpoch(peerID ServerID, args *PrepareEpochArgs, reply *PrepareEpochReply) {
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
func (mpx *MultiPaxos) acceptPhase(seq int, v interface{}) bool {
  acceptOKs := 0
  for _, peer := range mpx.peers {
    acceptArgs := AcceptArgs{Seq: seq, N: mpx.epoch, V: v}
    //TODO: piggy-backing
    acceptReply := AcceptReply{}
    replyReceived := mpx.sendAccept(peer, &acceptArgs, &acceptReply)
    if replyReceived {
      if acceptReply.OK {
        acceptOKs += 1
      }
      //TODO: do we need to keep track of max proposal number? (as in 3a)
    }
  }
  return mpx.isMajority(acceptOKs)
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
    decideArgs := DecideArgs{Seq: seq, V: v}
    //TODO: piggy-backing
    decideReply := DecideReply{}
    mpx.sendDecide(peer, &decideArgs, &decideReply)
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

func (mpx *MultiPaxos) ping(peerID ServerID, rollcall chan int, done chan bool) {
  if peerID != mpx.me {
    args := PingArgs{}
    reply := PingReply{}
    replyReceived := call(mpx.peers[peerID], "MultiPaxos.PingHandler", &args, &reply)
    if replyReceived {
      mpx.processPiggyBack(reply.PiggyBack)
      mpx.presence[peerID] = Alive
    }else {
      if mpx.presence[peerID] == Alive {
        mpx.presence[peerID] = Missing
      }
      if mpx.presence[peerID] == Missing {
        mpx.presence[peerID] = Dead
      }
    }
  }
  // account for pinged server
  count := <- rollcall
  count += 1
  rollcall <- count
  if count == len(mpx.peers) { // accounted for all servers
    done <- true
  }
}

// ------------

// RPC handlers

func (mpx *MultiPaxos) PrepareEpochHandler(args *PrepareEpochArgs, reply *PrepareEpochReply) error {
  //TODO: locking
  //TODO: reply with a response map (filled with prepare responses for all existing acceptors at sequence >= args seq)
}

func (mpx *MultiPaxos) AcceptHandler(args *AcceptArgs, reply *AcceptReply) error {
  //TODO: locking
  //TODO: process piggy-backed info
  //TODO: update knownMax if necessary
  acceptor := mpx.summonAcceptor(args.Seq)
  if args.N >= acceptor.N_p {
    acceptor.N_p = n
    acceptor.N_a = n
    acceptor.V_a = v
    reply.OK = true
  }else {
    reply.OK = false
  }
  return nil
}

func (mpx *MultiPaxos) DecideHandler(args *DecideArgs, reply *DecideReply) error {
  //TODO: locking
  //TODO: process piggy-backed info
  //TODO: update knownMax if necessary
  learner := mpx.summonLearner(args.Seq)
  learner.Decided = true
  learner.V = args.V
  return nil
}

func (mpx *MultiPaxos) PingHandler(args *PingArgs, reply *PingReply) error {
  //TODO: locking
  reply.PiggyBack = PiggyBack{Me: mpx.me, LocalMin: mpx.localMin, MaxKnownMin: mpx.maxKnownMin}
  return nil
}

// ----------------

// Internal methods

func (mpx *MultiPaxos) isMajority(x int) bool {
  return x >= (len(mpx.peers)/2) + 1 // integer division == math.Floor()
}

func (mpx *MultiPaxos) leaderPropose(seq int, v interface{}) {
  proposer := mpx.summonProposer(seq)
  undecided:
  for !mpx.dead {
    //TODO: if we received any rejects from acceptors (since it has accepted an epoch number E > e, this leader's current epoch number),
    // then this leader should prepareEpoch(e, seq) AFTER e = E+1
  }
}

/*
Periodic tick function
Ping all servers
Once we have accounted for all servers, run the leader election protocol
If this server considers itself a leader, start acting as a leader
*/
func (mpx *MultiPaxos) tick() {
  rollcall := make(chan int, 1)
  rollcall <- 0
  done := make(chan bool, 1)
  for _, peer := range mpx.peers {
    go ping(peer, rollcall, done)
  }
  <- done
  // leader decision protocol
  //TODO: refactor below into helper methods
  highestID := mpx.me
  for peerID, presence := range mpx.presence {
    if presence == Alive || presence == Missing {
      if peerID > highestID {
        highestID = peerID
      }
    }
  }
  if highestID == mpx.me {
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
Called when this server starts considering itself a leader
*/
func (mpx *MultiPaxos) actAsLeader() {
  //TODO: locking
  mpx.actingAsLeader = true
  mpx.prepareEpochAll(e, mpx.localMin) //TODO: what sequence number should we prepareEpoch
}

/*
Called when this server no longer considers itself a leader
*/
func (mpx *MultiPaxos) relinquishLeadership() {
  //TODO: locking
  mpx.actingAsLeader = false
  //TODO: handle/respond to in-progress requests correctly
}

// -- Summoners (lazy instantiators) --

func (mpx *MultiPaxos) summonProposer(seq int) *Proposer {
  //TODO: locking
  proposer, exists := mpx.proposers[seq]
  if !exists {
    proposer = &Proposer{} //TODO: initialize properly
    mpx.proposers[seq] = proposer
  }
  return proposer
}

func (mpx *MultiPaxos) summonAcceptor(seq int) *Acceptor {
  //TODO: locking
  acceptor, exists := mpx.acceptors[seq]
  if !exists {
    acceptor = &Acceptor{}
    //TODO: initialize properly (prepare with epoch as round number if seq > promised epoch sequence start)
    //TODO: might need to initialize from disk
    mpx.acceptors = acceptor
  }
  return acceptor
}

func (mpx *MultiPaxos) summonLearner(seq int) *Learner {
  //TODO: locking
  learner, exists := mpx.learners[seq]
  if !exists {
    learner = &Learner{} //TODO: initialize properly
    mpx.learners[seq] = learner
  }
  return learner
}

/*
Processes the current state of the PB information.
*/
func (mpx *MultiPaxos) processPiggyBack(piggyBack PiggyBack){
  //TODO: locks
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
