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

/*
Catches up from the localMin to the highest local min known to this server.
Remember, only local mins guarantee that a decision has been made, so to guarantee correctness,
its better to just catch up to the maxKnownMin (largest known localMin).
Make sure mpx.maxKnownMin  and mpx.localMin has already been updated (process PB).
*/
func (mpx *MultiPaxos) catchUp(){
  seq := mpx.localMin
  for !mpx.dead{
    for seq <= mpx.maxKnownMin{ //while not caught up
      //TODO: OPTIMIZATION: we could query all the learners first before doing a "normal" px prepare.
      decided, decidedVal := mpx.Status(seq)
      if !decided{
        decidedVal := mpx.prepare(seq , INFINITY) //TODO: choose globally highest round-number, change name? weird that the prepare func. returns the decision. propose returns decision
        if decidedVal == nil{
          panic("If we are querying a value that was already decide, the val should be some OP not NIL!")
        }
        decideArgs := DecideArgs{Seq: seq, V: decidedVal, PB: nil}
        mpx.sendDecide(mpx.me, decidedVal, &decideArgs, decideReply{}) //TODO: should we send to everyone?
        acceptArgs := AcceptArgs{Seq: seq, V: decidedVal, PB: nil}
        mpx.sendAccept(mpx.me, decidedVal, &decideArgs, decideReply{})
        //make decision Persistent
        mpx.makeAcceptorsPersistent() //TODO: we want new accepted value to persist.
        mpx.makelearnersPersistent() //TODO: we want new decided value to persist.
        seq += 1
      }
    }
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
func (mpx *MultiPaxos) acceptMajority(seq int, v interface{}) bool {
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

func (mpx *MultiPaxos) decideAll(seq int, v interface{}) {
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

// ----------------

// Internal methods

func (mpx *MultiPaxos) isMajority(x int) bool {
  return x >= (len(mpx.peers)/2) + 1 // integer division == math.Floor()
}

func (mpx *MultiPaxos) preparedPropose() {
  //TODO: implement this
}

/*
Periodic tick function
*/
func (mpx *MultiPaxos) tick() {
  //TODO:
  //   ping all servers
  //   keep track of highest local min we hear // highest local mins piggy-backed in ping responses
  //   if a server has not responded to our pings for longer than twice the ping interval:
  //       consider them dead
  //   if I have the largest id amongst servers that I consider living:
  //       act as new leader (increment epoch/round number)
  //   else:
  //       catch_up (done at the rrkv level)
}

/*
Called when this server starts considering itself a leader
*/
func (mpx *MultiPaxos) actAsLeader() {
  mpx.actingAsLeader = true
  //TODO: send prepare_epoch
}

/*
Called when this server no longer considers itself a leader
*/
func (mpx *MultiPaxos) relinquishLeadership() {
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
func (mpx *MultiPaxos) processPB(pb PiggyBack){
  //TODO: locks
  mpx.mins[pb.Me] = pb.LocalMin
  min := mpx.GlobalMin()
  if pb.MaxKnownMin > mpx.maxKnownMin{
    mpx.maxKnownMin = mpx.MaxKnownMin
  }

  for s, _ := range px.acceptors {
    if s < min {
      delete(px.acceptors, s)
    }
  }
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
