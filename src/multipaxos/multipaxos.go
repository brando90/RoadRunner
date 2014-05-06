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

  localMin int
  localMax int

  proposers map[int]*Proposer
  acceptors map[int]*Acceptor
  learners  map[int]*Learner

  mins map[int]int // for calculating GlobalMin()

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
    return nil, ""
  }else {
    return Err{Msg: NotLeader}, //TODO: return servername of who we think is the leader
  }
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (mpx *MultiPaxos) Done(seq int) {
  //TODO: locking
  if seq > mpx.localMin {
    mpx.localMin = seq // update local min
  }
  mpx.forgetUntil(mpx.proposers, seq)
  mpx.forgetUntil(mpx.learners, seq)
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (mpx *MultiPaxos) Max() int {
  //TODO: implement this
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
func (mpx *MultiPaxos) GlobalMin() int {
  //TODO: locking (is using a pointer safe here? what if a min value gets updated mid-execution)
  var globalMin *int
  for _, min := range mpx.mins {
    if globalMin == nil {
      globalMin = &min
    }else if min < *globalMin {
      globalMin = &min
    }
  }
  return *globalMin
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
  if seq < mpx.GlobalMin() { //TODO: is this check necessary
    return false, nil
  }
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

// ------------

// RPC handlers

//TODO: define rpc handlers

// ----------------

// Internal methods

func (mpx *MultiPaxos) tick() {
  //TODO:
  //   ping all servers
  //   keep track of highest local min we hear // highest local mins piggy-backed in ping responses
  //   if a server has not responded to our pings for longer than twice the ping interval:
  //       consider them dead
  //   if I have the largest id amongst servers that I consider living:
  //       act as new leader (increment epoch/round number)
  //   else:
  //       catch_up
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