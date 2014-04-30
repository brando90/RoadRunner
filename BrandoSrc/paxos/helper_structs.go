package paxos

import "sync"
import "net"

type Acceptor struct{
  N_p int //highest prepare seem
  N_a int //highest_prepare
  V_a interface{} //highest_accept_seq
}

type PrepareArgs struct{
  Seq_inst int
  N int
}

type PrepareReply struct{
  Seq_inst int
  OK bool
  N_a int
  V_a interface{}
}

type AcceptArgs struct{
  Seq_inst int
  N int
  V interface{}
}

type AcceptReply struct{
  Seq_inst int
  OK bool
  N int
}

type DecideArgs struct{
  Seq_inst int
  V interface{}
}

type DecideReply struct{
  OK bool
}

type DoneArgs struct{
  Peer_number int
  Seq_inst int
}

type DoneReply struct{
}

type MinArgs struct{
}

type MinReply struct{
	Min int
	//OK bool
}

type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]
  Minv int
  Maxv int
  acceptorInst map[int]Acceptor
  decidedValues map[int]interface{}
  decidedInst map[int]bool
}