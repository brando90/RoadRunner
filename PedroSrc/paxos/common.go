package paxos

type Proposer struct {
	MaxN int
}

type Acceptor struct {
	N_P int
	N_A int
	V_A interface{}
}

type Learner struct {
	Decided bool
	V interface{}
}

type PrepareArgs struct {
	Seq int
	N int
	PBDone PBDone
}

type PrepareReply struct {
	OK bool
	N_A int
	V_A interface{}
	MaxN int
}

type AcceptArgs struct {
	Seq int
	N int
	V interface{}
	PBDone PBDone
}

type AcceptReply struct {
	OK bool
	N int
	MaxN int
}

type DecideArgs struct {
	Seq int
	V interface{}
	PBDone PBDone
}

type DecideReply struct {
	// Empty
}

type PBDone struct {
	Me int
	MinSeq int
}

/* these are now piggybacked in agreement messages
type DoneArgs struct {
	// Empty
}

type DoneReply struct {
	Done int
}
*/