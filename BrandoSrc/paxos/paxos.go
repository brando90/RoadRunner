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
// px.Maxv() int -- highest instance seq known, or -1
// px.Minv() int -- instances before this seq have been forgotten
//

//import "helper_structs"
import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
//import "sync"
import "fmt"
import "math/rand"
import "time"
import "math"

const PROCESSED = "PROCESSED"
const NOT_PROCSSED = "NOT_PROCSSED"

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
  px.mu.Lock()
  if seq > px.Maxv{
    px.Maxv = seq
  }
  px.mu.Unlock()
  go px.StartPaxosInstance(seq, v)
}

func (px *Paxos) StartPaxosInstance(seq int, v interface{}){
  decided := false
  n := px.me
  for !decided{
    n+=len(px.peers)
    prepare_args := &PrepareArgs{seq, n}

    prepare_ok_count := 0 
    value_to_propose := v
    highest_n_a := -1
    for peer_index := 0;  peer_index < len(px.peers); peer_index++{
      prepare_reply := &PrepareReply{OK: false}

      px.sendPrepareToPeer(peer_index, prepare_args, prepare_reply)
      //after this line we will know if ONE prepare msg was succesfully sent
      if prepare_reply.OK{
        prepare_ok_count++
        if prepare_reply.N_a > highest_n_a{
          highest_n_a = prepare_reply.N_a
          value_to_propose = prepare_reply.V_a
        }
      }
    }

    if prepare_ok_count > len(px.peers)/2{
      accept_ok_count := 0
      accept_args := &AcceptArgs{seq, n, value_to_propose} // TODO
      //accept_args := &AcceptArgs{seq, highest_n_a, value_to_propose}
      for peer_index := 0; peer_index < len(px.peers); peer_index++{
        accept_reply := &AcceptReply{OK: false, Seq_inst: -1, N: -1}
        px.sendAcceptToPeer(peer_index, accept_args, accept_reply)
        if accept_reply.OK{
          accept_ok_count++
        }
      }

      if accept_ok_count > len(px.peers)/2{
        decide_args := &DecideArgs{seq, value_to_propose}
        for peer_index := 0; peer_index < len(px.peers); peer_index++{
          decide_reply := &DecideReply{OK: false}
          px.sendDecideToPeer(peer_index, decide_args, decide_reply)
        }//send decide to peer
      }//send decide to all
    }// if majority accepted

    time.Sleep(time.Duration(rand.Int() % 30) * time.Millisecond)
    decided, _ = px.Status(seq)
  }// while not decided loop
}

func (px *Paxos) sendDecideToPeer(peer_index int, args *DecideArgs, reply *DecideReply){
  if px.me == peer_index{
    px.Decide(args, reply)
  }else{
    call(px.peers[peer_index], "Paxos.Decide", args, reply)
  }
}

func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error{
  px.mu.Lock()
  defer px.mu.Unlock()
  if args.Seq_inst > px.Maxv{
    px.Maxv = args.Seq_inst
  }
  //fmt.Println("Srv:",px.me," Received Decide: Seq", args.Seq_inst, " ; V:", args.V)
  px.decidedValues[args.Seq_inst] = args.V
  px.decidedInst[args.Seq_inst] = true
  reply.OK = true
  return nil
}

// func (px *Paxos) mod(i, n int) int{
//   return int(Mod( float64(i), float64( len(n) ) ))
// }
// func (px *Paxos) getPeersToProccess() map[int]string{
//   peers_to_process:= make(map[int]bool)
//   for i := 0; i < len(px.peers); i++{
//     peers_to_process[i] = NOT_PROCSSED
//   }
//   return peers_to_process
// }

func (px *Paxos) sendAcceptToPeer(peer_index int, args *AcceptArgs, reply *AcceptReply){
  if px.me == peer_index{
    px.Accept(args, reply)
  }else{
    call(px.peers[peer_index], "Paxos.Accept", args, reply)
  }
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error{
  px.mu.Lock()
  defer px.mu.Unlock()
  if args.Seq_inst > px.Maxv{
    px.Maxv = args.Seq_inst
  }
  //fmt.Println("Srv:",px.me," Received Accept: Seq", args.Seq_inst, " ; N:",args.N, "; V:", args.V)
  current_acceptor, ok := px.acceptorInst[args.Seq_inst]
  defer func(){px.acceptorInst[args.Seq_inst] = current_acceptor}()
  if !ok{
    current_acceptor = Acceptor{-1, -1, nil}
  }
  if args.N >= current_acceptor.N_p{
    current_acceptor.N_p = args.N
    current_acceptor.N_a = args.N
    current_acceptor.V_a = args.V
    reply.OK = true
    //reply.Seq_inst = args.Seq_inst
    reply.N = args.N
  }else{
    reply.OK = false
    //reply.Seq_inst = args.Seq_inst
    reply.N = -1
  }
  //fmt.Println("======= Srv:",px.me," Received Accept: Seq", args.Seq_inst, " ; N:",args.N, "current_acceptor", current_acceptor)
  //fmt.Println("Srv:",px.me," Received Accept: Seq", args.Seq_inst, " ; N:",args.N ,"AcceptReplyOk: ", reply.OK)
  return nil
}

//simply calls prepare(n) for peer with index peer_index
//handles special case when peer_index == me and rpc calls otherwise
func (px *Paxos) sendPrepareToPeer(peer_index int, args *PrepareArgs, reply *PrepareReply){
  if px.me == peer_index{
    px.Prepare(args, reply)
  }else{
    call(px.peers[peer_index], "Paxos.Prepare", args, reply)
  }
}

//Promise not accepting proposals numbered less than n
//send highest numbered proposal accepted with number less than n (promise)
func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error{
  px.mu.Lock()
  defer px.mu.Unlock()
  if args.Seq_inst > px.Maxv{
    px.Maxv = args.Seq_inst
  }
  //fmt.Println("Srv:",px.me," Received Prepare: Seq", args.Seq_inst, " ; N:", args.N)

  current_acceptor, ok := px.acceptorInst[args.Seq_inst]
  defer func(){px.acceptorInst[args.Seq_inst] = current_acceptor}()
  //fmt.Println("OK? : ", ok)
  if !ok{
    current_acceptor = Acceptor{-1, -1, nil}
  }
  if args.N > current_acceptor.N_p {
    //reply prepare_ok(n_a, v_a)
    //reply.Seq_inst = args.Seq_inst
    current_acceptor.N_p = args.N
    reply.OK = true
    reply.N_a = current_acceptor.N_a
    reply.V_a = current_acceptor.V_a //TODO check
  }else{
    //reject anything that is less than promise
    //reply.Seq_inst = args.Seq_inst
    reply.OK = false
    reply.N_a = -1
    reply.V_a = nil
  }
  //fmt.Println("--->Srv:",px.me," Received Prepare: Seq", args.Seq_inst, " ; N:",args.N, "current_acceptor", current_acceptor)

  return nil
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  if px.Minv < seq {
    px.Minv = seq
  }
  // args := &DoneArgs{px.me, seq}
  // reply := &DoneReply{}
  // for i := 0; i < len(px.peers); i++ {
  //   call(px.peers[i], "Paxos.ProcessDone", args, reply)
  // }
  px.mu.Lock()
  for i := 0; i < len(px.decidedInst); i++{
    if i <= px.Minv {
      delete(px.acceptorInst, i) //TODO fix. Done should not get rid of acceptors. It should be deleted at Min() i.e. at Globalmin, when we know when no-one needs any of our acceptors.
      delete(px.decidedValues, i)
      delete(px.decidedInst, i)
    }
  }
  px.mu.Unlock()
}

// func (px *Paxos) ProcessDone(args *DoneArgs, reply *DoneReply) error{
//   //fmt.Println("ProcessDone")
//   min := px.Mivn()

//   if min >= 0 {
//     px.mu.Lock()
//     for i := 0; i < len(px.decidedInst); i++{
//       if i < min {
//         delete(px.acceptorInst, i)
//         delete(px.decidedValues, i)
//         delete(px.decidedInst, i)
//       }
//     }//loop
//     px.mu.Unlock()
//   }
//   return nil
// }

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  return px.Maxv
}

//
// Min() returns the smallest (global) sequence number ever
// called by done by the system (of processes). 
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
// missed -- the other peers therefor cannot forget these
// instances.
// 
func (px *Paxos) Min() int {
  min := int(math.MaxInt32)
  minArgs := &MinArgs{}
  for peer_index := 0; peer_index < len(px.peers); peer_index++{
    reply := &MinReply{}
    call(px.peers[peer_index], "Paxos.GetMin", minArgs, reply)
    current := reply.Min
    if current < min {
      min = current
    }
  }
  //fmt.Println("MinReturn")
  return min + 1
}

func (px *Paxos) GetMin(args *MinArgs, reply *MinReply) error{
   reply.Min = px.Minv
   return nil
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  //fmt.Println("Status")
  // if seq < px.GetMin() {
  //   return false, nil
  // }
  px.mu.Lock()
  decided, _ := px.decidedInst[seq]
  val, _ := px.decidedValues[seq]
  px.mu.Unlock()
  return decided, val
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
  px.acceptorInst = make(map[int]Acceptor)
  px.decidedValues = make(map[int]interface{})
  px.decidedInst = make(map[int]bool)
  px.Maxv = -1
  px.Minv = -1
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

        // decide_args := &DecideArgs{seq, value_to_propose}
        // decide_reply := &DecideReply{} //TODO
        // succesCount := 0
        // peer_index := 0
        // peers_to_process := px.getPeersToProccess()
        // attempts := 0
        // for len(peers_to_process) > 0{
        //   v, ok := peers_to_process[peer_index]
        //   if !ok{
        //     v = PROCESSED
        //     peer_index = px.mod(peer_index + 1, len(peers))
        //     continue
        //   }
        //   attempts = 0
        //   for true{
        //     px.sendDecideToPeer(peer_index, decide_args, decide_reply)
        //     if decide_reply.OK{
        //       Delete(peers_to_process, peer_index)
        //       break
        //     }
        //     if attempts > 10{
        //       peer_index = px.mod(peer_index + 1, len(peers))
        //       break
        //     }
        //     //sleep
        //     time.Sleep(time.Duration(rand.Int() % 30) * time.Millisecond)
        //   }//keep sending to individual server
        // }// keep sending decides