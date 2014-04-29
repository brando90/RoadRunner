package shardmaster

import (
  "net"
  "fmt"
  "net/rpc"
  "log"
  "paxos"
  "sync"
  "os"
  "syscall"
  "encoding/gob"
  "math/rand"
  crypto_rand "crypto/rand"
  "math/big"
  "time"
  "errors"
)

const Debug=0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf("\t"+format, a...)
  }
  return
}

type ShardMaster struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  configs []Config // indexed by config num
  ap int // application point
}

type Op struct {
  ID int64
  Type string
  Shard int
  GID int64
  Servers []string
  Num int
}

func generateUniqueID() int64 {
  max := big.NewInt(int64(1) << 62)
  bigx, _ := crypto_rand.Int(crypto_rand.Reader, max)
  x := bigx.Int64()
  return x
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
  op := Op{Type: JOIN, GID: args.GID, Servers: args.Servers} // prepare join op
  op.ID = generateUniqueID()
  _, err := sm.process(op)
  return err
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
  op := Op{Type: LEAVE, GID: args.GID} // prepare leave op
  op.ID = generateUniqueID()
  _, err := sm.process(op)
  return err
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
  op := Op{Type: MOVE, Shard: args.Shard, GID: args.GID} // prepare move args
  op.ID = generateUniqueID()
  _, err := sm.process(op)
  return err
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
  op := Op{Type: QUERY, Num: args.Num} // prepare query args
  op.ID = generateUniqueID()
  config, err := sm.process(op)
  reply.Config = config
  return err
}

func (sm *ShardMaster) process(op Op) (Config, error) {
  sm.mu.Lock()
  defer sm.mu.Unlock()
  seq, err := sm.commit(op) // push query op onto paxos log
  if err != nil {
    return Config{}, err
  }
  return sm.interpret(seq, op) // apply paxos log up to (and including) seq
}

func (sm *ShardMaster) died() error {
  return errors.New(fmt.Sprintf("Shardmaster %d died.", sm.me))
}

func (sm *ShardMaster) decisionWait(seq int) (Op, error) {
  to := 10 * time.Millisecond
  for !sm.dead {
    decided, oper := sm.px.Status(seq)
    if decided {
      op := oper.(Op)
      return op, nil
    }
    time.Sleep(to)
    if to < 10 * time.Second {
      to *= 2
    }
  }
  return Op{}, sm.died()
}

func (sm *ShardMaster) interpret(until int, op Op) (Config, error) {
  start := sm.ap + 1
  for seq := start; seq < until; seq++ {
    err := sm.execute(seq)
    if err != nil {
      return Config{}, err
    }
  }
  newConfig, err := sm.apply(op)
  sm.px.Done(until)
  sm.ap = until
  return newConfig, err
}

func (sm *ShardMaster) execute(seq int) error {
  sm.px.Start(seq, Op{Type: NOP})
  op, errd := sm.decisionWait(seq)
  if errd != nil {
    return errd
  }
  _, erra := sm.apply(op)
  return erra
}

func (sm *ShardMaster) commit(op Op) (int, error) {
  for !sm.dead {
    seq := sm.px.Max() + 1
    if decided, _ := sm.px.Status(seq); !decided{
      sm.px.Start(seq, op)
      decidedOp, err := sm.decisionWait(seq)
      if err != nil {
        return 0, err
      }else if op.ID == decidedOp.ID { // checks if our operation was chosen
        return seq, nil
      }
    }
  }
  return 0, sm.died()
}

func (sm *ShardMaster) apply(op Op) (Config, error) {
  switch op.Type {
  case JOIN:
    sm.applyJoin(op.GID, op.Servers)
  case LEAVE:
    sm.applyLeave(op.GID)
  case MOVE:
    sm.applyMove(op.Shard, op.GID)
  case QUERY:
    return sm.query(op.Num)
  case NOP:
    // do nothing
  default:
    fmt.Println("Op type not recognized:")
    panic(op.Type)
  }
  return Config{}, nil
}

func (sm *ShardMaster) applyJoin(gid int64, servers []string) {
  newConfig := sm.currentConfig().copy()
  newConfig.Num += 1

  newConfig.Groups[gid] = servers
  newConfig.balance()
  sm.append(newConfig)
}

func (sm *ShardMaster) applyLeave(gid int64) {
  newConfig := sm.currentConfig().copy()
  newConfig.Num += 1

  delete(newConfig.Groups, gid)
  for shard, g := range newConfig.Shards {
    if g == gid { // if shard belongs to leaving group
      newConfig.Shards[shard] = 0 // unassign shard
    } 
  }
  newConfig.balance()
  sm.append(newConfig)
}

func (sm *ShardMaster) applyMove(shard int, gid int64) {
  newConfig := sm.currentConfig().copy()
  newConfig.Num += 1

  newConfig.Shards[shard] = gid
  sm.append(newConfig)
}

func (sm *ShardMaster) query(num int) (Config, error) {
  if num == -1 || num >= len(sm.configs) {
    return *sm.currentConfig(), nil
  }else if 0 <= num && num < len(sm.configs) {
    return sm.configs[num], nil
  }else {
    return Config{}, errors.New(fmt.Sprintf("No config numbered %d", num))
  }
}

func (sm *ShardMaster) append(newConfig *Config) {
  sm.configs = append(sm.configs, *newConfig)
}

func (sm *ShardMaster) currentConfig() *Config {
  return &sm.configs[len(sm.configs) - 1]
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
  sm.dead = true
  sm.l.Close()
  sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *ShardMaster {
  gob.Register(Op{})

  sm := new(ShardMaster)
  sm.me = me

  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = map[int64][]string{}

  sm.ap = -1

  rpcs := rpc.NewServer()
  rpcs.Register(sm)

  sm.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  sm.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for sm.dead == false {
      conn, err := sm.l.Accept()
      if err == nil && sm.dead == false {
        if sm.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if sm.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && sm.dead == false {
        fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
        sm.Kill()
      }
    }
  }()

  return sm
}
