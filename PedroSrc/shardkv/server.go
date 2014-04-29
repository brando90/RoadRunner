package shardkv
/*
TODO:
- check implementation of get/put/puthash
- check duplicate detection
- check shard xfer procedure
*/
import (
  "net"
  "fmt"
  "net/rpc"
  "log"
  "time"
  "paxos"
  "sync"
  "os"
  "syscall"
  "encoding/gob"
  "math/rand"
  "shardmaster"
  "strconv"
  "errors"
)

const Debug=0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
  }
  return
}

type Op struct {
  Type string
  Key string
  Value string
  DoHash bool
  Config shardmaster.Config
  Adoptees map[int]map[string]string
  AdopteeHistories map[int]map[int64]RequestResult
  ID ID
}


type ShardKV struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  sm *shardmaster.Clerk
  px *paxos.Paxos

  gid int64 // my replica group ID

  config shardmaster.Config
  shards [shardmaster.NShards]map[string]string
  isFrozen [shardmaster.NShards]bool
  reconfiguring bool
  shardHistories [shardmaster.NShards]map[int64]RequestResult // maps clients to their last requests (for each shard)
  lb int
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  op := Op{Type: Get, Key: args.Key, ID: args.ID}
  value, err, errx := kv.process(op)
  reply.Value = value
  reply.Err = err
  return errx
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  op := Op{Key: args.Key, Value: args.Value, ID: args.ID}
  if args.DoHash {
    op.Type = PutHash
  }else {
    op.Type = Put
  }
  oldValue, err, errx := kv.process(op)
  reply.PreviousValue = oldValue
  reply.Err = err
  return errx
}

func (kv *ShardKV) reconfigure(nextConfig shardmaster.Config) {
  kv.beginReconfig(nextConfig)
  adoptees, adopteeHistories := kv.adoptees(nextConfig)
  kv.endReconfig(nextConfig, adoptees, adopteeHistories)
}

func (kv *ShardKV) beginReconfig(nextConfig shardmaster.Config) {
  id := ID{ClientID: int64(kv.me)}
  op := Op{Type: BeginReconfig, Config: nextConfig, ID: id}
  kv.process(op)
}

func (kv *ShardKV) endReconfig(nextConfig shardmaster.Config, adoptees map[int]map[string]string, adopteeHistories map[int]map[int64]RequestResult) {
  id := ID{ClientID: int64(kv.me)}
  op := Op{Type: EndReconfig, Config: nextConfig, Adoptees: adoptees, AdopteeHistories: adopteeHistories, ID: id}
  kv.process(op)
}

func (kv *ShardKV) process(op Op) (string, Err, error) {
  seq, err := kv.commit(op) // push query op onto paxos log
  if err != nil {
    return "", "", err
  }
  return kv.interpret(seq, op) // apply paxos log up to (and including) seq
}

func (kv *ShardKV) commit(op Op) (int, error) {
  for !kv.dead {
    seq := kv.px.Max() + 1
    if decided, _ := kv.px.Status(seq); !decided {
      kv.px.Start(seq, op)
      decidedOp, err := kv.decisionWait(seq)
      if err != nil {
        return 0, err
      }else if op.ID == decidedOp.ID { // checks if our operation was chosen
        return seq, nil
      }
    }
  }
  return 0, kv.died()
}

func (kv *ShardKV) interpret(until int, op Op) (string, Err, error) {
  start := kv.lb + 1
  for seq := start; seq < until; seq++ {
    _, err := kv.execute(seq)
    if err != nil {
      return "", "", err
    }
  }
  value, err := kv.apply(op)
  kv.px.Done(until)
  kv.lb = until
  return value, err, nil
}

func (kv *ShardKV) execute(seq int) (Err, error) {
  kv.px.Start(seq, Op{Type: NOP})
  op, errd := kv.decisionWait(seq)
  if errd != nil {
    return "", errd
  }
  _, erra := kv.apply(op)
  return erra, nil
}

func (kv *ShardKV) apply(op Op) (string, Err) {
  if isDuplicate, previous := kv.duplicate(op); isDuplicate {
    return previous.Result, previous.Err
  }else {
    switch op.Type { // handle op by type
    case Get:
      value, err := kv.get(op.Key)
      kv.log(op, value, err) //TODO: should logging be abstracted to applying??
      return value, err
    case Put:
      err := kv.applyPut(op.Key, op.Value)
      kv.log(op, "", err)
      return "", err
    case PutHash:
      oldValue, err := kv.applyPutHash(op.Key, op.Value)
      kv.log(op, oldValue, err)
      return oldValue, err
    case BeginReconfig:
      kv.applyBeginReconfig(op.Config)
    case EndReconfig:
      kv.applyEndReconfig(op.Config, op.Adoptees, op.AdopteeHistories)
    case NOP:
      // do nothing
    default:
      fmt.Println("Op type not recognized:")
      panic(op.Type)
    }
    return "", OK
  }
}

func (kv *ShardKV) log(op Op, result string, err Err) {
  if err != ErrWrongGroup {
    kv.shardHistories[key2shard(op.Key)][op.ID.ClientID] = RequestResult{Op: op, Result: result, Err: err}
  }
}

func (kv *ShardKV) duplicate(op Op) (bool, RequestResult) {
  if op.Type == NOP { // ignore NOPs
    return false, RequestResult{}
  }else {
    shardHistory := kv.shardHistories[key2shard(op.Key)]
    if requestResult, exists := shardHistory[op.ID.ClientID]; exists {
      if op.ID.ReqNum <= requestResult.Op.ID.ReqNum { // duplicate detected!
        return true, requestResult
      }
    }
  }
  return false, RequestResult{}
}

func (kv *ShardKV) get(key string) (string, Err) {
  if kv.shouldService(key) {
    value, exists := kv.shards[key2shard(key)][key]
    if exists {
      return value, OK
    }else {
      return "", ErrNoKey
    }
  }else {
    return "", ErrWrongGroup   
  }
}

func (kv *ShardKV) applyPut(key string, value string) Err {
  if kv.shouldService(key) {
    kv.shard(key)[key] = value
    return OK
  }else {
    return ErrWrongGroup   
  }
}

func (kv *ShardKV) applyPutHash(key string, value string) (string, Err) {
  if kv.shouldService(key) {
    oldValue, _ := kv.shard(key)[key]
    kv.shard(key)[key] = strconv.Itoa(int(hash(oldValue + value)))
    return oldValue, OK
  }else {
    return "", ErrWrongGroup   
  }
}

func (kv *ShardKV) applyBeginReconfig(config shardmaster.Config) {
  if config.Num == kv.config.Num + 1 && !kv.reconfiguring {
    kv.reconfiguring = true
    for _, shardNum := range kv.disownedNums(config) {
      kv.isFrozen[shardNum] = true // freeze disowned shards
    }
  }
}

func (kv *ShardKV) applyEndReconfig(config shardmaster.Config, adoptedShards map[int]map[string]string, adoptedHistories map[int]map[int64]RequestResult) {
  if config.Num == kv.config.Num + 1 && kv.reconfiguring {
    for shardNum, shardHistory := range adoptedHistories {
      kv.shardHistories[shardNum] = copyHistory(shardHistory) // copy over adopoted shard history
    }
    for shardNum, shard := range adoptedShards {
      kv.shards[shardNum] = copyShard(shard) // copy over adopted shard
      kv.isFrozen[shardNum] = false // unfreeze adopted shard
    }
    kv.config = config // transition to new config
    kv.reconfiguring = false
  }
}

func (kv *ShardKV) shouldService(key string) bool {
  return !kv.isFrozen[key2shard(key)]
}

func (kv *ShardKV) decisionWait(seq int) (Op, error) {
  to := 10 * time.Millisecond
  for !kv.dead {
    decided, oper := kv.px.Status(seq)
    if decided {
      op := oper.(Op)
      return op, nil
    }
    time.Sleep(to)
    if to < 10 * time.Second {
      to *= 2
    }
  }
  return Op{}, kv.died()
}

func (kv *ShardKV) died() error {
  return errors.New(fmt.Sprintf("ShardKV %d died.", kv.me))
}

func (kv *ShardKV) shard(key string) map[string]string {
  return kv.shards[key2shard(key)]
}

func (kv *ShardKV) adopteeNums(nextConfig shardmaster.Config) []int {
  adopted := []int{}
  for shardNum, gid := range nextConfig.Shards {
    if gid == kv.gid {
      if kv.config.Shards[shardNum] != kv.gid {
        adopted = append(adopted, shardNum)
      }
    }
  }
  return adopted
}

func (kv *ShardKV) disownedNums(nextConfig shardmaster.Config) []int {
  disowned := []int{}
  for shardNum, gid := range kv.config.Shards {
    if gid == kv.gid { // is assigned to this group @ current config
      if nextConfig.Shards[shardNum] != kv.gid { // will no longer be assigned to this group @ next config
        disowned = append(disowned, shardNum)
      }
    }
  }
  return disowned
}

func (kv *ShardKV) adoptees(nextConfig shardmaster.Config) (map[int]map[string]string, map[int]map[int64]RequestResult) {
  adoptees := make(map[int]map[string]string)
  adopteeHistories := make(map[int]map[int64]RequestResult)
  for _, shardNum := range kv.adopteeNums(nextConfig) {
    adoptees[shardNum], adopteeHistories[shardNum] = kv.adoptee(shardNum, nextConfig)
  }
  return adoptees, adopteeHistories
}

func (kv *ShardKV) adoptee(shardNum int, nextConfig shardmaster.Config) (map[string]string, map[int64]RequestResult) {
  disowningFamily := kv.config.Shards[shardNum]
  if disowningFamily == 0 { // orphan
    kv.shardHistories[shardNum] = make(map[int64]RequestResult)
    return make(map[string]string), make(map[int64]RequestResult)
  }else {
    shard, history := kv.xferAdoptedShard(shardNum, disowningFamily, nextConfig)
    return shard, history
  }
}

func (kv *ShardKV) xferAdoptedShard(shardNum int, disowningFamily int64, nextConfig shardmaster.Config) (map[string]string, map[int64]RequestResult) {
  for !kv.dead {
    for _, disowner := range kv.config.Groups[disowningFamily] {
      args := XferArgs{ShardNum: shardNum, Config: nextConfig}
      reply := XferReply{Err: OK}
      replyReceived := call(disowner, "ShardKV.XferDisownedShard", &args, &reply)
      if replyReceived {
        if reply.Err == OK {
          return reply.Shard, reply.ShardHistory
        }
      }
    }
    time.Sleep(100 * time.Millisecond) // give group a chance to catch up to our config
  }
  return nil, nil
}

func (kv *ShardKV) XferDisownedShard(args *XferArgs, reply *XferReply) error {
  if kv.config.Num >= args.Config.Num {
    reply.Shard = copyShard(kv.shards[args.ShardNum]) // shard is locally frozen (no need to lock!)
    reply.ShardHistory = copyHistory(kv.shardHistories[args.ShardNum]) // shard history is locally frozen (no need to lock!)
  }else { // behind sender's config
    reply.Err = ErrBehind // reply immediately, then attempt to catch up to sender's config
    go kv.attemptNextReconfig()
  }
  return nil
}

//TODO: check if query-result config actually is next config --> abstract ticks into attemptNextReconfig
func (kv *ShardKV) attemptNextReconfig(){
  kv.mu.Lock()
  defer kv.mu.Unlock()
  nextConfig := kv.sm.Query(kv.config.Num + 1)
  kv.reconfigure(nextConfig)
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  currentConfig := kv.sm.Query(kv.config.Num + 1) // try to get the next config
  if currentConfig.Num == kv.config.Num + 1 { // new, next config
    kv.reconfigure(currentConfig)
  }
}


// tell the server to shut itself down.
func (kv *ShardKV) kill() {
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
                 servers []string, me int) *ShardKV {
  gob.Register(Op{})

  kv := new(ShardKV)
  kv.me = me
  kv.gid = gid
  kv.sm = shardmaster.MakeClerk(shardmasters)

  // Your initialization code here.
  // Don't call Join().
  kv.config = shardmaster.Config{Num: 0}
  kv.shards = [shardmaster.NShards]map[string]string{}
  kv.isFrozen = [shardmaster.NShards]bool{}
  for shard, _ := range kv.isFrozen {
    kv.isFrozen[shard] = true
  }
  kv.reconfiguring = false
  kv.shardHistories = [shardmaster.NShards]map[int64]RequestResult{} // maps clients to their last requests
  kv.lb = -1

  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)


  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && kv.dead == false {
        fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  go func() {
    for kv.dead == false {
      kv.tick()
      time.Sleep(250 * time.Millisecond)
    }
  }()

  return kv
}
