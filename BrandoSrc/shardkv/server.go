package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
import "strconv"

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
  ClientID string
  OpID int
  Config shardmaster.Config
  ReceivedShards map[int]map[string]string
  ReceivedShardsHistory map[int]map[string]Result
}

type Result struct {
  Op Op
  Result string
  Err Err
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

  sh_kvs [shardmaster.NShards]map[string]string //map[shard]map[key]value
  shardsHistory [shardmaster.NShards]map[string]Result
  localMin int
  reconfiguring bool //true if during a reconfiguration
  config shardmaster.Config
  frozenVector [shardmaster.NShards]bool
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  op := Op{Type:GET, Key: args.Key, ClientID: args.ClientID, OpID: args.OpID}
  seq := kv.putIntoPxLog(op)
  val, err := kv.applyLogUpTo(seq)
  reply.Value = val
  reply.Err = err
  DPrintf("{>>>> GET: %+v \n >>>> %+v reply \n}", args, reply)
  return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  var put_op_str string
  if args.DoHash{
    put_op_str = PUTHASH
  }else{
    put_op_str = PUT
  }
  op := Op{Type:put_op_str, Key: args.Key, Value: args.Value, ClientID: args.ClientID, OpID: args.OpID}
  seq := kv.putIntoPxLog(op)
  val, err := kv.applyLogUpTo(seq)
  reply.PreviousValue = val
  reply.Err = err
  DPrintf("{>>>> PUT: %+v \n >>>> BEFORE PUT: %s \n >>>> %+v reply \n}", args, kv.sh_kvs[key2shard(args.Key)][args.Key], reply)
  return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  shardmasterConfig := kv.sm.Query(kv.config.Num + 1)
  if shardmasterConfig.Num == kv.config.Num + 1 {
    kv.reconfigure(shardmasterConfig)
  }
}

func (kv *ShardKV) reconfigure(nextConfig shardmaster.Config){
  //do a Begin Reconfiguring
  begin_op := Op{Type: BEGIN, Config: nextConfig}
  seq := kv.putIntoPxLog(begin_op)
  kv.applyLogUpTo(seq)
  //get missing shards
  //shardmaster.NShards]map[string]string
  receivedShards, receivedShardsHistory := kv.requestForShardsNeedToReceive(nextConfig)
  //do an End Reconfiguring
  end_op := Op{Type: END, Config: nextConfig, ReceivedShards: receivedShards, ReceivedShardsHistory: receivedShardsHistory}
  seq = kv.putIntoPxLog(end_op)
  kv.applyLogUpTo(seq)
}

//puts an op into the paxos log
//TODO: make sure that if we are in a reconfiguration, to "ignore"
//the things we need to ignore
func (kv *ShardKV) putIntoPxLog(op Op) int{
  for !kv.dead {
    seq := kv.px.Max() + 1
    decided, _ := kv.px.Status(seq)
    if !decided{
      kv.px.Start(seq, op)
      decidedOp := kv.waitForDecision(seq)
      //need to make sure this op gets put into the paxos log eventually
      //so if its not equal to the current op, then try to suggest it
      //on the next Paxos round
      if kv.equal(decidedOp, op) { //TODO
        return seq
      }
    }
  }
  return -1
}

//TODO
func (kv *ShardKV) equal(op1 Op, op2 Op) bool{
  if op1.Type != op2.Type{
    return false
  }
  if op1.Type == NOP{ //TODO
    return true
  }
  if op1.Type == GET || op1.Type == PUT || op1.Type == PUTHASH{
    if op1.ClientID == op2.ClientID{
      if op1.OpID == op2.OpID{
        return true
      }else{
        return false
      }
    }else{
      return false
    }
  }else{
    if op1.Config.Num == op2.Config.Num{
      return true
    }else{
      return false
    }
  }
}

//waits and returns the decided decision
func (kv *ShardKV) waitForDecision(seq int) Op{
  to := 10 * time.Millisecond
  for !kv.dead{
    if kv.dead{
      return Op{}
    }
    decided, decided_op := kv.px.Status(seq)
    if decided {
      return decided_op.(Op)
    }
    time.Sleep(to)
    if to < 10 * time.Second {
      to *= 2
    }
  }
  return Op{}
}

//Apply log up to and including seq
func (kv *ShardKV) applyLogUpTo(seq int) (string, Err) {
  if kv.dead {
    return "", ""
  }
  nop_op := Op{Type: NOP}
  start := kv.localMin + 1
  for current_seq := start; current_seq < seq; current_seq++ {
    kv.px.Start(current_seq, nop_op) 
    op := kv.waitForDecision(current_seq)
    kv.executeOp(op)
  }
  _, operation := kv.px.Status(seq)
  op := operation.(Op)
  reply_s, err := kv.executeOp(op)
  kv.px.Done(seq)
  kv.localMin = seq //points to last thing that was executed
  return reply_s, err
}

func (kv *ShardKV) isDuplicateOp(op Op) (bool, Result){
  if op.Type == NOP || op.Type == BEGIN || op.Type == END{
    return false, Result{}
  }
  shard := key2shard(op.Key)
  shardHistory := kv.shardsHistory[shard]
  old_result, ok := shardHistory[op.ClientID]
  if ok{
    if kv.duplicateClientOp(op, old_result.Op){
      return true, old_result
    }else{
      return false, Result{}
    }
  }else{
    //never seen this op from thisclient
    return false, Result{}
  }

}

func (kv *ShardKV) duplicateClientOp(op1 Op, op2 Op) bool{
  if op1.ClientID == op2.ClientID{
    if op1.OpID <= op2.OpID{
      return true
    }else{
      return false
    }
  }else{
    return false
  }
}

//detect duplicates and execute the appropriate Op and changing the state
func (kv *ShardKV) executeOp(op Op) (string, Err) {
  duplicateOp, previousResult := kv.isDuplicateOp(op)
  if (duplicateOp){
    return previousResult.Result, previousResult.Err
  }
  switch op.Type{
  case PUT:
    return kv.executePut(op)
  case PUTHASH:
    return kv.executePutHash(op)
  case GET:
    return kv.executeGet(op)
  case NOP:
    // do nothing
  case BEGIN:
    kv.executeBegin(op)
  case END:
    kv.executeEnd(op)
  default:
    fmt.Println(op)
    panic("Error in op, what operation type are you trying to do?")
  }
  return "", ""
}

func (kv *ShardKV) executePut(op Op) (string, Err){
  shard := key2shard(op.Key)
  if kv.isShardFrozen(shard){
    return "", ErrWrongGroup
  }else{
    var err Err
    kv.sh_kvs[shard][op.Key] = op.Value
    err = OK
    kv.shardsHistory[shard][op.ClientID] = Result{Op: op, Result: op.Value, Err: err}
    //v, _ := kv.sh_kvs[shard][op.Key]
    return "" , err
  }
}

func (kv *ShardKV) executePutHash(op Op) (string, Err){
  shard := key2shard(op.Key)
  if kv.isShardFrozen(shard){
    return "", ErrWrongGroup
  }else{
    var err Err 
    previousValue, _ := kv.sh_kvs[shard][op.Key]
    newValue := strconv.Itoa(int(hash(previousValue + op.Value)))
    kv.sh_kvs[shard][op.Key] = newValue
    err = OK
    kv.shardsHistory[shard][op.ClientID] = Result{Op: op, Result: newValue, Err: err}
    return previousValue, err
  }
}

func (kv *ShardKV) executeGet(op Op) (string, Err){
  shard := key2shard(op.Key)
  if kv.isShardFrozen(shard){
    return "", ErrWrongGroup
  }else{
    v, ok := kv.sh_kvs[shard][op.Key]
    var result string
    var err Err
    if ok {
      result = v
      err = OK
    }else{
      result = ""
      err = ErrNoKey
    }
    kv.shardsHistory[shard][op.ClientID] = Result{Op: op, Result: result, Err: err}
    return result, err
  }
}

func (kv *ShardKV) executeBegin(op Op){
  if !kv.reconfiguring && op.Config.Num == kv.config.Num + 1 {
    kv.reconfiguring = true
    kv.frozenVector = kv.computeFrozenVector(kv.config, op.Config)
  }
}

func (kv *ShardKV) executeEnd(op Op){
  // var ReceivedShards [shardmaster.NShards]map[string]string
  // var ReceivedShardsHistory [shardmaster.NShards]map[string]Result
  if kv.reconfiguring && op.Config.Num == kv.config.Num + 1 {
    kv.reconfiguring = false
    kv.config = op.Config
    for newShardNum, kv_map := range op.ReceivedShards{
      kv.sh_kvs[newShardNum] = mcopy_kv(kv_map)
      kv.frozenVector[newShardNum] = false
    }
    for newShardNum, shardHist := range op.ReceivedShardsHistory{
      kv.shardsHistory[newShardNum] = mcopy_cid2result(shardHist)
    }
  }
}

//returns shards we need to receive and their new history
func (kv *ShardKV) requestForShardsNeedToReceive(nextConfig shardmaster.Config) (map[int]map[string]string, map[int]map[string]Result){
  receivedShards := make(map[int]map[string]string)
  receivedShardsHistory := make(map[int]map[string]Result)
  list_shards_need_2_receive := kv.getListOfShardsNeedToReceive(nextConfig)
  for _, shardNumNeed := range list_shards_need_2_receive{
    //receive current kv for current shard
    //receive history for current shard
    received_kv, receivedHistory := kv.requestShardNeedToReceive(shardNumNeed, nextConfig)
    receivedShards[shardNumNeed] = received_kv
    receivedShardsHistory[shardNumNeed] = receivedHistory
  }
  return receivedShards, receivedShardsHistory
}

func (kv *ShardKV) requestShardNeedToReceive(shardNum int, nextConfig shardmaster.Config) (map[string]string, map[string]Result){
  gid := kv.config.Shards[shardNum]
  if gid == 0{
    //at the beginning of the Universe, no one owns this shards, so initiliaze them!
    return make(map[string]string), make(map[string]Result)
  }
  servers, ok := kv.config.Groups[gid]
  if ok{
    for _, srv := range servers {
      args := GetMissingShardArgs{MissingShard: shardNum, Config: nextConfig}
      reply := GetMissingShardReply{Err: OK}
      responseReceived := call(srv, "ShardKV.TransferShardRPC", &args, &reply)
      if responseReceived && reply.Err == OK{
        return reply.Shard_kvs, reply.ShardHistory
      }
    }
    time.Sleep(100 * time.Millisecond)
  }
  return nil, nil
}

//RPC Handler
func (kv *ShardKV) TransferShardRPC(args *GetMissingShardArgs, reply *GetMissingShardReply) error{
  if kv.config.Num >= args.Config.Num{
    //if we are at the correct config or more, we can give them the shards the other group needs.
    kv_map := kv.sh_kvs[args.MissingShard]
    reply.Shard_kvs = mcopy_kv(kv_map)
    history := kv.shardsHistory[args.MissingShard]
    reply.ShardHistory = mcopy_cid2result(history)
  }else{
    reply.Err = ErrBehinde
    //we must be a config number behinde the request sent to us.
    //so tell them that immediately so that he can try a different function, but meanwhile...
    //try to move to the next config without blocking him
    go func(){
      kv.mu.Lock()
      //defer kv.mu.Unlock()
      nextConfig := kv.sm.Query(kv.config.Num + 1)
      kv.reconfigure(nextConfig)
      kv.mu.Unlock()
    }()
  }
  return nil
}

//Shard list we need to get from some other group
func (kv *ShardKV) getListOfShardsNeedToReceive(config shardmaster.Config) []int{
  list := make([]int, 0)
  for shard , new_gid := range config.Shards {
    //if kv.isShardFrozen(shard) && new_gid == kv.gid{
    if kv.frozenVector[shard] && new_gid ==kv.gid {
      //we need to request this shard
      list = append(list, shard)
    }
  }
  return list
}

//true when we can't server shard.
func (kv *ShardKV) isShardFrozen(shard int) bool{
  owner_gid := kv.config.Shards[shard]
  if kv.reconfiguring{
    return true
  }else{
    return owner_gid != kv.gid
  }
  //return kv.reconfiguring
  //return kv.frozenVector[shard]
}

//computes difference between shards of the configs
func (kv *ShardKV) computeFrozenVector(config1 shardmaster.Config, config2 shardmaster.Config) [shardmaster.NShards]bool{
  n := int(len(config1.Shards))
  //frozenVector := make([]bool, 0)
  var frozenVector [shardmaster.NShards]bool
  for i := 0; i < n; i++{
    gid1 := config1.Shards[i]
    gid2 := config2.Shards[i]
    //frozen := !(gid1 == kv.gid && gid2 == kv.gid)
    frozen := (gid1 == kv.gid && gid2 != kv.gid)
    //append(frozenVector, difference)
    frozenVector[i] = frozen
  }
  return frozenVector
}


// func xor(X bool, Y bool) bool{
//   return (X != Y)
// }

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
  kv.sh_kvs = [shardmaster.NShards]map[string]string{}
  kv.shardsHistory = [shardmaster.NShards]map[string]Result{}
  kv.reconfiguring = false
  kv.config = shardmaster.Config{Num: 0}
  var frozenVector [shardmaster.NShards]bool
  for shardNum, _ := range frozenVector{
    frozenVector[shardNum] = true
  }
  kv.frozenVector = frozenVector

  kv.localMin = -1

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
