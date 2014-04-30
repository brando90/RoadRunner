package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
//import "math"
//import "strconv"

import "time"

const Debug=0

func DPrintf(format string, a ...interface{}) (n int, err error) {
		if Debug > 0 {
				fmt.Printf(format, a...)
		}
		return
}

// func nrand() int64 {
//   max := big.NewInt(int64(1) << 62)
//   bigx, _ := rand.Int(rand.Reader, max)
//   x := bigx.Int64()
//   return x
// }

type ShardMaster struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos
  configs []Config // indexed by config num
  localMin int //points to one before the executed
  seqID int
}


type Op struct {
  // Your data here.
  Type string
  GID int64
  Servers []string
  Shard int
  Num int
  OpID int
  ServerID int
}

//Arguments:
//  JoinArgs.GID = unique non-zero replica group identifier (GID) {int64}
//  JoinArgs.Servers = array of server ports {[]string}
//Return:
//  JoinReply = Nothing!
//Does:
//  -shardmaster should create a new configuration that includes 
//    the new replica group.
//  -the new configuration should divide the shards as evenly as 
//    possible among the groups.
//  -should move as few shards as possible to achieve these goals.
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
  sm.mu.Lock()
  defer sm.mu.Unlock()
  DPrintf("\t START me %d Join Shards %+v\n" , sm.me ,sm.configs[len(sm.configs) - 1].Shards)
  DPrintf("\t START me %d Join Groups %+v\n" , sm.me ,sm.configs[len(sm.configs) - 1].Groups)
  sm.seqID += 1
  op := Op{Type: JOIN, GID: args.GID, Servers: args.Servers, OpID:sm.seqID, ServerID: sm.me}
  //DPrintf("JOIN")
  seq := sm.putIntoPxLog(op)
  sm.applyLogUpTo(seq)
  DPrintf("\t END me %d Join Shards %+v\n" , sm.me ,sm.configs[len(sm.configs) - 1].Shards)
  DPrintf("\t END me %d Join Groups %+v\n" , sm.me ,sm.configs[len(sm.configs) - 1].Groups)
  return nil
}

//Arguments:
//  LeaveArgs.GID = the GID of a previously joined group {int64}
//Return:
//  LeaveReply = Nothing!
//Does:
//  -create a new configuration that does not include the group leaving
//  -(re)assigns the the shards of the leaving group to the remaining groups.
//  -The new configuration should divide the shards as evently as possible.
func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
  sm.mu.Lock()
  defer sm.mu.Unlock()
  DPrintf("\t START me %d Leave Shards %+v\n" , sm.me, sm.configs[len(sm.configs) - 1].Shards)
  DPrintf("\t START me %d Leave Groups %+v\n" , sm.me, sm.configs[len(sm.configs) - 1].Groups)
  sm.seqID += 1
  op := Op{Type:LEAVE, GID: args.GID, OpID:sm.seqID, ServerID: sm.me}
  seq := sm.putIntoPxLog(op)
  sm.applyLogUpTo(seq)
  DPrintf("\t END me %d Leave Shards %+v\n" , sm.me, sm.configs[len(sm.configs) - 1].Shards)
  DPrintf("\t END me %d Leave Groups %+v\n" , sm.me, sm.configs[len(sm.configs) - 1].Groups)
  return nil
}

//Arguments:
//  MoveArgs.Shard = the shard number {int}
//  MoveArgs.GID = unique non-zero replica group identifier (GID) {int64}
//Return:
//  MoveReply = Nothing!
//Does:
//  -create a new configuration where the targeted/moving args.Shard is move to the
//    corresponding group (args.GID)
//  -purpose: it might be useful to fine-tune load balance if some shards are
//    more popular than others or some replica groups are slower than others.
func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
  sm.mu.Lock()
  defer sm.mu.Unlock()
  DPrintf("\t START me %d Move Shards %+v\n" , sm.me, sm.configs[len(sm.configs) - 1].Shards)
  DPrintf("\t START me %d Move Groups %+v\n" , sm.me, sm.configs[len(sm.configs) - 1].Groups)
  sm.seqID += 1
  op := Op{Type: MOVE, GID: args.GID, Shard: args.Shard, OpID:sm.seqID, ServerID: sm.me}
  seq := sm.putIntoPxLog(op)
  sm.applyLogUpTo(seq)
  DPrintf("\t END me %d Move Shards %+v\n" , sm.me, sm.configs[len(sm.configs) - 1].Shards)
  DPrintf("\t END me %d Move Groups %+v\n" , sm.me, sm.configs[len(sm.configs) - 1].Groups)
  return nil
}

//Arguments:
//  QueryArgs.Num = a configuration number {int}
//Return:
//  QueryReply.Config = the shardmaster replies with the configuration object
//                      that has the queried number {Config}
//Does:
//  -if args.Num == -1 or args.Num > biggestConfigurationNumber
//      => then; the shardmaster should reply with the latest configuration
//  -Query(-1) should reflect every Join, Leave or Move that completed before the
//    Query(-1) RPC was sent.
func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
  sm.mu.Lock()
  defer sm.mu.Unlock()
  DPrintf("\t START me %d Query Shards %+v\n" , sm.me, sm.configs[len(sm.configs) - 1].Shards)
  DPrintf("\t START me %d Query Groups %+v\n" , sm.me, sm.configs[len(sm.configs) - 1].Groups)
  sm.seqID += 1
  op := Op{Type: QUERY, Num: args.Num, OpID:sm.seqID, ServerID: sm.me}
  seq := sm.putIntoPxLog(op)
  reply.Config = sm.applyLogUpTo(seq)
  DPrintf("\t END me %d Query Shards %+v\n" , sm.me, sm.configs[len(sm.configs) - 1].Shards)
  DPrintf("\t END me %d Query Groups %+v\n" , sm.me, sm.configs[len(sm.configs) - 1].Groups)
  return nil
}

//puts an op into the paxos log
func (sm *ShardMaster) putIntoPxLog(op Op) int{
  for !sm.dead {
  	seq := sm.px.Max() + 1
  	decided, _ := sm.px.Status(seq)
    if !decided{
      sm.px.Start(seq, op)
      decidedOp := sm.waitForDecision(seq)
      if sm.equal(decidedOp, op) {
        return seq
      }
    }
  }
  return -1
}

func (sm *ShardMaster) equal(op1 Op, op2 Op) bool{
  if op1.ServerID == op2.ServerID && op1.OpID == op2.OpID{
    return true
  }else{
    return false
  }
 //  if len(op1.Servers) != len(op2.Servers) {
	// return false
 //  }
 //  areEqual := true
 //  areEqual = areEqual && op1.Type == op2.Type
 //  areEqual = areEqual && op1.GID == op2.GID
 //  //os.Exit(0)
 //  if len(op1.Servers) != 0{
	// for i := 0; i < len(op1.Servers); i++{
	//   s1 := op1.Servers[i]
	//   s2 := op2.Servers[i]
	//   areEqual = areEqual && (s1 == s2)
	// }
 //  }
 //  areEqual = areEqual && (op1.Shard == op2.Shard)
 //  areEqual = areEqual && (op1.Num == op2.Num)
  // return areEqual
}

//waits and returns the decided decision
func (sm *ShardMaster) waitForDecision(seq int) Op{
  to := 10 * time.Millisecond
  for !sm.dead{
    decided, decided_op := sm.px.Status(seq)
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
func (sm *ShardMaster) applyLogUpTo(seq int) Config{
  if sm.dead {
	return Config{}
  }
  start := sm.localMin + 1
  //DPrintf("\nstart = ", start)
  for current_seq := start; current_seq < seq; current_seq++ {
    sm.px.Start(current_seq, Op{Type: NOP})
    op := sm.waitForDecision(current_seq)
    sm.executeOp(op)
  }
  //_, op := sm.px.Status(seq)
	sm.px.Start(seq, Op{Type: NOP})
  op := sm.waitForDecision(seq)
  lastConfig := sm.executeOp(op)
  sm.px.Done(seq)
  sm.localMin = seq //points to last thing that was executed
  return lastConfig
}

//execute the appropriate Op and changes the state if neccesery
func (sm *ShardMaster) executeOp(op Op) Config {
  current_config := sm.configs[len(sm.configs) - 1]
  var new_conf Config
  switch op.Type {
  case JOIN:
    new_conf = sm.executeJoin(op) //add the new GID and its Servers to the a new config
    sm.configs = append(sm.configs, new_conf)
    DPrintf("{ >>> CURRENT_OP: %+v \n >>> OLD: %+v \n >>> NEW: %+v } \n >>>\n", op, current_config, new_conf)
    break
  case LEAVE:
    new_conf = sm.executeLeave(op)
    DPrintf("{ >>> CURRENT_OP: %+v \n >>> OLD: %+v \n >>> NEW: %+v } \n >>>\n", op, current_config, new_conf)
    break
  case MOVE:
    new_conf = sm.executeMove(current_config, op.GID, op.Shard)
    //sm.configs[new_conf.Num] = new_conf
    sm.configs = append(sm.configs, new_conf)
    DPrintf("{ >>> CURRENT_OP: %+v \n >>> OLD: %+v \n >>> NEW: %+v } \n >>>\n", op, current_config, new_conf)

    break
  case QUERY:
    new_conf = sm.executeQuery(op.Num)
    break
  case NOP:
    new_conf = Config{}
    break
  }
  return new_conf
}

/*type Op struct {
  Type string
  GID int64
  Servers []string
  Shard int
  Num int  
}*/
func (sm *ShardMaster) executeJoin(op Op) Config{
  mostRecentConfig := sm.configs[len(sm.configs) - 1]
  var gid2shardNums map[int64][]int = mostRecentConfig.GetGroupToShardNum()
  _, ok := gid2shardNums[op.GID]
  if !ok{
    gid2shardNums[op.GID] = make([]int, 0) //make empty shards group
  }
  //if the current gid to shards has only 1 group, then assign all shards to it
  if len(gid2shardNums) == 1{
    for shardNum, _ := range mostRecentConfig.Shards{
      gid2shardNums[op.GID] = append(gid2shardNums[op.GID], shardNum)
    }
  }
  balanced_groupToShards := sm.balanceLoad(gid2shardNums)
  new_conf := sm.MakeConfigFromGroupToShards(mostRecentConfig, balanced_groupToShards)
  new_conf.Groups[op.GID] = op.Servers
	return new_conf
}

func (sm *ShardMaster) executeLeave(op Op) Config{
  mostRecentConfig := sm.configs[len(sm.configs) - 1]
  var gid2shardNums map[int64][]int = mostRecentConfig.GetGroupToShardNum()
  //check if they are asking to remove an group GID that isn't part of the current configuration
  _ , ok := gid2shardNums[op.GID]
  if !ok{
    //of they are trying to remove a group that isn't part of the config, don't actually change config
    return mostRecentConfig
  }
  //if config has no groups in charge of shards, then just move to next config
  if len(gid2shardNums) == 0{
    //if your removing no one from the group, its not really changing configuration
    return Config{Num: mostRecentConfig.Num + 1}
  }else if len(gid2shardNums) == 1{
    new_conf := Config{Num: mostRecentConfig.Num + 1}
    sm.configs = append(sm.configs, new_conf)
    return new_conf
  }
  //get a (safe) gid that is safe to assign the lonely shards (i.e. not a gid leaving)
  var safeGid int64
  for gid, _ := range gid2shardNums{
    if gid != op.GID{
      safeGid = gid
      break
    }
  }
  //assign the lonely shards to some safe gid (i.e. group not leaving)
  for _ , lonelyShardNum := range gid2shardNums[op.GID]{
    gid2shardNums[safeGid] = append(gid2shardNums[safeGid], lonelyShardNum)
  }
  delete(gid2shardNums, op.GID)
  balanced_groupToShards := sm.balanceLoad(gid2shardNums)
  new_conf := sm.MakeConfigFromGroupToShards(mostRecentConfig, balanced_groupToShards)
  sm.configs = append(sm.configs, new_conf)
  return new_conf
}

func (sm *ShardMaster) MakeConfigFromGroupToShards(oldConfig Config, groupToShards map[int64][]int) Config{
  //for the shards assigned to a gid, reverse map the Shards[shardnum] -> gid
  var new_shards [NShards]int64 //[shard_num]int -> gid
  newGroups := make(map[int64][]string) //[gid]int64 -> []servers
  for gid, shards := range groupToShards{
    if gid == 0{
      DPrintf("ERROR PANIC GID shouldnt be zero")
    }
    //assigns shard to gid
    for _, shardNum := range shards{
      new_shards[shardNum] = gid
    }
    newGroups[gid] = oldConfig.Groups[gid] //assigns the servers to the new group
  }
  new_config := Config{Num: oldConfig.Num + 1, Shards: new_shards, Groups: newGroups}
  return new_config
}

func (sm *ShardMaster) executeMove(current_config Config, targeted_group int64, shard_to_move int) Config{
  new_shards := current_config.Shards
  new_shards[shard_to_move] = targeted_group
  new_groups := current_config.MakeCopyOfMap(current_config.Groups)
  copy_shards := current_config.MakeCopyOfArray(new_shards)
  new_conf := Config{current_config.Num + 1, copy_shards, new_groups}
  return new_conf
}

//  -if args.Num == -1 or args.Num > biggestConfigurationNumber
//      => then; the shardmaster should reply with the latest configuration
//  -Query(-1) should reflect every Join, Leave or Move that completed before the
//    Query(-1) RPC was sent.
func (sm *ShardMaster) executeQuery(num int) Config{
  if num <= -1 || num >= len(sm.configs){
	return sm.configs[len(sm.configs) - 1]
  }
  return sm.configs[num]
}

func (sm *ShardMaster) balanceLoad(groupToShards map[int64][]int) map[int64][]int{
  for gid, _ := range groupToShards{
  	if gid == 0{
  		DPrintf("Error Panic balanceLoad: ZERO ZERO :'(")
  	}
  }
  //sm.count = 0
  for ! sm.isBalanced(groupToShards){
    max_gid := sm.extractMax(groupToShards)
    current_max_shards := groupToShards[max_gid]
    shard_to_swap := current_max_shards[len(current_max_shards) - 1]
    groupToShards[max_gid] = current_max_shards[:len(current_max_shards) - 1] //pop
    min_gid := sm.extractMin(groupToShards)
    min_shards := groupToShards[min_gid]
    groupToShards[min_gid] = append(min_shards, shard_to_swap)
    //sm.count ++
  }
  return groupToShards
}

func (sm *ShardMaster) isBalanced(groupToShards map[int64][]int) bool{
  num_groups := len(groupToShards)
  num_shards := NShards
  if num_groups == 0{
    return true
  }
  average := num_shards/num_groups
  for _ , shards := range groupToShards{
    current_len := len(shards)
    if current_len < average || current_len > average + 1{
      return false
    }
  }
  //DPrintf("Is the balanced? ", groupToShards)
  return true
}

func (sm *ShardMaster) extractMax(groupToShards map[int64][]int) int64{
  max_shards_len := -1
  var max_gid int64
  for gid, shards := range groupToShards {
    if len(shards) > max_shards_len{
      max_shards_len = len(shards)
      max_gid = gid
    }
  }
  return max_gid
}

func (sm *ShardMaster) extractMin(groupToShards map[int64][]int) int64{
  min_shards_len := -1
  var min_gid int64
  for gid, shards := range groupToShards {
	if min_shards_len == -1 {
	  min_shards_len = len(shards)
	  min_gid = gid
	}
	if len(shards) < min_shards_len{
	  min_shards_len = len(shards)
	  min_gid = gid
	}
  }
  return min_gid
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
  //sm.initHappend = false

  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = map[int64][]string{}

  sm.localMin = -1
  sm.seqID = -1

  rpcs := rpc.NewServer()
  rpcs.Register(sm)

  sm.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
	log.Fatal("listen error: ", e);
  }
  sm.l = l

  //DPrintf("SHARDS", sm.configs[0].Shards)
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
