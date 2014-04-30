package shardkv
import "hash/fnv"
import "shardmaster"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
  OK = "OK"
  PUT = "PUT"
  PUTHASH = "PUTHASH"
  GET = "GET"
  NOP = "NOP"
  BEGIN = "BEGIN"
  END = "END"
  ErrNoKey = "ErrNoKey"
  ErrWrongGroup = "ErrWrongGroup"
  ErrBehinde = "ErrorBehinde"
)
type Err string

type PutArgs struct {
  Key string
  Value string
  DoHash bool // For PutHash
  ClientID string
  OpID int
}

type PutReply struct {
  Err Err
  PreviousValue string   // For PutHash
}

type GetArgs struct {
  Key string
  ClientID string
  OpID int
}

type GetReply struct {
  Err Err
  Value string
}

type GetMissingShardArgs struct{
  MissingShard int
  Config shardmaster.Config
}

type GetMissingShardReply struct{
  Shard_kvs map[string]string
  ShardHistory map[string]Result
  Err Err
}

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}

func mcopy_kv(m map[string]string) map[string]string{
  c := make(map[string]string)
  for k , v := range m{
    c[k] = v
  }
  return c
}

func mcopy_cid2result(shardHistory map[string]Result) map[string]Result {
  c := make(map[string]Result)
  for k, v := range shardHistory {
    op := v.Op
    result := v.Result
    c[k] = Result{Op: op, Result: result}
  }
  return c
}

// type Op struct {
//   Type string
//   Key string
//   Value string
//   ClientID string
//   OpID int
//   Config shardmaster.Config
//   ReceivedShards [shardmaster.NShards]map[string]string
//   ReceivedShardsHistory [shardmaster.NShards]map[string]Result
// }

// type ShardKV struct {
//   mu sync.Mutex
//   l net.Listener
//   me int
//   dead bool // for testing
//   unreliable bool // for testing
//   sm *shardmaster.Clerk
//   px *paxos.Paxos

//   gid int64 // my replica group ID

//   sh_kvs [shardmaster.NShards]map[string]string //map[shard]map[key]value
//   shardsHistory [shardmaster.NShards]map[string]Result
//   localMin int
//   reconfiguring bool //true if during a reconfiguration
//   config shardmaster.Config
//   frozenVector [shardmaster.NShards]bool
// }
