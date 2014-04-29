package shardkv
import (
  "hash/fnv"
  "shardmaster"
)

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
  ErrNoKey = "ErrNoKey"
  ErrWrongGroup = "ErrWrongGroup"
  ErrBehind = "ErrBehind"
)

type Err string

type ID struct {
  ClientID int64
  ReqNum int
}

const (
  Get = "Get"
  Put = "Put"
  PutHash = "PutHash"
  BeginReconfig = "BeginReconfig"
  EndReconfig = "EndReconfig"
  NOP = "NOP"
)

type PutArgs struct {
  Key string
  Value string
  DoHash bool  // For PutHash
  ID ID
}

type PutReply struct {
  Err Err
  PreviousValue string   // For PutHash
  ID ID
}

type GetArgs struct {
  Key string
  ID ID
}

type GetReply struct {
  Err Err
  Value string
  ID ID
}

//TODO: rename...?
type RequestResult struct {
  Op Op
  Result string
  Err Err
}

type XferArgs struct {
  Config shardmaster.Config
  ShardNum int
}

type XferReply struct {
  Err Err
  Shard map[string]string
  ShardHistory map[int64]RequestResult
}

func copyShard(shard map[string]string) map[string]string {
  copy := make(map[string]string)
  for k, v := range shard {
    copy[k] = v
  }
  return copy
}

func copyHistory(history map[int64]RequestResult) map[int64]RequestResult {
  copy := make(map[int64]RequestResult)
  for k, v := range history {
    copy[k] = v
  }
  return copy
}


func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}

