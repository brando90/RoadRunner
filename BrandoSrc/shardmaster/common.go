package shardmaster

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(gid, servers) -- replica group gid is joining, give it some shards.
// Leave(gid) -- replica group gid is retiring, hand off all its shards.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// A GID is a replica group ID. GIDs must be uniqe and > 0.
// Once a GID joins, and leaves, it should never join again.
//
// Please don't change this file.
//


import "fmt"

const (
  NOP = "NOP"
  JOIN = "JOIN"
  LEAVE = "LEAVE"
  MOVE = "MOVE"
  QUERY= "QUERY"
  OK = "OK"
  ErrNoKey = "ErrNoKey"
)

const NShards = 10

type Config struct {
  Num int // config number
  Shards [NShards]int64 // shardNumber -> gid (of group in charge of that shard)
  Groups map[int64][]string // gid -> servers[]
}

/**
* Returns a map[gid] -> [ShardNum]int
* If the shards are assigned to gid = 0, returns empty map[gid][]int
**/
func (c *Config) GetGroupToShardNum() map[int64][]int{
  //if the config has not groups, then return empty map (since no gid is in charge of shards)
  //note that in this case, all the Shards should be assigned to 0
  if len(c.Groups) == 0{ //no gid is in charge of shards
    for _, gid := range c.Shards{ // make sure thats the case, that all shards are assigned to no group (i.e. all gids are zero)
      if gid != 0{
        msg := "In this config not shards are assigned. But...Why is one of them assigned?"
        fmt.Println("All gids should be zero") //dont delete so that compiler doesnt complain about fmt
        fmt.Println(c)
        panic(msg)
      }
    }
    return make(map[int64][]int)
  }
  //---- after this, no gid should be equal to zero
  groups2ShardNums := make(map[int64][]int)
  for gid, _ := range c.Groups{
    groups2ShardNums[gid] = []int{}
  }
  for shardNum, gid := range c.Shards{
    if gid != 0{
      groups2ShardNums[gid] = append(groups2ShardNums[gid], shardNum)
    }else{
      panic("Some shard assigned to GID == 0")
    }
  }
  return groups2ShardNums
}

func (c *Config) MakeCopyOfMap(m map[int64][]string) map[int64][]string{
  cp := make(map[int64][]string)
  for key, value := range m{
    copy_value := make([]string, len(value))
    copy(copy_value, value)
    cp[key] = copy_value
  }
  return cp
}

func (c *Config) MakeCopyOfArray(array [NShards]int64) [NShards]int64{
  copy_array := [NShards]int64{}
  for i, val := range array{
    copy_array[i] = val
  }
  return copy_array
}

type JoinArgs struct {
  GID int64       // unique replica group ID
  Servers []string // group server ports
}

type JoinReply struct {
}

type LeaveArgs struct {
  GID int64
}

type LeaveReply struct {
}

type MoveArgs struct {
  Shard int
  GID int64
}

type MoveReply struct {
}

type QueryArgs struct {
  Num int // desired config number
}

type QueryReply struct {
  Config Config
}
