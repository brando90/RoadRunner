package shardmaster

import "math"

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

const NShards = 10

type Config struct {
  Num int // config number
  Shards [NShards]int64 // shard index -> gid
  Groups map[int64][]string // gid -> servers[]
}

func (c *Config) copy() *Config {
  // copy shards
  var shardsCopy [NShards]int64
  for shard, gid := range c.Shards {
    shardsCopy[shard] = gid
  }

  // copy groups
  groupsCopy := make(map[int64][]string)
  for gid, servers := range c.Groups {
    groupsCopy[gid] = servers
  }

  // return deep copy
  return &Config{Num: c.Num, Shards: shardsCopy, Groups: groupsCopy}
}

func (c *Config) balance() {
  oldShards := c.Shards
  // populate groups map
  groups := make(map[int64][]int)
  for gid, _ := range c.Groups {
    groups[gid] = []int{}
  }

  // process shard assignment
  unassigned := []int{}
  for shard, gid := range oldShards {
    if gid == 0 {
      unassigned = append(unassigned, shard)
    }else {
      groups[gid] = append(groups[gid], shard)
    }
  }

  minGID := minShardsGID(groups)
  groups[minGID] = append(groups[minGID], unassigned...)

  // balance groups
  for !balanced(groups) {
    maxGID := maxShardsGID(groups)
    minGID := minShardsGID(groups)
    maxShards := groups[maxGID]
    minShards := groups[minGID]
    groups[maxGID] = maxShards[1:]
    groups[minGID] = append(minShards, maxShards[0])
  }

  var newShards [NShards]int64
  for gid, shards := range groups {
    for _, shard := range shards {
      newShards[shard] = gid
    }
  }

  c.Shards = newShards
}

func balanced(groups map[int64][]int) bool {
  //TODO: reimplement using brando's algorithm

  if len(groups) == 1 {
    return true
  }

  size1 := -1
  size2 := -1
  for _, shards := range groups {
    l := len(shards)
    if size1 == -1 {
      size1 = l
    }else if l != size1 {
      if size2 == -1 {
        size2 = l
      }else if l != size2 {
        return false
      }
    }
  }
  if size2 == -1 {
    return true
  }
  return math.Abs(float64(size1 - size2)) <= 1
}

func maxShardsGID(groups map[int64][]int) int64 {
  maxGID := int64(0)
  maxShards := -1
  for gid, shards := range groups {
    if maxShards == -1 {
      maxGID = gid
      maxShards = len(shards)
    }else if len(shards) > maxShards {
      maxGID = gid
      maxShards = len(shards)
    }
  }
  return maxGID
}

func minShardsGID(groups map[int64][]int) int64 {
  minGID := int64(0)
  minShards := -1
  for gid, shards := range groups {
    if minShards == -1 {
      minGID = gid
      minShards = len(shards)
    }else if len(shards) < minShards {
      minGID = gid
      minShards = len(shards)
    }
  }
  return minGID
}

const (
  JOIN = "JOIN"
  LEAVE = "LEAVE"
  MOVE = "MOVE"
  QUERY = "QUERY"
  NOP = "NOP"
)

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
