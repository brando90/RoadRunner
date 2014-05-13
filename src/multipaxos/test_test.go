package multipaxos

import (
  "testing"
  "runtime"
  "strconv"
  "os"
  "time"
  "fmt"
  "encoding/gob"
  "log"
  //"math/rand"
)

/*Test outline
DONE: leader vs. non-leader
DONE: leader changes...?
1 leader
  decision reached
  decison doesnt change
multiple leaders
  same seq, same v
  same seq, diff v
  diff seq, same/diff v
garbage collection
  deleting proposers/learners
  deleting acceptors
unreliable connections
unreliable servers
concurrent requests
unreliable & concurrent
persistence
*/

// testing types

type DeepString struct {
  Str string
}

func (dstr DeepString) DeepCopy() DeepCopyable {
  return DeepString{Str: dstr.Str}
}

func start(mpxa []*MultiPaxos, seq int, v DeepCopyable) {
  for {
    for _, mpx := range mpxa {
      err := mpx.Push(seq, v)
      if err == Nil { // mpx is a leader... push successful
        return
      }else {
        // not a leader
      }
    }
    time.Sleep(10*time.Millisecond)
  }
}

func port(tag string, host int) string {
  s := "/var/tmp/824-"
  s += strconv.Itoa(os.Getuid()) + "/"
  os.Mkdir(s, 0777)
  s += "px-"
  s += strconv.Itoa(os.Getpid()) + "-"
  s += tag + "-"
  s += strconv.Itoa(host)
  return s
}

func ndecided(t *testing.T, pxa []*MultiPaxos, seq int) int {
  count := 0
  var v interface{}
  for i := 0; i < len(pxa); i++ {
    if pxa[i] != nil {
      decided, v1 := pxa[i].Status(seq)
      if decided {
        if count > 0 && v != v1 {
          t.Fatalf("decided values do not match; seq=%v i=%v v=%v v1=%v",
            seq, i, v, v1)
        }
        count++
        v = v1
      }
    }
  }
  return count
}

func waitn(t *testing.T, pxa[]*MultiPaxos, seq int, wanted int) {
  to := 10 * time.Millisecond
  for iters := 0; iters < 30; iters++ {
    if ndecided(t, pxa, seq) >= wanted {
      break
    }
    time.Sleep(to)
    if to < time.Second {
      to *= 2
    }
  }
  nd := ndecided(t, pxa, seq)
  if nd < wanted {
    t.Fatalf("too few decided; seq=%v ndecided=%v wanted=%v", seq, nd, wanted)
  }
}

func waitmajority(t *testing.T, pxa[]*MultiPaxos, seq int) {
  waitn(t, pxa, seq, (len(pxa) / 2) + 1)
}

func checkmax(t *testing.T, pxa[]*MultiPaxos, seq int, max int) {
  time.Sleep(3 * time.Second)
  nd := ndecided(t, pxa, seq)
  if nd > max {
    t.Fatalf("too many decided; seq=%v ndecided=%v max=%v", seq, nd, max)
  }
}

func cleanup(pxa []*MultiPaxos) {
  for i := 0; i < len(pxa); i++ {
    if pxa[i] != nil {
      pxa[i].Kill()
    }
  }
}

func setup(nmultipaxos int) ([]*MultiPaxos, []string) {
  gob.Register(DeepString{})

  var mpxa []*MultiPaxos = make([]*MultiPaxos, nmultipaxos)
  var mpxh []string = make([]string, nmultipaxos)

  for i := 0; i < nmultipaxos; i++ {
    mpxh[i] = port("time", i)
  }
  for i := 0; i < nmultipaxos; i++ {
    mpxa[i] = Make(mpxh, i, nil)
  }
  return mpxa, mpxh
}

func TPrintf(format string, a ...interface{}) (n int, err error) {
  prefix := "\nTest: "
  log.Printf(prefix + format, a...)
  return
}

func TestLeadershipReliable(t *testing.T) {
  runtime.GOMAXPROCS(4)

  const nmultipaxos = 5
  mpxa, _ := setup(nmultipaxos)
  defer cleanup(mpxa)

  TPrintf("Converge on 1 Leader w/ Highest ID ...\n")
  passed1 := false
  const iters = 5 * nmultipaxos
  for i := 0; i < iters; i++ {
    leaders := 0
    highestIDLeading := false
    for id, mpx := range mpxa {
      err := mpx.Push(0, DeepString{Str: "hello"})
      if err == Nil {
        leaders += 1
        if id == nmultipaxos - 1 {
          highestIDLeading = true
        }
      }
    }
    if leaders == 1 && highestIDLeading {
      passed1 = true
      break
    }
    time.Sleep(250 * time.Millisecond)
  }
  if !passed1 {
    t.Fatalf("didn't converge on 1 leader w/ highest id in time")
  }
  fmt.Printf("  ... Passed\n")

  TPrintf("Converge on new leader when old leader dies ...\n")
  leader := mpxa[nmultipaxos-1]
  leader.Kill()
  passed2 := false
  for i := 0; i < iters; i++ {
    leaders := 0
    livingHighestIDLeading := false
    for id, mpx := range mpxa {
      if id != nmultipaxos - 1 { // don't send requests to old, dead leader
        err := mpx.Push(0, DeepString{Str: "hello"})
        if err == Nil {
          leaders += 1
          if id == nmultipaxos - 2 {
            livingHighestIDLeading = true
          }
        }
      }
    }
    if leaders == 1 && livingHighestIDLeading {
      passed2 = true
      break
    }
    time.Sleep(250 * time.Millisecond)
  }
  if !passed2 {
    t.Fatalf("didn't converge on new leader w/ highest living id in time")
  }
  fmt.Printf("  ... Passed\n")
}

func TestConsensusStableReliable(t *testing.T) {
  runtime.GOMAXPROCS(4)

  const nmultipaxos = 3
  mpxa, _ := setup(nmultipaxos)

  TPrintf("Consensus stability w/ stable leader...\n")

  time.Sleep(500*time.Millisecond) // wait for system to converge on leader
  leader := mpxa[nmultipaxos - 1]

  err := leader.Push(0, DeepString{Str: "good"})
  if err != Nil {
    t.Fatalf("did not converge on leader in time")
  }
  fmt.Printf("... waiting on initial decision\n")
  waitn(t, mpxa, 0, nmultipaxos)
  fmt.Printf("...intial decision reached\n")
  _, val1 := leader.Status(0)

  leader.Push(0, DeepString{Str: "bad"})
  time.Sleep(1000*time.Millisecond)
  waitn(t, mpxa, 0, nmultipaxos)
  decided2, val2 := leader.Status(0)
  if !decided2 {
    t.Fatalf("Consensus is unstable; Now undecided")
  }

  if val1 != val2 {
    t.Fatalf("Consensus is unstable; changed value: %+v -> %+v", val1, val2)
  }
  fmt.Printf("  ... Passed\n")

  cleanup(mpxa)

  mpxa, _ = setup(nmultipaxos)

  TPrintf("Concensus stability w/ leader change...\n")
  time.Sleep(500*time.Millisecond) // wait for system to converge on leader
  leader1 := mpxa[nmultipaxos - 1]

  err1 := leader1.Push(0, DeepString{Str: "did not change"})
  if err1 != Nil {
    t.Fatalf("did not converge on leader in time")
  }
  fmt.Printf("... waiting on initial decision")
  waitn(t, mpxa, 0, nmultipaxos)
  fmt.Printf("... intial decison reached")
  _, v1 := leader1.Status(0)
  leader1.Kill()
  time.Sleep(500*time.Millisecond) // wait for the system to converge on new leader
  leader2 := mpxa[nmultipaxos - 2]
  err2 := leader2.Push(0, DeepString{Str: "changed!"})
  if err2 != Nil {
    t.Fatalf("did not converge on new leader in time")
  }
  time.Sleep(1000*time.Millisecond)
  fmt.Printf("... unstable value propagating")
  decided2, v2 := leader2.Status(0)
  if !decided2 {
    t.Fatalf("Consensus is unstable; Now undecided")
  }
  if v1 != v2 {
    t.Fatalf("Consensus is unstable; changed value: %+v -> %+v", v1, v2)
  }
  fmt.Printf("  ... Passed\n")
  cleanup(mpxa)
}

func TestBasic(t *testing.T) {
  gob.Register(DeepString{})
  runtime.GOMAXPROCS(4)

  const npaxos = 3
  var pxa []*MultiPaxos = make([]*MultiPaxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)

  for i := 0; i < npaxos; i++ {
    pxh[i] = port("basic", i)
  }
  for i := 0; i < npaxos; i++ {
    pxa[i] = Make(pxh, i, nil)
  }

  fmt.Printf("Test: Single proposer ...\n")

  start(pxa, 0, DeepString{Str: "hello"})

  waitn(t, pxa, 0, npaxos)

  fmt.Printf("  ... Passed\n")

  //fmt.Printf("Test: Many proposers, same value ...\n")
  //
  // for i := 0; i < npaxos; i++ {
  //   pxa[i].Start(1, 77)
  // }
  // waitn(t, pxa, 1, npaxos)
  //
  // fmt.Printf("  ... Passed\n")
  //
  //fmt.Printf("Test: Many proposers, different values ...\n")
  //
  // pxa[0].Start(2, 100)
  // pxa[1].Start(2, 101)
  // pxa[2].Start(2, 102)
  // waitn(t, pxa, 2, npaxos)
  //
  // fmt.Printf("  ... Passed\n")
  //
  //fmt.Printf("Test: Out-of-order instances ...\n")
  //
  // pxa[0].Start(7, 700)
  // pxa[0].Start(6, 600)
  // pxa[1].Start(5, 500)
  // waitn(t, pxa, 7, npaxos)
  // pxa[0].Start(4, 400)
  // pxa[1].Start(3, 300)
  // waitn(t, pxa, 6, npaxos)
  // waitn(t, pxa, 5, npaxos)
  // waitn(t, pxa, 4, npaxos)
  // waitn(t, pxa, 3, npaxos)
  //
  // if pxa[0].Max() != 7 {
  //   t.Fatalf("wrong Max()")
  // }

  //fmt.Printf("  ... Passed\n")
}

/*
func TestDeaf(t *testing.T) {
  runtime.GOMAXPROCS(4)

  const npaxos = 5
  var pxa []*MultiPaxos = make([]*MultiPaxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)

  for i := 0; i < npaxos; i++ {
    pxh[i] = port("deaf", i)
  }
  for i := 0; i < npaxos; i++ {
    pxa[i] = Make(pxh, i, nil)
  }

  fmt.Printf("Test: Deaf proposer ...\n")

  start(pxa, 0, DeepString{Str:"hello"})
  waitn(t, pxa, 0, npaxos)

  os.Remove(pxh[0])
  os.Remove(pxh[npaxos-1])

  time.Sleep(1 * time.Second) // let system converge to new leaders
  start(pxa, 1, DeepString{Str:"goodbye"})
  waitmajority(t, pxa, 1)
  time.Sleep(1 * time.Second)
  if ndecided(t, pxa, 1) != npaxos - 2 {
    t.Fatalf("a deaf peer heard about a decision")
  }

  start(pxa, 1, DeepString{Str: "xxx"})
  waitn(t, pxa, 1, npaxos-1)
  time.Sleep(1 * time.Second)
  if ndecided(t, pxa, 1) != npaxos - 1 {
    t.Fatalf("a deaf peer heard about a decision")
  }

  //pxa[npaxos-1].Start(1, "yyy")
  //waitn(t, pxa, 1, npaxos)

  fmt.Printf("  ... Passed\n")
}
*/


/* SUPER COMMENT

func TestForget(t *testing.T) {
  runtime.GOMAXPROCS(4)

  const npaxos = 6
  var pxa []*MultiPaxos = make([]*MultiPaxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)

  for i := 0; i < npaxos; i++ {
    pxh[i] = port("gc", i)
  }
  for i := 0; i < npaxos; i++ {
    pxa[i] = Make(pxh, i, nil)
  }

  fmt.Printf("Test: Forgetting ...\n")

  // initial Min() correct?
  for i := 0; i < npaxos; i++ {
    m := pxa[i].Min()
    if m > 0 {
      t.Fatalf("wrong initial Min() %v", m)
    }
  }

  pxa[0].Start(0, "00")
  pxa[1].Start(1, "11")
  pxa[2].Start(2, "22")
  pxa[0].Start(6, "66")
  pxa[1].Start(7, "77")

  waitn(t, pxa, 0, npaxos)

  // Min() correct?
  for i := 0; i < npaxos; i++ {
    m := pxa[i].Min()
    if m != 0 {
      t.Fatalf("wrong Min() %v; expected 0", m)
    }
  }

  waitn(t, pxa, 1, npaxos)

  // Min() correct?
  for i := 0; i < npaxos; i++ {
    m := pxa[i].Min()
    if m != 0 {
      t.Fatalf("wrong Min() %v; expected 0", m)
    }
  }

  // everyone Done() -> Min() changes?
  for i := 0; i < npaxos; i++ {
    pxa[i].Done(0)
  }
  for i := 1; i < npaxos; i++ {
    pxa[i].Done(1)
  }
  for i := 0; i < npaxos; i++ {
    pxa[i].Start(8 + i, "xx")
  }
  allok := false
  for iters := 0; iters < 12; iters++ {
    allok = true
    for i := 0; i < npaxos; i++ {
      s := pxa[i].Min()
      if s != 1 {
        allok = false
      }
    }
    if allok {
      break
    }
    time.Sleep(1 * time.Second)
  }
  if allok != true {
    t.Fatalf("Min() did not advance after Done()")
  }

  fmt.Printf("  ... Passed\n")
}

//
// does paxos forgetting actually free the memory?
//
func TestForgetMem(t *testing.T) {
  runtime.GOMAXPROCS(4)

  fmt.Printf("Test: MultiPaxos frees forgotten instance memory ...\n")

  const npaxos = 3
  var pxa []*MultiPaxos = make([]*MultiPaxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)

  for i := 0; i < npaxos; i++ {
    pxh[i] = port("gcmem", i)
  }
  for i := 0; i < npaxos; i++ {
    pxa[i] = Make(pxh, i, nil)
  }

  pxa[0].Start(0, "x")
  waitn(t, pxa, 0, npaxos)

  runtime.GC()
  var m0 runtime.MemStats
  runtime.ReadMemStats(&m0)
  // m0.Alloc about a megabyte

  for i := 1; i <= 10; i++ {
    big := make([]byte, 1000000)
    for j := 0; j < len(big); j++ {
      big[j] = byte('a' + rand.Int() % 26)
    }
    pxa[0].Start(i, string(big))
    waitn(t, pxa, i, npaxos)
  }

  runtime.GC()
  var m1 runtime.MemStats
  runtime.ReadMemStats(&m1)
  // m1.Alloc about 90 megabytes

  for i := 0; i < npaxos; i++ {
    pxa[i].Done(10)
  }
  for i := 0; i < npaxos; i++ {
    pxa[i].Start(11 + i, "z")
  }
  time.Sleep(3 * time.Second)
  for i := 0; i < npaxos; i++ {
    if pxa[i].Min() != 11 {
      t.Fatalf("expected Min() %v, got %v\n", 11, pxa[i].Min())
    }
  }

  runtime.GC()
  var m2 runtime.MemStats
  runtime.ReadMemStats(&m2)
  // m2.Alloc about 10 megabytes

  if m2.Alloc > (m1.Alloc / 2) {
    t.Fatalf("memory use did not shrink enough")
  }

  fmt.Printf("  ... Passed\n")
}

func TestRPCCount(t *testing.T) {
  runtime.GOMAXPROCS(4)

  fmt.Printf("Test: RPC counts aren't too high ...\n")

  const npaxos = 3
  var pxa []*MultiPaxos = make([]*MultiPaxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)

  for i := 0; i < npaxos; i++ {
    pxh[i] = port("count", i)
  }
  for i := 0; i < npaxos; i++ {
    pxa[i] = Make(pxh, i, nil)
  }

  ninst1 := 5
  seq := 0
  for i := 0; i < ninst1; i++ {
    pxa[0].Start(seq, "x")
    waitn(t, pxa, seq, npaxos)
    seq++
  }

  time.Sleep(2 * time.Second)

  total1 := 0
  for j := 0; j < npaxos; j++ {
    total1 += pxa[j].rpcCount
  }

  // per agreement:
  // 3 prepares
  // 3 accepts
  // 3 decides
  expected1 := ninst1 * npaxos * npaxos
  if total1 > expected1 {
    t.Fatalf("too many RPCs for serial Start()s; %v instances, got %v, expected %v",
      ninst1, total1, expected1)
  }

  ninst2 := 5
  for i := 0; i < ninst2; i++ {
    for j := 0; j < npaxos; j++ {
      go pxa[j].Start(seq, j + (i * 10))
    }
    waitn(t, pxa, seq, npaxos)
    seq++
  }

  time.Sleep(2 * time.Second)

  total2 := 0
  for j := 0; j < npaxos; j++ {
    total2 += pxa[j].rpcCount
  }
  total2 -= total1

  // worst case per agreement:
  // Proposer 1: 3 prep, 3 acc, 3 decides.
  // Proposer 2: 3 prep, 3 acc, 3 prep, 3 acc, 3 decides.
  // Proposer 3: 3 prep, 3 acc, 3 prep, 3 acc, 3 prep, 3 acc, 3 decides.
  expected2 := ninst2 * npaxos * 15
  if total2 > expected2 {
    t.Fatalf("too many RPCs for concurrent Start()s; %v instances, got %v, expected %v",
      ninst2, total2, expected2)
  }

  fmt.Printf("  ... Passed\n")
}

//
// many agreements (without failures)
//
func TestMany(t *testing.T) {
  runtime.GOMAXPROCS(4)

  fmt.Printf("Test: Many instances ...\n")

  const npaxos = 3
  var pxa []*MultiPaxos = make([]*MultiPaxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)

  for i := 0; i < npaxos; i++ {
    pxh[i] = port("many", i)
  }
  for i := 0; i < npaxos; i++ {
    pxa[i] = Make(pxh, i, nil)
    pxa[i].Start(0, 0)
  }

  const ninst = 50
  for seq := 1; seq < ninst; seq++ {
    // only 5 active instances, to limit the
    // number of file descriptors.
    for seq >= 5 && ndecided(t, pxa, seq - 5) < npaxos {
      time.Sleep(20 * time.Millisecond)
    }
    for i := 0; i < npaxos; i++ {
      pxa[i].Start(seq, (seq * 10) + i)
    }
  }

  for {
    done := true
    for seq := 1; seq < ninst; seq++ {
      if ndecided(t, pxa, seq) < npaxos {
        done = false
      }
    }
    if done {
      break
    }
    time.Sleep(100 * time.Millisecond)
  }

  fmt.Printf("  ... Passed\n")
}

//
// a peer starts up, with proposal, after others decide.
// then another peer starts, without a proposal.
//
func TestOld(t *testing.T) {
  runtime.GOMAXPROCS(4)

  fmt.Printf("Test: Minority proposal ignored ...\n")

  const npaxos = 5
  var pxa []*MultiPaxos = make([]*MultiPaxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)

  for i := 0; i < npaxos; i++ {
    pxh[i] = port("old", i)
  }

  pxa[1] = Make(pxh, 1, nil)
  pxa[2] = Make(pxh, 2, nil)
  pxa[3] = Make(pxh, 3, nil)
  pxa[1].Start(1, 111)

  waitmajority(t, pxa, 1)

  pxa[0] = Make(pxh, 0, nil)
  pxa[0].Start(1, 222)

  waitn(t, pxa, 1, 4)

  if false {
    pxa[4] = Make(pxh, 4, nil)
    waitn(t, pxa, 1, npaxos)
  }

  fmt.Printf("  ... Passed\n")
}

//
// many agreements, with unreliable RPC
//
func TestManyUnreliable(t *testing.T) {
  runtime.GOMAXPROCS(4)

  fmt.Printf("Test: Many instances, unreliable RPC ...\n")

  const npaxos = 3
  var pxa []*MultiPaxos = make([]*MultiPaxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)

  for i := 0; i < npaxos; i++ {
    pxh[i] = port("manyun", i)
  }
  for i := 0; i < npaxos; i++ {
    pxa[i] = Make(pxh, i, nil)
    pxa[i].unreliable = true
    pxa[i].Start(0, 0)
  }

  const ninst = 50
  for seq := 1; seq < ninst; seq++ {
    // only 3 active instances, to limit the
    // number of file descriptors.
    for seq >= 3 && ndecided(t, pxa, seq - 3) < npaxos {
      time.Sleep(20 * time.Millisecond)
    }
    for i := 0; i < npaxos; i++ {
      pxa[i].Start(seq, (seq * 10) + i)
    }
  }

  for {
    done := true
    for seq := 1; seq < ninst; seq++ {
      if ndecided(t, pxa, seq) < npaxos {
        done = false
      }
    }
    if done {
      break
    }
    time.Sleep(100 * time.Millisecond)
  }

  fmt.Printf("  ... Passed\n")
}

func pp(tag string, src int, dst int) string {
  s := "/var/tmp/824-"
  s += strconv.Itoa(os.Getuid()) + "/"
  s += "px-" + tag + "-"
  s += strconv.Itoa(os.Getpid()) + "-"
  s += strconv.Itoa(src) + "-"
  s += strconv.Itoa(dst)
  return s
}

func cleanpp(tag string, n int) {
  for i := 0; i < n; i++ {
    for j := 0; j < n; j++ {
      ij := pp(tag, i, j)
      os.Remove(ij)
    }
  }
}

func part(t *testing.T, tag string, npaxos int, p1 []int, p2 []int, p3 []int) {
  cleanpp(tag, npaxos)

  pa := [][]int{p1, p2, p3}
  for pi := 0; pi < len(pa); pi++ {
    p := pa[pi]
    for i := 0; i < len(p); i++ {
      for j := 0; j < len(p); j++ {
        ij := pp(tag, p[i], p[j])
        pj := port(tag, p[j])
        err := os.Link(pj, ij)
        if err != nil {
          // one reason this link can fail is if the
          // corresponding MultiPaxos peer has prematurely quit and
          // deleted its socket file (e.g., called px.Kill()).
          t.Fatalf("os.Link(%v, %v): %v\n", pj, ij, err)
        }
      }
    }
  }
}

func TestPartition(t *testing.T) {
  runtime.GOMAXPROCS(4)

  tag := "partition"
  const npaxos = 5
  var pxa []*MultiPaxos = make([]*MultiPaxos, npaxos)
  defer cleanup(pxa)
  defer cleanpp(tag, npaxos)

  for i := 0; i < npaxos; i++ {
    var pxh []string = make([]string, npaxos)
    for j := 0; j < npaxos; j++ {
      if j == i {
        pxh[j] = port(tag, i)
      } else {
        pxh[j] = pp(tag, i, j)
      }
    }
    pxa[i] = Make(pxh, i, nil)
  }
  defer part(t, tag, npaxos, []int{}, []int{}, []int{})

  seq := 0

  fmt.Printf("Test: No decision if partitioned ...\n")

  part(t, tag, npaxos, []int{0,2}, []int{1,3}, []int{4})
  pxa[1].Start(seq, 111)
  checkmax(t, pxa, seq, 0)

  fmt.Printf("  ... Passed\n")

  fmt.Printf("Test: Decision in majority partition ...\n")

  part(t, tag, npaxos, []int{0}, []int{1,2,3}, []int{4})
  time.Sleep(2 * time.Second)
  waitmajority(t, pxa, seq)

  fmt.Printf("  ... Passed\n")

  fmt.Printf("Test: All agree after full heal ...\n")

  pxa[0].Start(seq, 1000) // poke them
  pxa[4].Start(seq, 1004)
  part(t, tag, npaxos, []int{0,1,2,3,4}, []int{}, []int{})

  waitn(t, pxa, seq, npaxos)

  fmt.Printf("  ... Passed\n")

  fmt.Printf("Test: One peer switches partitions ...\n")

  for iters := 0; iters < 20; iters++ {
    seq++

    part(t, tag, npaxos, []int{0,1,2}, []int{3,4}, []int{})
    pxa[0].Start(seq, seq * 10)
    pxa[3].Start(seq, (seq * 10) + 1)
    waitmajority(t, pxa, seq)
    if ndecided(t, pxa, seq) > 3 {
      t.Fatalf("too many decided")
    }

    part(t, tag, npaxos, []int{0,1}, []int{2,3,4}, []int{})
    waitn(t, pxa, seq, npaxos)
  }

  fmt.Printf("  ... Passed\n")

  fmt.Printf("Test: One peer switches partitions, unreliable ...\n")


  for iters := 0; iters < 20; iters++ {
    seq++

    for i := 0; i < npaxos; i++ {
      pxa[i].unreliable = true
    }

    part(t, tag, npaxos, []int{0,1,2}, []int{3,4}, []int{})
    for i := 0; i < npaxos; i++ {
      pxa[i].Start(seq, (seq * 10) + i)
    }
    waitn(t, pxa, seq, 3)
    if ndecided(t, pxa, seq) > 3 {
      t.Fatalf("too many decided")
    }

    part(t, tag, npaxos, []int{0,1}, []int{2,3,4}, []int{})

    for i := 0; i < npaxos; i++ {
      pxa[i].unreliable = false
    }

    waitn(t, pxa, seq, 5)
  }

  fmt.Printf("  ... Passed\n")
}

func TestLots(t *testing.T) {
  runtime.GOMAXPROCS(4)

  fmt.Printf("Test: Many requests, changing partitions ...\n")

  tag := "lots"
  const npaxos = 5
  var pxa []*MultiPaxos = make([]*MultiPaxos, npaxos)
  defer cleanup(pxa)
  defer cleanpp(tag, npaxos)

  for i := 0; i < npaxos; i++ {
    var pxh []string = make([]string, npaxos)
    for j := 0; j < npaxos; j++ {
      if j == i {
        pxh[j] = port(tag, i)
      } else {
        pxh[j] = pp(tag, i, j)
      }
    }
    pxa[i] = Make(pxh, i, nil)
    pxa[i].unreliable = true
  }
  defer part(t, tag, npaxos, []int{}, []int{}, []int{})

  done := false

  // re-partition periodically
  ch1 := make(chan bool)
  go func() {
    defer func(){ ch1 <- true }()
    for done == false {
      var a [npaxos]int
      for i := 0; i < npaxos; i++ {
        a[i] = (rand.Int() % 3)
      }
      pa := make([][]int, 3)
      for i := 0; i < 3; i++ {
        pa[i] = make([]int, 0)
        for j := 0; j < npaxos; j++ {
          if a[j] == i {
            pa[i] = append(pa[i], j)
          }
        }
      }
      part(t, tag, npaxos, pa[0], pa[1], pa[2])
      time.Sleep(time.Duration(rand.Int63() % 200) * time.Millisecond)
    }
  }()

  seq := 0

  // periodically start a new instance
  ch2 := make(chan bool)
  go func () {
    defer func() { ch2 <- true } ()
    for done == false {
      // how many instances are in progress?
      nd := 0
      for i := 0; i < seq; i++ {
        if ndecided(t, pxa, i) == npaxos {
          nd++
        }
      }
      if seq - nd < 10 {
        for i := 0; i < npaxos; i++ {
          pxa[i].Start(seq, rand.Int() % 10)
        }
        seq++
      }
      time.Sleep(time.Duration(rand.Int63() % 300) * time.Millisecond)
    }
  }()

  // periodically check that decisions are consistent
  ch3 := make(chan bool)
  go func() {
    defer func() { ch3 <- true }()
    for done == false {
      for i := 0; i < seq; i++ {
        ndecided(t, pxa, i)
      }
      time.Sleep(time.Duration(rand.Int63() % 300) * time.Millisecond)
    }
  }()

  time.Sleep(20 * time.Second)
  done = true
  <- ch1
  <- ch2
  <- ch3

  // repair, then check that all instances decided.
  for i := 0; i < npaxos; i++ {
    pxa[i].unreliable = false
  }
  part(t, tag, npaxos, []int{0,1,2,3,4}, []int{}, []int{})
  time.Sleep(5 * time.Second)

  for i := 0; i < seq; i++ {
    waitmajority(t, pxa, i)
  }

  fmt.Printf("  ... Passed\n")
}

END OF SUPER COMMENT*/

/*
TODO: rewrite this to work with multipaxos
func noTestSpeed(t *testing.T) {
  runtime.GOMAXPROCS(4)

  const npaxos = 3
  var pxa []*MultiPaxos = make([]*MultiPaxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)

  for i := 0; i < npaxos; i++ {
    pxh[i] = port("time", i)
  }
  for i := 0; i < npaxos; i++ {
    pxa[i] = Make(pxh, i, nil)
  }

  t0 := time.Now()

  for i := 0; i < 20; i++ {
    pxa[0].Push(i, "x")
    waitn(t, pxa, i, npaxos)
  }

  d := time.Since(t0)
  fmt.Printf("20 agreements %v seconds\n", d.Seconds())
}
*/
