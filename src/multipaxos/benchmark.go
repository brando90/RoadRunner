package multipaxos
/*
package multipaxos


type DeepString struct {
  Str string
}

func (dstr DeepString) DeepCopy() DeepCopyable {
  return DeepString{Str: dstr.Str}
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

func waitn(t *testing.T, mpxa[]*MultiPaxos, seq int, wanted int) {
  to := 10 * time.Millisecond
  for iters := 0; iters < 30; iters++ {
    if ndecided(t, mpxa, seq) >= wanted {
      break
    }
    time.Sleep(to)
    if to < time.Second {
      to *= 2
    }
  }
  nd := ndecided(t, mpxa, seq)
  if nd < wanted {
    t.Fatalf("too few decided; seq=%v ndecided=%v wanted=%v", seq, nd, wanted)
  }
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

func start(mpxa []*MultiPaxos, seq int, v DeepCopyable) {
  var leader *MultiPaxos
  for {
    if leader != nil {
      err := leader.Push(seq, v)
      if err != Nil {
        leader = nil
      }
    }else {
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
}

func cleanup(pxa []*MultiPaxos) {
  for i := 0; i < len(pxa); i++ {
    if pxa[i] != nil {
      pxa[i].Kill()
    }
  }
}

func TestBenchmark(t *testing.T) {
  t0 := time.Now()

  const nmultipaxos = 3
  mpxa, _ = setup(nmultipaxos)
  defer cleanup(mpxa)

  const iters = 10000;
  for i := 0; i < iters; i++ {
    stri := DeepString{Str: fmt.Sprintf("v%d", i)}
    start(mpxa, i, stri)
  }
  for i := 0; i < iters; i++ {
    waitn(t, i, nmultipaxos)
  }
  d := time.Since(t0)
  fmt.Printf("%d agreements in %v seconds", iters, d.Seconds())
}
*/
