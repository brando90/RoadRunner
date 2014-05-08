//TODO: implement RoadRunner kv servers here
/*
Catches up from the localMin to the highest local min known to this server.
Remember, only local mins guarantee that a decision has been made, so to guarantee correctness,
its better to just catch up to the maxKnownMin (largest known localMin).
Make sure mpx.maxKnownMin  and mpx.localMin has already been updated (process PB).
*/
func (mpx *MultiPaxos) catchUp(){
  for !mpx.dead{
    for seq := mpx.localMin; seq <= mpx.maxKnownMin; seq++ { //while not caught up
      //TODO: OPTIMIZATION: we could query all the learners first before doing a "normal" px prepare.
      decided, decidedVal := mpx.Status(seq)
      if !decided{
        decidedVal := mpx.propose(seq , INFINITY) //TODO: choose globally highest round-number, change name? weird that the prepare func. returns the decision. propose returns decision
        if decidedVal == nil{
          panic("If we are querying a value that was already decide, the val should be some OP not NIL!")
        }
      }
    }
  }
}
