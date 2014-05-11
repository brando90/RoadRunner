leader_propose(seq, v):
    send accepts to all acceptors with round number e.
    if we receive any rejects with epoch E;
        then e=E+1, prepare_phase(seq)
    else if we didnt receive any rejects but didnt form a majority;
        then try leader_propose again.
    else; //we got a majority
        move to the decide phase //send decides to all learners.

prepare_epoch_phase(seq):
    send prepare rpc to each server until at least a majority is formed on each sequence >= seq.
    if we received a reject for e cuz E > e, then change epoch to e = E+1;
        then try prepare_epoch_phase again
    //TODO: if we receive a reject:
    //        set epoch number to E+1 (where E is the round number that caused the reject)
    //        make sure we retry prepareEpochPhase (either now or when loop ends (in case we hear an even higher reject during this same attempt))
    //      else if we didn't receive a reject, and we received a majority from all sequences number:
    //        // NB: we only need a majority in one sequence number to guarantee a majority for all sequence numbers in the above condition
    //        phase success!
    //      else: (no rejects but no majority)
    //        try phase again (with same epoch number)

prepare_epoch_handler(e, seq):
    if e > current_epoch:
        current_epoch = e
    for each acceptor from seq to the highest sequence number we know of:
        process prepare with round number e
        record response (ok/reject, n_a, v_a) in response map (mapping sequence numbers to their responses)
    reply with response map
    //NB: we must take care to immediately prepare any newly initialized acceptors with seq number >= seq with round number e (saved in current_epoch)

mpx.tick: // called periodically
    ping all servers
    keep track of highest local min we hear // highest local mins piggy-backed in ping responses
    if a server has not responded to our pings for longer than twice the ping interval:
        consider them dead
    if I have the largest id amongst servers that I consider living:
        act as new leader (increment epoch/round number)
    else:
        relinquish leadership

kv.tick:
    if !leader:
        catch_up to maxLocalMin (because we are behind)
    else:
        //do nothing, we are already waiting on client requests

catch_up:
    catch up to the maximum local min that we know about // since local mins guarantee that every sequence before them has been decided
    // a server can piggy-back its local min (for global min updating -> garbage collection) as well as the highest local min it has heard (to improve our catch up routine)
    while behind the catch up point:
        /*TODO: OPTIMIZATION
            we could query all learners to try to get the decision more efficiently
            then tell all learners who responded that they did not know the decision with the decision just learned by the local learner
            if this fails, continue with normal, catch up routine described below
            we expect to be able to contact a learner who knows the decision in most cases (and it only takes 1 RTT) thus immproving our expected performance
        */
        call status to see if decision is available on the servers local learner
        if not yet decided:
            prepare NOP with a special, globally highest round number
            wait for decision...
            execute and persist decision to KV
            proceed to next sequence number

persistence:
    before an acceptor replies to a proposer, it must persist changes to its acceptor state
    // before the rpc handler returns, ensure state changes have persisted (in prepare handler, and accept handler)

//RecoverFromLocalDisk
Reboot:
    if disk is dead:
        query a majority of localMax, choose max(localMaxes)
        ping for localMin until one localMin > max(localMaxes);
            sleep for a bit if cant find localMin > max(localMaxes) //give time to system to progress
        once localMin > max(localMaxes) == success;
            then, copy everything about them (get their KV and acceptors, localMin, etc)
    else: //disk is not dead
        get the kv from the local disk
        set the multi-paxos state up 
        set ourselves to be live
