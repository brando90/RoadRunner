prepare_epoch(e, seq):
    send 1 prepare rpc to each server in the group, telling them to prepare all sequences >= seq with round number e 

prepare_epoch_handler(e, seq):
    if e > current_epoch:
        current_epoch = e
    for each acceptor from seq to the highest sequence number we know of:
        process prepare with round number e
        record response (ok/reject, n_a, v_a) in response map (mapping sequence numbers to their responses)
    reply with response map 
    //NB: we must take care to immediately prepare any newly initialized acceptors with seq number >= seq with round number e (saved in current_epoch)

tick: // called periodically
    ping all servers
    if a server has not responded to our pings for longer than twice the ping interval:
        consider them dead
    if I have the largest id amongst servers that I consider living:
        act as new leader (increment epoch/round number)
    else:
        catch_up
    
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
        call status to see if decision is available on the server's local learner
        if not yet decided:
            prepare NOP with a special, globally highest round number
            wait for decision...
            execute and persist decision to KV
            proceed to next sequence number

persistence:
    before an acceptor replies to a proposer, it must persist changes to its acceptor state
    // before the rpc handler returns, ensure state changes have persisted (in prepare handler, and accept handler)
