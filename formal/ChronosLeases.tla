------------------------------ MODULE ChronosLeases ------------------------------
EXTENDS FiniteSets, Naturals

CONSTANTS Tasks, Workers, Completions, NoWorker, MaxCredit, MaxFence

ASSUME /\ Tasks # {}
       /\ Workers # {}
       /\ Completions # {}
       /\ IsFiniteSet(Tasks)
       /\ IsFiniteSet(Workers)
       /\ IsFiniteSet(Completions)
       /\ NoWorker \notin Workers
       /\ MaxCredit \in Nat \ {0}
       /\ MaxFence \in Nat \ {0}

CompletionOutcomes == {"none", "accepted", "rejected"}

VARIABLES credits, inflight, leaseActive, leaseWorker, fence, expiredFence,
          accepted, acceptedFence, completionOutcome, presentedFence

vars == <<credits, inflight, leaseActive, leaseWorker, fence, expiredFence,
          accepted, acceptedFence, completionOutcome, presentedFence>>

Init ==
    /\ credits = [w \in Workers |-> MaxCredit]
    /\ inflight = [w \in Workers |-> 0]
    /\ leaseActive = [t \in Tasks |-> FALSE]
    /\ leaseWorker = [t \in Tasks |-> NoWorker]
    /\ fence = [t \in Tasks |-> 0]
    /\ expiredFence = [t \in Tasks |-> 0]
    /\ accepted = [t \in Tasks |-> {}]
    /\ acceptedFence = [t \in Tasks |-> 0]
    /\ completionOutcome = [t \in Tasks |-> "none"]
    /\ presentedFence = [t \in Tasks |-> 0]

SetCredit(w, amount) ==
    /\ amount \in 0..MaxCredit
    /\ amount >= inflight[w]
    /\ credits' = [credits EXCEPT ![w] = amount]
    /\ UNCHANGED <<inflight, leaseActive, leaseWorker, fence, expiredFence,
                    accepted, acceptedFence, completionOutcome, presentedFence>>

Dispatch(t, w) ==
    /\ accepted[t] = {}
    /\ ~leaseActive[t]
    /\ inflight[w] < credits[w]
    /\ fence[t] < MaxFence
    /\ leaseActive' = [leaseActive EXCEPT ![t] = TRUE]
    /\ leaseWorker' = [leaseWorker EXCEPT ![t] = w]
    /\ fence' = [fence EXCEPT ![t] = @ + 1]
    /\ inflight' = [inflight EXCEPT ![w] = @ + 1]
    /\ UNCHANGED <<credits, expiredFence, accepted, acceptedFence,
                    completionOutcome, presentedFence>>

Expire(t) ==
    /\ leaseActive[t]
    /\ leaseActive' = [leaseActive EXCEPT ![t] = FALSE]
    /\ leaseWorker' = [leaseWorker EXCEPT ![t] = NoWorker]
    /\ expiredFence' = [expiredFence EXCEPT ![t] = fence[t]]
    /\ inflight' = [inflight EXCEPT ![leaseWorker[t]] = @ - 1]
    /\ UNCHANGED <<credits, fence, accepted, acceptedFence,
                    completionOutcome, presentedFence>>

AcceptCompletion(t, w, submittedFence, completion) ==
    /\ leaseActive[t]
    /\ leaseWorker[t] = w
    /\ submittedFence = fence[t]
    /\ accepted[t] = {}
    /\ accepted' = [accepted EXCEPT ![t] = {completion}]
    /\ acceptedFence' = [acceptedFence EXCEPT ![t] = submittedFence]
    /\ completionOutcome' = [completionOutcome EXCEPT ![t] = "accepted"]
    /\ presentedFence' = [presentedFence EXCEPT ![t] = submittedFence]
    /\ leaseActive' = [leaseActive EXCEPT ![t] = FALSE]
    /\ leaseWorker' = [leaseWorker EXCEPT ![t] = NoWorker]
    /\ inflight' = [inflight EXCEPT ![w] = @ - 1]
    /\ UNCHANGED <<credits, fence, expiredFence>>

RejectCompletion(t, w, submittedFence) ==
    /\ \/ ~leaseActive[t]
       \/ leaseWorker[t] # w
       \/ submittedFence # fence[t]
       \/ accepted[t] # {}
    /\ completionOutcome' = [completionOutcome EXCEPT ![t] = "rejected"]
    /\ presentedFence' = [presentedFence EXCEPT ![t] = submittedFence]
    /\ UNCHANGED <<credits, inflight, leaseActive, leaseWorker, fence,
                    expiredFence, accepted, acceptedFence>>

Next ==
    \/ \E w \in Workers, amount \in 0..MaxCredit : SetCredit(w, amount)
    \/ \E t \in Tasks, w \in Workers : Dispatch(t, w)
    \/ \E t \in Tasks : Expire(t)
    \/ \E t \in Tasks, w \in Workers, submittedFence \in 0..MaxFence,
          completion \in Completions : AcceptCompletion(t, w, submittedFence, completion)
    \/ \E t \in Tasks, w \in Workers, submittedFence \in 0..MaxFence :
          RejectCompletion(t, w, submittedFence)

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ credits \in [Workers -> 0..MaxCredit]
    /\ inflight \in [Workers -> 0..MaxCredit]
    /\ leaseActive \in [Tasks -> BOOLEAN]
    /\ leaseWorker \in [Tasks -> Workers \union {NoWorker}]
    /\ fence \in [Tasks -> 0..MaxFence]
    /\ expiredFence \in [Tasks -> 0..MaxFence]
    /\ accepted \in [Tasks -> SUBSET Completions]
    /\ acceptedFence \in [Tasks -> 0..MaxFence]
    /\ completionOutcome \in [Tasks -> CompletionOutcomes]
    /\ presentedFence \in [Tasks -> 0..MaxFence]

CreditBounds == \A w \in Workers : inflight[w] <= credits[w]

CreditAccounting ==
    \A w \in Workers :
        inflight[w] = Cardinality({t \in Tasks : leaseActive[t] /\ leaseWorker[t] = w})

LeaseShape ==
    \A t \in Tasks : leaseActive[t] <=> leaseWorker[t] \in Workers

StrictReassignmentFence ==
    \A t \in Tasks :
        /\ expiredFence[t] <= fence[t]
        /\ leaseActive[t] /\ expiredFence[t] > 0 => fence[t] > expiredFence[t]

CurrentFenceOnlyAcceptance ==
    \A t \in Tasks :
        accepted[t] # {} => /\ acceptedFence[t] = fence[t]
                             /\ acceptedFence[t] > 0

PresentedFenceRequired ==
    \A t \in Tasks :
        completionOutcome[t] = "accepted" => presentedFence[t] = fence[t]

AtMostOneLogicalAcceptance ==
    \A t \in Tasks : Cardinality(accepted[t]) <= 1

AcceptedLeaseClosed ==
    \A t \in Tasks : accepted[t] # {} => ~leaseActive[t]

FenceStep == \A t \in Tasks : fence'[t] >= fence[t]
FenceMonotonic == [][FenceStep]_vars

=============================================================================
