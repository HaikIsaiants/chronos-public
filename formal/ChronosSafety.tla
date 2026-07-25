------------------------------ MODULE ChronosSafety ------------------------------
EXTENDS FiniteSets, Naturals

CONSTANTS Tasks, Dependencies, Completions, Attempts, NoAttempt

ASSUME /\ Tasks # {}
       /\ Completions # {}
       /\ Attempts # {}
       /\ NoAttempt \notin Attempts
       /\ IsFiniteSet(Tasks)
       /\ IsFiniteSet(Completions)
       /\ IsFiniteSet(Attempts)
       /\ Dependencies \subseteq Tasks \X Tasks

TaskStates == {"pending", "ready", "running", "completed", "failed", "cancelled"}
WorkflowStates == {"running", "completed", "failed"}
TerminalWorkflowStates == WorkflowStates \ {"running"}

VARIABLES taskState, accepted, activeAttempt, acceptedAttempt, workflowState
vars == <<taskState, accepted, activeAttempt, acceptedAttempt, workflowState>>

NoPredecessor(t) == ~\E p \in Tasks : <<p, t>> \in Dependencies
PredecessorsCompleted(t) == \A p \in Tasks : <<p, t>> \in Dependencies => taskState[p] = "completed"
DiamondDependencies == {<<"t0", "t1">>, <<"t0", "t2">>, <<"t1", "t3">>, <<"t2", "t3">>}

Init ==
    /\ taskState = [t \in Tasks |-> IF NoPredecessor(t) THEN "ready" ELSE "pending"]
    /\ accepted = [t \in Tasks |-> {}]
    /\ activeAttempt = [t \in Tasks |-> NoAttempt]
    /\ acceptedAttempt = [t \in Tasks |-> NoAttempt]
    /\ workflowState = "running"

Ready(t) ==
    /\ workflowState = "running"
    /\ taskState[t] = "pending"
    /\ PredecessorsCompleted(t)
    /\ taskState' = [taskState EXCEPT ![t] = "ready"]
    /\ UNCHANGED <<accepted, activeAttempt, acceptedAttempt, workflowState>>

Start(t, attempt) ==
    /\ workflowState = "running"
    /\ taskState[t] = "ready"
    /\ taskState' = [taskState EXCEPT ![t] = "running"]
    /\ activeAttempt' = [activeAttempt EXCEPT ![t] = attempt]
    /\ UNCHANGED <<accepted, acceptedAttempt, workflowState>>

Complete(t, attempt, completion) ==
    /\ workflowState = "running"
    /\ taskState[t] = "running"
    /\ activeAttempt[t] = attempt
    /\ accepted[t] = {}
    /\ taskState' = [taskState EXCEPT ![t] = "completed"]
    /\ accepted' = [accepted EXCEPT ![t] = {completion}]
    /\ activeAttempt' = [activeAttempt EXCEPT ![t] = NoAttempt]
    /\ acceptedAttempt' = [acceptedAttempt EXCEPT ![t] = attempt]
    /\ UNCHANGED workflowState

Fail(t, attempt) ==
    /\ workflowState = "running"
    /\ taskState[t] = "running"
    /\ activeAttempt[t] = attempt
    /\ taskState' = [x \in Tasks |->
           IF x = t THEN "failed"
           ELSE IF taskState[x] \in {"completed", "failed"} THEN taskState[x]
           ELSE "cancelled"]
    /\ activeAttempt' = [x \in Tasks |-> NoAttempt]
    /\ workflowState' = "failed"
    /\ UNCHANGED <<accepted, acceptedAttempt>>

FinishCompleted ==
    /\ workflowState = "running"
    /\ \A t \in Tasks : taskState[t] = "completed"
    /\ workflowState' = "completed"
    /\ UNCHANGED <<taskState, accepted, activeAttempt, acceptedAttempt>>

Next ==
    \/ \E t \in Tasks : Ready(t)
    \/ \E t \in Tasks, attempt \in Attempts : Start(t, attempt)
    \/ \E t \in Tasks, attempt \in Attempts, completion \in Completions : Complete(t, attempt, completion)
    \/ \E t \in Tasks, attempt \in Attempts : Fail(t, attempt)
    \/ FinishCompleted

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ taskState \in [Tasks -> TaskStates]
    /\ accepted \in [Tasks -> SUBSET Completions]
    /\ activeAttempt \in [Tasks -> Attempts \union {NoAttempt}]
    /\ acceptedAttempt \in [Tasks -> Attempts \union {NoAttempt}]
    /\ workflowState \in WorkflowStates

DependencySafety == \A t \in Tasks : taskState[t] \in {"ready", "running", "completed"} => PredecessorsCompleted(t)
ActiveAttemptConsistency == \A t \in Tasks : (taskState[t] = "running") <=> (activeAttempt[t] \in Attempts)
AtMostOneAccepted == \A t \in Tasks : Cardinality(accepted[t]) <= 1
AcceptedCompletionConsistency == \A t \in Tasks : accepted[t] # {} => /\ taskState[t] = "completed"
                                                                    /\ acceptedAttempt[t] \in Attempts
TerminalQuiescence == workflowState \in TerminalWorkflowStates => \A t \in Tasks : taskState[t] \in {"completed", "failed", "cancelled"}
TerminalStep == (workflowState \in TerminalWorkflowStates) => UNCHANGED vars
TerminalMonotonic == [][TerminalStep]_vars

=============================================================================
