------------------------------ MODULE ChronosWorkflow ------------------------------
EXTENDS FiniteSets, Naturals, Sequences

CONSTANTS Tasks, ChildIDs, Generations, CompTasks, NoComp, CompletionOrder

ASSUME /\ Tasks # {}
       /\ ChildIDs # {}
       /\ Generations # {}
       /\ IsFiniteSet(Tasks)
       /\ IsFiniteSet(ChildIDs)
       /\ IsFiniteSet(Generations)
       /\ IsFiniteSet(CompTasks)
       /\ NoComp \notin CompTasks
       /\ Len(CompletionOrder) = Cardinality(CompTasks)
       /\ {CompletionOrder[i] : i \in 1..Len(CompletionOrder)} = CompTasks

TimerStates == {"scheduled", "fired", "cancelled"}
FireOutcomes == {"none", "accepted", "rejected"}
AttemptOutcomes == {"none", "completed", "timed_out"}
AggregateStates == {"pending", "ready", "completed"}
WorkflowStates == {"running", "compensating", "completed", "cancelled", "compensated", "failed"}
TerminalStates == {"completed", "cancelled", "compensated", "failed"}
SampleCompletionOrder == <<"deploy", "publish">>

VARIABLES timerState, timerGeneration, presentedGeneration, fireOutcome,
          attemptOutcome, children, childDone, expansionCount, expansionClosed,
          aggregateState, workflowState, normalStarted, compensated, activeComp

vars == <<timerState, timerGeneration, presentedGeneration, fireOutcome,
          attemptOutcome, children, childDone, expansionCount, expansionClosed,
          aggregateState, workflowState, normalStarted, compensated, activeComp>>

Init ==
    /\ timerState = [t \in Tasks |-> "scheduled"]
    /\ timerGeneration = [t \in Tasks |-> 1]
    /\ presentedGeneration = [t \in Tasks |-> 0]
    /\ fireOutcome = [t \in Tasks |-> "none"]
    /\ attemptOutcome = [t \in Tasks |-> "none"]
    /\ children = {}
    /\ childDone = {}
    /\ expansionCount = [c \in ChildIDs |-> 0]
    /\ expansionClosed = FALSE
    /\ aggregateState = "pending"
    /\ workflowState = "running"
    /\ normalStarted = {}
    /\ compensated = {}
    /\ activeComp = NoComp

ScheduleTimer(t, generation) ==
    /\ workflowState = "running"
    /\ attemptOutcome[t] = "none"
    /\ generation \in Generations
    /\ generation > timerGeneration[t]
    /\ timerState' = [timerState EXCEPT ![t] = "scheduled"]
    /\ timerGeneration' = [timerGeneration EXCEPT ![t] = generation]
    /\ fireOutcome' = [fireOutcome EXCEPT ![t] = "none"]
    /\ UNCHANGED <<presentedGeneration, attemptOutcome, children, childDone,
                    expansionCount, expansionClosed, aggregateState, workflowState,
                    normalStarted, compensated, activeComp>>

Complete(t) ==
    /\ workflowState = "running"
    /\ attemptOutcome[t] = "none"
    /\ attemptOutcome' = [attemptOutcome EXCEPT ![t] = "completed"]
    /\ timerState' = [timerState EXCEPT ![t] = "cancelled"]
    /\ UNCHANGED <<timerGeneration, presentedGeneration, fireOutcome, children,
                    childDone, expansionCount, expansionClosed, aggregateState,
                    workflowState, normalStarted, compensated, activeComp>>

FireTimeout(t, generation) ==
    /\ workflowState = "running"
    /\ timerState[t] = "scheduled"
    /\ attemptOutcome[t] = "none"
    /\ generation = timerGeneration[t]
    /\ timerState' = [timerState EXCEPT ![t] = "fired"]
    /\ attemptOutcome' = [attemptOutcome EXCEPT ![t] = "timed_out"]
    /\ presentedGeneration' = [presentedGeneration EXCEPT ![t] = generation]
    /\ fireOutcome' = [fireOutcome EXCEPT ![t] = "accepted"]
    /\ UNCHANGED <<timerGeneration, children, childDone, expansionCount,
                    expansionClosed, aggregateState, workflowState, normalStarted,
                    compensated, activeComp>>

RejectTimer(t, generation) ==
    /\ generation \in Generations
    /\ workflowState \notin TerminalStates
    /\ \/ timerState[t] # "scheduled"
       \/ generation # timerGeneration[t]
       \/ attemptOutcome[t] # "none"
       \/ workflowState # "running"
    /\ presentedGeneration' = [presentedGeneration EXCEPT ![t] = generation]
    /\ fireOutcome' = [fireOutcome EXCEPT ![t] = "rejected"]
    /\ UNCHANGED <<timerState, timerGeneration, attemptOutcome, children,
                    childDone, expansionCount, expansionClosed, aggregateState,
                    workflowState, normalStarted, compensated, activeComp>>

Expand(c) ==
    /\ workflowState = "running"
    /\ ~expansionClosed
    /\ c \notin children
    /\ children' = children \union {c}
    /\ expansionCount' = [expansionCount EXCEPT ![c] = @ + 1]
    /\ UNCHANGED <<timerState, timerGeneration, presentedGeneration, fireOutcome,
                    attemptOutcome, childDone, expansionClosed, aggregateState,
                    workflowState, normalStarted, compensated, activeComp>>

CloseExpansion ==
    /\ workflowState = "running"
    /\ ~expansionClosed
    /\ expansionClosed' = TRUE
    /\ UNCHANGED <<timerState, timerGeneration, presentedGeneration, fireOutcome,
                    attemptOutcome, children, childDone, expansionCount,
                    aggregateState, workflowState, normalStarted, compensated,
                    activeComp>>

CompleteChild(c) ==
    /\ workflowState = "running"
    /\ c \in children \ childDone
    /\ childDone' = childDone \union {c}
    /\ UNCHANGED <<timerState, timerGeneration, presentedGeneration, fireOutcome,
                    attemptOutcome, children, expansionCount, expansionClosed,
                    aggregateState, workflowState, normalStarted, compensated,
                    activeComp>>

ReadyAggregate ==
    /\ workflowState = "running"
    /\ expansionClosed
    /\ childDone = children
    /\ aggregateState = "pending"
    /\ aggregateState' = "ready"
    /\ UNCHANGED <<timerState, timerGeneration, presentedGeneration, fireOutcome,
                    attemptOutcome, children, childDone, expansionCount,
                    expansionClosed, workflowState, normalStarted, compensated,
                    activeComp>>

CompleteAggregate ==
    /\ workflowState = "running"
    /\ aggregateState = "ready"
    /\ aggregateState' = "completed"
    /\ UNCHANGED <<timerState, timerGeneration, presentedGeneration, fireOutcome,
                    attemptOutcome, children, childDone, expansionCount,
                    expansionClosed, workflowState, normalStarted, compensated,
                    activeComp>>

StartNormal(t) ==
    /\ workflowState = "running"
    /\ t \notin normalStarted
    /\ normalStarted' = normalStarted \union {t}
    /\ UNCHANGED <<timerState, timerGeneration, presentedGeneration, fireOutcome,
                    attemptOutcome, children, childDone, expansionCount,
                    expansionClosed, aggregateState, workflowState, compensated,
                    activeComp>>

Cancel ==
    /\ workflowState = "running"
    /\ workflowState' = IF Len(CompletionOrder) = 0 THEN "cancelled" ELSE "compensating"
    /\ timerState' = [t \in Tasks |-> IF timerState[t] = "scheduled" THEN "cancelled" ELSE timerState[t]]
    /\ UNCHANGED <<timerGeneration, presentedGeneration, fireOutcome,
                    attemptOutcome, children, childDone, expansionCount,
                    expansionClosed, aggregateState, normalStarted, compensated,
                    activeComp>>

StartCompensation(t) ==
    /\ workflowState = "compensating"
    /\ activeComp = NoComp
    /\ Cardinality(compensated) < Len(CompletionOrder)
    /\ t = CompletionOrder[Len(CompletionOrder) - Cardinality(compensated)]
    /\ activeComp' = t
    /\ UNCHANGED <<timerState, timerGeneration, presentedGeneration, fireOutcome,
                    attemptOutcome, children, childDone, expansionCount,
                    expansionClosed, aggregateState, workflowState, normalStarted,
                    compensated>>

FinishCompensation(t) ==
    /\ workflowState = "compensating"
    /\ activeComp = t
    /\ compensated' = compensated \union {t}
    /\ activeComp' = NoComp
    /\ workflowState' = IF Cardinality(compensated') = Len(CompletionOrder) THEN "compensated" ELSE workflowState
    /\ UNCHANGED <<timerState, timerGeneration, presentedGeneration, fireOutcome,
                    attemptOutcome, children, childDone, expansionCount,
                    expansionClosed, aggregateState, normalStarted>>

CompleteWorkflow ==
    /\ workflowState = "running"
    /\ aggregateState = "completed"
    /\ \A t \in Tasks : attemptOutcome[t] = "completed"
    /\ workflowState' = "completed"
    /\ UNCHANGED <<timerState, timerGeneration, presentedGeneration, fireOutcome,
                    attemptOutcome, children, childDone, expansionCount,
                    expansionClosed, aggregateState, normalStarted, compensated,
                    activeComp>>

Next ==
    \/ \E t \in Tasks, generation \in Generations : ScheduleTimer(t, generation)
    \/ \E t \in Tasks : Complete(t)
    \/ \E t \in Tasks, generation \in Generations : FireTimeout(t, generation)
    \/ \E t \in Tasks, generation \in Generations : RejectTimer(t, generation)
    \/ \E c \in ChildIDs : Expand(c)
    \/ CloseExpansion
    \/ \E c \in ChildIDs : CompleteChild(c)
    \/ ReadyAggregate
    \/ CompleteAggregate
    \/ \E t \in Tasks : StartNormal(t)
    \/ Cancel
    \/ \E t \in CompTasks : StartCompensation(t)
    \/ \E t \in CompTasks : FinishCompensation(t)
    \/ CompleteWorkflow

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ timerState \in [Tasks -> TimerStates]
    /\ timerGeneration \in [Tasks -> Nat]
    /\ presentedGeneration \in [Tasks -> Nat]
    /\ fireOutcome \in [Tasks -> FireOutcomes]
    /\ attemptOutcome \in [Tasks -> AttemptOutcomes]
    /\ children \subseteq ChildIDs
    /\ childDone \subseteq ChildIDs
    /\ expansionCount \in [ChildIDs -> Nat]
    /\ expansionClosed \in BOOLEAN
    /\ aggregateState \in AggregateStates
    /\ workflowState \in WorkflowStates
    /\ normalStarted \subseteq Tasks
    /\ compensated \subseteq CompTasks
    /\ activeComp \in CompTasks \union {NoComp}

CurrentTimerOnly ==
    \A t \in Tasks : fireOutcome[t] = "accepted" =>
        /\ presentedGeneration[t] = timerGeneration[t]
        /\ timerState[t] = "fired"

CompletionTimeoutExclusive ==
    \A t \in Tasks :
        /\ attemptOutcome[t] = "completed" => timerState[t] = "cancelled"
        /\ attemptOutcome[t] = "timed_out" => timerState[t] = "fired"

FanoutUnique == \A c \in ChildIDs : expansionCount[c] <= 1
FanoutShape == childDone \subseteq children

AggregateGate == aggregateState \in {"ready", "completed"} =>
    /\ expansionClosed
    /\ childDone = children

CancellationGate == workflowState # "running" =>
    \A t \in Tasks : timerState[t] # "scheduled"

CompensatedSuffix ==
    compensated = {CompletionOrder[i] : i \in (Len(CompletionOrder) - Cardinality(compensated) + 1)..Len(CompletionOrder)}

StrictReverseCompensation ==
    /\ CompensatedSuffix
    /\ activeComp # NoComp =>
        /\ Cardinality(compensated) < Len(CompletionOrder)
        /\ activeComp = CompletionOrder[Len(CompletionOrder) - Cardinality(compensated)]

NoNormalStartStep == workflowState # "running" => UNCHANGED normalStarted
NoNormalStartsAfterExit == [][NoNormalStartStep]_vars

TerminalStep == workflowState \in TerminalStates => UNCHANGED vars
TerminalMonotonic == [][TerminalStep]_vars

=============================================================================
