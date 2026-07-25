# Safety models

- `ChronosSafety` covers command transitions, dependencies, completion, and terminal state.
- `ChronosLeases` covers worker credits, lease expiry, reassignment, and fencing.
- `ChronosWorkflow` covers timers, retries, fan-out, cancellation, and compensation.

Run the models with the TLA+ tools:

```text
java -cp <tla2tools.jar> tlc2.TLC -config formal/ChronosSafety.cfg formal/ChronosSafety.tla
java -cp <tla2tools.jar> tlc2.TLC -config formal/ChronosLeases.cfg formal/ChronosLeases.tla
java -cp <tla2tools.jar> tlc2.TLC -config formal/ChronosWorkflow.cfg formal/ChronosWorkflow.tla
```
