# Queue service review exercise

Clients submit request IDs, may cancel before or after dispatch, and expect one
terminal outcome per accepted submission. An external worker can finish after
a cancellation. Duplicate IDs are possible after a client reconnects.

The example queue has no persistence, locking, admission limit or request-ID
validation. These are declared limitations, not measured bottlenecks. Decide
which invariants are needed before claiming that a change improves service.
There are no performance measurements in this exercise.
