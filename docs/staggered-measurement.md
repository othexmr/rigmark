# Reading staggered measurements

The optional staggered suite is a controlled scheduler-interference experiment,
not a production traffic model. The standard suite and historical receipts remain
separate from `rigmark-application-replay.1`.

Report both directions: a long arrival during short streams, and short arrivals
during a long prefill. Keep absolute TTFT, solo baseline, incumbent delivery gaps,
output during the arrival window, long-request TTFT and every individual round.
A submitted level is not a GPU-active decoder count. A four-slot server tested
at level six includes queued load. An open SSE connection can have stopped
producing output; version-2 evidence records both observations separately.

The legacy arrival-window gap includes inter-event gaps whose ending event falls
inside the window. It can omit a pause spanning the complete window. New evidence
reports the longest intersecting gap and the portion clipped to the window.
Silence between the last event and stream completion is reported separately as
terminal silence. None of these is GPU step time or exact token ITL. Event counts
are not token counts; see token-delivery.md for optional token accounting.

Use a new `--comparison-id` for each independent cold experiment, shared between
matched arms. Sampling `--seed` is not a prompt cache-busting namespace. Reusing
an ID across separate invocations can reuse the same prefix. Verify observed
prefill duration and, where available, cached-token counters. Run-isolation is
not a cache flush. Retain invalid-overlap rounds; don't count an arrival after
prefill ended as an interference measurement. Check real output caps fit the
advertised context, alongside the prompt.

Use several rounds and independently crossed boots. Analyse pair, level and
direction separately; rounds within a boot are not independent boot replicates.
A single worst sample does not establish a latency-tail regression or its cause.
Report unknowns and invalid rounds alongside results, rather than removing an
outlier after seeing which arm it favours. Fixed arrival offsets measure specific
phases; application replay offers independent arrivals and think-time follow-ups
when that workload is the intended question.

Metrics version 2 is explicit in settings. The receipt validator recomputes
new evidence and summaries from raw clocks; comparisons reject mixed metric
versions. Old unstaggered receipts remain comparable and old staggered fields
retain their original meaning. Content hashes identify bytes, not authenticity.
