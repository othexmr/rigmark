# Application replay (experimental)

This is a separate protocol, `rigmark-application-replay.1`. It does not replace
RigMark's standard suite or merge into its historical scores. It schedules
independent users, complete answers, think time and follow-ups containing the
actual previous answer. It measures serving behaviour, not answer correctness.
The public examples are small authored exercises, not production traces or
32K workloads. Supply representative, shareable source material for your use case.

## Prepare and inspect

```sh
./rigmark replay --trace examples/replay/sessions-6.json
./rigmark prepare-replay --code examples/replay/queue_example.py \
  --document examples/replay/service_notes.md \
  --context examples/replay/queue_example.py \
  --context examples/replay/service_notes.md \
  --direction sessions --users 6 --output my-trace.json
```

These commands send no requests. Review the trace before running: its complete
input text and provenance are retained. Directions are `short-first`,
`long-first`, and `sessions`. Context is never padded to claim a token depth;
server usage reports actual token counts where available.

**Output budgets.** Each turn's `max_tokens` comes from the trace. The defaults are:
- 1,024 for a first turn;
- 1,536 for the long-context review;
- 768 for a follow-up.

A reasoning model can spend such a budget on reasoning alone. The request then ends with `finish_reason: length`,
is recorded as `truncated`, and counts against completion and SLO attainment. On GLM-5.3 at `reasoning_effort: low`,
10 of 12 first turns in one validation run were truncated. Six of them spent the whole budget on reasoning and
returned no visible text.

One task was replaced. "Review this code for a cancellation or concurrency bug" is an open-ended search. All four of
those first turns in the second validation run reasoned past 4,096 tokens without answering. Naming a no-bug outcome
("if you find none, say so and name the riskiest code path") and a 300-word bound did not help: 31 of its 32 first
turns in the third run did the same. Over those two runs, the constructive and descriptive tasks answered in every
case. The first code task now asks for the code's control flow, its entry point, main steps and the state each reads
or writes, in under 300 words. Traces prepared before this change differ in that text; the example traces are
regenerated.

For such models, declare larger budgets with `--max-tokens`, `--long-max-tokens` and `--followup-max-tokens`
(16 to 16,384). `replay-sweep` accepts the first two. The trace records its budgets under
`provenance.output_budgets`. Budgets are part of the workload: compare only runs prepared with the same budgets.

## Execute and compare

```sh
./rigmark replay --trace my-trace.json --base-url http://SERVER:8000 \
  --model MODEL --identity metadata.json --run-id control-boot-1 \
  --output results/replay-control-1 --run
./rigmark compare-replay results/replay-control-1 results/replay-candidate-1
```

Use a fresh output directory and run ID per execution. The identity file is a
user-supplied JSON snapshot; this command does not inspect or attest the live
server. Verify model, checkpoint, engine, topology, cache settings and competing
traffic independently. An unchanged file does not prove an unchanged server.
Authentication reads `OPENAI_API_KEY`; the key and endpoint are not written to
the manifest. Inputs, answers and identity metadata are retained: review them
before publishing any receipts.

Arrivals use a common client clock, with no barrier waiting for incumbents to
finish. Follow-up turns wait for the preceding answer and declared think time.
Failures and capped answers remain in the denominator; a failed turn blocks its
own dependent turns. There are no retries, cache flushes or server lifecycle
commands. `--timeout` bounds HTTP stream reads, including trickled headers.
Operating-system DNS resolution and connection address attempts can take longer;
resolve endpoint connectivity before a timed campaign. `--max-dispatch-lag` (default 0.05 s)
is a client scheduling validity bound, not a server latency target.

`--delivery-tokens usage|ids` adds exact completion-token delivery accounting to every request (see
[token delivery accounting](token-delivery.md)). Run both arms of a comparison with the same mode; the reader refuses
mixed modes.

Optional SLO scoring requires all three second-valued flags:
`--slo-visible`, `--slo-gap`, `--slo-total`. Goodput includes only completed
requests meeting every threshold, with all planned requests in the denominator.

`--slo-basis` chooses what the first-output and gap thresholds time.
- `visible` (the default and the original definition) times answer text only. For a reasoning model, that TTFT
  includes all of the reasoning time.
- `output` also counts reasoning deltas: the first output of any kind, and gaps between any two output deltas. Use
  it for applications that stream the model's reasoning to the user.

The basis is recorded in the manifest's `slo`, and `compare-replay` refuses runs with different bases. `score.json`
reports both first-output distributions (`visible_ttft_s`, `output_ttft_s`) and both gap distributions whatever the
basis.
Delivery gaps are client-visible event gaps, not GPU steps or token inter-arrival
times. Missing token timelines stay unknown; SSE chunks are never counted as
tokens. Sample p95/p99 need at least 20/100 observations and are not population
confidence bounds. Review individual requests and achieved overlap, not only
aggregate scores.

`examples/replay/quality-16.json` is a small **quality sanity set**, not an accuracy benchmark. It has 16
deterministic-answer tasks (arithmetic, code tracing, conversions, counting), each checked for one exact
`ANSWER: <value>` line. `tests/test_replay_quality.py` re-derives every expected answer in Python.

Two tasks were revised after the first real-server run:
- q03 reversed a word letter by letter. That failed once in three runs on a healthy server (a tokenization
  artefact), so it now reverses a word order.
- q13 asked for "digits only". That conflicted with the ANSWER line, and the model once replied with the bare
  number, so the prompt now says the digits go in the ANSWER line.

The letter-count task q08 is the next candidate for the same artefact. A single miss in one run is therefore weak
evidence; misses that repeat across matched runs are what the set exists to catch. `score.json` reports
`content_checks_pass_fraction` over every planned request that declares checks, counting blocked and failed
requests as misses. Its purpose is to catch gross output breakage between matched runs, for example a numerics or
synchronisation bug that still produces fluent text. A failed check marks the request `invalid_answer`, so the run
exits with status 2 (`COMPLETED_WITH_FAILURES_NO_RETRY`); that is expected for a sanity set.

`run-isolated` adds a shared namespace per run. This reduces accidental reuse
between runs but is not proof of coldness. `natural` preserves shared prefixes.
Neither cache policy promises exact hit rates. Record cached-token counters
when evaluating a cache claim. Use repeated, crossed boots and retain every round;
a single comparison supplies no significance or promotion decision.

The comparison reader verifies saved trace and identity hashes, request coverage,
and summaries recomputed from raw request receipts. Hashes detect inconsistent
receipts; they do not attest genuine measurements. Both arms must share the
trace, runner, request settings, deadline and client/SLO bounds. Models and
identity may differ for an explicitly labelled appliance comparison. Actual
answers and therefore follow-up context can differ.

## Arrival-rate sweep (open loop)

```sh
./rigmark prepare-replay --code CODE --document DOC --context CTX1 --context CTX2 \
  --direction open-loop --rate 0.2 --duration 120 --seed 1 --output rate-0.2.json
./rigmark replay-sweep --code CODE --document DOC --context CTX1 --context CTX2 \
  --rates 0.05,0.1,0.2,0.4 --duration 120 --seed 1 --base-url http://SERVER:8000 \
  --model MODEL --identity metadata.json --output results/sweep-1 \
  --slo-visible 10 --slo-gap 2 --slo-total 120 --target 0.9
```

Open-loop traces are single-turn arrivals from a seeded Poisson process. One unit-rate exponential sequence is
scaled by 1 / rate, so every rate replays the same prompts in the same order with only the clock compressed.
`--long-every N` makes every Nth arrival the long-context review (0 = none). The bounded client caps a trace at 128
arrivals; lower rate x duration if it refuses.

`replay-sweep` runs the rates in ascending order, each as an ordinary replay receipt under `rate-R/`, which
`compare-replay` can re-verify. It writes `sweep.json` with, per rate:
- completion;
- SLO attainment over all planned requests;
- goodput;
- visible-TTFT median and p95;
- client schedule validity.

`max_rate_meeting_target_per_s` is the highest rate whose attainment reaches `--target`, with every lower rate also
reaching it. A non-monotone curve is flagged (`monotone: false`), not smoothed. SLO thresholds are required. The
sweep stops after a rate whose completion fraction falls below `--stop-below` (default 0.5) and never retries.

## Receipt files

- `manifest.json`, `trace.json`, `identity.json`: exact inputs and protocol settings.
- `request-*.json`: each planned turn, actual clocks, output, usage and status.
- `score.json`: overall and category summaries with achieved overlap.
- `terminal.json`: completion/failure, client validity and client CPU seconds, without automatic retry.
- `sweep.json` (sweep only): the per-rate table and the monotone capacity estimate.

CPU and loopback tests qualify the collector logic only. They do not qualify its
compatibility or measurement overhead on a particular inference server.
