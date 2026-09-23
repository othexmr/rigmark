# Completion-token delivery accounting (opt-in)

An SSE event may contain several accepted speculative tokens. `event_seconds`
continues to timestamp text/reasoning delivery for TTFT and freeze analysis. Its
length is an event count, never a token count.

For a separately identified staggered run, use `--delivery-tokens usage`. This
adds `stream_options.continuous_usage_stats=true` to requests whose timelines
are recorded. It records cumulative completion-usage differences at client
receipt time. `--delivery-tokens ids` instead requests `return_token_ids=true`
and counts each choice's delta IDs, including token-bearing chunks with no text.
IDs mode may also return a large prompt-ID list; usage is preferable when the
server supports it. Neither option changes requests without recorded timelines.
Default `off` preserves the current stream requests and timing method.

Each stream gains `token_delivery`: event times, exact nonnegative counts, mode,
scope and status. The total must match final `completion_tokens`. Missing chunk
metadata, decreasing counters, malformed IDs or a mismatch produce UNAVAILABLE
with null counts, not an inferred distribution. Final-only usage cannot recover
when tokens were delivered. Old receipts stay unavailable. The collector does
not retokenize chunks, divide final usage by the event count or assume one token
per chunk. No automatic retry is performed when a server rejects the option.

`stall.arrival_window_completion_tokens` sums reconciled counts in the half-open
interval [newcomer arrival, newcomer first output). It describes **completion
tokens reported by the server at delivery**, including reasoning/control tokens;
it is not a count of visible words or independently timed generated tokens.
Existing text-event freeze and TTFT fields retain their definitions.

Source support was checked in the prepared vLLM OpenAI chat/completion serving
code: both endpoints emit per-chunk usage when continuous stats are requested;
optional token IDs are delta output IDs. This is source evidence, not a probe of
any current server image. An owner must first check that the actual image emits
reconcilable counts. Run matched arms with the same option and inspect overhead
before comparing them with historical uninstrumented timing. Receipt settings
record `delivery_token_accounting`; source identity also includes the new module.

This change is prepared on the local RigMark fork. It does not modify the active
benchmark checkout or any queued campaign.

The comparison gate rejects different delivery-accounting modes even at the same
source revision; absent settings in older receipts mean `off`.
