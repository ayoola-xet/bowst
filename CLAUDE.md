# Engineering Standards — Bowst

These rules apply to every change in this repository, from anyone. Bowst trades real capital, so a bug here is a financial incident. The architecture and the reasons behind it are in [`README.md`](README.md). This file covers how code gets written.

When a rule here conflicts with speed of delivery, the rule wins. When a rule seems wrong, change it in a PR. Do not quietly ignore it.

---

## 1. Non-negotiables

1. **No attribution to tools or assistants, anywhere.** Commit messages, commit trailers (`Co-Authored-By`, `Generated-by` and similar), branch names, PR titles and bodies, code comments, docs, config, binary names and log output must not name any AI tool, assistant or model. Commits use the repository owner's configured git identity.
2. **Money never goes through floating point.** Prices are `i64` ticks and quantities are `i64` lots (see `bowst-core`). `f64` is allowed only inside strategy model math, and converting back to ticks or lots uses an explicit rounding direction.
3. **The risk gate cannot be bypassed.** Order gateways accept only risk-approved order types. No change may add a path around the gate, including for "tests", "manual orders" or "emergencies".
4. **Fail closed.** Unknown state, parse failures, sequence gaps, stale data and reconciliation mismatches must lead to pulling quotes or blocking orders. Never guess and continue.
5. **No secrets in the repo.** No keys, tokens, account IDs or real host names in code, config, tests, fixtures or commit history. Secrets are loaded from the secrets manager at runtime and are never logged.
6. **The hot path stays hot.** The hot-path rules in README §4 (no allocation after warm-up, no locks, no blocking, no formatting) are enforced by tests and benchmarks. A change that breaks them is rejected.

## 2. Design principles

- **DRY, applied with judgment.** Every fact lives in exactly one place: domain types in `bowst-core`, venue mechanics (connections, signing, rate limiting, book sync) in the shared venue layer, limits in the risk config. When two adapters need the same logic, move it into the shared layer; do not copy it. When you are unsure whether two pieces of code are really the same concept, wait for the third case before abstracting. Two things that merely look alike are not duplication.
- **One owner per piece of state.** Each instrument's book, orders and inventory belong to exactly one thread. Other threads get messages, not shared references.
- **Pure core, thin I/O shell.** Strategy, risk rules, the OMS state machine and book logic are pure and deterministic, with no I/O, no clock reads and no randomness. Time and I/O are passed in. That keeps them testable and replayable.
- **Make invalid states unrepresentable.** Use newtypes (`Price`, `Qty`, `ClientOrderId`), enums with data, and typestate where it helps. Avoid naked `i64`, `String` or `bool` flags in public APIs.
- **Explicit over clever.** Readable code that an on-call engineer can follow at 3 a.m. beats a clever trick. Optimize only with a benchmark that shows the gain.
- **No speculative features.** Build what the current phase needs (README §15). Leave room for growth through clean interfaces, not unused code.

## 3. Rust rules

### Workspace lints (set in the root `Cargo.toml`, applied to every crate)
- `unsafe_code = "deny"`. `unsafe` is allowed only in `bowst-core` (ring buffers, clock), each block with a `// SAFETY:` comment explaining why it is sound, covered by tests and run under Miri in CI.
- Clippy: `all` and `pedantic` as warnings, with CI running `-D warnings`. Additionally denied: `unwrap_used`, `expect_used`, `panic`, `todo`, `unimplemented`, `dbg_macro`, `print_stdout`, `print_stderr`, `float_cmp`, and `indexing_slicing` in hot-path crates.
- `missing_docs` on public items in library crates.

### Errors
- Library crates define typed errors with `thiserror`. Binaries may use `anyhow` only for startup and shutdown code.
- No `unwrap()` or `expect()` outside tests. The only exception is truly unreachable invariants, documented with a comment and allowed with a scoped `#[allow]`.
- Errors carry context: which venue, instrument, order ID and sequence number.
- The release profile uses `panic = "abort"`. A panic is a crash, and the venue-side cancel-on-disconnect plus the watchdog handle cleanup. Code must never rely on catching a panic.

### Performance
- Hot-path types are `Copy`, fixed-size and allocation-free. Pre-allocate at startup.
- Every hot-path change includes a criterion benchmark result in the PR description (before and after).
- Do not add `async` to the hot path. Async (tokio) belongs to the control plane only.

### Dependencies
- Every new dependency needs a one-line justification in the PR. Prefer well-maintained crates with few transitive dependencies.
- `cargo deny check` (licenses, advisories, duplicates, sources) and `cargo audit` must pass. `Cargo.lock` is committed.

## 4. Testing

- **Every change ships with tests.** A bug fix starts with a failing regression test.
- **Invariants use property tests** (`proptest`): book never crossed after valid updates, OMS transitions valid for any message order, fixed-point rounding direction, risk rules block every breaching input.
- **Every venue decoder is fuzzed** (`cargo fuzz`). Malformed input must return an error, never panic.
- **Every venue adapter passes the shared conformance suite** before it can be enabled (README §9).
- **Every risk rule has a test proving it blocks.** Risk code changes need a second reviewer.
- Tests are deterministic: no sleeps, no wall-clock reads, no network. Use the simulated clock and recorded venue traffic in `tests/fixtures/`.

## 5. Workflow

- **Branches:** `feat/<short-name>`, `fix/<short-name>`, `perf/<short-name>`, `refactor/<short-name>`, `docs/<short-name>`, `chore/<short-name>`. Never commit directly to `main`.
- **Commits:** Conventional Commits (`feat(book): add sequence-gap resync`). Imperative mood, subject ≤ 72 characters, body explains why. One logical change per commit.
- **PRs:** small and focused. The description states what changed, why, how it was tested, and any risk or latency impact. CI must be green and at least one review is required; two for anything under `bowst-risk`, `bowst-oms` or order entry.
- **Architecture changes** get an ADR in `docs/adr/` and an update to `README.md` in the same PR.

## 6. Definition of done

Run these before every push. CI runs the same set.

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo nextest run --workspace --all-features
cargo test --workspace --doc
cargo deny check
cargo audit
cargo bench -p <changed hot-path crate>   # when the hot path changed
```

A change is done when:
- [ ] All the checks above pass.
- [ ] New behavior has tests, including failure and edge cases.
- [ ] Public items are documented.
- [ ] Hot-path changes include benchmark numbers and show no allocations.
- [ ] Logs and metrics exist for any new failure mode, and a runbook entry exists for any new alert.
- [ ] `README.md`, ADRs and config examples are updated if behavior or architecture changed.
- [ ] No secrets, and no tool or assistant attribution (§1.1).

## 7. Logging and observability

- Structured logs only, with venue, instrument and order IDs as fields. Hot-path code writes fixed-size binary records to a ring buffer, and formatting happens on another thread.
- Log levels: `error` needs a human, `warn` is a degraded but safe state, `info` is a lifecycle event, `debug` and `trace` are off in production.
- Never log API keys, signatures, full request bodies containing credentials, or account balances in plain logs.
- Every alert has a runbook in `docs/runbooks/`.
