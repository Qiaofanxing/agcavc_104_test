# Realtime Value Tabs Implementation Plan

> **For agentic workers:** Execute inline in the current session. The user explicitly opted out of TDD; add regression tests after the implementation and run the full verification suite.

**Goal:** Make tabs 6 and 7 display current values that refresh as protocol/domain events arrive.

**Architecture:** Preserve the existing complete-interrogation snapshots and add separate dispatch-current collections. The dispatch task emits parsed point updates for eligible incoming measurement ASDUs; the coordinator merges them by IOA and compatible point family, and the TUI renders those collections.

**Tech Stack:** Rust, Tokio watch/mpsc channels, iec104 0.4, Ratatui.

## Global Constraints

- Keep complete general/counter interrogation snapshots atomic.
- Ignore CA-mismatched, test, and negative measurement frames for current values.
- Treat timestamped and untimestamped single/double indications as compatible current-value families.
- Do not parse log strings to recover protocol values.
- Do not use TDD; verify after implementation.

---

### Task 1: Add dispatch-current runtime state

**Files:**
- Modify: `src/model.rs`
- Modify: `src/runtime.rs`

**Interfaces:**
- Produce: `RuntimeEvent::DispatchValues { kind: SnapshotKind, values: Vec<PointView> }`
- Produce: `AppSnapshot::dispatch_current_general` and `AppSnapshot::dispatch_current_energy`

- [x] Add the event and snapshot fields.
- [x] Initialize the new collections and preserve configured values during point reload.
- [x] Merge updates by IOA and compatible TypeID family without modifying complete snapshots.

### Task 2: Publish current values from incoming dispatch ASDUs

**Files:**
- Modify: `src/protocol/dispatch.rs`

**Interfaces:**
- Consume: `rows_from_asdu(&Asdu) -> Vec<PointView>`
- Produce: `RuntimeEvent::DispatchValues`

- [x] Pass the configured common address into incoming-ASDU handling.
- [x] Emit general or energy current-value events for supported, non-test, non-negative measurement ASDUs with matching CA.
- [x] Leave the existing interrogation-round accumulation and ACTTERM snapshot commit unchanged.

### Task 3: Render both tabs as realtime current values

**Files:**
- Modify: `src/ui.rs`

- [x] Label tab 6 as a realtime collection-current table.
- [x] Render tab 7 from the new dispatch-current collections with realtime titles.
- [x] Update all `AppSnapshot` test fixtures for the new fields.

### Task 4: Regression verification

**Files:**
- Modify: `src/protocol/dispatch.rs`
- Modify: `src/runtime.rs`
- Modify: `src/ui.rs`

- [x] Add tests for spontaneous updates without a total-call round.
- [x] Add tests for SOE/current-value compatible merging and snapshot isolation.
- [x] Add rendering assertions for both realtime titles.
- [x] Run `cargo test --locked`.
- [x] Run `cargo fmt --all -- --check`.
- [x] Run `cargo clippy --all-targets --all-features --locked -- -D warnings`.
