# Realtime Value Scrolling Implementation Plan

> **For agentic workers:** Execute inline in the current session. The user explicitly requested the modification, opted out of TDD, and did not request subagent delegation.

**Goal:** Add bounded mouse-wheel scrolling to the collection-current table and both dispatch-current tables without changing realtime protocol state flow.

**Architecture:** Store three independent offsets in `UiState`. Derive the visible table areas from the same Ratatui layouts used by rendering, clamp offsets against visible row capacity, render point slices from those offsets, and include the collection offset when mapping a click back to a point.

**Tech Stack:** Rust, Crossterm mouse events, Ratatui tables.

## Global Constraints

- Do not change dispatch ASDU filtering, current-value merging, or watch snapshot publication.
- The dispatch general and energy tables scroll independently according to the pointer position.
- Preserve offsets across tab switches and clamp them after terminal resize or point reload.
- Do not use TDD; add and run regression tests after implementation.

---

### Task 1: Add bounded value-table scrolling

**Files:**
- Modify: `src/ui.rs`
- Modify: `docs/superpowers/specs/2026-07-16-realtime-value-tabs-design.md`

**Interfaces:**
- Consume: `MouseEventKind::ScrollUp`, `MouseEventKind::ScrollDown`, `AppSnapshot` point collections, and the existing content layout.
- Produce: `UiState::{collect_value_offset, dispatch_general_offset, dispatch_energy_offset}` plus shared offset-clamping and table-area helpers.

- [x] Add the three offsets to `UiState` and initialize them to zero.
- [x] Route wheel events on tab 6 to the collection table and on tab 7 to the dispatch table beneath the pointer.
- [x] Clamp every offset to `point_count - visible_row_count`, using the table's bordered/header layout.
- [x] Pass offsets into point-table rendering and skip preceding rows.
- [x] Add the effective collection offset when converting a clicked visible row into `collect_values[index]`.
- [x] Add post-implementation tests for scroll direction, boundary clamping, independent dispatch offsets, rendered rows, and scrolled click mapping.
- [x] Run `RUSTC_WRAPPER= cargo test --locked ui::tests:: -- --nocapture`.
- [x] Run `RUSTC_WRAPPER= cargo test --locked` in an environment that permits local TCP bind tests.
- [x] Run `cargo fmt --all -- --check`.
- [x] Run `RUSTC_WRAPPER= cargo clippy --all-targets --all-features --locked -- -D warnings`.

## Verification Result

- UI tests: 9 passed, 0 failed.
- Full suite outside the restricted network sandbox: 47 passed, 0 failed.
- Formatting check: passed.
- Strict Clippy: passed; only Cargo's existing future-incompatibility notice for `iec104 0.4.0` remains.
