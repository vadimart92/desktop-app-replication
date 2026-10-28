# Review: docs / comments / tests vs code

## Context
User asked for analysis of whether design docs, README, inline comments and tests match functionality. Read-only audit by 3 agents; nothing run, nothing edited. Proposed follow-up = fix list below (execute only if approved). Confidence: H verified in code, M inferred.
`src/plans/rework-instance-snapshots.md` deleted by user, findings from it dropped (it was implemented/obsolete anyway).

## 1. Docs

### src/README.md
- :71-72 (H) `AddSyncGrpcService()` / `AddSyncKestrelLimits()` do not exist. Real: `AddGrpc(...).ConfigureSyncGrpcServiceOptions()` + `kestrel.ConfigureSyncKestrelServerOptions()` (`Owner/OwnerHostingExtensions.cs:25,38`; see `Sample.Lab/OwnerHost.cs:37-55`).
- :7 (M) `SyncService` -> class is internal `SyncGrpcService`.
- :86 (L) `InstallClientStore()` is on `IServiceProvider`, not `IServiceCollection`.
- :9,:11 (L) "17 scenarios": `Scenarios.All` has 18 (`free` has no steps).
- :98-103 "where design differs" list incomplete (see design section).

### design/sync-design.md — stale / wrong
- Queue "transferred" into new snapshot (:594, :731, :737, :769, and :43/:19 "unsent survive replica reload"): code DROPS outbox on `InstallSnapshot` / `ResetReplicaAsync`. Doc self-contradicts with 10.3 (:627).
- :731/:737 snapshot after schema change: not so. `SchemaMismatch` only turns link off; client resumes with old cursors (`SyncAgent.cs:381-386`; `schema` scenario).
- :122 BLOB mask for >63 cols: code throws `NotSupportedException` (`SyncModel.cs:44`).
- `_sync_meta` DDL lacks `clock_offset`; `_sync_tombstones` lacks `origin`; `_sync_outbox.pk` shown NOT NULL, real nullable; `_sync_notes` table missing; `sent` 0/1/2 states undefined (:142-156, :215, :262-305).
- :259/:803 cloned owner "will not load": code deletes other instances' live rows with same Id and loads; archives win via `INSERT OR IGNORE` (`ReplicaWriter.cs:222-225`).
- :361 snapshot install SQL description simplified (also deletes outbox + other-instance same-Id rows, uses OR IGNORE).
- :360 temp snapshot file deleted on completion: actually only by `Cleanup` on next `CreateAsync`, 3h keep.
- :367 sequential snapshot downloads: not implemented. (also sim-review #5)
- :376 floor recompute throttled: recomputed every Ack.
- :392 `Progress` per-table remaining: owner sends `Progress{Done}` once; estimate is `Batch.remaining` (single total, undocumented).
- :424 10 000 segment cap: none in code (`CursorState.AddRange`); :767 segment-limit test absent.
- :436 ~64KB min batch / `extra` tail: code uses `CatchupBatchRows=500`, tail in same Batch.
- :438 BDP in-flight cap: not implemented.
- :497/:797 noisy-table throttle config: none; `DataChanged` fires per batch.
- :508-509 WriteRouter captures `ExecuteUpdate/ExecuteDelete`: WriteRouter is `SaveChangesInterceptor` only. Bulk = `DeleteWhereAsync`.
- :543 "bulk" class includes import/ExecuteUpdate: only `DeleteWhereAsync` + archive are `Bulk`; SaveChanges is always `Interactive`.
- Archive (:461,:556,:560,:655-656): doc = predicate; code = key set (`rows[]` + `expected_version`, `ApplyHandler.cs:279-283`). `Rejected: changed` does not exist (returns Applied with `changed[]`, `children[]`). `children` undocumented. `echo` reason undocumented.
- :726 crash during snapshot -> resume file: `_download` is in-memory; `.part` orphaned after restart.
- :241-243 in-app "restore from backup" with new instance_id: not present; :239/:770 2^31 counter test: absent.
- :344-350 handshake table omits `minC > head` -> SnapshotRequired.
- Missing: `AgentState.Duplicate` + one-agent-per-instance guard (README only), "Connecting" state, :26 "one client per owner" outdated; `_sync_columns`/`RemapMasks`, `.syncstate` file, snapshot dir/options, `OnlineBatchRows`, `Revert`/NeedFull, `SkipSnapshotOnEmptyReplica` fallback, complex-type flattening counts toward 63 mask, AgentOptions names.
- Header date/status (:3 vs :41) inconsistent.

### design/sim-findings-review.md
Normative-sounding but decisions not followed: BDP (#1), "two Apply in flight, taking" vs design deferring (#2), option A vs chosen B, "8.6" is 8.5, predicate archive, ~64KB (#3), auto ETA snapshot rule + sequential (#5), segment limit 8 + "fast table read from owner" (#6), "three RPCs" now four (#7), dead artifact paths. Suggest banner "historical" or update.

### plans
- `archive-findings.md`: valid, unimplemented; refs dead `simple-snapshot-import.md`; point 3 now actual behaviour.
- `online-step-with-priorities.md`: unimplemented, line refs stale (`SubscribeSession.cs:219->221`, `SyncAgent.cs:473->552`, `:464->543`, `ClientNode.cs:40->52`), test numbering off, `openfirst` scenario absent.
- `plans/rpc-detailed-errors.md`: unimplemented; refs stale (`SyncAgent.cs:571,597 -> 640,666`, `:279 -> 285`, `AgentViewModel.cs:136 -> 137,147`, `SubscribeSession.cs:51 -> 50`).

## 2. Inline comments
Wrong / stale:
- `ClientStore.cs:61` orphan `<param foreignKeys>` above `_holders`; belongs on `Open` (:70) (H).
- `OwnerHostingExtensions.cs:40` stray `</summary>` in `//` comment (H).
- `ClientStore.cs:90` `_sync_instances` row "appears with first snapshot": created at adoption (`SyncAgent.cs:442-449`) (M-H).
- `SyncAgent.cs:849-864` ResetReplica "empties replica": keeps `X:archive` rows (M-H).
- `ReplicaWriter.cs:17` `NeedFull` doc incomplete (`Revert`:191, `ArchiveLeftovers`:1091).
- `ReplicaWriter.cs:96-100` NULL-InstanceId row with same Id also triggers NeedFull; undocumented. `:8-12` "every statement carries InstanceId" vs unfiltered SELECT and `InstallSnapshot` deleting by Id.
- `SyncAgent.cs:57` "one per owner instance": per address, instance dynamic. `:136` HasPendingArchiveWork says outbox rows, is in-memory `_archiveRetry`.
- `WriteRouter.cs:228` `ModifiedProperties` yields all properties.
- `SnapshotStore.cs:31-33` "same operation" registration is separate steps (small purge window).
- `ApplyBatchBuilder.cs:99` omitted action: seq assigned + sent=1, may stall if highest seq / all omitted.
- `TriggerSql.cs:53` vs `SyncAgent.cs:788`/`ApplyHandler.cs:147` "no echo": second consecutive patch from same client resets SyncOrigin to NULL -> echo. Logic vs comment mismatch; trigger line uncommented.
- `Sample.Lab/Lab.cs:128-131` vs `TwoOwnerLab.IsQuiet` inconsistent; `Scenarios.cs:203` "Journal last" but `StockObject` now after Log.
- `WriteRouter.cs:274` literal `"local"` in error text -> "own data". `_sync_local` table name misleading (uncommented).
- Proto: `reserved 8` no note; `SnapshotChunk.instance_id`, `Progress.owner_time_unix`, `ActionResult.deleted` never read by client; `SnapshotRequired.instance_id` undocumented though drives duplicate/swap logic.
No TODO/FIXME anywhere. No stale `'local'` InstanceId string.

Uncommented non-obvious logic: `SnapshotAsync` sets `_instance` w/o `Claim`/ch.InstanceId check (possible guard hole, `SyncAgent.cs:818-847`); magic numbers `sent<2`, `kind IN (3,5)` (use enums); `Duplicate` sets `_instance=null`; `ApplyHandler.cs:60-64` unknown-table Archive silently skipped; `ReplicationModelBuilderExtensions.cs:25` owned-type skip; `Wire.ToValue` DateTime via invariant string; `InstallSnapshot` OR IGNORE drops snapshot row colliding with own (NULL) row; `WriteRouter.cs:195-209` opened/own pairing.

## 3. Tests
Misnamed / weak:
- `EmptyReplicaTests:68-96` asserts only `SubsetOf` -> vacuous (use `EquivalentTo`); `:30-65` duplicate `SkipSnapshotOnEmptyReplicaTests`; `Is.True` on comparisons (also FkClosure:96).
- `CursorCommitTests:12-38` doesn't prove rows+cursor roll back together; `:40-49` belongs in `CursorStateTests`.
- `IgnoredReplyTests` don't assert `Ignored` path was taken (just rows gone).
- `ApplyHandlerTests` class doc says Apply (6.5) but only `Unique`.
- `PurgeTests` "in batches" can't observe batching; test 3 timing-flaky.
- `ScenarioTests` `free` scenario asserts nothing; `Rows.Diff` ignores InstanceId/SyncBase/SyncMask -> wrong mask/base undetectable.
- `ConflictTests` #5 "two clients editing" is sequential, no conflict; echo reason never asserted.
- `ComplexTypeTests` `Snapshot_carries_the_complex_type` doesn't verify snapshot; no member-edit test.
- `SnapshotDecisionTests:25` negative assertion only.
- `RecoveryTests:66-70` `StartClient()` already sets Link=true -> "InstanceId null before link" is racy.
- `TwoOwnerTests` (Lab, regex-log) duplicates `TwoOwnerCoreTests` -> drop. `Row_pointing_to_another_source_is_refused` overclaims "no transaction left behind" (CheckSources throws before tx opens).
- `ArchiveRetryTests` race probabilistic; tries>=3 never hit. `OwnerPanelViewModelTests` tests Sample.App in Core test project. `ColumnMaskTests` in OwnerVersioningTests file; `Node/Dept/Emp` helpers in FkClosureTests. `SchemaMigrationTests` one 100-line mega-test, one direction only. Rig/test temp dirs not always cleaned; ClearAllPools under parallel fixtures (L-M); missing finally-dispose in HookUp/AgentName tests.
- Timing-dependent: TwoOwnerCore reconnect (150ms delay), `One_owner_offline` race, DuplicateAgent 1.5s negative wait.
No stale names from snapshots refactor / complex types / duplicate guard / NULL-InstanceId.

Missing cases (priority):
- P1: complex-type member edit (owner trigger partial mask `TriggerSql.cs:563`, client `ModifiedProperties`, client-created StockObject, null/nested); ReplicaWriter same-Id-across-owners (rows lost while cursor advances, `ReplicaWriter.cs:81,223,225`); Revert/NeedFull when owner purged tombstone (`SubscribeSession.cs:804-808`); cursor+rows atomic rollback; WriteRouter failure paths (concurrency on delete, cancel after own tx, app-owned tx, restore Deleted on failed archive, sync `SaveChanges()`); handshake reasons (other instance_id, minC>head, SchemaMismatch raw, non-Start first msg); snapshot multi-chunk/resume/sha mismatch/truncate/sweep.
- P2 owner Apply: unknown table/column, duplicate seq retry, create vs tombstone, rerun/rollback cascade, archive Kept.Gone/Changed/Children, PredicateDelete changed/expected_version, echo reason.
- P3 client: `ClientStore.Put` matrix, `ApplyBatchBuilder` byte-budget split / unsent-create FK / row gone, `DeleteConfirmed` sent=2 no-resurrect, `HandleAsync` order, Lag warning, backoff/lost reply/ApplyTimeout, `ArchiveAsync` guards, SetSchemaVersion owner-older direction, MarkSubscribed overlap, AsyncSignal, housekeeping service, ShapedStream/WireMeter.
- P4: SyncModel >63 cols (63/64), composite FK, owned types, cycles; trigger edge (re-insert removes tombstone, no-op update, cascade origin); Install branches (auto_vacuum, RemapMasks drop/rename, corrupt version file); Wire edge types; Predicate JSON round-trip; Purge floor/guard/monotonic; CursorState Gaps/Reset/odd RangeDeltas/lo>hi.

## 4. Possible real bugs surfaced (verify before fixing)
1. `SnapshotAsync` skips `Claim` -> duplicate guard hole on mid-download owner swap.
2. Same-Id rows from second owner silently ignored while cursors advance.
3. Rejected-edit dead end when owner has neither row nor tombstone.
4. Trigger origin reset -> self-echo on consecutive patches.
5. ApplyBatchBuilder omitted-action stall.
6. Possible empty `catch { }` in `SyncHousekeepingJobsService.StopAsync:278` vs slopwatch gate.

## Proposed follow-up (if approved)
1. Docs: fix README API names; update design sections listed (queue dropped, schema change, DDL, archive-as-keyset, Progress/remaining, Duplicate state); mark unimplemented features as "not implemented" or remove; banner sim-review as historical; refresh plan line refs.
2. Comments: fix items in section 2; add comments to trigger origin rule and proto fields.
3. Tests: fix vacuous/misnamed tests, delete `TwoOwnerTests` duplicate, relocate misfiled; add P1 cases first.
4. Triage bugs in section 4 separately.

Verification: `dotnet build` + `dotnet test` in `src/`; grep doc API names against code.
