# Historical compile-only inline validation

This records the earlier compile-only checkpoint. Superseded by [fresh runtime validation](runtime-validation.md) on 2026-10-12: 770 Maven tests, browser3 and Python78 PASS. Production remains NOT_QUALIFIED.

Final 12-module compile

```text
[INFO] Reactor Summary for signaling-parent 0.1.0-SNAPSHOT:
[INFO]
[INFO] signaling-parent ................................... SUCCESS [  0.039 s]
[INFO] signaling-protocol ................................. SUCCESS [  7.748 s]
[INFO] signaling-observability ............................ SUCCESS [  0.242 s]
[INFO] signaling-auth ..................................... SUCCESS [  0.520 s]
[INFO] signaling-storage .................................. SUCCESS [  2.612 s]
[INFO] signaling-actors ................................... SUCCESS [  2.282 s]
[INFO] signaling-control-api .............................. SUCCESS [  0.561 s]
[INFO] signaling-cell-rpc ................................. SUCCESS [  1.894 s]
[INFO] signaling-gateway .................................. SUCCESS [  1.031 s]
[INFO] signaling-app ...................................... SUCCESS [  1.558 s]
[INFO] signaling-integration-tests ........................ SUCCESS [  0.068 s]
[INFO] signaling-loadgen .................................. SUCCESS [  0.659 s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  20.131 s
[INFO] Finished at: 2026-10-12T11:59:42+07:00
[INFO] ------------------------------------------------------------------------
```

Scoped changed-Java Spotless check

```text
[INFO] Reactor Summary for signaling-parent 0.1.0-SNAPSHOT:
[INFO]
[INFO] signaling-parent ................................... SUCCESS [  0.800 s]
[INFO] signaling-protocol ................................. SUCCESS [  0.950 s]
[INFO] signaling-observability ............................ SUCCESS [  0.054 s]
[INFO] signaling-auth ..................................... SUCCESS [  0.037 s]
[INFO] signaling-storage .................................. SUCCESS [  0.294 s]
[INFO] signaling-actors ................................... SUCCESS [  0.043 s]
[INFO] signaling-control-api .............................. SUCCESS [  0.026 s]
[INFO] signaling-cell-rpc ................................. SUCCESS [  0.049 s]
[INFO] signaling-gateway .................................. SUCCESS [  0.039 s]
[INFO] signaling-app ...................................... SUCCESS [  0.057 s]
[INFO] signaling-integration-tests ........................ SUCCESS [  0.028 s]
[INFO] signaling-loadgen .................................. SUCCESS [  0.031 s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  3.392 s
[INFO] Finished at: 2026-10-12T11:59:26+07:00
[INFO] ------------------------------------------------------------------------
```

Browser native-client.mjs, media-recovery-controller.mjs, durable-command-journal.mjs: node --check PASS. git diff --check PASS.

Full verify: pre-existing observability formatting violations. Scoped verify: missing storage test-jar while maven.test.skip=true prevents packaging. Both failures are recorded in the ledger; neither is reported PASS.

GitNexus all: 88 changed symbols, 51 flows, CRITICAL (before final formatting; includes existing worktree changes). compare(main): 6614 symbols, 298 flows, CRITICAL for the release branch. New classes unindexed/UNKNOWN, reviewed directly. No new commit/push.
