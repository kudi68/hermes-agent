WP4 已以 MODELLED fixtures 完成離線整合並通過作者測試 104/104；WP0.5 真實 transport 仍 UNPROVEN，不得當 live PASS。
方案 B 的 r2 M1／M2 已 local commit：5060638；本輪 delta 15 passed／0 skipped／0 failed；real-boot 未跑，SKIPPED_NEEDS_ENV。
三個指定 feature branch 均為 PUSH NOT_EXECUTED：平台政策審查以「最新全域指示禁止 push」拒絕；main 未變，未 merge／開 PR。

# 夜班交接 — WP4 ＋ 方案 B（2026-10-22）

## 0. 授權與邊界
- 全程離線：未讀寫 live DB、未讀 `.env`／secrets／Keychain、未執行 `hermes`、未碰 `~/.hermes`、未用 `sudo`、未動 gateway 程序／launchd／plist／`/Library`。
- 未修改 chief 執行中的 `~/Projects/c1-runtime/hermes-agent-c1-fe6ab90` 工作樹，也未切換它的分支；方案 B 全部在 `c1-planB/` 隔離 worktree。
- N2、N3 未動；HD3 仍 BLOCKED；未實作 Keychain／gateway helper。
- 未撰寫保留給獨立審查的 `conduit-c1-review-r7.md` 與 `c1-planB-review-r1.md`。

## 1. WP4（段 1）

### Commits
| repo | branch | commit | parent |
|---|---|---|---|
| agent-ops | `codex/conduit-c1-wp1-20261022` | `08fc83e1ddcf8dc6249e08175df4f8c08e4c73ef` | `68ec4ec` |
| personal-ai-os | `codex/conduit-c1-wp1-20261022` | `336338c29f8aa2b7bcbdc8bbfca41e1753f7a7b3` | `7294f84` |

### 測試
- 私有 `0700` TMPDIR／假 HOME 一次跑完：`Ran 104 tests ... OK`。
- 分模組：WP1 33、WP2 30、WP3 29、WP4 11、no-I/O guard 1。
- 原 92 個 test method 全保留（AST／`grep` 比對：33→33、29→30、29→29、1→1），新增 12 個（WP2 1、WP4 11）。
- WP4 新增 `tools/conduit_c1_wp4.py` 與 `tools/test_conduit_c1_wp4_unit.py`；只注入 synthetic WP2 store、MODELLED transport、假 32-byte key、in-process workflow。

### 驗收對照
| 設計離開條件 | 證據 |
|---|---|
| 單 intent RECEIVED → result → receipt | `test_received_result_and_receipt_round_trip` |
| 三層重算與 lookup 同值 | `test_three_layer_recomputation_matches_store_readback` |
| replay fail closed 且不重跑 | `test_replay_is_idempotent_and_does_not_rerun_workflow` |
| timeout | `test_timeout_before_admission_writes_no_rows` |
| lost reply | `test_lost_reply_retry_returns_the_original_receipt` |
| conflict | `test_same_event_key_with_different_digest_conflicts` |
| wrong request | `test_wrong_request_fails_closed` |
| 零 raw 值外洩 | `test_public_artifacts_have_no_raw_identity_event_id_or_output` |

### r6 P3
- **N1**：WP2 `_validate_path` 現在先檢查「宣告的 `tempfile.gettempdir()` 路徑」含所有上層皆無 symlink；呼叫端改傳 realpath 也拒絕。新增 `test_rejects_realpath_under_symlinked_tmpdir`；設計 §7 措辭同步收緊。
- **N4**：WP1 新增 `1e400` 頂層與 `intent_id` 欄位案例；WP3 新增 `enabled:1e400` 與 `revision:1e400` 案例。均不依賴非標準 `Infinity` token。
- **N5**：no-I/O guard 在 import 後安裝 CPython audit hook，`open` 事件預設拒絕（allowlist 空集合），可涵蓋 `io.open_code` 等包裝；socket／subprocess／sqlite3／ctypes 等非檔案面仍是列舉式。設計 §7 已寫明「不能形式化證明未來 API」。

### 殘留風險與待決
- **WP0.5 UNPROVEN**：真實 ingress、retry／reconnect stable event ID、conversation binding 沒有本輪證據；WP4 全部 fixtures 都是 MODELLED。
- **結果耐久性不在 WP4**：result 只存在 session 記憶體。新 session 對既有 receipt 會回 `RESULT_UNAVAILABLE`，不重跑 workflow。
- **workflow 失敗窗口**：admission 已 commit 而 workflow 丟錯時，receipt 會留下但沒有 result；重試 fail closed。此為 exactly-once 取捨，WP5 前需決定補償策略。
- **待決（不實作）**：gateway 執行檔身分、Keychain 專用 helper／ACL；HD3 仍 BLOCKED。

## 2. 方案 B — r2 M1／M2 修正與原地部署草稿（2026-10-23）

### 候選與狀態
- 作者／owner：Codex；worktree `c1-planB/`；分支 `codex/c1-planB-20261022`；程式候選 commit `50606383a8b3d7676a347f51fd5a2aee9099264d`。
- 本輪父 commit `7b70af9913c863aef47538de69615b8bdff78f4b`；方案 A 基準 `b6e7d10e8b04fd10e7a1d72563a40c343a11181e`。r2 報告記錄的是較早的未 commit 狀態，本輪以實際 Git 狀態為準。
- F3：C1 identity＋planned marker＋`SIGTERM` 或 watcher 的 `None` 均要求 exit 75；SIGINT、takeover、其他 label 保留原行為。
- F1／M1：`HERMES_TEST_ONLY_ALLOW_C1_GATEWAY_BOOT_IN_TMP_HOME=1` **且** HOME resolve 後嚴格位於 `Path(tempfile.gettempdir()).resolve()` 或 `/private/tmp` 解析後的路徑底下，才略過啟動防呆；排除該根目錄本身與 symlink 逃逸，接受 macOS 預設 TMPDIR。
- real-boot 子程序明確設定該旗標；未替換 guard。其他情境仍要求 ppid=1 與 c1-runtime code root，拒絕 78；C1 label 下拒絕映射 0。
- 本輪只執行經授權的既有 Python；另經 Human 明確例外允許 local commit 寫入共用 `.git` 必要 metadata，未修改 runtime 工作目錄或其他 ref；live 部署、plist 修改、gateway 操作均 NOT_EXECUTED。

### 作者驗證（本輪 delta）
- 未修 production `7b70af9`＋新測試：4 個預期失敗、3 passed，確實攔下 macOS TMPDIR 拒絕、已卸載回滾中止、缺 PID／ppid、先換碼才遇 bootout 權限錯誤；證據 `/private/tmp/c1-planB-r3.gwnnRz/red.log`、`red.xml`。
- 修正後：**15 passed／0 skipped／0 failed**。`tests/hermes_cli/test_c1_runtime_launch_guard.py` 12 案例、`tests/scripts/test_c1_planb_rollback.py` 3 案例；證據 `/private/tmp/c1-planB-r3.gwnnRz/green.log`、`green.xml`。
- macOS `/private/var/folders/…/T` 路徑由 monkeypatch 提供，含子目錄放行、根目錄／同前綴 sibling 拒絕、`/private/tmp` fallback；屬單元證據，不代表 real-boot。
- 回滾測試以假的 launchctl 函式涵蓋「已卸載」「仍載入」，兩者均 bootstrap 一次並印出 PID／ppid；另驗未預期的權限錯誤不被吞掉、也不 checkout。
- harness 把 git／檔案操作／ps 等全部替換為 shell 函式，PATH=/nonexistent，未執行真 launchctl 或讀寫 live plist／runtime 工作目錄。`bash -n scripts/c1-planB-rollback.sh` PASS；實際回滾 NOT_EXECUTED。
- 直譯器：`/Users/kudi68-15mba/Projects/c1-runtime/hermes-agent-c1-fe6ab90/.venv/runtime/bin/python`；import 已確認來自 planB。依本次指示採直接 pytest：`env -i`、PYTHONPATH=planB、HOME／HERMES_HOME／TMPDIR 在 `/private/tmp/c1-planB-r3.gwnnRz/`，`-B -p no:cacheprovider`。
- real-boot **未執行，SKIPPED_NEEDS_ENV**，不列入本輪 pytest 的 skipped 數，也不宣稱通過。
- 上一輪歷史結果（本輪未重跑）：18 檔 569 passed／37 skipped；加 real-boot 所在檔共 576 passed／38 skipped／0 failed。原證據 `/private/tmp/c1-planB-r2.fQcHg2/` 的 suite／retry log 與 XML；當時 9 個 sandbox 失敗只重跑該 9 個並通過。
- 本輪 quota preflight：Codex 5h 14%／7d 2%，低於 85%；估計局部 delta 額外不超過 10pp，START_NOW。未委派。

### 原地部署草稿 — 全部 NOT_EXECUTED，須另取得 live 授權
固定使用原 `hermes-agent-c1-fe6ab90` checkout／既有 launcher；不新增 checkout／venv。以下在同一 bash session 逐段執行，任一步失敗就停。禁止 `reset --hard`、`clean`，保留所有 untracked 檔。

1. 確認現況與新 commit，再備份 plist 並記錄 SHA-256；現況與 r1 不同就停。

```bash
set -euo pipefail
NIGHT=/Users/kudi68-15mba/Projects/night/20261022
ROOT=/Users/kudi68-15mba/Projects/c1-runtime/hermes-agent-c1-fe6ab90
PLIST=/Users/kudi68-15mba/Library/LaunchAgents/local.hermes.c1-chief.plist
PLIST_A="$NIGHT/plist-A/local.hermes.c1-chief.plist"
NEW_COMMIT=50606383a8b3d7676a347f51fd5a2aee9099264d
BASE=b6e7d10e8b04fd10e7a1d72563a40c343a11181e
LABEL=local.hermes.c1-chief
DOMAIN="gui/$(id -u)"
ls -ld "$ROOT" "$PLIST" "$PLIST_A" "$ROOT/.venv/bin/hermes"
test "$(git -C "$ROOT" branch --show-current)" = feat/c1-wiring
test "$(git -C "$ROOT" rev-parse HEAD)" = "$BASE"
git -C "$ROOT" status --short
git -C "$ROOT" diff --quiet
git -C "$ROOT" diff --cached --quiet
git -C "$ROOT" cat-file -e "$NEW_COMMIT^{commit}"
git -C "$ROOT" merge-base --is-ancestor HEAD "$NEW_COMMIT"
cmp -s "$PLIST" "$PLIST_A"
BACKUP="$NIGHT/local.hermes.c1-chief.pre-B.$(date +%Y%m%dT%H%M%S).plist"
test ! -e "$BACKUP"
cp -p "$PLIST" "$BACKUP"
shasum -a 256 "$PLIST" "$BACKUP" "$PLIST_A" | tee "$BACKUP.sha256"
shasum -a 256 "$PLIST_A" > "$PLIST_A.sha256"
```

2. 先卸載停止 chief，再 fast-forward **feat/c1-wiring**；不 checkout 已被 night worktree 佔用的分支。Git 若遇 untracked 衝突會拒絕，停下處理，不能清檔。

```bash
launchctl bootout "$DOMAIN/$LABEL"
git -C "$ROOT" merge --ff-only "$NEW_COMMIT"
test "$(git -C "$ROOT" rev-parse HEAD)" = "$NEW_COMMIT"
```

3. 用 plutil 保留其餘 plist 值，只補明確 identity；再 bootstrap。若已有 EnvironmentVariables，保留其餘 keys。

```bash
if ! plutil -extract EnvironmentVariables xml1 -o - "$PLIST" >/dev/null 2>&1; then
  plutil -insert EnvironmentVariables -xml '<dict/>' "$PLIST"
fi
if plutil -extract EnvironmentVariables.HERMES_LAUNCHD_LABEL raw -o - "$PLIST" >/dev/null 2>&1; then
  plutil -replace EnvironmentVariables.HERMES_LAUNCHD_LABEL -string "$LABEL" "$PLIST"
else
  plutil -insert EnvironmentVariables.HERMES_LAUNCHD_LABEL -string "$LABEL" "$PLIST"
fi
plutil -lint "$PLIST"
test "$(plutil -extract EnvironmentVariables.HERMES_LAUNCHD_LABEL raw -o - "$PLIST")" = "$LABEL"
launchctl bootstrap "$DOMAIN" "$PLIST"
```

4. 確認 B 的 code／launcher／job 都生效，不能只看 PID 存在。

```bash
test "$(git -C "$ROOT" rev-parse HEAD)" = "$NEW_COMMIT"
test "$(plutil -extract ProgramArguments.0 raw -o - "$PLIST")" = "$ROOT/.venv/bin/hermes"
sed -n '1,12p' "$ROOT/.venv/bin/hermes"
launchctl print "$DOMAIN/$LABEL"
pgrep -l python
```

人工核對 launcher 的 `sys.path.insert(0, ...)` 根目錄恰為 `$ROOT`（fe6ab90），不是 night worktree 或其他副本；ProgramArguments 必須直接 exec 原 launcher，沒有 wrapper。從 `launchctl print` 取得 chief PID，執行 `ps -o pid,ppid,comm -p <chief-PID>`，確認 PID 相同、ppid=1；print 的 environment 必須看得到 `HERMES_LAUNCHD_LABEL => local.hermes.c1-chief`。保留輸出，記錄起始 PID。
由人在既有 Telegram chief 對話送一則無敏感資料的測試訊息，確認有回覆；不得只以 health 取代。觀察至少一個 ThrottleInterval（r1 記錄 30 秒），確認沒有反覆重啟。

5. **人在場 Stop 實測**：先記錄時間／PID，在桌面 chief 按 Stop 一次。依 plist 的 StandardOutPath／StandardErrorPath 讀取本次時間後的 log，確認新出現精確片語 `"the C1 launchd job will restart after shutdown"`；不能用歷史行當證據。等待 launchd 拉回 chief，再 `launchctl print`／`ps` 證明 PID 已換、ppid=1、identity 仍在。再送 Telegram 測試並收到回覆；任一缺證據即 UNPROVEN，不能宣稱部署 PASS。本步只觀察與人為 Stop，不執行 hermes CLI。

### 單一回滾 script 草稿 — NOT_EXECUTED
- 檔案：`c1-planB/scripts/c1-planB-rollback.sh`。部署前必須已產生上面的 `plist-A/local.hermes.c1-chief.plist.sha256`；script 先驗 SHA／plist 格式與 tracked worktree 無變更。
- 另取得回滾授權後，單一入口：`bash /Users/kudi68-15mba/Projects/night/20261022/c1-planB/scripts/c1-planB-rollback.sh`。
- M2：preflight 後先 `launchctl bootout`，成功或回覆 `"No such process"`／`"Could not find service"` 時繼續；其他錯誤立即停下。再 `checkout --detach b6e7d10…` → 還原 plist-A → **一定執行 bootstrap**（前置步驟皆成功時），保留 untracked。
- 最後以 `launchctl print` 最多 35 次／間隔 1 秒等 PID，印 job 狀態及 `ps -o pid,ppid,comm -p <PID>`；逾時 exit 1，不當成功。
- 回滾後 HEAD detached 在方案 A；`feat/c1-wiring` 保留新 commit，不移動分支、不刪候選。仍須確認 launcher、ppid=1、job 與 Telegram；fake 測試不等於實際回滾 PASS。

### 殘留風險
- **F6**：guard 作用於此 c1 build 的所有 profile／run_gateway 路徑，foreground fallback、idle-exit 自動啟動仍被拒；有 C1 label 的永久拒絕會 exit 0 並 park。不得 upstream 或套到需要手動 gateway 的分支。
- **F7**：這是同一使用者下的防呆，不是安全邊界。double-fork／reparent、runtime 內其他副本、HOME／symlink 可影響判斷；TMPDIR 亦會影響測試 opt-out 的暫存根，不提供對同一使用者的隔離保證。
- **F8**：SIGINT 與 `--replace` takeover 保留 exit 0；chief 可能停著。stock takeover 已取得鎖，單改 75 也無法保證搶回，本輪不擴修。
- wrapper 會使 ppid 非 1；新 plist 必須維持直接 launcher。F2 的顯式 label 只有草稿，live identity 尚未驗證。
- stop→restart 仍是原本立即 stop，不新增 after-turn drain。real-boot、live Stop、Telegram、實際部署／回滾皆無本輪 PASS。

## 3. 本輪續作狀態
- 本輪 M1／M2、測試與回滾 script 已 PERSISTED 至 local commit `50606383a8b3d7676a347f51fd5a2aee9099264d`；分支 `codex/c1-planB-20261022`，父 commit `7b70af9`。未同步／push／merge／開 PR。
- 依 Human 指定再用獨立 docs commit 保存本報告 `c1-planB/NIGHT-REPORT-WP4.md`；原夜班路徑保存相同內容的 LOCAL_ONLY 副本，不另建 state／handoff。部署固定上述程式 SHA，後續 docs commit 只更新報告。
- VERIFIED：本輪 15 個 delta 測試、fake 回滾兩狀態與 bash 語法；SKIPPED_NEEDS_ENV：real-boot 未跑；NOT_EXECUTED：部署、真實回滾、live Stop、Telegram。UNKNOWN：實際環境行為，須另取得 live 授權。
- 唯一下一步：Claude Code fresh review 本次 M1／M2 delta、候選 commit 與證據；Human 目前無需操作。獨立審查報告留給 reviewer，本輪不建立。完成後先 `/clear`。
