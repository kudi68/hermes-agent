# F2 第二輪設計（2026-10-18；LOCAL_ONLY）

## 範圍與驗收
基線 `45dc82cf080ad7498bc9bb18562e323be1a51a57`；新隔離 clone
`/private/tmp/hermes-f2-round2-20261018`。只改 consumer、指定測試、部署清單契約段，
本檔兼作本輪續接紀錄。原 checkout dirty 保留。不 push／merge／留言／設定／金鑰／live。
設計階段先完成本檔才改 code；總上限 75 分鐘。同一失敗再卡一次即停，不第三次硬試。

## 根因與前兩次不足
依 `/private/tmp/review-45dc82c/REVIEW.md` R1、R2 與 Telegram binding 契約：
1. 0dc24c 把 gate 移至 transport，避免 preparation 過期，卻留下 worker 最後檢查到送出的交錯空窗。
2. 45dc82c 把 callback 搬到 timeout owner loop，序列化取消；但先查時間再同步 SQLite
   readback。真正 BEGIN EXCLUSIVE 等鎖可跨過期限，沒有 await 仍然經過時間。
3. 同步 callback 在 loop 上跑：慢 I/O 凍結 heartbeat／timeout；同 loop future.result()
   會死鎖。原 late-receipt 測試由 Timer 解鎖，只證明 UNCERTAIN 不被覆蓋。
4. 真 Telegram adapter.send 是 async，內有 chat lock、pacing、chunk/retry；不能用
   同步 shim、create_task 或把整個 workflow 包在 gate 中冒充一次立即 transport。

## 決策與新契約
採 async 單次 transport；保留 worker binding 的同步 run_effect 作橋接，但它只接受
async callable。新增 async arun_effect；同步 transport 一律 fail-closed，呼叫 body 前拒絕。
這是刻意的 C1 plugin 契約變更；沒有 production binding，不影響一般 hook 行為。

- consumer loop 是 timeout、取消與 transport 的唯一 owner。
- ledger 初始化、claim、readback、finish、UNCERTAIN 等 SQLite I/O 全由 to_thread 執行。
  production 走 async factory；建構程序不在 loop 做 blocking DB 初始化。
- arun_effect 的 asyncio.Lock 序列化同 dispatch 的 claim／最後 durable read／送出入口。
  durable EFFECT_STARTED commit 後重新 readback；其後在同一臨界區、同一 loop 執行
  closed／monotonic／message expiry 最後檢查，立即 await 單次 I/O coroutine。
  readback 回來到 I/O coroutine 入口之間不得再 await 或做 blocking I/O。
- timeout／取消先在 owner loop 關閉記憶體 gate，再 offload durable UNCERTAIN；不用等
  effect lock 才關閉。DB 失敗仍 fail-closed；若鎖未能釋放，durable 寫入失敗須明示，
  已有 intent 仍阻止 replay，不能宣稱已持久化 UNCERTAIN。
- 同步 run_effect 只允許 binding worker 使用 run_coroutine_threadsafe(...).result()；
  owner loop 上使用立即拒絕，連 coroutine 都不排入，禁止同 loop .result()。
- transport 必須 cooperative async，await 底層單次 API I/O；阻塞準備／I/O 移到 worker。
  chat lock、pacing、格式化都在 gate 前完成。不得把現有 adapter.send 整段直接傳入，
  因其內部等待／重試會越過最後檢查；必須分出準備後單次 API attempt。
- 一個 dispatch 只允許一次 attempt；不自動 chunk／retry。多段／重試目前拒絕，未驗證
  production wiring。若未來要支援，須每次 I/O 重新取得期限＋state 授權，另行設計。
- timeout 後取消在途 coroutine；取消不等於 Telegram 沒收到。late receipt 不能把
  UNCERTAIN 改回 FINISHED；restart／duplicate 不重送。transport 不得吞取消後再送。

## 驗證設計
正式測試放指定 test 檔，先在基線跑新探針，保留 RED log，再套實作：
- 真 SQLite BEGIN EXCLUSIVE：read 等鎖跨過 monotonic deadline；send 次數必為零，
  loop heartbeat 仍能解除 lock，不靠偽造 readback 或時間跳躍。
- 慢同步 I/O：callback 不得凍結 loop；新契約直接拒絕同步 body，async 慢 I/O
  另驗 timeout、heartbeat、取消與 late receipt 保留 UNCERTAIN。
- 同 loop .result() shim：隔離子程序有 bounded timeout；修後拒絕同步 body，
  owner loop 誤用 bridge 立即拒絕；正常 worker -> async send 橋接可以完成。
- 保留既有 F1/F3、取消空窗、重啟、不重送等行為測試，按新 async 契約調整 fixture。
- scripts/run_tests.sh 七檔（REVIEW 的原命令），file-retries=0，不重跑不相關全 suite。

## 收尾狀態與續接（本檔為本輪唯一續接文件）
VERIFIED／AUTHOR_OFFLINE_PASS；PERSISTED、LOCAL_ONLY；owner：本輪 Codex。
設計完成後才新增探針、修改產品程式；未超過 75 分鐘，未觸發同一失敗再次卡住條件。

- RED：45dc82cf 的產品 bytes 不變，先加入三個探針。SQLite 探針實際 late send、慢
  同步 I/O 被接受、同 loop shim 子程序 4 秒死鎖；3 failed、42 deselected，exit 1。
  `/private/tmp/hermes-f2-round2-red.log` 保存 traceback 與探針 source。
  契約切成 async 後，正式測試改用 async factory／async send；基線探針當時用原同步 API。
- GREEN 第一輪：七檔 63 passed；新增 async heartbeat／晚 receipt／owner-loop 誤用／
  transport SystemExit 驗證後，最終七檔 68 passed、0 failed、12.9 秒、exit 0。
  `/private/tmp/hermes-f2-round2-final-tests.log`。
- `scripts/check --base 45dc82cf080ad7498bc9bb18562e323be1a51a57`：12 checks ok，
  health 0 blocking／0 advisory；`/private/tmp/hermes-f2-round2-check.log`。
- runner 首次沙箱 PermissionError，0 tests；相同離線命令經自動審查准許沙箱外執行，
  才取得 RED／GREEN。此環境失敗不算測試 FAIL 或 PASS，未改設定／安裝套件。
- 既有 F1／F3、取消空窗、duplicate／restart 不重送保留。同步 transport 一律拒絕；
  原 fixture 按 async 契約改寫。SystemExit 轉 RuntimeError，不終止 owner loop。
- 分支 `fix/f2-async-round2`；本輪 commit 由本 checkout `git log -1` 精確讀回。
  四檔 scoped commit：consumer、指定 test、清單追加契約段、本設計。未 push。
- 原 `/private/tmp/hermes-f2-patch-20261018` 仍 clean、HEAD 45dc82cf；主工作區及
  agent-ops 的既有 dirty 未修改。未同步到其他機器。

最終命令（沿用既有 interpreter，runner 自行隔離 HERMES_HOME）：
```sh
env -u HERMES_TEST_SSH_KEY -u HERMES_TEST_SSH_HOST -u HERMES_TEST_SSH_USER -u HERMES_TEST_SSH_PORT \
  HERMES_PYTHON=/private/tmp/hermes-chief-c1-patch/.venv/bin/python \
  HOME=/private/tmp/hermes-f2-round2-home scripts/run_tests.sh -j 4 --file-retries 0 \
  tests/gateway/test_c1_telegram_personal.py tests/gateway/test_session_identity.py \
  tests/gateway/test_identity_canonical_ingress.py tests/gateway/test_multiplex_transport_matrix.py \
  tests/gateway/test_post_gateway_admission.py tests/gateway/test_pre_gateway_dispatch.py \
  tests/gateway/test_ingress_admission_off_loop.py
```

UNKNOWN／NOT_EXECUTED：fresh independent Review、production binding、真 Bot API、
production admission／receipt、部署與 live delivery。沒有 runtime PASS。
DB 持續等鎖可能使 UNCERTAIN 寫入失敗；程式不宣稱成功，記憶體 fence 已先關閉，
已存在的 durable intent 禁止 replay。沒有自動重送／清理資料。
下一步：在此 frozen candidate 上做限定範圍 fresh review，尤其核對最終 durable read
到 coroutine I/O 入口的契約；若需正式 binding，另定單次 attempt seam 的授權範圍。
Human 現在無需操作；不把離線 PASS 當成解閘授權。

## R4 修補與本輪續接（2026-10-18；取代上方歷史下一步）
本輪唯一 checkout `/private/tmp/hermes-r4-fix-20261018`，detached 基線
`b41385cd2df2de5b4a96217670876b808431a132`；只改 consumer、指定 test、本段。
全域 nonblocking threading.Lock 准入最多 1 個 binding；忙碌的新請求直接 consume，
真 SQLite intent 收為 UNCERTAIN 並記 rejected/UNCERTAIN，不排 binding、不 send；
duplicate 保留既有 durable state。SQLite 初始化／check_active read／claim／readback／
finish／取消收尾改用專屬 max_workers=2 executor，保留 contextvars；不擴大 default 池。
wait_for shield 真 binding task；timeout／取消先 fence 並 durable UNCERTAIN，准入直到
worker 真退出才釋放，晚 exception 有取回；其他 Gateway 路徑不變。
正式測試以獨立子程序限時，觀察而不修改 default capacity；真 bridge／gate／SQLite／
consumer 收尾保留。consumer 的 identity／callback／預初始化 factory 是離線 seam，
不證明 production identity。飽和時驗有界結束、UNCERTAIN、零 send、無關 executor 恢復，
及晚 worker 未退出時拒絕新請求、退出後恢復准入。直接 N-worker claim 探針用離線
transport 在網路 send 前回報失敗；consumer 探針則證明 timeout／取消後不進 transport。
VERIFIED RED：產品 bytes 為 b41385cd2df 時，consumer timeout／取消 2 failed，
`/private/tmp/hermes-r4-red.log`；同步所有 bridge 後的真 claim 探針 1 failed，
`/private/tmp/hermes-r4-red-dispatch.log`。`scripts/run_tests.sh -j 1 --file-retries 0
 tests/gateway/test_c1_telegram_personal.py -k saturated_executor`（後者加 `and dispatch`）。
失敗嘗試：沙箱 runner PermissionError、0 tests；經自動審查准許的離線執行才得證據。
早期 direct 探針在 claim 前過期，第二次取消會放走 worker，基線 PASS；已改真 claim 路徑。
Human 限制原話：「不准 push、merge、留言、改設定或讀金鑰。」；「上限 60 分鐘。」
本段為唯一續接文件，PERSISTED／LOCAL_ONLY，未同步其他裝置；既有 dirty 保留。
UNKNOWN／NOT_EXECUTED：production binding／SDK、admission／receipt、live、獨立審查；
慢磁碟／SQLite 持續鎖不是 executor 飽和保證，仍可能使 durable 寫入失敗，不能宣稱 live PASS。
VERIFIED GREEN：沿用上方七檔完整 runner 命令（HOME 改 `/private/tmp/hermes-r4-test-home`），
含 3 個新回歸，71 passed／0 failed／16.5s／exit 0；完整輸出
`/private/tmp/hermes-r4-green-final.log`。`/private/tmp/hermes-chief-c1-patch/.venv/bin/python
 scripts/check --base b41385cd2df`：12 checks ok、0 blocking／0 advisory、exit 0，
`/private/tmp/hermes-r4-check.log`；`git diff --check` exit 0。僅三個核准檔案變更。
owner 本輪 Codex；本地 scoped commit 由此 checkout `git log -1` 取得；無 push／merge。
唯一下一步：對此 frozen R4 candidate 做獨立離線驗收，需另行交辦授權；live 不在範圍內。
