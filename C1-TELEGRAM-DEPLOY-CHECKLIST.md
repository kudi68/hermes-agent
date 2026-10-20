# C1 Telegram 部署 Checklist

**狀態：僅準備，未部署。** 不裝金鑰、不重啟、不解閘、不送 live 訊息。候選程式固定為 `4d17946314363b39cdf7a712b34655d9e2b03f21`（含 F1–F3／F2 async 與 R4 修補；本清單的後續文件 commit 不改程式）；在部署程式 checkout 以 `git rev-parse HEAD` 與該完整 SHA 比對，不以可移動的分支 HEAD 作部署 pin；本地 commit 不代表遠端 PR 已更新。

## 部署前停止條件

- [ ] 已審查並整合 Hermes 候選與正式 C1 binding；目前候選只呼叫 `post_gateway_admission` hook，沒有 production hook registration。
- [ ] PR #15 的 production admission proof 已補齊。若仍回報 `TRUSTED_ADMISSION_BOUNDARY_UNPROVEN`，保持 fail-closed，不解閘；`C1_RUNTIME_PASS=false`。
- [ ] 已確認 serving Gateway 與 Profile。多 Profile 環境只重啟實際承載 `chief` 的 Gateway。
- [ ] 已確認 HMAC secret lookup 名稱與來源。候選目前沒有 production secret 欄位；未定義前不可安裝金鑰或啟用 binding。

## 設定與順序

1. **既有 Telegram bot：**沿用現有 bot，不建立新 bot/token。Hermes 使用 `TELEGRAM_BOT_TOKEN` 與 `TELEGRAM_ALLOWED_USERS`；只允許已核實的 Owner 數字 user ID，不使用 `*`。只核對是否存在，不輸出值。
2. **Profile 範圍：**設定留在 `chief` 的 profile-scoped secret store（named profile 預設位於 `~/.hermes/profiles/chief/.env`；以實際 `HERMES_HOME` 為準）。不可寫到 `default` 或全域環境。
3. **HMAC 根金鑰：**最多新增一把，值一律遮蔽。預定位置為 `chief` profile secret store；精確欄位／lookup 尚未定義，先完成 binding 並驗證讀取來源，再安裝。
4. 記錄目前 Hermes 版本、候選 commit、Gateway serving Profile 與回復版本；保存不含秘密值的現況紀錄。
5. 在核准的維護時段套用已審查程式與 binding，確認 allowlist 與可信 Telegram principal proof 都映射到 `human:kudi68`。
6. 先讓 Gateway graceful drain，再以服務擁有者 Profile 執行 `hermes --profile <serving-profile> gateway restart`；等待 replacement process，接著以 `hermes --profile <serving-profile> gateway status` 確認健康與版本。
7. 只有 production issuer、admission boundary、durable request identity 與 result routing 都有可讀證據後，才依已審查的正式操作解除 PR #15 閘門。**目前沒有確認可用的解閘命令；不得用手動改旗標替代證據。**

## 單筆驗收與回退

- 在既有 bot 的 Owner 私人對話，只送一次：`回傳 kudi68/personal-ai-os 的 NEXT.md 第一行`。保存回覆所屬的同一 `request_id`，並在 fresh session 對 durable C1 store 做 readback；只核對 receipt 狀態、conversation target 與 request identity，不保存私人回覆內容。
- 無回覆、逾時或 `RETURN_UNCERTAIN` 時**不重送**；先以同一 `request_id` 做 readback。現有 fixture CLI 不是 production ingress/readback 證據，部署前須確認正式 readback 工具與 store 路徑。
- 回退：先保持／恢復 fail-closed 閘門，停止新 binding；還原部署前 Hermes 版本與程式，移除本次新增的 HMAC secret（若已安裝），保留既有 Telegram bot token 與 allowlist；再 graceful restart 原 serving Gateway，確認狀態正常且 C1 未執行。

## F2 async transport binding 契約（第二輪；部署前必讀）

本段取代舊同步 effect 假設；本清單唯一候選程式 pin 為
`4d17946314363b39cdf7a712b34655d9e2b03f21`，包含 F2 async 與 R4 修補。
F2-DESIGN.md 的歷史基線不作部署 pin；正式 binding／production admission 仍須
另行取得證據，離線獨立審查不代表部署授權，維持不部署。

- consumer 使用 `await C1TelegramDispatch.create(...)`，SQLite 初始化／claim／readback／
  terminal state 都移到 worker；最後 durable read 回來後，在同一 owner-loop 臨界區
  重查 closed、monotonic deadline、message expiry，才開始單次 async I/O。
- binding 必須明列 `c1_dispatch`。準備／principal 驗證／阻塞 I/O 留 worker；admission 前
  `check_active()`；worker 最後 `run_effect(async_send_once, ...)`，owner loop 則
  `await arun_effect(async_send_once, ...)`。owner loop 禁止 `.result()`；同步 callable
  在 body 執行前拒絕。不可用 lambda 回傳 coroutine、create_task、queue、同步 HTTP shim。
- `async_send_once` 必須立即 await 一次真正的 Bot API I/O，且遵守取消。chat lock／pacing／
  格式化在 gate 前完成。現有 Telegram `adapter.send()` 內含 lock／pacing／chunk／retry，
  不能直接傳入。正式 binding 必須取得準備後的單次 attempt seam，並另作驗證。
- 同一 dispatch 不自動 retry／chunk；這份修補未授權修改 Telegram adapter。多次 I/O
  不可沿用一次 gate；若無單次 attempt seam，保持 fail-closed，不以 shim 解閘。
- timeout／取消先關閉記憶體 gate，再 durable UNCERTAIN 並取消在途 coroutine。
  取消等待不代表 Telegram 沒收到；晚 receipt 不清除 UNCERTAIN，restart 不重送。
  DB 寫入失敗不得宣稱 UNCERTAIN 已持久化；既有 intent 仍須阻止 replay，人工 readback。
- 離線鎖競爭／慢 I/O／同 loop 死鎖測試通過不等於真 Telegram binding 或 live delivery PASS。
