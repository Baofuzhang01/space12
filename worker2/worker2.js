// worker2.js
// 独立兜底 Worker：通过 Cloudflare KV REST API 跨账号读取 tongyi 的心跳 KV，并用小时锁避免重复兜底

function beijingNow() {
  return new Date(Date.now() + 8 * 3600 * 1000);
}

function beijingHHMM() {
  const d = beijingNow();
  return `${String(d.getUTCHours()).padStart(2, "0")}:${String(d.getUTCMinutes()).padStart(2, "0")}`;
}

function beijingHMS() {
  const d = beijingNow();
  return [
    String(d.getUTCHours()).padStart(2, "0"),
    String(d.getUTCMinutes()).padStart(2, "0"),
    String(d.getUTCSeconds()).padStart(2, "0"),
  ].join(":");
}

function jsonResp(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { "Content-Type": "application/json; charset=utf-8" },
  });
}

const HEARTBEAT_LAST_TS_KEY = "meta:heartbeat:last_ts";
const HEARTBEAT_TIMEOUT_MS = 60 * 1000;
const FALLBACK_HOUR_LOCK_PREFIX = "meta:fallback_hour_lock";
const FALLBACK_HOUR_LOCK_TTL_SECONDS = 48 * 60 * 60;

function beijingDateHour() {
  const d = beijingNow();
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  const hour = String(d.getUTCHours()).padStart(2, "0");
  return `${y}-${m}-${day}-${hour}`;
}

function getRemoteKvConfig(env) {
  const accountId = String(env.HEARTBEAT_SOURCE_ACCOUNT_ID || "").trim();
  const namespaceId = String(env.HEARTBEAT_SOURCE_NAMESPACE_ID || "").trim();
  const apiToken = String(env.HEARTBEAT_SOURCE_API_TOKEN || "").trim();
  if (!accountId || !namespaceId || !apiToken) {
    throw new Error("heartbeat source KV config missing: HEARTBEAT_SOURCE_ACCOUNT_ID / HEARTBEAT_SOURCE_NAMESPACE_ID / HEARTBEAT_SOURCE_API_TOKEN");
  }
  return { accountId, namespaceId, apiToken };
}

function buildRemoteKvValueUrl(env, key, extraParams = {}) {
  const { accountId, namespaceId } = getRemoteKvConfig(env);
  const url = new URL(
    `https://api.cloudflare.com/client/v4/accounts/${accountId}/storage/kv/namespaces/${namespaceId}/values/${encodeURIComponent(key)}`
  );
  for (const [name, value] of Object.entries(extraParams)) {
    if (value === undefined || value === null || value === "") continue;
    url.searchParams.set(name, String(value));
  }
  return url.toString();
}

function getRemoteKvHeaders(env) {
  const { apiToken } = getRemoteKvConfig(env);
  return {
    Authorization: `Bearer ${apiToken}`,
  };
}

async function getRemoteKvText(env, key) {
  const response = await fetch(buildRemoteKvValueUrl(env, key), {
    headers: getRemoteKvHeaders(env),
  });

  if (response.status === 404) return null;
  if (!response.ok) {
    throw new Error(`KV GET failed for ${key}: HTTP ${response.status} ${await response.text()}`);
  }
  return await response.text();
}

async function putRemoteKvText(env, key, value, options = {}) {
  const response = await fetch(
    buildRemoteKvValueUrl(env, key, {
      expiration_ttl: options.expirationTtl,
    }),
    {
      method: "PUT",
      headers: getRemoteKvHeaders(env),
      body: String(value),
    }
  );

  if (!response.ok) {
    throw new Error(`KV PUT failed for ${key}: HTTP ${response.status} ${await response.text()}`);
  }
}

async function getHeartbeatTimestamp(env) {
  const raw = await getRemoteKvText(env, HEARTBEAT_LAST_TS_KEY);
  const ts = parseInt(String(raw || "").trim(), 10);
  if (Number.isNaN(ts) || ts <= 0) return null;
  return ts;
}

function buildFallbackHourLockKey(hourKey) {
  return `${FALLBACK_HOUR_LOCK_PREFIX}:${hourKey}`;
}

async function getFallbackHourLock(env, hourKey) {
  const raw = await getRemoteKvText(env, buildFallbackHourLockKey(hourKey));
  return raw ? JSON.parse(raw) : null;
}

async function saveFallbackHourLock(env, hourKey, record) {
  await putRemoteKvText(
    env,
    buildFallbackHourLockKey(hourKey),
    JSON.stringify(record),
    {
      expirationTtl: FALLBACK_HOUR_LOCK_TTL_SECONDS,
    },
  );
  return record;
}

function parseTimeToSeconds(text) {
  const match = String(text || "").trim().match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (!match) return null;

  const hour = parseInt(match[1], 10);
  const minute = parseInt(match[2], 10);
  const second = parseInt(match[3] || "0", 10);
  if (hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59) {
    return null;
  }

  return hour * 3600 + minute * 60 + second;
}

function shouldTriggerSchoolNow(school) {
  const nowHHMM = beijingHHMM();
  const nowHMS = beijingHMS();
  const triggerTime = String(school?.trigger_time || "").trim();
  const endtime = String(school?.endtime || "").trim();

  if (!triggerTime) return false;
  if (nowHHMM < triggerTime) return false;

  if (!endtime) return true;

  const nowSeconds = parseTimeToSeconds(nowHMS);
  const endSeconds = parseTimeToSeconds(endtime);
  if (nowSeconds === null || endSeconds === null) return true;

  return nowSeconds <= endSeconds;
}

async function fetchJson(url, init = {}) {
  const res = await fetch(url, init);
  const text = await res.text();
  let data = {};

  try {
    data = text ? JSON.parse(text) : {};
  } catch (e) {
    data = { raw: text };
  }

  if (!res.ok) {
    const detail = typeof data?.error === "string" ? data.error : text || `HTTP ${res.status}`;
    throw new Error(`${url} -> HTTP ${res.status}: ${detail}`);
  }

  return data;
}

async function getSchools(env) {
  const data = await fetchJson(`${env.TRIGGER_API}/schools`, {
    headers: { "X-API-Key": env.API_KEY },
  });
  return data.schools || [];
}

async function getDueSchools(env) {
  const schools = await getSchools(env);
  return schools.filter(shouldTriggerSchoolNow);
}

async function triggerSchool(env, schoolId, options = {}) {
  const headers = { "X-API-Key": env.API_KEY };
  if (options.triggerSource) headers["X-Trigger-Source"] = options.triggerSource;
  if (options.fallbackMode) headers["X-Fallback-Mode"] = options.fallbackMode;

  return fetchJson(`${env.TRIGGER_API}/trigger/${schoolId}`, {
    method: "POST",
    headers,
  });
}

async function sendFeishuText(env, msg) {
  const webhook = String(env.FEISHU_WEBHOOK || "").trim();
  if (!webhook) {
    return {
      ok: false,
      skipped: true,
      reason: "webhook_missing",
    };
  }
  const keyword = String(env.FEISHU_KEYWORD || "检测").trim() || "检测";
  const text = String(msg || "").includes(keyword)
    ? String(msg || "")
    : `${keyword}\n${String(msg || "")}`;

  try {
    const response = await fetch(webhook, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ msg_type: "text", content: { text } }),
    });
    const detail = (await response.text()).trim();
    const result = {
      ok: response.ok,
      status: response.status,
      detail: detail.slice(0, 300),
    };
    console.log("Feishu send result:", JSON.stringify(result));
    return result;
  } catch (e) {
    const result = {
      ok: false,
      error: e.message || String(e),
    };
    console.log("Feishu send error:", JSON.stringify(result));
    return result;
  }
}

async function sendFeishuAlerts(env, messages) {
  const normalized = (messages || [])
    .map(msg => String(msg || "").trim())
    .filter(Boolean);

  if (normalized.length === 0) return [];

  const results = [];
  for (let i = 0; i < normalized.length; i++) {
    const prefix = normalized.length > 1 ? `[${i + 1}/${normalized.length}]\n` : "";
    results.push(await sendFeishuText(env, prefix + normalized[i]));
  }
  return results;
}

function summarizeTriggered(results) {
  const ok = results.filter(r => r.ok && !r.skipped).length;
  const skipped = results.filter(r => r.ok && r.skipped).length;
  return {
    ok,
    skipped,
    fail: results.filter(r => !r.ok).length,
  };
}

function chunkLines(lines, maxChars = 900) {
  const chunks = [];
  let current = [];
  let currentLen = 0;

  for (const rawLine of lines) {
    const line = String(rawLine || "").trim();
    if (!line) continue;

    const nextLen = currentLen + (current.length ? 1 : 0) + line.length;
    if (current.length && nextLen > maxChars) {
      chunks.push(current.join("\n"));
      current = [line];
      currentLen = line.length;
    } else {
      current.push(line);
      currentLen = nextLen;
    }
  }

  if (current.length) chunks.push(current.join("\n"));
  return chunks;
}

function formatFallbackMessages(title, lines, fallback) {
  const messages = [];
  const results = fallback?.results || [];
  const summary = summarizeTriggered(results);
  const successLines = results
    .filter(item => item.ok && !item.skipped)
    .map(item => `成功 ${item.name}(${item.id}) users=${item.triggeredUsers} batches=${item.okBatches}/${item.totalBatches}`);
  const skippedLines = results
    .filter(item => item.ok && item.skipped)
    .map(item => `跳过 ${item.name}(${item.id}) ${item.reason || "fallback_already_triggered_today"}`);
  const failLines = results
    .filter(item => !item.ok)
    .map(item => `失败 ${item.name}(${item.id}) ${item.error}`);

  messages.push(
    [
      title,
      ...lines,
      `兜底候选学校: ${fallback?.dueCount || 0}`,
      `成功学校: ${summary.ok}`,
      `跳过学校: ${summary.skipped}`,
      `失败学校: ${summary.fail}`,
    ].filter(Boolean).join("\n")
  );

  if (successLines.length) {
    for (const chunk of chunkLines(successLines)) {
      messages.push(["兜底触发成功明细", chunk].join("\n"));
    }
  }

  if (failLines.length) {
    for (const chunk of chunkLines(failLines)) {
      messages.push(["兜底触发失败明细", chunk].join("\n"));
    }
  }

  if (skippedLines.length) {
    for (const chunk of chunkLines(skippedLines)) {
      messages.push(["兜底触发跳过明细", chunk].join("\n"));
    }
  }

  return messages;
}

async function triggerDueSchools(env, options = {}, dueSchools = null) {
  const schoolsToTrigger = Array.isArray(dueSchools) ? dueSchools : await getDueSchools(env);
  const results = [];

  for (const school of schoolsToTrigger) {
    try {
      const result = await triggerSchool(env, school.id, options);
      results.push({
        ok: true,
        id: school.id,
        name: school.name,
        triggeredUsers: result.triggeredUsers || 0,
        okBatches: result.okBatches || 0,
        totalBatches: result.totalBatches || 0,
        skipped: !!result.skipped,
        reason: result.reason || "",
      });
    } catch (e) {
      results.push({
        ok: false,
        id: school.id,
        name: school.name,
        error: e.message || String(e),
      });
    }
  }

  return {
    checkedAt: new Date().toISOString(),
    dueCount: schoolsToTrigger.length,
    results,
  };
}

async function runWatchdog(env, options = {}) {
  const nowIso = new Date().toISOString();
  const hourKey = beijingDateHour();
  let heartbeatTs = null;
  try {
    heartbeatTs = await getHeartbeatTimestamp(env);
  } catch (e) {
    const notification = await sendFeishuText(
      env,
      [
        "worker2 告警：无法读取 tongyi 心跳 KV，已跳过兜底。",
        `错误: ${e.message || String(e)}`,
        `北京时间: ${beijingHMS()}`,
        `小时锁: ${hourKey}`,
      ].join("\n")
    );
    return {
      ok: false,
      mode: "kv_unreachable",
      manual: !!options.manual,
      skipped: true,
      reason: e.message || String(e),
      now: nowIso,
      beijing_time: beijingHMS(),
      heartbeatKey: HEARTBEAT_LAST_TS_KEY,
      fallbackHourKey: hourKey,
      notification,
    };
  }
  const diffMs = heartbeatTs === null ? null : Math.max(0, Date.now() - heartbeatTs);
  const diffSeconds = diffMs === null ? null : Math.floor(diffMs / 1000);
  const isStale = heartbeatTs === null || diffMs > HEARTBEAT_TIMEOUT_MS;
  const fallbackOptions = {
    triggerSource: "worker2",
    fallbackMode: options.manual ? "manual" : "scheduled",
  };

  if (!isStale) {
    return {
      ok: true,
      mode: "healthy",
      now: nowIso,
      beijing_time: beijingHMS(),
      heartbeatKey: HEARTBEAT_LAST_TS_KEY,
      heartbeatTs,
      diffMs,
      diffSeconds,
      thresholdMs: HEARTBEAT_TIMEOUT_MS,
      fallbackHourKey: hourKey,
    };
  }

  const dueSchools = await getDueSchools(env);
  if (dueSchools.length === 0) {
    return {
      ok: false,
      mode: "stale_no_due_school",
      manual: !!options.manual,
      skipped: true,
      reason: "no_due_school_in_current_minute",
      now: nowIso,
      beijing_time: beijingHMS(),
      heartbeatKey: HEARTBEAT_LAST_TS_KEY,
      heartbeatTs,
      diffMs,
      diffSeconds,
      thresholdMs: HEARTBEAT_TIMEOUT_MS,
      fallbackHourKey: hourKey,
    };
  }

  let existingLock = null;
  try {
    existingLock = await getFallbackHourLock(env, hourKey);
  } catch (e) {
    const notification = await sendFeishuText(
      env,
      [
        "worker2 告警：无法读取兜底小时锁 KV，已跳过兜底。",
        `错误: ${e.message || String(e)}`,
        `北京时间: ${beijingHMS()}`,
        `小时锁: ${hourKey}`,
      ].join("\n")
    );
    return {
      ok: false,
      mode: "fallback_lock_unreachable",
      manual: !!options.manual,
      skipped: true,
      reason: e.message || String(e),
      now: nowIso,
      beijing_time: beijingHMS(),
      heartbeatKey: HEARTBEAT_LAST_TS_KEY,
      heartbeatTs,
      diffMs,
      diffSeconds,
      thresholdMs: HEARTBEAT_TIMEOUT_MS,
      fallbackHourKey: hourKey,
      notification,
    };
  }
  if (existingLock) {
    return {
      ok: false,
      mode: "stale_locked",
      manual: !!options.manual,
      skipped: true,
      reason: "fallback_already_executed_this_hour",
      now: nowIso,
      beijing_time: beijingHMS(),
      heartbeatKey: HEARTBEAT_LAST_TS_KEY,
      heartbeatTs,
      diffMs,
      diffSeconds,
      thresholdMs: HEARTBEAT_TIMEOUT_MS,
      fallbackHourKey: hourKey,
      fallbackLock: existingLock,
    };
  }

  let fallbackLock = null;
  try {
    fallbackLock = await saveFallbackHourLock(env, hourKey, {
      source: "worker2",
      mode: options.manual ? "manual" : "scheduled",
      hourKey,
      at: nowIso,
      beijing_time: beijingHMS(),
      heartbeatTs,
      diffMs,
      diffSeconds,
    });
  } catch (e) {
    const notification = await sendFeishuText(
      env,
      [
        "worker2 告警：无法写入兜底小时锁 KV，已跳过兜底。",
        `错误: ${e.message || String(e)}`,
        `北京时间: ${beijingHMS()}`,
        `小时锁: ${hourKey}`,
      ].join("\n")
    );
    return {
      ok: false,
      mode: "fallback_lock_write_failed",
      manual: !!options.manual,
      skipped: true,
      reason: e.message || String(e),
      now: nowIso,
      beijing_time: beijingHMS(),
      heartbeatKey: HEARTBEAT_LAST_TS_KEY,
      heartbeatTs,
      diffMs,
      diffSeconds,
      thresholdMs: HEARTBEAT_TIMEOUT_MS,
      fallbackHourKey: hourKey,
      notification,
    };
  }

  const heartbeatLabel = heartbeatTs === null ? "无记录" : String(heartbeatTs);
  const preNotification = await sendFeishuText(
    env,
    [
      "worker2 告警：检测到 tongyi 心跳超时，准备执行兜底任务。",
      `最近心跳(ms): ${heartbeatLabel}`,
      diffSeconds === null ? "" : `距离上次心跳: ${diffSeconds} 秒`,
      `超时阈值: ${Math.floor(HEARTBEAT_TIMEOUT_MS / 1000)} 秒`,
      `北京时间: ${beijingHMS()}`,
      `小时锁: ${hourKey}`,
    ].filter(Boolean).join("\n")
  );

  let fallback = {
    checkedAt: new Date().toISOString(),
    dueCount: 0,
    results: [],
  };
  let fallbackError = "";
  try {
    fallback = await triggerDueSchools(env, fallbackOptions, dueSchools);
  } catch (e) {
    fallbackError = e.message || String(e);
  }

  const notifications = await sendFeishuAlerts(
    env,
    formatFallbackMessages(
      "worker2 告警：tongyi 心跳超时，已执行兜底触发。",
      [
      `最近心跳(ms): ${heartbeatLabel}`,
      diffSeconds === null ? "" : `距离上次心跳: ${diffSeconds} 秒`,
      `超时阈值: ${Math.floor(HEARTBEAT_TIMEOUT_MS / 1000)} 秒`,
      `北京时间: ${beijingHMS()}`,
      `小时锁: ${hourKey}`,
      fallbackError ? `兜底执行错误: ${fallbackError}` : "",
      ],
      fallback
    )
  );

  return {
      ok: false,
      mode: "stale",
      manual: !!options.manual,
      now: nowIso,
      beijing_time: beijingHMS(),
      heartbeatKey: HEARTBEAT_LAST_TS_KEY,
      heartbeatTs,
      diffMs,
      diffSeconds,
      thresholdMs: HEARTBEAT_TIMEOUT_MS,
      fallbackHourKey: hourKey,
      fallbackLock,
      preNotification,
      fallback,
      fallbackError,
      notifications,
    };
}

export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil(runWatchdog(env, { manual: false }));
  },

  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (request.method === "POST" && url.pathname === "/run") {
      return jsonResp(await runWatchdog(env, { manual: true }));
    }

    if (request.method === "GET" && url.pathname === "/health") {
      let heartbeatTs = null;
      let heartbeatError = "";
      try {
        heartbeatTs = await getHeartbeatTimestamp(env);
      } catch (e) {
        heartbeatError = e.message || String(e);
      }
      return jsonResp({
        ok: true,
        worker: "worker2",
        now: new Date().toISOString(),
        beijing_time: beijingHMS(),
        heartbeatKey: HEARTBEAT_LAST_TS_KEY,
        heartbeatTs,
        heartbeatError,
      });
    }

    return jsonResp({
      ok: true,
      worker: "worker2",
      message: "Use POST /run to execute the watchdog manually.",
      now: new Date().toISOString(),
      beijing_time: beijingHMS(),
    });
  },
};
