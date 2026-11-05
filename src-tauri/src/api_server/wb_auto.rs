//! workbuddy-auto 虚拟模型：WorkBuddy 侧的「模型级 + 账号级」智能调度。
//!
//! 定位（与既有 WB 管线的关系，刻意不重复实现）：
//! - **模型级**决策在本模块：免费(rate==0) → 低倍率 → 高倍率（倍率升序）。
//! - **账号级**轮换仍由 `wb_route::wb_aggregate_chat` / `wb_stream_chat` 内部的
//!   账号循环负责（取号/粘性/CAS/故障转移一份实现），本模块只做两件事：
//!   ① 把「本次实际选中的账号」取回来（`WbExecTrace`，见 wb_route）；
//!   ② 按 `uid + model + period` 记录额度状态，供下一轮决策跳过。
//!
//! 数据来源与不确定性（如实标注，不伪造）：
//! - 免费身份：上游目录 `rate == 0.0`（`unified_catalog.rs` 既有语义「0 = 免费」）。
//! - 每日免费额度**数值**：上游未提供查询接口 → 本模块记 UNKNOWN，仅依据真实响应
//!   话术落状态；绝不自造额度数字，也不把网络/5xx/429/鉴权错误误判为额度耗尽。
//! - 额度重置时刻：`reset_at` 仅在能从上文化术中解析到可靠时刻时填入；否则为 None，
//!   此时只能由「后续成功调用的实证」恢复（不猜本地零点）。
//! - 账号积分余额：由 `pool`/`wb_credits` 既有链路维护，本模块只读不改。
//!
//! 隔离性：
//! - 仅 `workbuddy-auto` 一个模型 ID 进入本模块；其他模型、其他产品线
//!   （Trae/Qoder/豆包/BuddyAI）的调度语义完全不变。
//! - 只读 `wb_catalog` / `wb_pool` / `store` kv，不写账号、不改倍率、不动凭据。

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::http::StatusCode;
use axum::response::Response;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::routes::Protocol;
use super::{wb_catalog, wb_route, ApiSharedState};

/// 虚拟模型 ID（不得与上游真实模型重名）
pub const AUTO_MODEL_ID: &str = "workbuddy-auto";
/// 额度状态持久化键（kv，走既有 store；重启不丢）
const KV_KEY: &str = "wb_auto_quota_v1";
/// 账号维度不可知时的占位 uid（与真实 uid 不会冲突）
const UNKNOWN_UID: &str = "unknown";

/// 请求的 model 是否为本虚拟模型（大小写归一，容忍首尾空白）
pub fn is_auto(model: &str) -> bool {
    model.trim().eq_ignore_ascii_case(AUTO_MODEL_ID)
}

// ==================== 状态模型 ====================

/// 三种「不可用」必须严格区分（规范 §3.3 / §3.1）
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QuotaStatus {
    /// 免费额度耗尽（仅当上游话术确认是「免费额度」时）
    FreeExhausted,
    /// 该账号积分不足 / 触及保留线
    CreditExhausted,
    /// 模型暂不可用（下架、维护等非额度原因）
    ModelUnavailable,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct QuotaEntry {
    /// 账号 UID；无法确定时为 `unknown`
    pub uid: String,
    pub model: String,
    /// 额度周期分桶（东八区自然日；仅统计用，恢复以 reset_at / 成功实证为准）
    pub period: String,
    pub status: QuotaStatus,
    /// 判定原因（人可读，不含凭据）
    pub reason: String,
    /// 上游响应片段（截断 + 去换行，不含凭据）
    pub evidence: String,
    pub marked_at: i64,
    /// 上游给出的重置时刻（epoch 秒）；None = 未提供 → 只能靠成功实证恢复
    pub reset_at: Option<i64>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct QuotaFile {
    #[serde(default)]
    pub version: u32,
    #[serde(default)]
    pub entries: BTreeMap<String, QuotaEntry>,
}

fn entry_key(uid: &str, model: &str, period: &str) -> String {
    format!("{uid}|{model}|{period}")
}

pub fn now_epoch() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// 额度周期分桶：东八区自然日（UTC+8）。仅用于统计与日志分桶，**不用于自动恢复**。
pub fn period_of(epoch: i64) -> String {
    let t = epoch + 8 * 3600;
    let days = t.div_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}")
}

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

// ==================== 倍率与候选规划 ====================

/// rate==0 → 免费；NaN（元数据缺失）**不算**免费（不得把未知伪装成免费）
pub fn is_free_rate(rate: f64) -> bool {
    rate.is_finite() && rate == 0.0
}

#[derive(Clone, Debug, PartialEq)]
pub struct Candidate {
    pub model: String,
    pub rate: f64,
    pub free: bool,
}

/// 规划候选顺序：免费（按 id 稳定） → 付费（倍率升序，同倍率按 id）
/// `skip` 用于排除当前不可用（额度耗尽、模型冷却等）。
pub fn plan_candidates(
    models: &[wb_catalog::WbModel],
    skip: &dyn Fn(&str) -> bool,
) -> Vec<Candidate> {
    let mut free: Vec<Candidate> = Vec::new();
    let mut paid: Vec<Candidate> = Vec::new();
    for m in models {
        let rate = m.rate;
        if !rate.is_finite() {
            continue; // 倍率未知：不参与自动调度
        }
        if skip(&m.id) {
            continue;
        }
        let c = Candidate {
            model: m.id.clone(),
            rate,
            free: is_free_rate(rate),
        };
        if c.free {
            free.push(c);
        } else if rate > 0.0 {
            paid.push(c);
        }
        // rate < 0 属脏数据：跳过（既非免费也非可计价）
    }
    free.sort_by(|a, b| a.model.cmp(&b.model));
    paid.sort_by(|a, b| {
        a.rate
            .partial_cmp(&b.rate)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.model.cmp(&b.model))
    });
    free.extend(paid);
    free
}

// ==================== 错误分类（按真实话术，不猜状态码） ====================

/// 免费额度耗尽话术
pub fn looks_like_free_quota(msg: &str) -> bool {
    let m = msg.to_ascii_lowercase();
    const FREE: &[&str] = &[
        "free quota",
        "free_quota",
        "daily free",
        "free usage",
        "free limit",
        "免费额度",
        "免费次数",
        "免费体验",
        "赠送额度",
        "赠送次数",
        "今日免费",
        "每日免费",
        "活动额度",
        "限时免费",
    ];
    FREE.iter().any(|k| m.contains(k))
}

/// 账号积分/余额不足话术
pub fn looks_like_credit(msg: &str) -> bool {
    let m = msg.to_ascii_lowercase();
    const CREDIT: &[&str] = &[
        "insufficient credit",
        "insufficient_credit",
        "credit exhausted",
        "no credit",
        "not enough credit",
        "out of credits",
        "积分不足",
        "积分耗尽",
        "余额不足",
        "余额耗尽",
        "积分已用完",
        "额度不足",
        "额度已用完",
    ];
    CREDIT.iter().any(|k| m.contains(k))
}

/// 鉴权类：绝不判定为额度耗尽（规范明确禁止误判）
pub fn looks_like_auth(status: u16, msg: &str) -> bool {
    if status == 401 || status == 403 {
        return true;
    }
    let m = msg.to_ascii_lowercase();
    [
        "unauthorized",
        "invalid token",
        "token expired",
        "invalid api key",
        "登录已失效",
        "鉴权失败",
    ]
    .iter()
    .any(|k| m.contains(k))
}

/// 返回应落库的状态；None = 与额度无关（网络/5xx/429/鉴权/审核等，只走既有冷却）
pub fn classify(status: u16, body: &str) -> Option<QuotaStatus> {
    // 先排鉴权与非额度类状态码，避免 "quota" 字样混在 5xx 里被误判
    if looks_like_auth(status, body) {
        return None;
    }
    if status == 429 {
        // 429 只代表该账号/模型暂时限流，**不得**升级为免费额度耗尽或积分耗尽
        return None;
    }
    if status >= 500 {
        return None;
    }
    if looks_like_free_quota(body) {
        return Some(QuotaStatus::FreeExhausted);
    }
    if looks_like_credit(body) {
        return Some(QuotaStatus::CreditExhausted);
    }
    None
}

// ==================== 持久化 ====================

fn load_file(state: &ApiSharedState) -> QuotaFile {
    let store = crate::store::db(&state.data_dir);
    let mut f: QuotaFile = store.kv_get(KV_KEY);
    if f.version == 0 {
        f.version = 1;
    }
    f
}

fn save_file(state: &ApiSharedState, f: &QuotaFile) {
    let store = crate::store::db(&state.data_dir);
    let _ = store.kv_set(KV_KEY, f);
    crate::api_server::config_cache::invalidate(&state.data_dir, KV_KEY);
}

/// 单条状态是否仍生效（同周期且未到重置时刻）
fn entry_active(e: &QuotaEntry, now: i64) -> bool {
    match e.reset_at {
        Some(r) => now < r, // 上游给了重置时刻：到点自动恢复
        None => true,       // 上游未给：只能由成功调用实证恢复
    }
}

/// 该账号 + 该模型当前是否不可用
pub fn account_model_blocked(f: &QuotaFile, uid: &str, model: &str, now: i64) -> bool {
    let period = period_of(now);
    f.entries
        .values()
        .any(|e| e.uid == uid && e.model == model && e.period == period && entry_active(e, now))
}

/// 模型级兜底判定：任意账号有生效状态即视为不可用（账号集合不可知时使用，保守）
pub fn model_blocked_unknown_accounts(f: &QuotaFile, model: &str, now: i64) -> bool {
    let period = period_of(now);
    f.entries
        .values()
        .any(|e| e.model == model && e.period == period && entry_active(e, now))
}

/// 模型是否应对**所有**可调度账号都不可用。
///
/// 关键修正（账号维度）：只要还有任意一个账号在该模型上「未耗尽」，模型就不该被
/// 全局跳过——否则账号 A 的免费额度用尽会导致本可用 A 以外账号的新额度被白白浪费。
/// 新增账号（D）因没有任何历史条目，天然不会被拦，符合「新账号自动参与」。
pub fn model_blocked(f: &QuotaFile, model: &str, now: i64, uids: &[String]) -> bool {
    if uids.is_empty() {
        return model_blocked_unknown_accounts(f, model, now);
    }
    uids.iter().all(|u| account_model_blocked(f, u, model, now))
}

/// 记录一次额度类失败（同账号同模型同周期幂等覆盖）
pub fn mark(
    f: &mut QuotaFile,
    uid: &str,
    model: &str,
    status: QuotaStatus,
    reason: &str,
    evidence: &str,
    now: i64,
    reset_at: Option<i64>,
) {
    let period = period_of(now);
    let k = entry_key(uid, model, &period);
    f.entries.insert(
        k,
        QuotaEntry {
            uid: uid.to_string(),
            model: model.to_string(),
            period,
            status,
            reason: reason.to_string(),
            evidence: sanitize(evidence),
            marked_at: now,
            reset_at,
        },
    );
}

/// 成功调用即实证「该账号在该模型上额度可用」：只清除该账号该模型的状态。
/// `uid=None`（账号维度不可知）时退化为清除该模型的全部状态。
pub fn clear_on_success(f: &mut QuotaFile, uid: Option<&str>, model: &str) {
    match uid {
        Some(u) => f
            .entries
            .retain(|_, e| !(e.uid == u && e.model == model)),
        None => f.entries.retain(|_, e| e.model != model),
    }
}

fn sanitize(s: &str) -> String {
    let t: String = s.chars().take(200).collect();
    t.replace(['\n', '\r'], " ")
}

// ==================== 并发额度占用 ====================

fn reserved_set() -> &'static Mutex<HashSet<String>> {
    static R: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    R.get_or_init(|| Mutex::new(HashSet::new()))
}

/// 尝试占用某模型的一个免费额度槽位；已被并发请求占用则返回 false。
///
/// 说明：占用键为 `model`（取号前账号未定，无法按 uid 精确占位）。这只防「同一
/// 免费模型的并发重复消耗」，不改变账号级并发语义（后者由 pool 的 inflight 负责）。
pub fn try_reserve(model: &str) -> bool {
    let mut g = match reserved_set().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    if g.contains(model) {
        false
    } else {
        g.insert(model.to_string());
        true
    }
}

pub fn release(model: &str) {
    let mut g = match reserved_set().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    g.remove(model);
}

// ==================== 执行编排 ====================

/// 可调度的账号 UID 集合（用于账号维度判定；不含余额过滤，见 pool::selectable_uids 注释）
fn pool_uids(state: &ApiSharedState) -> Vec<String> {
    state.wb_pool.selectable_uids(true)
}

/// 依策略选出的候选列表（已排除额度耗尽/冷却中的模型）
fn candidates_now(state: &Arc<ApiSharedState>) -> Result<Vec<Candidate>, Response> {
    if !state
        .wb_enabled
        .load(std::sync::atomic::Ordering::Relaxed)
    {
        return Err(super::routes::openai_error(
            StatusCode::BAD_REQUEST,
            "wb_upstream_disabled",
            "WB 上游未启用（api_pool.json wb_enabled），workbuddy-auto 不可用",
        ));
    }
    let catalog = wb_catalog::load(&state.data_dir);
    if catalog.is_empty() {
        return Err(super::routes::openai_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "auto_catalog_empty",
            "workbuddy-auto：模型目录为空（尚未同步上游目录）",
        ));
    }
    let f = load_file(state);
    let now = now_epoch();
    let uids = pool_uids(state);
    let view = wb_route::WbView::wb(state.clone());
    Ok(plan_candidates(&catalog, &|id: &str| {
        model_blocked(&f, id, now, &uids)
            || wb_route::model_cooling_remaining(&view, id).is_some()
    }))
}

/// 调度日志（不含任何凭据；uid 仅本地日志可见，不回传客户端）
#[allow(clippy::too_many_arguments)]
fn log_decision(
    state: &ApiSharedState,
    uid: Option<&str>,
    model: &str,
    rate: f64,
    free: bool,
    reason: &str,
    tried: &[String],
) {
    let line = format!(
        "wb-auto uid={} model={} rate={} free={} reason={} tried=[{}]",
        uid.unwrap_or("-"),
        model,
        rate,
        if free { 1 } else { 0 },
        reason,
        tried.join(",")
    );
    state.logger.log_sched_event(&line);
}

/// 非流式：逐候选尝试，额度类失败自动换下一个（模型级闭环）
pub async fn aggregate_auto(
    state: Arc<ApiSharedState>,
    chat_body: Value,
    proto: Protocol,
    key_str: String,
    encoded: HashSet<String>,
) -> Response {
    let cands = match candidates_now(&state) {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    if cands.is_empty() {
        return super::routes::openai_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "auto_unavailable",
            "workbuddy-auto：目录内无可用候选（倍率未知、额度耗尽或全部冷却）",
        );
    }

    let mut last: Option<Response> = None;
    let mut tried: Vec<String> = Vec::new();

    for cand in cands {
        // 免费模型做并发额度占用保护；付费模型按积分消耗，不占免费槽
        if cand.free && !try_reserve(&cand.model) {
            continue;
        }
        tried.push(cand.model.clone());

        let mut body = chat_body.clone();
        body["model"] = Value::String(cand.model.clone());
        body["stream"] = Value::Bool(false);
        let body_vec = match serde_json::to_vec(&body) {
            Ok(v) => v,
            Err(_) => {
                if cand.free {
                    release(&cand.model);
                }
                return super::routes::openai_error(
                    StatusCode::BAD_REQUEST,
                    "invalid_request_error",
                    "workbuddy-auto：请求体重新序列化失败",
                );
            }
        };

        // 每次尝试独立计时（上游延迟口径不被前一次尝试污染）
        let start_ts = std::time::Instant::now();
        let guard = state.inflight_guard();
        let trace = wb_route::WbExecTrace::new();
        let resp = wb_route::wb_aggregate_chat(
            state.clone(),
            false,
            body_vec,
            encoded.clone(),
            cand.model.clone(),
            false,
            start_ts,
            proto,
            key_str.clone(),
            guard,
            Some(trace.clone()),
        )
        .await;

        if cand.free {
            release(&cand.model);
        }
        let uid = trace.uid();

        if resp.status().is_success() {
            let mut f = load_file(&state);
            clear_on_success(&mut f, uid.as_deref(), &cand.model);
            f.version = 1;
            save_file(&state, &f);
            log_decision(
                &state,
                uid.as_deref(),
                &cand.model,
                cand.rate,
                cand.free,
                "success",
                &tried,
            );
            return tag(resp, &cand.model, cand.rate, cand.free, &tried, uid.as_deref());
        }

        // 失败：读错误体做额度归因（仅此路径缓冲，成功响应体一律透传不解析）
        let (parts, body) = resp.into_parts();
        let bytes = match axum::body::to_bytes(body, 1 << 20).await {
            Ok(b) => b,
            Err(_) => {
                return super::routes::openai_error(
                    StatusCode::BAD_GATEWAY,
                    "auto_body_error",
                    "workbuddy-auto：读取上游错误响应失败",
                )
            }
        };
        let text = String::from_utf8_lossy(&bytes).to_string();

        match classify(parts.status.as_u16(), &text) {
            Some(kind) => {
                let mut f = load_file(&state);
                mark(
                    &mut f,
                    uid.as_deref().unwrap_or(UNKNOWN_UID),
                    &cand.model,
                    kind,
                    match kind {
                        QuotaStatus::FreeExhausted => "免费额度耗尽（上游话术确认）",
                        QuotaStatus::CreditExhausted => "账号积分不足/触及保留线",
                        QuotaStatus::ModelUnavailable => "模型不可用",
                    },
                    &text,
                    now_epoch(),
                    wb_route::parse_quota_reset_at(&text),
                );
                f.version = 1;
                save_file(&state, &f);
                log_decision(
                    &state,
                    uid.as_deref(),
                    &cand.model,
                    cand.rate,
                    cand.free,
                    match kind {
                        QuotaStatus::FreeExhausted => "free_quota_exhausted",
                        QuotaStatus::CreditExhausted => "credit_exhausted",
                        QuotaStatus::ModelUnavailable => "model_unavailable",
                    },
                    &tried,
                );

                if kind == QuotaStatus::CreditExhausted {
                    // 账号积分不足是**账号级**结论：换模型不可能绕过保留线，
                    // 直接返回该错误（账号轮换已由 wb_route 内部完成）。
                    return Response::from_parts(parts, Body::from(bytes));
                }
                last = Some(Response::from_parts(parts, Body::from(bytes)));
                continue; // 换下一个模型
            }
            None => {
                // 非额度类（网络/5xx/429/鉴权/审核）：保留原响应，不盲目换模型
                log_decision(
                    &state,
                    uid.as_deref(),
                    &cand.model,
                    cand.rate,
                    cand.free,
                    &format!("passthrough_http_{}", parts.status.as_u16()),
                    &tried,
                );
                return Response::from_parts(parts, Body::from(bytes));
            }
        }
    }

    last.unwrap_or_else(|| {
        super::routes::openai_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "auto_unavailable",
            &format!("workbuddy-auto：候选全部失败（已尝试 {}）", tried.join(", ")),
        )
    })
}

/// 流式：按策略取首个候选直接流式（SSE 首字下发后不可回退）；
/// 失败仅落状态供下次决策参考，不在流内重试。
pub fn stream_auto(
    state: Arc<ApiSharedState>,
    mut chat_body: Value,
    proto: Protocol,
    key_str: String,
    encoded: HashSet<String>,
) -> Response {
    let cands = match candidates_now(&state) {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    let Some(cand) = cands.first() else {
        return super::routes::openai_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "auto_unavailable",
            "workbuddy-auto：目录内无可用候选",
        );
    };
    chat_body["model"] = Value::String(cand.model.clone());
    chat_body["stream"] = Value::Bool(true);
    let body_vec = serde_json::to_vec(&chat_body).unwrap_or_default();
    let start_ts = std::time::Instant::now();
    let guard = state.inflight_guard();
    let trace = wb_route::WbExecTrace::new();
    let resp = wb_route::wb_stream_chat(
        state.clone(),
        false,
        body_vec,
        cand.model.clone(),
        start_ts,
        proto,
        key_str,
        guard,
        encoded,
        Some(trace.clone()),
    );
    log_decision(
        &state,
        trace.uid().as_deref(),
        &cand.model,
        cand.rate,
        cand.free,
        "stream_start",
        &[cand.model.clone()],
    );
    tag(
        resp,
        &cand.model,
        cand.rate,
        cand.free,
        &[cand.model.clone()],
        None,
    )
}

/// 统一入口：按 body.stream 分流（两个端点共用）
pub async fn handle(
    state: Arc<ApiSharedState>,
    chat_body: Value,
    proto: Protocol,
    key_str: String,
    encoded: HashSet<String>,
) -> Response {
    let stream = chat_body
        .get("stream")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if stream {
        stream_auto(state, chat_body, proto, key_str, encoded)
    } else {
        aggregate_auto(state, chat_body, proto, key_str, encoded).await
    }
}

/// 打可观测标头（不含凭据；uid 不回传，避免把账号标识暴露给客户端）
fn tag(
    resp: Response,
    model: &str,
    rate: f64,
    free: bool,
    tried: &[String],
    uid: Option<&str>,
) -> Response {
    let (mut parts, body) = resp.into_parts();
    if let Ok(v) = axum::http::HeaderValue::from_str(model) {
        parts.headers.insert("x-wb-auto-model", v);
    }
    if let Ok(v) = axum::http::HeaderValue::from_str(&format!("{rate}")) {
        parts.headers.insert("x-wb-auto-rate", v);
    }
    if let Ok(v) = axum::http::HeaderValue::from_str(if free { "1" } else { "0" }) {
        parts.headers.insert("x-wb-auto-free", v);
    }
    if let Ok(v) = axum::http::HeaderValue::from_str(&tried.join(",")) {
        parts.headers.insert("x-wb-auto-tried", v);
    }
    if uid.is_some() {
        // 只暴露「是否已完成账号归因」，不暴露 uid 本身
        parts.headers.insert(
            "x-wb-auto-uid-known",
            axum::http::HeaderValue::from_static("1"),
        );
    }
    Response::from_parts(parts, body)
}

/// /v1/models 注入条目（虚拟模型，非上游真实模型）
pub fn model_record() -> Value {
    serde_json::json!({
        "id": AUTO_MODEL_ID,
        "object": "model",
        "created": 0,
        "owned_by": "aiwork-auto",
        "display": "WorkBuddy Auto",
        "description": "WorkBuddy 智能调度：免费模型优先 → 倍率递增 → 账号池自动接管",
        "context_length": 200000,
        "max_tokens": 64000,
        "supports_image": false,
        "supported_efforts": [],
        "rate": 0.0
    })
}

// ==================== 单元测试（纯逻辑，不触网、不碰生产库/Keychain） ====================

#[cfg(test)]
mod tests {
    use super::*;

    fn m(id: &str, rate: f64) -> wb_catalog::WbModel {
        wb_catalog::WbModel {
            id: id.to_string(),
            display: id.to_string(),
            context_length: 0,
            max_tokens: 0,
            supports_image: false,
            supported_efforts: vec![],
            effort_override: None,
            rate,
        }
    }

    #[test]
    fn free_first_then_rate_ascending() {
        let models = vec![
            m("paid-big", 1.5),
            m("paid-low", 0.2),
            m("free-b", 0.0),
            m("free-a", 0.0),
            m("paid-mid", 0.69),
        ];
        let plan = plan_candidates(&models, &|_| false);
        let ids: Vec<&str> = plan.iter().map(|c| c.model.as_str()).collect();
        assert_eq!(
            ids,
            vec!["free-a", "free-b", "paid-low", "paid-mid", "paid-big"]
        );
        assert!(plan[0].free && plan[1].free && !plan[2].free);
    }

    #[test]
    fn nan_rate_not_treated_as_free() {
        let models = vec![m("unknown", f64::NAN), m("paid", 0.5), m("free", 0.0)];
        let plan = plan_candidates(&models, &|_| false);
        let ids: Vec<&str> = plan.iter().map(|c| c.model.as_str()).collect();
        assert_eq!(ids, vec!["free", "paid"], "倍率未知的模型不得参与自动调度");
        assert!(!is_free_rate(f64::NAN));
    }

    #[test]
    fn exhausted_free_switches_to_next_then_paid() {
        // 模拟：free-a 耗尽后应跳到 free-b；两个免费都耗尽后进入最低倍率付费
        let models = vec![m("free-a", 0.0), m("free-b", 0.0), m("paid-low", 0.1)];
        let mut f = QuotaFile::default();
        let now = 1_760_000_000;
        mark(
            &mut f,
            "uid-a",
            "free-a",
            QuotaStatus::FreeExhausted,
            "t",
            "free quota exhausted",
            now,
            None,
        );
        let uids = vec!["uid-a".to_string()];
        let plan = plan_candidates(&models, &|id| model_blocked(&f, id, now, &uids));
        let ids: Vec<&str> = plan.iter().map(|c| c.model.as_str()).collect();
        assert_eq!(ids, vec!["free-b", "paid-low"]);

        // 再耗尽 free-b → 只剩付费
        mark(
            &mut f,
            "uid-a",
            "free-b",
            QuotaStatus::FreeExhausted,
            "t",
            "free quota exhausted",
            now,
            None,
        );
        let plan = plan_candidates(&models, &|id| model_blocked(&f, id, now, &uids));
        let ids: Vec<&str> = plan.iter().map(|c| c.model.as_str()).collect();
        assert_eq!(ids, vec!["paid-low"], "免费全部耗尽后进入低倍率付费");
    }

    #[test]
    fn account_dimension_isolation() {
        // 关键回归：uid-a 在 free-a 上耗尽，**不应**导致 uid-b 也被跳过
        let mut f = QuotaFile::default();
        let now = 1_760_000_000;
        mark(
            &mut f,
            "uid-a",
            "free-a",
            QuotaStatus::FreeExhausted,
            "t",
            "免费额度耗尽",
            now,
            None,
        );
        // 只有 uid-a 可调度 → 模型对该账号不可用
        assert!(model_blocked(&f, "free-a", now, &["uid-a".to_string()]));
        // 新增 uid-b（无历史条目）→ 模型重新可用
        assert!(!model_blocked(
            &f,
            "free-a",
            now,
            &["uid-a".to_string(), "uid-b".to_string()]
        ));
        // 账号集合未知 → 保守按模型级判定
        assert!(model_blocked(&f, "free-a", now, &[]));
    }

    #[test]
    fn success_only_clears_that_account() {
        let mut f = QuotaFile::default();
        let now = 1_760_000_000;
        mark(&mut f, "uid-a", "m1", QuotaStatus::FreeExhausted, "t", "x", now, None);
        mark(&mut f, "uid-b", "m1", QuotaStatus::FreeExhausted, "t", "x", now, None);
        clear_on_success(&mut f, Some("uid-a"), "m1");
        assert!(!account_model_blocked(&f, "uid-a", "m1", now));
        assert!(account_model_blocked(&f, "uid-b", "m1", now));
    }

    #[test]
    fn reset_at_expiry_restores() {
        let mut f = QuotaFile::default();
        let now = 1_760_000_000;
        mark(
            &mut f,
            "uid-a",
            "free-a",
            QuotaStatus::FreeExhausted,
            "t",
            "x",
            now,
            Some(now + 60),
        );
        assert!(account_model_blocked(&f, "uid-a", "free-a", now));
        assert!(!account_model_blocked(&f, "uid-a", "free-a", now + 61));
    }

    #[test]
    fn concurrency_slot_not_double_taken() {
        // 用独立模型名避开并行测试干扰
        let model = "concurrency-probe-model";
        release(model);
        assert!(try_reserve(model), "首次占用成功");
        assert!(!try_reserve(model), "并发第二请求不得重复占用");
        release(model);
        assert!(try_reserve(model), "释放后可再次占用");
        release(model);
    }

    #[test]
    fn period_buckets_by_utc_plus_8() {
        // 2026-11-05 00:00:00 +08:00 == 2026-11-04 16:00:00 UTC
        let epoch = 1_762_272_000; // 2025-11-05T16:00:00Z 附近的固定点
        assert_eq!(period_of(epoch).len(), 10);
        // 相差一个自然日 → 分桶必须不同
        assert_ne!(period_of(epoch), period_of(epoch + 86_400));
        // 相差 1 小时（且不跨日）→ 分桶相同
        let base = epoch + 3600;
        assert_eq!(period_of(base), period_of(base + 1800));
    }

    #[test]
    fn status_codes_never_misjudged_as_quota() {
        // 429 / 5xx / 401 即使话术里出现 quota/credit 也不得判为额度耗尽
        assert_eq!(classify(429, "rate limit exceeded, quota per minute"), None);
        assert_eq!(classify(503, "upstream quota unavailable"), None);
        assert_eq!(classify(401, "insufficient credit"), None);
        assert_eq!(classify(403, "free quota exceeded"), None);
        // 真正的额度话术才落状态
        assert_eq!(
            classify(400, "免费额度已用完，请明日再试"),
            Some(QuotaStatus::FreeExhausted)
        );
        assert_eq!(
            classify(400, "insufficient credits for this request"),
            Some(QuotaStatus::CreditExhausted)
        );
    }
}

// ==================== 隔离集成测试 ====================
//
// 覆盖「账号层闭环」：A/B/C 运行中 → 依次触线 → 新增 D 无需重启即入池接管。
// 全部使用**内存池**（`ApiPool`），不读生产数据库、不读 Vault、不触 Keychain、
// 不发真实请求、不消耗任何积分。
#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::api_server::pool::{ApiPool, WbSyncAccount};
    use std::collections::HashSet;

    fn wb_account(uid: &str, credits: f64) -> WbSyncAccount {
        WbSyncAccount {
            uid: uid.to_string(),
            name: format!("acct-{uid}"),
            // 池装载只校验非空（不发请求），用占位串避免携带任何真实凭据
            token: format!("test-token-{uid}"),
            // domain 非空 → 该条目被识别为 WB 形态（sync_from_wb 的 retain 语义）
            domain: "cn".to_string(),
            enterprise_id: String::new(),
            global_region: false,
            credits: Some(credits),
            credits_expire_at: None,
            // 必须有新鲜快照时间，否则 reserve 过滤 fail-open（池的既有设计）
            credits_fetched_at: Some(now_epoch()),
            needs_relogin: false,
            group_id: String::new(),
        }
    }

    fn enabled(uids: &[&str]) -> Vec<String> {
        uids.iter().map(|s| s.to_string()).collect()
    }

    /// A/B/C 运行中 → 依次触及 50 积分保留线 → 新增 D 自动入池并接管。
    /// 全程**没有重启**，也没有手动改白名单：D 只是出现在一次新的池快照里。
    #[test]
    fn account_failover_closed_loop_with_new_account() {
        let pool = ApiPool::new();
        pool.set_reserve_credits(50.0);

        // 初始：A/B/C 各 60 积分（高于保留线 50）→ 三个都可调度
        pool.sync_from_wb(
            &[
                wb_account("A", 60.0),
                wb_account("B", 60.0),
                wb_account("C", 60.0),
            ],
            &enabled(&["A", "B", "C"]),
        );
        assert_eq!(pool.selectable_uids(false).len(), 3, "三个账号均应可调度");

        // A 跌破保留线（60 → 40）→ 仅 A 退出消费调度，B/C 不受影响
        pool.sync_from_wb(
            &[
                wb_account("A", 40.0),
                wb_account("B", 60.0),
                wb_account("C", 60.0),
            ],
            &enabled(&["A", "B", "C"]),
        );
        let uids = pool.selectable_uids(false);
        assert!(!uids.contains(&"A".to_string()), "A 触线后不得参与调度");
        assert_eq!(uids.len(), 2, "B/C 不受 A 触线影响");

        // A/B/C 全部触线 → 消费调度无候选（此时才轮到新账号）
        pool.sync_from_wb(
            &[
                wb_account("A", 40.0),
                wb_account("B", 45.0),
                wb_account("C", 49.0),
            ],
            &enabled(&["A", "B", "C"]),
        );
        assert!(
            pool.selectable_uids(false).is_empty(),
            "三个账号全部触及保留线后不应再有可消费候选"
        );

        // 新增 D（90 积分）：新的池快照包含 D → 无需重启即入池
        pool.sync_from_wb(
            &[
                wb_account("A", 40.0),
                wb_account("B", 45.0),
                wb_account("C", 49.0),
                wb_account("D", 90.0),
            ],
            &enabled(&["A", "B", "C", "D"]),
        );
        assert_eq!(
            pool.selectable_uids(false),
            vec!["D".to_string()],
            "新账号 D 应自动成为唯一可调度账号"
        );

        // 下一次取号必须落到 D（这就是「新账号接管后续请求」）
        let picked = pool.pick_excluding_constrained_ev(&HashSet::new(), None, None, false);
        assert_eq!(
            picked.map(|(p, _)| p.uid).as_deref(),
            Some("D"),
            "切号时应选中新账号 D"
        );
    }

    /// 账号集合变更不影响别的账号的免费额度判定：D 无任何历史额度条目 → 模型立即可用
    #[test]
    fn new_account_reopens_model_blocked_by_others() {
        let mut f = QuotaFile::default();
        let now = now_epoch();
        mark(&mut f, "A", "free-a", QuotaStatus::FreeExhausted, "t", "免费额度", now, None);
        mark(&mut f, "B", "free-a", QuotaStatus::FreeExhausted, "t", "免费额度", now, None);

        // A/B 都耗尽 → 模型对 A/B 均不可用
        assert!(model_blocked(
            &f,
            "free-a",
            now,
            &["A".to_string(), "B".to_string()]
        ));
        // D 加入（无历史条目）→ 模型重新可用，不得因 A/B 耗尽被全局跳过
        assert!(!model_blocked(
            &f,
            "free-a",
            now,
            &["A".to_string(), "B".to_string(), "D".to_string()]
        ));
    }

    /// 保留线口径：WB 池取 max(全局, 50)；其余池不受影响（全局值原样）
    #[test]
    fn wb_reserve_floor_is_50_and_scoped() {
        use crate::commands::api_server::wb_reserve_credits;
        assert_eq!(wb_reserve_credits(0.0), 50.0, "未配置时 WB 池保 50");
        assert_eq!(wb_reserve_credits(10.0), 50.0, "低于 50 的配置不得取消底线");
        assert_eq!(wb_reserve_credits(50.0), 50.0, "等于 50 保持");
        assert_eq!(wb_reserve_credits(200.0), 200.0, "更高配置被尊重");
        // 说明：Trae/Qoder/BuddyAI 三池在装配处直接使用 policy.reserve_credits，
        // 不经过本函数，因此保留 50 的规则在结构上不可能外溢到其他产品线。
    }
}
