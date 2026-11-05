//! workbuddy-auto 虚拟模型：WorkBuddy 侧的模型级智能调度。
//!
//! 设计要点（与既有 WB 管线的关系）：
//! - 只做「模型级」决策：免费(rates==0) → 低倍率 → 高倍率；账号级选择仍由
//!   `wb_route` 内部的账号轮换负责（不重复实现、不改变其语义）。
//! - 倍率与免费身份**全部动态**来自 `wb_catalog`（上游同步落库），不硬编码任何
//!   模型名、倍率或活动时间；rate 为 NaN（元数据缺失）的模型不参与自动调度，
//!   避免把未知倍率误判为免费。
//! - 免费额度状态按 `model + 额度周期` 维度持久化（kv），重启不丢；是否恢复由
//!   `reset_at`（上游给出时）或「成功调用的实证」决定，不猜测本地零点。
//! - 状态三态分离：免费额度耗尽 / 账号积分耗尽 / 模型暂不可用，互不混淆。
//!
//! 数据来源与不确定性（诚实标注）：
//! - 免费身份：上游目录 `rate == 0.0`（`unified_catalog.rs` 既有语义「0 = 免费」）。
//! - 每日免费额度数值：上游**未提供**查询接口 → 本模块记 UNKNOWN，仅依据真实
//!   响应中的额度耗尽话术落状态；绝不自造额度数字。
//! - 账号 UID 归属：`wb_aggregate_chat` 不回传本次选中的账号 → 免费额度状态按
//!   模型维度记录，账号维度为 UNKNOWN（见 `account_dimension_known()`）。

use std::collections::{BTreeMap, HashSet};
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::http::StatusCode;
use axum::response::Response;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

use super::routes::Protocol;
use super::{wb_catalog, wb_route, ApiSharedState};

/// 虚拟模型 ID（不得与上游真实模型重名；上游 `auto` 已被 `is_noise_model_id` 过滤）
pub const AUTO_MODEL_ID: &str = "workbuddy-auto";
/// 免费额度状态持久化键
const KV_KEY: &str = "wb_auto_quota_v1";

// ==================== 状态模型 ====================

/// 免费额度 / 账号积分 / 模型可用性，三者必须区分（规范 §3.3）
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QuotaStatus {
    /// 免费额度耗尽（仅当上游话术确认是免费额度时）
    FreeExhausted,
    /// 账号积分不足/达到保留线
    CreditExhausted,
    /// 模型暂不可用（下架、冷却等）
    ModelUnavailable,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct QuotaEntry {
    /// 账号 UID；账号维度不可知时为 "unknown"
    pub uid: String,
    pub model: String,
    /// 额度周期标识（东八区日期，仅作统计分桶；恢复以 reset_at 为准）
    pub period: String,
    pub status: QuotaStatus,
    /// 判定原因（人可读，不含凭据）
    pub reason: String,
    /// 上游响应片段（截断，脱敏）
    pub evidence: String,
    pub marked_at: i64,
    /// 上游给出的重置时刻（epoch 秒）；None = 上游未提供 → 只能靠成功实证恢复
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
    // 1970-01-01 起算的民用日 → YYYY-MM-DD
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}")
}

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as i64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

// ==================== 倍率与候选规划 ====================

/// rate==0 → 免费；NaN（元数据缺失）不算免费
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
/// `skip` 用于排除当前不可用（额度耗尽且未到重置、模型冷却等）。
pub fn plan_candidates(models: &[wb_catalog::WbModel], skip: &dyn Fn(&str) -> bool) -> Vec<Candidate> {
    let mut free: Vec<Candidate> = Vec::new();
    let mut paid: Vec<Candidate> = Vec::new();
    for m in models {
        let rate = m.rate;
        if !rate.is_finite() {
            continue; // 倍率未知：不参与自动调度（不得伪造成免费）
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

// ==================== 错误分类（真实话术判定） ====================

/// 免费额度耗尽话术（命中即判定免费额度，而非积分）
pub fn looks_like_free_quota(msg: &str) -> bool {
    let m = msg.to_ascii_lowercase();
    const FREE: &[&str] = &[
        "free quota",
        "free_quota",
        "daily free",
        "free usage",
        "免费额度",
        "免费次数",
        "免费体验",
        "赠送额度",
        "赠送次数",
        "今日免费",
        "每日免费",
        "活动额度",
    ];
    FREE.iter().any(|k| m.contains(k))
}

/// 账号积分不足话术
pub fn looks_like_credit(msg: &str) -> bool {
    let m = msg.to_ascii_lowercase();
    const CREDIT: &[&str] = &[
        "insufficient credit",
        "insufficient_credit",
        "credit exhausted",
        "no credit",
        "not enough credit",
        "quota exceeded",
        "积分不足",
        "积分耗尽",
        "余额不足",
        "余额耗尽",
        "额度不足",
        "额度已用完",
    ];
    CREDIT.iter().any(|k| m.contains(k))
}

/// 鉴权类错误：绝不判定为额度耗尽（规范明确禁止误判）
pub fn looks_like_auth(status: u16, msg: &str) -> bool {
    if status == 401 || status == 403 {
        return true;
    }
    let m = msg.to_ascii_lowercase();
    ["unauthorized", "invalid token", "token expired", "登录已失效", "鉴权"].iter().any(|k| m.contains(k))
}

/// 返回应落库的状态；None = 与额度无关（网络/5xx/429/鉴权等，只走既有冷却）
pub fn classify(status: u16, body: &str) -> Option<QuotaStatus> {
    if looks_like_auth(status, body) {
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

/// 该模型当前是否应跳过（额度耗尽且未到上游重置时刻 / 未获成功实证）
pub fn is_blocked(f: &QuotaFile, model: &str, now: i64) -> bool {
    let period = period_of(now);
    f.entries
        .values()
        .filter(|e| e.model == model && e.period == period)
        .any(|e| match e.reset_at {
            Some(r) => now < r,     // 上游给了重置时刻：到点自动恢复
            None => true,           // 上游未给：只能由成功调用实证恢复
        })
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

/// 成功调用即实证「额度可用」：清除该模型的额度类状态（含跨账号）
pub fn clear_model(f: &mut QuotaFile, model: &str) {
    f.entries.retain(|_, e| e.model != model);
}

fn sanitize(s: &str) -> String {
    let t: String = s.chars().take(200).collect();
    t.replace(['\n', '\r'], " ")
}

/// 账号维度是否可知：当前为 false（`wb_aggregate_chat` 不回传选中账号）
pub fn account_dimension_known() -> bool {
    false
}

// ==================== 并发额度占用 ====================

fn reserved_set() -> &'static Mutex<HashSet<String>> {
    static R: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    R.get_or_init(|| Mutex::new(HashSet::new()))
}

/// 尝试占用某模型的免费额度槽位；已被并发请求占用则返回 false（不重复占用）
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
    if let Ok(mut g) = reserved_set().lock() {
        g.remove(model);
    }
}

// ==================== 执行编排 ====================

fn buddy_ai_flag(state: &ApiSharedState) -> Option<bool> {
    let wb = state.wb_enabled.load(std::sync::atomic::Ordering::Relaxed);
    let intl = state
        .buddy_ai_enabled
        .load(std::sync::atomic::Ordering::Relaxed);
    if !wb && !intl {
        None
    } else {
        Some(!wb && intl)
    }
}

/// 依策略选出的候选列表（已排除额度耗尽/冷却中的模型）
fn candidates_now(state: &Arc<ApiSharedState>, buddy_ai: bool) -> Vec<Candidate> {
    let catalog = wb_catalog::load(&state.data_dir);
    let f = load_file(state);
    let now = now_epoch();
    let view = if buddy_ai {
        wb_route::WbView::intl(state.clone())
    } else {
        wb_route::WbView::wb(state.clone())
    };
    plan_candidates(&catalog, &|id: &str| {
        is_blocked(&f, id, now) || wb_route::model_cooling_remaining(&view, id).is_some()
    })
}

/// 非流式：逐候选尝试，额度类失败自动换下一个（模型级闭环）
pub async fn aggregate_auto(
    state: Arc<ApiSharedState>,
    chat_body: Value,
    proto: Protocol,
    key_str: String,
    encoded: HashSet<String>,
    start_ts: std::time::Instant,
) -> Response {
    let buddy_ai = match buddy_ai_flag(&state) {
        Some(b) => b,
        None => {
            return super::routes::openai_error(
                StatusCode::BAD_REQUEST,
                "wb_upstream_disabled",
                "WB 上游未启用（api_pool.json wb_enabled）",
            )
        }
    };
    let cands = candidates_now(&state, buddy_ai);
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
        let body_vec = serde_json::to_vec(&body).unwrap_or_default();
        let guard = state.inflight_guard();
        let resp = wb_route::wb_aggregate_chat(
            state.clone(),
            buddy_ai,
            body_vec,
            cand.model.clone(),
            false,
            start_ts,
            proto,
            key_str.clone(),
            guard,
            encoded.clone(),
        )
        .await;
        if cand.free {
            release(&cand.model);
        }
        if resp.status().is_success() {
            let mut f = load_file(&state);
            clear_model(&mut f, &cand.model);
            f.version = 1;
            save_file(&state, &f);
            return tag(resp, &cand.model, cand.rate, cand.free, &tried);
        }
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
        if let Some(kind) = classify(parts.status.as_u16(), &text) {
            let mut f = load_file(&state);
            mark(
                &mut f,
                "unknown", // 账号维度不可知（见 account_dimension_known）
                &cand.model,
                kind,
                match kind {
                    QuotaStatus::FreeExhausted => "免费额度耗尽（上游话术确认）",
                    QuotaStatus::CreditExhausted => "账号积分不足",
                    QuotaStatus::ModelUnavailable => "模型不可用",
                },
                &text,
                now_epoch(),
                None, // 上游未提供可靠重置时刻 → 靠成功实证恢复
            );
            f.version = 1;
            save_file(&state, &f);
            last = Some(Response::from_parts(parts, Body::from(bytes)));
            continue; // 换下一个模型
        }
        // 非额度类错误（网络/5xx/429/鉴权）：保留原响应，不再盲目换模型
        return Response::from_parts(parts, Body::from(bytes));
    }
    last.unwrap_or_else(|| {
        super::routes::openai_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "auto_unavailable",
            &format!("workbuddy-auto：候选全部失败（已尝试 {}）", tried.join(", ")),
        )
    })
}

/// 流式：按策略取首个候选直接流式（SSE 首字已下发后不可回退）；
/// 失败仅落状态供下次决策参考，不在流内重试。
pub fn stream_auto(
    state: Arc<ApiSharedState>,
    mut chat_body: Value,
    proto: Protocol,
    key_str: String,
    encoded: HashSet<String>,
    start_ts: std::time::Instant,
) -> Response {
    let buddy_ai = match buddy_ai_flag(&state) {
        Some(b) => b,
        None => {
            return super::routes::openai_error(
                StatusCode::BAD_REQUEST,
                "wb_upstream_disabled",
                "WB 上游未启用（api_pool.json wb_enabled）",
            )
        }
    };
    let cands = candidates_now(&state, buddy_ai);
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
    let guard = state.inflight_guard();
    let resp = wb_route::wb_stream_chat(
        state.clone(),
        buddy_ai,
        body_vec,
        cand.model.clone(),
        start_ts,
        proto,
        key_str,
        guard,
        encoded,
    );
    tag(resp, &cand.model, cand.rate, cand.free, &[cand.model.clone()])
}

fn tag(resp: Response, model: &str, rate: f64, free: bool, tried: &[String]) -> Response {
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
    Response::from_parts(parts, body)
}

/// /v1/models 注入条目（虚拟模型，非上游真实模型）
pub fn model_record() -> Value {
    serde_json::json!({
        "id": AUTO_MODEL_ID,
        "object": "model",
        "created": 0,
        "owned_by": "workbuddy-auto",
        "display": "WorkBuddy Auto",
        "description": "WorkBuddy 智能调度：免费模型优先 → 倍率递增 → 账号池自动接管"
    })
}

// ==================== 单元测试（纯逻辑，不触网、不碰生产库） ====================

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
        let models = vec![m("paid-big", 1.5), m("paid-low", 0.2), m("free-b", 0.0), m("free-a", 0.0), m("paid-mid", 0.69)];
        let plan = plan_candidates(&models, &|_| false);
        let ids: Vec<&str> = plan.iter().map(|c| c.model.as_str()).collect();
        assert_eq!(ids, vec!["free-a", "free-b", "paid-low", "paid-mid", "paid-big"]);
        assert!(plan[0].free && plan[1].free && !plan[2].free);
    }

    #[test]
    fn unknown_rate_excluded_not_faked_free() {
        let models = vec![m("nan-model", f64::NAN), m("free-a", 0.0)];
        let plan = plan_candidates(&models, &|_| false);
        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].model, "free-a");
        assert!(!is_free_rate(f64::NAN));
    }

    #[test]
    fn skip_excludes_candidate() {
        let models = vec![m("free-a", 0.0), m("free-b", 0.0)];
        let plan = plan_candidates(&models, &|id| id == "free-a");
        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].model, "free-b");
    }

    #[test]
    fn classify_separates_free_from_credit_and_auth() {
        assert_eq!(classify(429, "今日免费额度已用完"), Some(QuotaStatus::FreeExhausted));
        assert_eq!(classify(402, "insufficient credit"), Some(QuotaStatus::CreditExhausted));
        assert_eq!(classify(401, "unauthorized"), None);
        assert_eq!(classify(500, "upstream error"), None);
        assert_eq!(classify(429, "rate limit exceeded"), None);
    }

    #[test]
    fn blocked_then_cleared_by_success() {
        let mut f = QuotaFile { version: 1, entries: BTreeMap::new() };
        let now = 1_790_000_000;
        mark(&mut f, "unknown", "free-a", QuotaStatus::FreeExhausted, "free", "今日免费额度已用完", now, None);
        assert!(is_blocked(&f, "free-a", now));
        assert!(!is_blocked(&f, "free-b", now));
        clear_model(&mut f, "free-a");
        assert!(!is_blocked(&f, "free-a", now));
    }

    #[test]
    fn reset_at_allows_recovery() {
        let mut f = QuotaFile { version: 1, entries: BTreeMap::new() };
        let now = 1_790_000_000;
        mark(&mut f, "unknown", "free-a", QuotaStatus::FreeExhausted, "free", "free quota", now, Some(now + 100));
        assert!(is_blocked(&f, "free-a", now));
        assert!(!is_blocked(&f, "free-a", now + 200));
    }

    #[test]
    fn reservation_prevents_double_spend() {
        assert!(try_reserve("free-x"));
        assert!(!try_reserve("free-x"));
        release("free-x");
        assert!(try_reserve("free-x"));
        release("free-x");
    }

    #[test]
    fn period_is_utc8_date() {
        // 2026-11-05T16:00:00Z = 东八区 2026-11-06 00:00
        assert_eq!(period_of(1_793_894_400), "2026-11-06");
        assert_eq!(period_of(1_793_894_400 - 1), "2026-11-05");
    }
}
