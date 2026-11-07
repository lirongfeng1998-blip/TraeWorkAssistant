//! workbuddy-auto 虚拟模型：WorkBuddy 侧的「模型级 + 账号级」智能调度（V4 收费优先）。
//!
//! 定位（与既有 WB 管线的关系，刻意不重复实现）：
//! - **模型级**决策在本模块。V4 阶段瀑布（按目标账号余额 + DeepSeek 预算）：
//!   A 非 DeepSeek 低倍率收费 → B DeepSeek（独立安全预算）→ C 免费 → 下一账号。
//!   阶段内按「真实倍率升序」排（不写死模型 ID），只向前（向免费）回退、不回退到更贵。
//! - **账号级**轮换仍由 `wb_route::wb_aggregate_chat` / `wb_stream_chat` 内部的
//!   账号循环负责（取号/粘性/CAS/故障转移一份实现），本模块只做两件事：
//!   ① 把「本次实际选中的账号」取回来（`WbExecTrace`，见 wb_route）；
//!   ② 按 `uid + model + period` 记录额度状态，供下一轮决策跳过。
//!
//! 数据来源与不确定性（如实标注，不伪造）：
//! - 免费身份：上游目录 `rate == 0.0`（`unified_catalog.rs` 既有语义「0 = 免费」）。
//! - DeepSeek **官方是否公布独立额度**：上游未提供查询接口 → 见报告；本模块只按
//!   「每账号每周期保守预算」限制，绝不自造真实额度数字，也不把网络/5xx/429/鉴权
//!   错误误判为额度耗尽。
//! - DeepSeek 预算计量：为保 SSE 流式不缓冲响应体，用**保守估算**（按倍率缩放），
//!   属试运行值、需按真实历史消耗校准。
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
    /// 该候选所属调度阶段（V4：收费低倍率 → DeepSeek → 免费）
    pub stage: Stage,
}

// ==================== V4 调度阶段（收费优先） ====================
//
// 目标瀑布：稳定收费低倍率 → 余额约 250 时 DeepSeek 安全预算 → 免费模型 → 下一账号。
// 不写死模型 ID：按「真实倍率 + 上游健康」排序，DeepSeek 系列从阶段 A 排除。

/// 进入 DeepSeek 阶段的余额阈值（可配置；**不是** DeepSeek 真实额度）
pub const DEFAULT_STAGE_B_BALANCE: f64 = 250.0;
/// DeepSeek 每账号每周期保守试运行预算（积分；无官方上限时的安全值，可配置）
pub const DEFAULT_DEEPSEEK_BUDGET: f64 = 50.0;
/// 付费请求保留线（沿用上一轮：账号最后 50 积分不用于付费）
pub const RESERVE_CREDITS: f64 = 50.0;
/// 9934 账号「已实证今日 DeepSeek 耗尽」的人工确认日（东八区）；
/// 到真实额度条目恢复（reset_at 到 / 成功实证清零）前持续跳过 DeepSeek 系列。
pub const ACCOUNT_9934_DEEPSEEK_EXHAUSTED_DAY: &str = "2026-11-07";

/// DeepSeek 单请求积分**保守估算**系数（est_credits = rate × 此值）。
/// 为避免缓冲响应体（保 SSE 流式），预算计量用保守估算而非精确计费；
/// 系数按倍率缩放（flash≈5.5、pro≈25.5），使高倍率更快触及预算。属试运行值，
/// 需按真实历史消耗校准（规范 §二.2「根据实际历史证据调整」）。
pub const DEFAULT_DEEPSEEK_CREDITS_PER_RATE: f64 = 50.0;

/// 调度阶段（Ord：越小越优先/越贵，仅允许向前=向免费回退）
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Stage {
    /// A：非 DeepSeek 低倍率收费（稳定优先、倍率升序）
    PaidLow = 0,
    /// B：DeepSeek 系列（独立安全预算）
    DeepSeek = 1,
    /// C：免费模型（rate==0）
    Free = 2,
}

/// 是否 DeepSeek 系列（按 id 识别，不写死到阶段逻辑）
pub fn is_deepseek(model: &str) -> bool {
    model.to_ascii_lowercase().contains("deepseek")
}

/// 模型归属阶段：免费 → Free；DeepSeek 系列 → DeepSeek；其余收费 → PaidLow
pub fn stage_of_model(model: &str, rate: f64) -> Stage {
    if is_free_rate(rate) {
        return Stage::Free;
    }
    if is_deepseek(model) {
        return Stage::DeepSeek;
    }
    Stage::PaidLow
}

/// V4 调度上下文（纯输入，便于单测；生产由目标账号实况填充）
#[derive(Clone, Debug)]
pub struct PlanCtx {
    /// 目标账号「可消费」余额（积分）
    pub balance: f64,
    /// 该账号本额度周期已耗 DeepSeek 积分
    pub ds_spend: f64,
    /// DeepSeek 安全预算（积分）
    pub ds_budget: f64,
    /// false = 该账号 DeepSeek 跳过（如 9934 未恢复）
    pub deepseek_allowed: bool,
    /// 进入 DeepSeek 阶段的余额阈值
    pub stage_b_balance: f64,
}

impl Default for PlanCtx {
    fn default() -> Self {
        Self {
            // 默认余额充裕 → 阶段 A（收费优先，安全默认）
            balance: f64::INFINITY,
            ds_spend: 0.0,
            ds_budget: DEFAULT_DEEPSEEK_BUDGET,
            deepseek_allowed: true,
            stage_b_balance: DEFAULT_STAGE_B_BALANCE,
        }
    }
}

/// 由余额 + DeepSeek 预算 + 9934 规则决定当前调度阶段。
pub fn stage_for(ctx: &PlanCtx) -> Stage {
    // 付费保护线以下：任何付费（含 DeepSeek）都不再分配，只能免费
    if ctx.balance <= RESERVE_CREDITS {
        return Stage::Free;
    }
    // 余额充裕（> 阈值）：稳定低倍率收费优先（阶段 A）
    if ctx.balance > ctx.stage_b_balance {
        return Stage::PaidLow;
    }
    // 余额进入阈值区间（<=阈值 且 > 保留线）：DeepSeek 预算内 → DeepSeek
    if ctx.deepseek_allowed && ctx.ds_spend < ctx.ds_budget {
        return Stage::DeepSeek;
    }
    // DeepSeek 预算耗尽 / 9934 跳过 → 免费
    Stage::Free
}

/// 9934 账号 DeepSeek 是否可调度：
/// - 非 9934 → 恒可调度；
/// - 人工确认耗尽日（2026-11-07）→ 跳过；
/// - 其后仅当该账号 DeepSeek 额度条目已恢复（未处耗尽态）才恢复，不单凭本机日期清限。
pub fn deepseek_allowed_for(uid: &str, now: i64, quota: &QuotaFile) -> bool {
    if !uid.ends_with("9934") {
        return true;
    }
    if period_of(now) == ACCOUNT_9934_DEEPSEEK_EXHAUSTED_DAY {
        return false;
    }
    // 恢复依据真实额度条目（reset_at 到 / 成功清零后 account_model_blocked 为 false）
    let uids = vec![uid.to_string()];
    for m in ["deepseek-v4.1-flash", "deepseek-v4-pro"] {
        if account_model_blocked(quota, uid, m, now) || model_blocked(quota, m, now, &uids) {
            return false;
        }
    }
    true
}

/// 规划候选顺序（V4）：按当前阶段起、只向前（向免费）回退；不向更贵阶段回退。
/// 排序：阶段升序（A→B→C）→ 倍率升序 → id 稳定。倍率未知 / 脏数据 / skip 者排除。
/// `ctx.deepseek_allowed=false` 时 DeepSeek 候选一律排除（9934 等）。
pub fn plan_candidates(
    models: &[wb_catalog::WbModel],
    skip: &dyn Fn(&str) -> bool,
    ctx: &PlanCtx,
) -> Vec<Candidate> {
    let active = stage_for(ctx);
    let mut out: Vec<Candidate> = Vec::new();
    for m in models {
        let rate = m.rate;
        if !rate.is_finite() {
            continue; // 倍率未知：不参与自动调度
        }
        if skip(&m.id) {
            continue;
        }
        let stage = stage_of_model(&m.id, rate);
        if stage < active {
            continue; // 不回退到更贵阶段（余额不足时不得再分配不符合当前阶段的请求）
        }
        if stage == Stage::DeepSeek && !ctx.deepseek_allowed {
            continue; // 9934 等：DeepSeek 系列跳过
        }
        out.push(Candidate {
            model: m.id.clone(),
            rate,
            free: is_free_rate(rate),
            stage,
        });
    }
    out.sort_by(|a, b| {
        a.stage
            .cmp(&b.stage)
            .then_with(|| {
                a.rate
                    .partial_cmp(&b.rate)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| a.model.cmp(&b.model))
    });
    out
}

// ==================== DeepSeek 预算（每账号每周期独立） ====================

fn ds_key(uid: &str, period: &str) -> String {
    format!("{uid}|{period}")
}

/// 读取该账号本周期已耗 DeepSeek 积分
pub fn ds_spend(spends: &BTreeMap<String, f64>, uid: &str, period: &str) -> f64 {
    spends.get(&ds_key(uid, period)).copied().unwrap_or(0.0)
}

/// 记一次 DeepSeek 消耗（credits 为实测/保守估算积分）
pub fn note_ds_spend(spends: &mut BTreeMap<String, f64>, uid: &str, period: &str, credits: f64) {
    if credits <= 0.0 {
        return;
    }
    *spends.entry(ds_key(uid, period)).or_insert(0.0) += credits;
}

/// 该账号本周期 DeepSeek 是否已达安全预算
pub fn ds_over_budget(spends: &BTreeMap<String, f64>, uid: &str, period: &str, budget: f64) -> bool {
    ds_spend(spends, uid, period) >= budget
}

/// DeepSeek 单请求积分保守估算（按倍率缩放，见 DEFAULT_DEEPSEEK_CREDITS_PER_RATE）
pub fn estimate_deepseek_credits(rate: f64) -> f64 {
    if rate.is_finite() && rate > 0.0 {
        rate * DEFAULT_DEEPSEEK_CREDITS_PER_RATE
    } else {
        0.0
    }
}

/// 一次 DeepSeek 请求完成后记账（保守估算；不读响应体，保 SSE 流式）。
/// 仅对 DeepSeek 系列模型累计；非 DeepSeek 调用 no-op。
pub fn note_deepseek_spend(state: &ApiSharedState, uid: Option<&str>, model: &str, rate: f64) {
    if !is_deepseek(model) {
        return;
    }
    let Some(uid) = uid else { return };
    let now = now_epoch();
    let period = period_of(now);
    let est = estimate_deepseek_credits(rate);
    if est <= 0.0 {
        return;
    }
    let mut spends = load_ds_spend(state);
    note_ds_spend(&mut spends, uid, &period, est);
    save_ds_spend(state, &spends);
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

/// DeepSeek 预算消耗持久化键（每账号每周期独立累计）
const DS_KV_KEY: &str = "wb_auto_ds_spend_v1";

fn load_ds_spend(state: &ApiSharedState) -> BTreeMap<String, f64> {
    let store = crate::store::db(&state.data_dir);
    store.kv_get::<BTreeMap<String, f64>>(DS_KV_KEY)
}

fn save_ds_spend(state: &ApiSharedState, m: &BTreeMap<String, f64>) {
    let store = crate::store::db(&state.data_dir);
    let _ = store.kv_set(DS_KV_KEY, m);
    crate::api_server::config_cache::invalidate(&state.data_dir, DS_KV_KEY);
}

/// 目标账号（ExpireFirst：最早到期优先；返回 (uid, 可消费余额)）。
/// 供 V4 阶段决策使用——模型选择需反映「实际将服务的账号」余额/预算/9934 规则。
/// 无可调度账号时返回 None（由下游 pool 报 no_healthy，不在这里伪造健康）。
fn target_account(state: &ApiSharedState) -> Option<(String, f64)> {
    let mut best: Option<(String, f64, bool, i64)> = None; // (uid, credits, has_expiry, expiry)
    for d in state.wb_pool.diagnose() {
        if d.disabled || !d.reason.is_empty() {
            continue; // 仅可调度健康账号
        }
        let credits = d.credits.unwrap_or(0.0);
        if credits <= RESERVE_CREDITS {
            continue; // 低于保留线不作付费目标
        }
        let (has_exp, exp) = match d.credits_expire_at {
            Some(t) if t > 0 => (true, t),
            _ => (false, i64::MAX),
        };
        let better = match &best {
            None => true,
            Some((_, bc, bh, be)) => {
                // ExpireFirst：有到期时间者优先；再比更早到期；再比余额更多
                (has_exp, std::cmp::Reverse(exp), std::cmp::Reverse(credits))
                    > (*bh, std::cmp::Reverse(*be), std::cmp::Reverse(*bc))
            }
        };
        if better {
            best = Some((d.uid, credits, has_exp, exp));
        }
    }
    best.map(|(uid, credits, _, _)| (uid, credits))
}

/// 构造 V4 调度上下文（读目标账号余额 + DeepSeek 已耗 + 9934 规则）
fn plan_ctx(state: &ApiSharedState) -> PlanCtx {
    let now = now_epoch();
    let period = period_of(now);
    let spends = load_ds_spend(state);
    let quota = load_file(state);
    let (uid, balance) = target_account(state).unwrap_or((String::new(), f64::INFINITY));
    let deepseek_allowed = if uid.is_empty() {
        true
    } else {
        deepseek_allowed_for(&uid, now, &quota)
    };
    let ds_sp = if uid.is_empty() {
        0.0
    } else {
        ds_spend(&spends, &uid, &period)
    };
    PlanCtx {
        balance,
        ds_spend: ds_sp,
        ds_budget: DEFAULT_DEEPSEEK_BUDGET,
        deepseek_allowed,
        stage_b_balance: DEFAULT_STAGE_B_BALANCE,
    }
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
    let ctx = plan_ctx(state);
    Ok(plan_candidates(
        &catalog,
        &|id: &str| {
            model_blocked(&f, id, now, &uids)
                || wb_route::model_cooling_remaining(&view, id).is_some()
                // 本轮修复：该模型在该池**所有**可调度账号上都已触发上游限流 →
                // 本轮直接跳过，由下一个候选（阶段内向前回退）接管。
                || wb_route::model_exhausted_for_all_accounts(&view, id)
        },
        &ctx,
    ))
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
            // DeepSeek 预算记账（保守估算；仅 DeepSeek 系列累计）
            note_deepseek_spend(&state, uid.as_deref(), &cand.model, cand.rate);
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
                // 本轮修复：429 属「账号×模型」额度窗口（非整账号、非额度耗尽），
                // 聚合路径响应尚未下发给客户端 → 标记该组合后**换下一个模型**，
                // 而不是把 429 原样抛给客户端让整条任务失败。
                // 旧行为：`classify()` 对 429 明确返回 None（不误判为额度耗尽），
                // 但也因此直接 passthrough，自动切换链路在此断掉。
                if parts.status == StatusCode::TOO_MANY_REQUESTS {
                    if let Some(u) = uid.as_deref() {
                        wb_route::note_account_model_failure(
                            &wb_route::WbView::wb(state.clone()),
                            u,
                            &cand.model,
                            Some(&text),
                        );
                    }
                    log_decision(
                        &state,
                        uid.as_deref(),
                        &cand.model,
                        cand.rate,
                        cand.free,
                        "rate_limited_switch_next",
                        &tried,
                    );
                    last = Some(Response::from_parts(parts, Body::from(bytes)));
                    continue; // 换下一个模型
                }
                // 非额度类（网络/5xx/鉴权/审核）：保留原响应，不盲目换模型
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

/// 流式：按策略取首个**可用**候选直接流式（SSE 首字下发后不可回退）。
///
/// 本轮修复：首字节前可切换。`wb_route::wb_stream_chat` 现在会在提交 200/SSE
/// 之前做池内预检，发现「该模型在所有可调度账号上都被限流」时返回真实 HTTP 429；
/// 由于此时一字节未出，可以安全地换下一个候选（免费 → 低倍率 → 高倍率）。
/// SSE 一旦开始输出正文/工具调用，本函数不再重放整次请求（避免重复生成与扣费）。
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
    if cands.is_empty() {
        return super::routes::openai_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "auto_unavailable",
            "workbuddy-auto：目录内无可用候选（免费与付费模型均被限流/耗尽）",
        );
    }
    let total = cands.len();
    let mut tried: Vec<String> = Vec::new();
    for (idx, cand) in cands.iter().enumerate() {
        // 免费模型做并发额度占用保护；付费模型按积分消耗，不占免费槽
        let reserved = cand.free && try_reserve(&cand.model);
        if cand.free && !reserved {
            continue;
        }
        tried.push(cand.model.clone());
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
            key_str.clone(),
            guard,
            encoded.clone(),
            Some(trace.clone()),
        );
        if cand.free {
            release(&cand.model);
        }
        // 首字节前失败（预检 429）：一字节未出，换下一个候选是安全的
        if resp.status() == StatusCode::TOO_MANY_REQUESTS && idx + 1 < total {
            log_decision(
                &state,
                trace.uid().as_deref(),
                &cand.model,
                cand.rate,
                cand.free,
                "stream_precheck_switch_next",
                &tried,
            );
            continue;
        }
        log_decision(
            &state,
            trace.uid().as_deref(),
            &cand.model,
            cand.rate,
            cand.free,
            if resp.status().is_success() { "stream_start" } else { "stream_precheck_reject" },
            &tried,
        );
        // 流式成功启动即记 DeepSeek 预算（保守估算；SSE 提交后不再中断）
        if resp.status().is_success() {
            note_deepseek_spend(&state, trace.uid().as_deref(), &cand.model, cand.rate);
        }
        return tag(resp, &cand.model, cand.rate, cand.free, &tried, None);
    }
    super::routes::openai_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "auto_unavailable",
        &format!("workbuddy-auto：候选全部在上游限流（已尝试 {}）", tried.join(", ")),
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

/// /v1/models 注入条目（虚拟模型，非上游真实模型）。
///
/// 刻意**只声明可确证的字段**：不写 `rate`（自动模型可能落到付费模型，写 0 会被
/// 前端当作「免费」展示，属于伪造）、不写 `context_length` / `max_tokens` /
/// `supports_image` / `supported_efforts`（随实际选中模型而变，写死即是假信息）。
/// 实际执行信息由响应头 `x-wb-auto-model` / `-rate` / `-free` 给出。
pub fn model_record() -> Value {
    serde_json::json!({
        "id": AUTO_MODEL_ID,
        "object": "model",
        "created": 0,
        "owned_by": "aiwork-auto",
        "display": "WorkBuddy Auto",
        "description": "WorkBuddy 智能调度：免费模型优先 → 倍率递增 → 账号池自动接管（实际模型见响应头 x-wb-auto-model）"
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

    fn ctx_bal(balance: f64) -> PlanCtx {
        PlanCtx {
            balance,
            ..Default::default()
        }
    }

    #[test]
    fn v4_balance_300_prefers_non_deepseek_low_rate() {
        // 场景 1：余额 300 → 非 DeepSeek 低倍率收费优先；DeepSeek 全排除于阶段 A 之外靠 stage 排序殿后
        let models = vec![
            m("paid-big", 1.5),
            m("deepseek-v4.1-flash", 0.11),
            m("glm-5.3-flash", 0.08),
            m("free-b", 0.0),
            m("mimo-v2.6-pro", 0.13),
        ];
        let plan = plan_candidates(&models, &|_| false, &ctx_bal(300.0));
        let ids: Vec<&str> = plan.iter().map(|c| c.model.as_str()).collect();
        // 阶段 A（非 DeepSeek 收费，倍率升序）在前：0.08 → 0.13 → 1.5；随后 DeepSeek(0.11)；最后免费
        assert_eq!(
            ids,
            vec![
                "glm-5.3-flash",
                "mimo-v2.6-pro",
                "paid-big",
                "deepseek-v4.1-flash",
                "free-b"
            ]
        );
        assert_eq!(plan[0].stage, Stage::PaidLow);
        assert!(!is_deepseek(&plan[0].model), "首选不得是 DeepSeek");
    }

    #[test]
    fn nan_rate_not_treated_as_free() {
        let models = vec![m("unknown", f64::NAN), m("paid", 0.5), m("free", 0.0)];
        let plan = plan_candidates(&models, &|_| false, &ctx_bal(300.0));
        let ids: Vec<&str> = plan.iter().map(|c| c.model.as_str()).collect();
        // 余额 300 → 阶段 A 起：paid(0.5) 在 free 之前；unknown 因 NaN 被排除
        assert_eq!(ids, vec!["paid", "free"], "倍率未知的模型不得参与自动调度");
        assert!(!is_free_rate(f64::NAN));
    }

    #[test]
    fn exhausted_free_switches_to_next_free() {
        // 免费阶段（余额低 + 预算耗尽）：free-a 耗尽后跳 free-b（同阶段向前）
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
        // 余额 40（<= 保留线 50）→ 阶段 C（免费），不回退到付费
        let ctx = ctx_bal(40.0);
        let plan = plan_candidates(&models, &|id| model_blocked(&f, id, now, &uids), &ctx);
        let ids: Vec<&str> = plan.iter().map(|c| c.model.as_str()).collect();
        assert_eq!(ids, vec!["free-b"], "免费阶段 free-a 耗尽后只在免费内向前，不回退付费");

        // 再耗尽 free-b → 无免费候选（交由账号切换，不强行回退付费）
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
        let plan = plan_candidates(&models, &|id| model_blocked(&f, id, now, &uids), &ctx);
        assert!(plan.is_empty(), "免费全部耗尽且余额低于保留线 → 无候选（转账号切换）");
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
    fn period_buckets_by_utc_plus_8() {        // 2026-11-05 00:00:00 +08:00 == 2026-11-04 16:00:00 UTC
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

    /// 并发额度占用：同一模型的免费额度槽位同一时刻只允许一个请求占用，
    /// 其余必须失败（不得两个并发请求都以为「自己拿到了免费额度」）
    #[test]
    fn concurrent_free_slot_has_single_holder() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let model = "concurrent-free-slot-probe";
        release(model);
        let won = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let won = won.clone();
            handles.push(std::thread::spawn(move || {
                if try_reserve(model) {
                    won.fetch_add(1, Ordering::SeqCst);
                    std::thread::sleep(std::time::Duration::from_millis(20));
                    release(model);
                }
            }));
        }
        for h in handles {
            h.join().expect("线程不应 panic");
        }
        assert_eq!(won.load(Ordering::SeqCst), 1, "并发下只允许一个持有者");
        assert!(try_reserve(model), "全部释放后应可再次占用");
        release(model);
    }

    /// 并发写额度状态：不同账号的状态互不覆盖，同账号同模型同周期幂等（不膨胀）
    #[test]
    fn concurrent_marks_stay_per_account_and_idempotent() {
        use std::sync::Mutex;
        let now = 1_760_000_000;
        let shared = Arc::new(Mutex::new(QuotaFile::default()));
        let uids = ["uid-a", "uid-b", "uid-c", "uid-d"];
        let mut handles = Vec::new();
        for uid in uids {
            let shared = shared.clone();
            handles.push(std::thread::spawn(move || {
                for _ in 0..50 {
                    let mut f = shared.lock().unwrap();
                    mark(
                        &mut f,
                        uid,
                        "free-a",
                        QuotaStatus::FreeExhausted,
                        "免费额度耗尽",
                        "free quota exhausted",
                        now,
                        None,
                    );
                }
            }));
        }
        for h in handles {
            h.join().expect("线程不应 panic");
        }
        {
            let f = shared.lock().unwrap();
            assert_eq!(f.entries.len(), 4, "每个账号只有一条状态，不得重复膨胀");
            for uid in uids {
                assert!(
                    account_model_blocked(&f, uid, "free-a", now),
                    "{uid} 的状态必须独立成立"
                );
                let e = f
                    .entries
                    .values()
                    .find(|e| e.uid == uid)
                    .expect("条目必须存在");
                assert_eq!(e.model, "free-a");
                assert_eq!(e.status, QuotaStatus::FreeExhausted);
            }
            // 一个账号成功调用只清除自己的状态
            let mut f = f;
            clear_on_success(&mut f, Some("uid-a"), "free-a");
            assert!(!account_model_blocked(&f, "uid-a", "free-a", now));
            for uid in ["uid-b", "uid-c", "uid-d"] {
                assert!(
                    account_model_blocked(&f, uid, "free-a", now),
                    "{uid} 不得被别的账号的成功调用清除"
                );
            }
        }
    }

    // ==================== V4 阶段/预算/9934 场景 ====================

    #[test]
    fn v4_stage_transitions_by_balance_and_budget() {
        // 场景 2：余额 260（>250）→ 阶段 A；245（≤250）→ 阶段 B（DeepSeek）
        assert_eq!(stage_for(&ctx_bal(300.0)), Stage::PaidLow);
        assert_eq!(stage_for(&ctx_bal(260.0)), Stage::PaidLow);
        let mut c = ctx_bal(245.0);
        c.ds_spend = 0.0;
        assert_eq!(stage_for(&c), Stage::DeepSeek, "余额降到 245 → 下次进入 DeepSeek 阶段");

        // 场景 3：DeepSeek 达到安全预算 → 阶段 C（免费）
        c.ds_spend = DEFAULT_DEEPSEEK_BUDGET;
        assert_eq!(stage_for(&c), Stage::Free, "DeepSeek 预算耗尽 → 切免费");

        // 余额 ≤ 保留线：任何付费都停，只能免费
        assert_eq!(stage_for(&ctx_bal(40.0)), Stage::Free);
        assert_eq!(stage_for(&ctx_bal(RESERVE_CREDITS)), Stage::Free);
    }

    #[test]
    fn v4_deepseek_budget_accounting() {
        let mut spends = BTreeMap::new();
        assert_eq!(ds_spend(&spends, "u1", "2026-11-07"), 0.0);
        note_ds_spend(&mut spends, "u1", "2026-11-07", 30.0);
        note_ds_spend(&mut spends, "u1", "2026-11-07", 25.0);
        assert_eq!(ds_spend(&spends, "u1", "2026-11-07"), 55.0, "同账号同周期累计");
        // 账号维度隔离：另一账号不共享预算
        assert_eq!(ds_spend(&spends, "u2", "2026-11-07"), 0.0);
        // 周期维度隔离
        assert_eq!(ds_spend(&spends, "u1", "2026-11-08"), 0.0);
        assert!(ds_over_budget(&spends, "u1", "2026-11-07", 50.0));
        assert!(!ds_over_budget(&spends, "u2", "2026-11-07", 50.0));
        // 估算按倍率缩放（flash 低、pro 高），且 > 0
        assert!(estimate_deepseek_credits(0.11) > 0.0);
        assert!(
            estimate_deepseek_credits(0.51) > estimate_deepseek_credits(0.11),
            "高倍率应更快消耗预算"
        );
    }

    #[test]
    fn v4_9934_deepseek_skip_until_recovered() {
        let now_1107 = 1_794_024_000; // 2026-11-07 12:00 UTC+8
        let quota = QuotaFile::default();
        // 场景 4：9934 在 2026-11-07 → 不调用 DeepSeek
        assert!(!deepseek_allowed_for("wb-e1015c8f7-9934", now_1107, &quota));
        // 非 9934 → 恒可调度
        assert!(deepseek_allowed_for("wb-other-1234", now_1107, &quota));

        // 场景 5：11-08 但真实额度条目仍处耗尽（未 reset）→ 仍不调用
        let now_1108 = now_1107 + 86_400;
        let mut q2 = QuotaFile::default();
        mark(
            &mut q2,
            "wb-e1015c8f7-9934",
            "deepseek-v4.1-flash",
            QuotaStatus::FreeExhausted,
            "t",
            "DeepSeek 额度耗尽",
            now_1108,
            None, // 无 reset_at → 只能靠成功实证恢复
        );
        assert!(
            !deepseek_allowed_for("wb-e1015c8f7-9934", now_1108, &q2),
            "真实额度未恢复前持续跳过"
        );

        // 额度条目到 reset_at 后恢复 → 恢复 DeepSeek 候选
        let mut q3 = QuotaFile::default();
        mark(
            &mut q3,
            "wb-e1015c8f7-9934",
            "deepseek-v4.1-flash",
            QuotaStatus::FreeExhausted,
            "t",
            "DeepSeek 额度耗尽",
            now_1108,
            Some(now_1108 + 60),
        );
        assert!(
            deepseek_allowed_for("wb-e1015c8f7-9934", now_1108 + 61, &q3),
            "reset_at 到 → 恢复候选（不单凭本机日期硬清）"
        );
    }

    #[test]
    fn v4_9934_plan_excludes_deepseek() {
        // 余额 245（本应 DeepSeek 阶段）但 9934 跳过 DeepSeek → 直接免费
        let models = vec![
            m("deepseek-v4.1-flash", 0.11),
            m("glm-5.3-flash", 0.08),
            m("hy4free", 0.0),
        ];
        let mut c = ctx_bal(245.0);
        c.deepseek_allowed = false;
        let plan = plan_candidates(&models, &|_| false, &c);
        let ids: Vec<&str> = plan.iter().map(|x| x.model.as_str()).collect();
        assert_eq!(ids, vec!["hy4free"], "9934 跳过 DeepSeek，245 余额直接免费");
        assert!(ids.iter().all(|i| !is_deepseek(i)));
    }

    #[test]
    fn v4_free_a_429_falls_to_free_b_no_account_cool() {
        // 场景 6：免费 A 429 → 尝试免费 B，且不误冷却整个账号
        let models = vec![m("hy4free", 0.0), m("hy3", 0.0)];
        let mut f = QuotaFile::default();
        let now = 1_760_000_000;
        // hy4free 仅在 uid-a 上额度耗尽（模型级）→ 跳到 hy3
        mark(&mut f, "uid-a", "hy4free", QuotaStatus::FreeExhausted, "t", "x", now, None);
        let uids = vec!["uid-a".to_string()];
        let ctx = ctx_bal(40.0); // 免费阶段
        let plan = plan_candidates(&models, &|id| model_blocked(&f, id, now, &uids), &ctx);
        let ids: Vec<&str> = plan.iter().map(|x| x.model.as_str()).collect();
        assert_eq!(ids, vec!["hy3"], "免费 A 耗尽 → 免费 B");
        // 关键：仅冷却 uid-a 的 hy4free，uid-a 在 hy3 上仍可用（未整账号冷却）
        assert!(!account_model_blocked(&f, "uid-a", "hy3", now));
    }

    #[test]
    fn v4_context_overflow_not_quota_exhaustion() {
        // 场景 10：上下文超限 → 不误判额度耗尽
        assert_eq!(
            classify(400, "prompt is too long: 1124459 tokens > 1048576 maximum"),
            None
        );
        assert_eq!(classify(400, "context length exceeded maximum"), None);
        assert_eq!(classify(413, "request entity too large"), None);
    }

    #[test]
    fn v4_account_level_restriction_still_blocks_model() {
        // 场景 8：上游账号级限制（全部账号在该模型耗尽）→ 严格遵守，模型整体跳过
        let mut f = QuotaFile::default();
        let now = 1_760_000_000;
        for u in ["uid-a", "uid-b"] {
            mark(&mut f, u, "deepseek-v4.1-flash", QuotaStatus::CreditExhausted, "t", "账号积分不足", now, None);
        }
        let uids = vec!["uid-a".to_string(), "uid-b".to_string()];
        assert!(model_blocked(&f, "deepseek-v4.1-flash", now, &uids));
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

    /// 低倍率耗尽 → 切换到更高倍率（同一升序链，不设任何虚拟阈值）
    #[test]
    fn low_multiplier_exhausted_falls_to_higher() {
        let models = vec![m("paid-low", 0.1), m("paid-mid", 0.6), m("paid-high", 2.0)];
        let mut f = QuotaFile::default();
        let now = 1_760_000_000;
        let uids = vec!["A".to_string()];

        let ids = |f: &QuotaFile| -> Vec<String> {
            plan_candidates(&models, &|id| model_blocked(f, id, now, &uids), &PlanCtx::default())
                .into_iter()
                .map(|c| c.model)
                .collect()
        };
        assert_eq!(ids(&f), vec!["paid-low", "paid-mid", "paid-high"]);

        // paid-low 耗尽 → 落到 paid-mid（次低）
        mark(&mut f, "A", "paid-low", QuotaStatus::CreditExhausted, "t", "x", now, None);
        assert_eq!(ids(&f), vec!["paid-mid", "paid-high"]);

        // paid-mid 也耗尽 → 落到最高倍率兜底
        mark(&mut f, "A", "paid-mid", QuotaStatus::CreditExhausted, "t", "x", now, None);
        assert_eq!(ids(&f), vec!["paid-high"]);
    }

    /// 状态持久化编解码：写入 kv 的形式（serde JSON）能原样还原，
    /// 即「重启后额度状态不丢」的前提条件（真正的重启验证需实机，见报告）。
    #[test]
    fn quota_state_roundtrips_through_serialization() {
        let mut f = QuotaFile::default();
        let now = 1_760_000_000;
        mark(
            &mut f,
            "uid-a",
            "free-a",
            QuotaStatus::FreeExhausted,
            "免费额度耗尽",
            "free quota exhausted",
            now,
            Some(now + 3600),
        );
        f.version = 1;
        let json = serde_json::to_string(&f).expect("序列化");
        let back: QuotaFile = serde_json::from_str(&json).expect("反序列化");
        assert_eq!(back.version, 1);
        assert_eq!(back.entries.len(), 1);
        // 还原后判定语义必须一致：未到重置时刻仍为阻挡，过点自动恢复
        assert!(account_model_blocked(&back, "uid-a", "free-a", now));
        assert!(!account_model_blocked(&back, "uid-a", "free-a", now + 3601));
        // 周期分桶与状态都要原样带回
        let e = back.entries.values().next().unwrap();
        assert_eq!(e.status, QuotaStatus::FreeExhausted);
        assert_eq!(e.period, period_of(now));
        assert_eq!(e.reset_at, Some(now + 3600));
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
