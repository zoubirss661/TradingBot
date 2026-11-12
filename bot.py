import os, io, re, json, time, asyncio, logging, threading, functools
import datetime as dt
from concurrent.futures import ThreadPoolExecutor
import numpy as np, pandas as pd, requests, ccxt, matplotlib
import urllib.parse, concurrent.futures
import types as _types
# ===================== محرك الإشارات الدقيق (مدمج) =====================
# اتجاه يومي+4س، وقف هيكلي، أهداف من مستويات حقيقية، لا مطاردة، بدون repaint.
ENG_TP_FRACS = [0.50, 0.25, 0.15, 0.10]

ENG_CFG = dict(
    min_score=8.5,        # رفع الحد الأدنى: صفقات أقل لكنها ممتازة فقط (A/A+)
    grade_a=9.5,
    grade_aplus=11.0,
    min_adx=20.0,         # أقل من هذا = سوق عرضي
    max_ext_atr=2.5,      # أقصى بعد عن EMA20 عند الدخول (لا مطاردة)
    rsi_long_max=70.0,
    rsi_short_min=30.0,
    min_sl_atr=1.2,
    max_sl_atr=2.5,
    sl_buffer_atr=0.3,
    min_rr_tp1=1.5,       # أقل مسافة للهدف الأول بوحدات R
    max_tp_atr=6.0,       # سقف بعد أي هدف
    pivot_w=3,
    pivot_lb_entry=150,
    pivot_lb_htf=120,
    swing_lb=40,
    be_offset_r=0.08,     # نقطة التعادل = الدخول + 0.08R (تغطي الرسوم)
    trail_atr=2.5,
    max_risk_pct=0.03,    # سقف صارم: وقف الخسارة لا يتجاوز 3% من سعر الدخول أبداً
    min_price_pct=0.05,   # أدنى سعر هدف للشورت (لا أهداف سالبة)
    max_hold=6,          # يوم تداول: ~24 ساعة على فريم 4س
)


# ----------------------------------------------------------------- مؤشرات
def _eng_wilder(s, n):
    return s.ewm(alpha=1.0 / n, adjust=False, min_periods=n).mean()


def eng_prepare(df):
    """يحسب كل المؤشرات من الصفر (سببية: لا تنظر للمستقبل)."""
    df = df.copy().reset_index(drop=True)
    c, h, l, v = df["close"], df["high"], df["low"], df["volume"]
    for n in (20, 21, 50, 55, 200):
        df[f"ema{n}"] = c.ewm(span=n, adjust=False, min_periods=n).mean()
    pc = c.shift()
    tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    df["atr"] = _eng_wilder(tr, 14)
    d = c.diff()
    up, dn = _eng_wilder(d.clip(lower=0), 14), _eng_wilder((-d).clip(lower=0), 14)
    rs = up / dn.replace(0, np.nan)
    rsi = 100 - 100 / (1 + rs)
    rsi[(dn == 0) & up.notna()] = 100.0
    df["rsi"] = rsi
    macd = c.ewm(span=12, adjust=False, min_periods=12).mean() - c.ewm(span=26, adjust=False, min_periods=26).mean()
    df["macd_hist"] = macd - macd.ewm(span=9, adjust=False, min_periods=9).mean()
    df["rvol"] = v / v.shift(1).rolling(20).mean().replace(0, np.nan)
    # ADX
    upm, dnm = h.diff(), -l.diff()
    pdm = pd.Series(np.where((upm > dnm) & (upm > 0), upm, 0.0), index=df.index)
    mdm = pd.Series(np.where((dnm > upm) & (dnm > 0), dnm, 0.0), index=df.index)
    atr14 = _eng_wilder(tr, 14).replace(0, np.nan)
    pdi, mdi = 100 * _eng_wilder(pdm, 14) / atr14, 100 * _eng_wilder(mdm, 14) / atr14
    dx = 100 * (pdi - mdi).abs() / (pdi + mdi).replace(0, np.nan)
    df["pdi"], df["mdi"], df["adx"] = pdi, mdi, _eng_wilder(dx, 14)
    return df


def eng_resample_daily(df):
    """4س -> يومي (أيام مكتملة فقط). df فيه t(ms) open high low close volume."""
    day = (df["t"] // 86_400_000).astype("int64")
    g = df.groupby(day, sort=True)
    out = g.agg(t=("t", "first"), open=("open", "first"), high=("high", "max"),
                low=("low", "min"), close=("close", "last"), volume=("volume", "sum"),
                n=("t", "count"))
    full = out["n"].max()
    out = out[out["n"] == full].drop(columns="n").reset_index(drop=True)
    out["t"] = (out["t"] // 86_400_000) * 86_400_000
    return out


def eng_trend_state(df, i):
    """-2 هابط قوي، -1 هابط، 0 عرضي، 1 صاعد، 2 صاعد قوي."""
    if df is None or i is None or i < 55:
        return 0
    c = float(df["close"].iat[i])
    e20, e50, e200 = (float(df[k].iat[i]) for k in ("ema20", "ema50", "ema200"))
    if np.isnan(e20) or np.isnan(e50):
        return 0
    slope = e50 - float(df["ema50"].iat[i - 5])
    has200 = not np.isnan(e200)
    if e20 > e50 and c > e50 and slope > 0:
        return 2 if (has200 and e50 > e200 and c > e200) else 1
    if e20 < e50 and c < e50 and slope < 0:
        return -2 if (has200 and e50 < e200 and c < e200) else -1
    return 0


def eng_find_pivots(df, i, lookback, w):
    """قمم/قيعان مؤكدة (w شموع من كل جهة). ترجع قائمتين من (index, price)."""
    H, L = df["high"].to_numpy(), df["low"].to_numpy()
    lo, hi = max(w, i - lookback), i - w
    highs, lows = [], []
    for k in range(lo, hi + 1):
        if H[k] >= H[k - w:k + w + 1].max():
            highs.append((k, float(H[k])))
        if L[k] <= L[k - w:k + w + 1].min():
            lows.append((k, float(L[k])))
    return highs, lows


def eng_cluster(levels, tol):
    if not levels:
        return []
    levels = sorted(levels)
    out, cur = [], [levels[0]]
    for x in levels[1:]:
        if x - np.mean(cur) <= tol:
            cur.append(x)
        else:
            out.append(float(np.mean(cur)))
            cur = [x]
    out.append(float(np.mean(cur)))
    return out


# ----------------------------------------------------------------- مستويات الصفقة
def eng_build_levels(df, i, side, price, atr, htf=None, htf_i=None, cfg=ENG_CFG):
    """يرجع (lv, reason). lv=None إذا كانت الصفقة غير مجدية (reason يشرح السبب)."""
    w = cfg["pivot_w"]
    H, L = df["high"].to_numpy(), df["low"].to_numpy()
    highs, lows = eng_find_pivots(df, i, cfg["swing_lb"], w)

    # ---- وقف الخسارة الهيكلي
    buf = cfg["sl_buffer_atr"] * atr
    if side == 1:
        cands = [(k, p) for k, p in lows if price - p >= 0.6 * atr]
        swing = max((p for k, p in cands), default=None)  # أقرب قاع صالح
        if swing is None:
            swing = float(L[max(0, i - 11):i + 1].min())
        sl = swing - buf
        dist = price - sl
    else:
        cands = [(k, p) for k, p in highs if p - price >= 0.6 * atr]
        swing = min((p for k, p in cands), default=None)
        if swing is None:
            swing = float(H[max(0, i - 11):i + 1].max())
        sl = swing + buf
        dist = sl - price
    if dist > cfg["max_sl_atr"] * atr:
        return None, "الوقف الهيكلي بعيد (%.1f ATR) — المخاطرة كبيرة" % (dist / atr)
    if dist < cfg["min_sl_atr"] * atr:
        dist = cfg["min_sl_atr"] * atr
        sl = price - side * dist
    max_risk_pct = cfg.get("max_risk_pct", 0.03)
    if dist > price * max_risk_pct:
        dist = price * max_risk_pct
        sl = price - side * dist
    risk = dist

    # ---- مستويات الأهداف (قمم/قيعان 4س + يومي)
    ph, pl = eng_find_pivots(df, i, cfg["pivot_lb_entry"], w)
    pool = [p for _, p in (ph if side == 1 else pl)]
    if htf is not None and htf_i is not None and htf_i > 10:
        hh, hl = eng_find_pivots(htf, htf_i, cfg["pivot_lb_htf"], w)
        pool += [p for _, p in (hh if side == 1 else hl)]
    if side == 1:
        pool = [p for p in pool if p > price + 0.5 * atr]
    else:
        pool = [p for p in pool if p < price - 0.5 * atr]
    levels = eng_cluster(pool, 0.4 * atr)
    levels = sorted(levels) if side == 1 else sorted(levels, reverse=True)

    if levels:
        d1 = abs(levels[0] - price)
        if d1 < cfg["min_rr_tp1"] * risk:
            kind = "مقاومة" if side == 1 else "دعم"
            return None, "%s قريبة (%.2f ATR) — لا توجد مساحة كافية للصفقة" % (kind, d1 / atr)

    cap = cfg["max_tp_atr"] * atr
    targets, srcs = [], []
    for lv_px in levels:
        px = lv_px - side * 0.1 * atr           # نخرج قبل المستوى بقليل
        d = abs(px - price)
        if d < cfg["min_rr_tp1"] * risk or d > cap:
            continue
        if targets and abs(px - targets[-1]) < 0.5 * risk:
            continue
        targets.append(px)
        srcs.append("level")
        if len(targets) == 4:
            break
    for m in (1.5, 2.5, 3.5, 5.0, 6.5):
        if len(targets) >= 4:
            break
        px = price + side * risk * m
        if abs(px - price) > cap:
            break
        if not targets or abs(px - price) >= abs(targets[-1] - price) + 0.5 * risk:
            targets.append(px)
            srcs.append("R")
    step = max(0.5 * risk, 0.35 * atr)
    while len(targets) < 4:
        base = targets[-1] if targets else price + side * cfg["min_rr_tp1"] * risk
        targets.append(base + side * step)
        srcs.append("R")
    # سقف المخاطرة كنسبة من السعر (مهم للعملات الرخيصة مثل UAI)
    max_risk_pct = cfg.get("max_risk_pct", 0.03)
    if risk > price * max_risk_pct:
        risk = price * max_risk_pct
        sl = price - side * risk
    fixed, last = [], price
    for px in targets:
        if abs(px - price) < abs(last - price) + 0.35 * atr:
            px = price + side * (abs(last - price) + 0.35 * atr)
        # لا أهداف ≤ 0 أبداً (شورت على عملات رخيصة)
        if side == -1:
            px = max(px, price * cfg.get("min_price_pct", 0.05))
        else:
            px = max(px, price * 1.001)
        fixed.append(px)
        last = px
    # إعادة بناء أهداف R إذا بقيت أقل من 4 بعد القص
    while len(fixed) < 4:
        base = fixed[-1] if fixed else price + side * max(cfg["min_rr_tp1"] * risk, 0.01 * price)
        nxt = base + side * max(0.5 * risk, 0.35 * atr)
        if side == -1:
            nxt = max(nxt, price * cfg.get("min_price_pct", 0.05))
        fixed.append(nxt)
        srcs.append("R")
    fixed = fixed[:4]
    srcs = (srcs + ["R"] * 4)[:4]
    tps = [dict(px=float(px), pct=round(100 * side * (px - price) / price, 2),
                frac=ENG_TP_FRACS[k], hit=False, src=srcs[k]) for k, px in enumerate(fixed)]
    if side == -1:
        sl = min(sl, price * (1 + max_risk_pct))  # سقف وقف الشورت
    else:
        sl = max(sl, price * (1 - max_risk_pct))
    # ---- حماية صارمة: وقف الخسارة لا يتجاوز أبداً 3% من سعر الدخول (منع كارثة مثل JCT)
    if abs(price - sl) / price > 0.03:
        sl = price * (1 - side * 0.03)
    risk = abs(price - sl)
    if risk <= 0:
        return None, "مخاطرة غير صالحة"
    # إعادة ضبط أهداف TP1 لتضمن RR>=min_rr بعد قص الوقف
    min_tp1 = risk * cfg["min_rr_tp1"]
    if tps and abs(tps[0]["px"] - price) < min_tp1:
        tps[0]["px"] = float(price + side * min_tp1)
        tps[0]["pct"] = round(100 * side * (tps[0]["px"] - price) / price, 2)
        # إعادة ترتيب الأهداف اللاحقة إن لزم
        last_px = tps[0]["px"]
        for k in range(1, len(tps)):
            if abs(tps[k]["px"] - price) <= abs(last_px - price):
                step = max(0.5 * risk, 0.35 * atr)
                tps[k]["px"] = float(last_px + side * step)
                tps[k]["pct"] = round(100 * side * (tps[k]["px"] - price) / price, 2)
            last_px = tps[k]["px"]
    lv = dict(entry=float(price), sl=float(sl), risk=float(risk), tps=tps, atr=float(atr))
    return lv, ""


# ----------------------------------------------------------------- القرار
def _eng_sgn(x):
    return 1 if x > 0 else (-1 if x < 0 else 0)


def eng_evaluate(df, htf=None, ltf=None, price=None, i=None, htf_i=None, ltf_i=None,
             fut_score=0, btc_trend=0, patterns=None, cfg=None, force_side=None,
             funding=None, oi_change=None, ls_ratio=None, liq_data=None):
    """
    df: شموع إطار الدخول (4س) بعد eng_prepare(). htf: اليومي. ltf: ساعة (اختياري).
    يرجع dict: status (LONG/SHORT/WAIT) + score + grade + lv + reasons + wait_reasons.
    """
    cfg = cfg or ENG_CFG
    i = len(df) - 1 if i is None else i
    if htf is not None and htf_i is None:
        htf_i = len(htf) - 1
    if ltf is not None and ltf_i is None:
        ltf_i = len(ltf) - 1
    res = dict(status="WAIT", side=0, score=0.0, grade="WAIT", confidence=0, lv=None,
               reasons=[], warnings=[], wait_reasons=[], watch=None)

    def wait(msg):
        res["wait_reasons"].append(msg)
        return res

    if i < 60:
        return wait("بيانات غير كافية")
    atr = float(df["atr"].iat[i])
    close = float(df["close"].iat[i])
    price = float(price) if price else close
    if np.isnan(atr) or atr <= 0:
        return wait("ATR غير متاح")

    etf = eng_trend_state(df, i)
    htf_t = eng_trend_state(htf, htf_i) if htf is not None else 0
    ltf_t = eng_trend_state(ltf, ltf_i) if ltf is not None else 0
    res["trends"] = dict(d1=htf_t, h4=etf, h1=ltf_t)

    side = force_side if force_side else _eng_sgn(etf)
    if side == 0:
        return wait("السوق عرضي على إطار 4 ساعات")
    if etf * side <= 0:
        return wait("اتجاه 4س لا يدعم الاتجاه المطلوب")
    if htf is not None and htf_t * side < 0:
        return wait("الصفقة عكس الاتجاه اليومي")
    if htf is not None and htf_t == 0 and abs(etf) < 2:
        return wait("اليومي محايد واتجاه 4س ضعيف")

    adx = float(df["adx"].iat[i]) if not np.isnan(df["adx"].iat[i]) else 0.0
    rsi = float(df["rsi"].iat[i]) if not np.isnan(df["rsi"].iat[i]) else 50.0
    if adx < cfg["min_adx"]:
        return wait("سوق عرضي (ADX %.0f أقل من %d)" % (adx, cfg["min_adx"]))
    if side == 1 and rsi > cfg["rsi_long_max"]:
        return wait("RSI %.0f تشبع شراء — لا تشتري القمة" % rsi)
    if side == -1 and rsi < cfg["rsi_short_min"]:
        return wait("RSI %.0f تشبع بيع — لا تبيع القاع" % rsi)

    ema20 = float(df["ema20"].iat[i])
    ext = side * (price - ema20) / atr
    if ext > cfg["max_ext_atr"]:
        res["watch"] = float(ema20 + side * 0.3 * atr)
        return wait("السعر بعيد عن EMA20 (%.1f ATR) — انتظر ارتداداً قرب %.6g" % (ext, res["watch"]))

    # ---- النقاط
    score = 0.0
    why = res["reasons"]
    score += abs(htf_t) if htf is not None else 1.0
    score += abs(etf)
    why.append("اتجاه يومي %s / 4س %s" % ("متوافق" if htf_t * side > 0 else "محايد",
                                          "قوي" if abs(etf) == 2 else "متوافق"))
    if ltf is not None:
        score += 0.5 * ltf_t * side
        if ltf_t * side < 0:
            res["warnings"].append("إطار الساعة عكس الصفقة (دخول مبكر)")

    mh = float(df["macd_hist"].iat[i]) if not np.isnan(df["macd_hist"].iat[i]) else 0.0
    mh_prev = float(df["macd_hist"].iat[i - 1]) if not np.isnan(df["macd_hist"].iat[i - 1]) else 0.0
    if mh * side > 0 and side * (mh - mh_prev) > 0:
        score += 1.0
        why.append("زخم MACD متزايد")
    elif mh * side > 0:
        score += 0.5
    else:
        score -= 1.0
        res["warnings"].append("زخم MACD ضد الصفقة")

    if adx >= 25:
        score += 1.0
        why.append("ترند قوي (ADX %.0f)" % adx)
    else:
        score += 0.5

    good_lo, good_hi = (42, 65) if side == 1 else (35, 58)
    if good_lo <= rsi <= good_hi:
        score += 1.0
        why.append("RSI صحي %.0f" % rsi)
    else:
        score += 0.3

    rv = float(df["rvol"].iat[i]) if not np.isnan(df["rvol"].iat[i]) else 1.0
    if rv >= 1.2:
        score += 1.0
        why.append("حجم أعلى من المتوسط ×%.1f" % rv)
    elif rv >= 0.9:
        score += 0.3
    elif rv < 0.7:
        score -= 1.0
        res["warnings"].append("حجم ضعيف ×%.1f" % rv)

    if -1.0 <= ext <= 1.5:
        score += 1.0
        why.append("دخول قريب من المتوسط (ليس مطاردة)")
    elif ext < -1.0:
        score -= 0.5

    # ---- تقاطع EMA21 / EMA55 (اتجاه الترند على 4س)
    try:
        e21 = float(df["ema21"].iat[i]) if "ema21" in df.columns else float("nan")
        e55 = float(df["ema55"].iat[i]) if "ema55" in df.columns else float("nan")
        e21_prev = float(df["ema21"].iat[i-1]) if "ema21" in df.columns and i >= 1 else float("nan")
        e55_prev = float(df["ema55"].iat[i-1]) if "ema55" in df.columns and i >= 1 else float("nan")
        if not any(np.isnan(x) for x in (e21, e55, e21_prev, e55_prev)):
            # تقاطع صاعد: EMA21 يعبر فوق EMA55
            if e21_prev <= e55_prev and e21 > e55 and side == 1:
                score += 2.0
                why.append("تقاطع صاعد EMA21 فوق EMA55")
            # تقاطع هابط: EMA21 يعبر تحت EMA55
            elif e21_prev >= e55_prev and e21 < e55 and side == -1:
                score += 2.0
                why.append("تقاطع هابط EMA21 تحت EMA55")
            # استمرار الترند (محاذاة)
            elif (e21 > e55 and side == 1) or (e21 < e55 and side == -1):
                score += 0.5
    except Exception:
        pass

    # ---- Quality Score (قواعد تحليل فني — ليس نموذج ML/LLM)
    # قرب من EMA20 (<1.5 ATR) + حجم + نماذج: تعزيز جودة الإشارة فقط
    if abs(ext) <= 1.5:
        score += 1.0
        why.append("جودة: سعر قريب من EMA20")
    rv_q = float(df["rvol"].iat[i]) if not np.isnan(df["rvol"].iat[i]) else 1.0
    if rv_q >= 1.5:
        score += 1.0
        why.append("جودة: حجم استثنائي ×%.1f" % rv_q)
    if patterns:
        for p in patterns:
            b = p.get("bias")
            if (b == "bullish" and side == 1) or (b == "bearish" and side == -1):
                score += 1.0
                why.append("جودة: نموذج %s يدعم" % p.get("name", ""))
                break  # مرة واحدة فقط

    score += max(-1.5, min(1.5, 0.5 * fut_score))
    if fut_score >= 2:
        why.append("بيانات الفيوتشر تدعم الاتجاه")
    elif fut_score <= -2:
        res["warnings"].append("بيانات الفيوتشر ضد الاتجاه")

    if patterns:
        for p in patterns:
            b = p.get("bias")
            if (b == "bullish" and side == 1) or (b == "bearish" and side == -1):
                score += 1.0
                why.append("نموذج %s يدعم الصفقة" % p.get("name", ""))
            elif b in ("bullish", "bearish"):
                score -= 1.5
                res["warnings"].append("نموذج %s عكس الصفقة" % p.get("name", ""))
    if btc_trend * side < 0:
        score -= 1.0
        res["warnings"].append("البيتكوين عكس اتجاه الصفقة")
    elif btc_trend * side > 0:
        score += 0.5

    # ---- فلاتر الفيوتشر (Funding / OI / Long-Short / Liquidations)
    if funding is not None:
        if side == 1 and funding > 0.05:
            return wait("Funding %.3f%% مرتفع — ازدحام في الشراء، خطر على الـ Long" % funding)
        if side == -1 and funding < -0.05:
            return wait("Funding %.3f%% سلبي جداً — ازدحام في البيع، خطر على الـ Short" % funding)
    if oi_change is not None and oi_change > 5:
        score += 1.0
        why.append("Open Interest ↑ %.1f%% مع اتجاه الصفقة" % oi_change)
    if ls_ratio is not None:
        if side == 1 and ls_ratio > 2.0:
            res["warnings"].append("نسبة Long/Short %.2f — ازدحام شراء" % ls_ratio)
        elif side == -1 and ls_ratio < 0.5:
            res["warnings"].append("نسبة Long/Short %.2f — ازدحام بيع" % ls_ratio)
    if liq_data:
        own = float(liq_data.get("long_liq" if side == 1 else "short_liq", 0) or 0)
        opp = float(liq_data.get("short_liq" if side == 1 else "long_liq", 0) or 0)
        if opp > 0 and opp > 2 * own:
            res["warnings"].append("تصفيات الاتجاه المعاكس ضعف تصفيات اتجاه الصفقة")

    res["score"] = round(score, 2)
    if score < cfg["min_score"]:
        return wait("جودة الإعداد غير كافية (%.1f من %.1f)" % (score, cfg["min_score"]))

    lv, why_not = eng_build_levels(df, i, side, price, atr, htf, htf_i, cfg)
    if lv is None:
        return wait(why_not)

    grade = "A+" if score >= cfg["grade_aplus"] else ("A" if score >= cfg["grade_a"] else "B")
    res.update(status="LONG" if side == 1 else "SHORT", side=side, grade=grade, lv=lv,
               confidence=int(min(95, 35 + score * 4.5)))
    res["rr"] = [round(abs(t["px"] - price) / lv["risk"], 2) for t in lv["tps"]]
    return res


# ----------------------------------------------------------------- محاكاة / Backtest
def eng_simulate(df_raw, htf_raw=None, start=260, cfg=None, fee=0.0008, slip=0.0005, btc_trend=0, prepared=None):
    """Backtest مشي-للأمام: كل قرار يستخدم بيانات حتى الشمعة i فقط (لا تجسس على المستقبل)."""
    cfg = cfg or ENG_CFG
    if prepared is not None:
        if not isinstance(prepared, (tuple, list)) or len(prepared) != 2:
            log.warning("backtest: prepared data must be (ltf, htf)")
            return []
        d, htf = prepared
    else:
        if df_raw is None or not isinstance(df_raw, pd.DataFrame) or df_raw.empty:
            log.warning("backtest: missing/empty raw candle dataframe")
            return []
        d = eng_prepare(df_raw)
        daily_raw = htf_raw if htf_raw is not None else eng_resample_daily(df_raw)
        htf = eng_prepare(daily_raw) if isinstance(daily_raw, pd.DataFrame) and not daily_raw.empty else pd.DataFrame()
    if d is None or not isinstance(d, pd.DataFrame) or d.empty or len(d) <= max(60, int(start)):
        log.warning("backtest: insufficient prepared candles (got %s)", 0 if d is None else len(d))
        return []
    if htf is None or not isinstance(htf, pd.DataFrame):
        htf = pd.DataFrame()
    required = {"t", "open", "high", "low", "close", "volume", "atr", "adx"}
    if not required.issubset(d.columns):
        log.warning("backtest: prepared candles missing columns: %s", sorted(required - set(d.columns)))
        return []
    # n was previously missing here; that caused NameError for every symbol.
    n = len(d)
    start = max(0, int(start))
    if n <= max(60, start + 1):
        log.warning("backtest: insufficient candles for simulation (n=%s, start=%s)", n, start)
        return []
    diffs = np.diff(d["t"].to_numpy(dtype="int64")[-200:])
    diffs = diffs[diffs > 0]
    if diffs.size == 0:
        log.warning("backtest: invalid candle timestamps")
        return []
    tfms = int(np.median(diffs))
    # The daily timeframe can be empty for short/partial histories; never index a missing `t`.
    htf_close = (htf["t"].to_numpy(dtype="int64") + 86_400_000
                 if "t" in htf.columns else np.array([], dtype="int64"))
    t_arr = d["t"].to_numpy(dtype="int64")
    O, H, L, C, A = (d[k].to_numpy() for k in ("open", "high", "low", "close", "atr"))
    trades, i = [], start
    while i < n - 1:
        hi = int(np.searchsorted(htf_close, t_arr[i] + tfms, side="right") - 1)
        ev = eng_evaluate(d, htf, price=C[i], i=i, htf_i=hi, btc_trend=btc_trend, cfg=cfg)
        if ev["status"] == "WAIT":
            i += 1
            continue
        lv, s = ev["lv"], ev["side"]
        fill, risk, sl = lv["entry"], lv["risk"], lv["sl"]
        tps = [dict(t) for t in lv["tps"]]
        remaining, realized, be, trail, ext = 1.0, 0.0, False, False, fill
        kind, j = "TIME", i + 1
        while j < n:
            h, l = H[j], L[j]
            if (s == 1 and l <= sl) or (s == -1 and h >= sl):
                realized += remaining * s * (sl - fill) / risk
                kind = "TRAIL" if trail else ("BE" if be else "SL")
                remaining = 0.0
                break
            for t in tps:
                if t["hit"]:
                    continue
                if not ((s == 1 and h >= t["px"]) or (s == -1 and l <= t["px"])):
                    break
                t["hit"] = True
                realized += t["frac"] * s * (t["px"] - fill) / risk
                remaining -= t["frac"]
            if remaining <= 1e-9:
                kind, remaining = "TP", 0.0
                break
            if tps[0]["hit"] and not be:
                sl = max(sl, fill + cfg["be_offset_r"] * risk) if s == 1 else min(sl, fill - cfg["be_offset_r"] * risk)
                be = True
            ext = max(ext, h) if s == 1 else min(ext, l)
            if sum(t["hit"] for t in tps) >= 2:
                trail = True
                a = A[j]
                if a == a and a > 0:
                    nsl = ext - s * cfg["trail_atr"] * a
                    sl = max(sl, nsl) if s == 1 else min(sl, nsl)
            if j - i >= cfg["max_hold"]:
                realized += remaining * s * (C[j] - fill) / risk
                remaining = 0.0
                break
            j += 1
        if remaining > 1e-9:
            jj = min(j, n - 1)
            realized += remaining * s * (C[jj] - fill) / risk
        # حجم الصفقة على أساس مخاطرة ثابتة 1% من رأس المال الافتراضي (وحدة R = 1)
        # في الباك تست نعمل بوحدة R؛ التكلفة تشمل رسوم الدخول+الخروج + تقدير تمويل
        position_size = 1.0
        # رسوم تداول + انزلاق (دخول وخروج)
        trade_cost = 2 * (fee + slip) * fill * position_size / risk
        # تقدير رسوم التمويل: ~0.01% كل 8 ساعات × عدد الفترات خلال الصفقة
        bars_held = max(1, min(j, n - 1) - i)
        # tfms تقريباً 4h → كل شمعة ≈ 0.5 فترة تمويل (8h)
        funding_periods = bars_held * (tfms / 28_800_000.0)  # 8h = 28800000 ms
        # التمويل يُحوّل إلى R مثل الرسوم: نسبة التمويل × قيمة المركز / المخاطرة.
        funding_est = abs(0.0001 * funding_periods * fill * position_size / risk)
        cost = trade_cost + funding_est
        _adx = d["adx"].iat[i]
        trades.append(dict(t=int(t_arr[i]), side=s, grade=ev["grade"], R=round(realized - cost, 3),
                           exit=kind, bars=bars_held, tps=sum(t["hit"] for t in tps),
                           cost=round(float(cost), 4), score=ev["score"],
                           adx=float(_adx) if _adx == _adx else 0.0,
                           sl_atr=round(risk / lv["atr"], 2), rr1=ev["rr"][0]))
        i = min(j, n - 1) + 1
    return trades


def eng_metrics(trades):
    if not trades:
        return dict(n=0, wr=0.0, pf=0.0, total=0.0, avg=0.0, dd=0.0, tp1=0.0)
    R = np.array([x["R"] for x in sorted(trades, key=lambda z: z["t"])], dtype=float)
    g, l = R[R > 0].sum(), -R[R <= 0].sum()
    eq = np.cumsum(R)
    return dict(n=len(R), wr=round(100 * float((R > 0).mean()), 1),
                pf=round(float(g / l), 2) if l > 0 else 999.0,
                total=round(float(R.sum()), 1), avg=round(float(R.mean()), 3),
                dd=round(float((np.maximum.accumulate(eq) - eq).max()), 1),
                tp1=round(100 * float(np.mean([x["tps"] >= 1 for x in trades])), 1))

se=_types.SimpleNamespace(prepare=eng_prepare,resample_daily=eng_resample_daily,trend_state=eng_trend_state,
                          build_levels=eng_build_levels,evaluate=eng_evaluate,simulate=eng_simulate,
                          metrics=eng_metrics,CFG=ENG_CFG,TP_FRACS=ENG_TP_FRACS)
# ====================================================================
matplotlib.use("Agg")
matplotlib.rcParams["font.family"] = ["DejaVu Sans", "Noto Naskh Arabic", "Amiri", "sans-serif"]
from matplotlib.figure import Figure
from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.patches import FancyBboxPatch, Rectangle, Polygon
import matplotlib.patheffects as pe
from matplotlib.colors import LinearSegmentedColormap
try:
    import arabic_reshaper
    from bidi.algorithm import get_display
    def _ar_shape(s): return get_display(arabic_reshaper.reshape(s))
except Exception:
    def _ar_shape(s): return s
from telegram import (Update, InlineKeyboardButton, InlineKeyboardMarkup,
                      BotCommand, LinkPreviewOptions, CopyTextButton)
from telegram.constants import ParseMode
from telegram.ext import (ApplicationBuilder, CommandHandler, MessageHandler,
                          ContextTypes, filters, CallbackQueryHandler, TypeHandler)

def _e(k, d=""): return (os.getenv(k, d) or d).strip()
def _int(k, d=0):
    try: return int(_e(k, str(d)))
    except Exception: return d
def _chat(v):
    v = (v or "").strip()
    if not v: return None
    return int(v) if re.fullmatch(r"-?\d+", v) else v

BOT_TOKEN = _e("BOT_TOKEN")
CHANNEL_ID = _chat(_e("CHANNEL_ID"))
VIP_CHANNEL_ID = _chat(_e("VIP_CHANNEL_ID"))
ADMIN_ID = _int("ADMIN_ID", 0)  
GIST_ID = _e("GIST_ID")
GITHUB_TOKEN = _e("GITHUB_TOKEN")
WHALE_ALERT_KEY = _e("WHALE_ALERT_KEY")
ETHERSCAN_KEY = _e("ETHERSCAN_API_KEY")
ETH_WATCH = [a.strip().lower() for a in _e("ETH_EXCHANGE_ADDRESSES").split(",") if a.strip()]
ETH_MIN_TX = 100
CONTACT_LINK = _e("CONTACT_LINK")
CHANNEL_LINK = _e("CHANNEL_LINK", "https://t.me/rym_rima16")
VIP_INVITE_LINK = _e("VIP_INVITE_LINK", "https://t.me/+gTMJiBiiC_IyZjZk")
CONTACT_URL = f"https://t.me/{CONTACT_LINK.lstrip('@')}" if CONTACT_LINK else CHANNEL_LINK
PAYMENT_INFO = _e("PAYMENT_INFO")
PROTECT = _e("PROTECT_CONTENT", "1") == "1"
MAINTENANCE = _e("MAINTENANCE", "0") == "1"
BRAND = "💎 Ryma Crypto 💎"
BOT_VERSION = "daytrader-4.3-scanner · 2026-11-11"
# PRINCIPLE: alerts & analysis only — never places exchange orders
CYCLE_RADAR = 300
WHALE_MIN_USD = 500000

RISK_WARNING_AR = "⚠️ <i>التداول ينطوي على مخاطر عالية. البوت للتنبيه والتحليل فقط ولا ينفّذ صفقات. ليست نصيحة مالية.</i>"
RISK_WARNING_EN = "⚠️ <i>Trading involves high risk. This bot is alerts/analysis only and does not execute trades. Not financial advice.</i>"
NO_PREVIEW = LinkPreviewOptions(is_disabled=True)

def _btn(text, style=None, **kw):
    """أزرار بمظهر سكانر: 🟢 أخضر / 🔵 أزرق / 🟡 ذهبي / 🔴 خطر.
    تيليجرام لا يدعم ألوان CSS؛ نعتمد إيموجي + style إن توفر."""
    t = text or ""
    if style is None:
        if t.startswith("❌") or "إغلاق" in t or "Close" in t:
            style = "danger"
        elif any(x in t for x in ("اشترك", "Subscribe", "VIP", "ذهبية", "سبوت", "أرباح", "🚀", "🌱")):
            style = "success"
        elif any(x in t for x in ("شارت", "Chart", "تحديث", "Refresh", "تحليل", "رادار", "فحص", "⚡", "📊")):
            style = "primary"
        elif any(x in t for x in ("ذهبي", "💎", "⭐", "🏆")):
            style = "primary"
    # بادئات لونية إن لم تكن موجودة
    if style == "success" and not any(t.startswith(e) for e in ("🟢", "✅", "🌱", "🚀", "💎")):
        t = "🟢 " + t
    elif style == "primary" and not any(t.startswith(e) for e in ("🔵", "⚡", "📊", "🔮", "🐋")):
        t = "🔵 " + t
    elif style == "danger" and not t.startswith(("❌", "🔴")):
        t = "🔴 " + t
    try:
        return InlineKeyboardButton(t, style=style, **kw) if style else InlineKeyboardButton(t, **kw)
    except TypeError:
        return InlineKeyboardButton(t, **kw)

EXCLUDE_TICKERS = {"MU","SPCX","SNXX","SOXS","CL","TSLA","AAPL","NVDA","META","GOOG","AMZN","MSFT","NFLX","AMD","INTC","BABA","COIN","MSTR","GME","AMC"}

REFERRAL_LINKS = {
    "binance": ("Binance","https://www.binance.com/register?ref=WELCOMBAKC"),
    "okx":     ("OKX","https://okx.com/join/QW22UB2Y"),
    "bybit":   ("Bybit","https://www.bybit.com/invite?ref=A3YBDWZ"),
    "gateio":  ("Gate","https://www.gate.com/ar/referral/registry?ref=VANEUF1XBW"),
    "mexc":    ("MEXC","https://s.mexc.com/referral/o0HbNZxKxg"),
    "kucoin":  ("KuCoin","https://www.kucoin.com/r/rf/CXEJQGFK"),
    "bitget":  ("Bitget","https://www.bitget.com/ar/referral/register?clacCode=UX7TVDCT"),
    "bingx":   ("BingX","https://bingxdao.com/invite/6Y2TXMV4/"),
}

PLATFORM_META = {
    "binance":{"emoji":"🟡","name":"Binance Square","tags":"#Binance","ref":"binance"},
    "bybit":{"emoji":"🔵","name":"Bybit","tags":"#Bybit","ref":"bybit"},
    "okx":{"emoji":"⚫","name":"OKX","tags":"#OKX","ref":"okx"},
    "bitget":{"emoji":"🟢","name":"Bitget","tags":"#Bitget","ref":"bitget"},
    "kucoin":{"emoji":"🟢","name":"KuCoin","tags":"#KuCoin","ref":"kucoin"},
    "gateio":{"emoji":"⚪","name":"Gate.io","tags":"#Gate","ref":"gateio"},
    "twitter":{"emoji":"🐦","name":"Twitter (X)","tags":"#Crypto","ref":"binance"},
    "telegram":{"emoji":"📱","name":"Telegram","tags":"#Crypto","ref":"binance"},
    "facebook":{"emoji":"📘","name":"Facebook","tags":"#Crypto","ref":"binance"},
    "whatsapp":{"emoji":"💬","name":"WhatsApp","tags":"#Crypto","ref":"binance"},
    "reddit":{"emoji":"🔴","name":"Reddit","tags":"#Crypto","ref":"binance"},
    "discord":{"emoji":"🎮","name":"Discord","tags":"#Crypto","ref":"binance"},
}

def referral_for(source):
    s = (source or "binance").lower()
    if s.startswith("binance"): s = "binance"
    return REFERRAL_LINKS.get(s, REFERRAL_LINKS["binance"])

def display_source(src):
    s = (src or "binance").lower()
    if s.startswith("binance"): return "BINANCE FUTURES"
    return s.upper()

def coin_link(sym, source="binance_fut"):
    s = (source or "binance").lower(); u = sym.upper()
    if s.startswith("binance"): return f"https://www.binance.com/futures/{u}USDT?ref=WELCOMBAKC"
    if s == "okx": return f"https://www.okx.com/trade-swap/{u.lower()}-usdt-swap"
    if s == "bybit": return f"https://www.bybit.com/trade/usdt/{u}USDT"
    if s == "gateio": return f"https://www.gate.com/futures/USDT/{u}_USDT"
    if s == "mexc": return f"https://futures.mexc.com/exchange/{u}_USDT"
    if s == "kucoin": return f"https://www.kucoin.com/futures/trade/{u}USDTM"
    if s == "bitget": return f"https://www.bitget.com/futures/usdt/{u}USDT"
    if s == "bingx": return f"https://bingx.com/en-us/perpetual/{u}-USDT/"
    return REFERRAL_LINKS["binance"][1]

FAST_EXCHANGES = ["okx","bybit","gateio","mexc","kucoin","bitget","htx","binance"]
EXCHANGES = ["okx","bybit","gateio","mexc","kucoin","bitget","htx","binance","bingx","lbank","bitmart","poloniex"]
BINANCE_FAPI_BASE = "https://fapi.binance.com"
BINANCE_SPOT_MIRRORS = ["https://data-api.binance.vision","https://api1.binance.com","https://api2.binance.com","https://api3.binance.com"]
BINANCE_INTERVAL_MAP = {"15m":"15m","1h":"1h","4h":"4h","1d":"1d"}

MAJORS = {"BTC","ETH","BNB","SOL","XRP","ADA","DOGE","AVAX","LINK","DOT","LTC","TRX"}
CORE_COINS = ["BTC","ETH","BNB","XRP","ADA","DOGE","AVAX","LTC","NEAR","SUI","ATOM","TRX","BONK","OP"]
MEME_COINS = ["TIA","FLOKI","INJ"] 
ALPHA_COINS = ["ETH","BNB","SOL","SUI","NEAR","INJ","TIA","FLOKI","APT","ARB"]
COINS = CORE_COINS + MEME_COINS
WEAK_COINS = set()
DAILY_EXCLUDE = {"BTC","ETH","BNB","SOL","XRP","DOGE","ADA","TRX","LTC","DOT","LINK","AVAX","TON","SHIB","PEPE","USDC","FDUSD","TUSD","USDT","DAI","BUSD","USDP","USDD","USDE","PYUSD","EUR","AEUR"} | EXCLUDE_TICKERS
STABLES = {"USDC","FDUSD","TUSD","USDT","DAI","BUSD","USDP","USDD","USDE","PYUSD","EUR","AEUR"}

POINTS_DAILY = 1; POINTS_PROMO = 5; POINTS_REFERRAL = 20
VIP_THRESHOLD = 200; VIP_REWARD_DAYS = 1
ALPHA_MAJORS_EXCLUDE = {"BTC","ETH","BNB","SOL","XRP","ADA","DOGE","AVAX","LINK","DOT","LTC","TRX","TON","SHIB","PEPE","USDC","FDUSD","TUSD","USDT","DAI","BUSD","USDP","USDD","USDE","PYUSD"}
TRIAL_DAYS = 7
VIP_FORCE_DAY = 12 
FEE, SLIP = 0.0008, 0.0005
TF_MS = {"15m":900_000,"1h":3_600_000,"4h":14_400_000,"1d":86_400_000}
CANDLE_TTL, PRICE_TTL = 180, 45
MIN_BARS = 215; CHART_BARS = 90; SCAN_WORKERS = 28; MAX_OPEN = 100
VIP_POSTS_PER_CYCLE = 5; FREE_POSTS_PER_CYCLE = 10; FREE_MAX_PER_DAY = 40
CYCLE_AUTOPOST=12600; CYCLE_SHORT_FREE=14400; CYCLE_MARKET_SUMMARY=16200
CYCLE_PREDICT=14400; CYCLE_ALPHA_SUMMARY=14400; CYCLE_BOTTOMS_AUTO=21600
CYCLE_GOLDEN=18000; CYCLE_PROMO=604800; CROSS_CHECK_INTERVAL=3600
SMART_PROFIT_DAYS=1; SMART_MAX_AGE_DAYS=2; SMART_FORCE_DAYS=1
SMART_FILL_TRIGGER=0.8; CLEANUP_EVERY_CYCLES=12
PIVOT_WINDOW=100; PIVOT_LOOKBACK=3; MIN_PIVOT_DIST=0.5
CORE_SL_MAX=2.5; CORE_SL_MIN=1.5
MAX_TP_ATR = 6.0; MIN_RR_TP1 = 1.5
CORE_TP_FRACS=[0.50,0.25,0.15,0.10]
BE_OFFSET_R=0.05; TRAIL_ATR=2.0; MAX_HOLD=6   # يوم تداول ~24س على 4h
CORE_VOL_MULT=0.7; CORE_RSI_LONG=(35,70); CORE_RSI_SHORT=(50,80); EXT_MAX_ATR=3.5
MIN_DAILY_VOLUME_USD=5_000_000; VIP_MIN_LIQUIDITY=3_000_000; REQUIRE_MTF_FOR_SMALL=True
MEME_TP_PCTS=[3.0,6.0,10.0,15.0]; MEME_TP_FRACS=[0.50,0.25,0.15,0.10]
MEME_SL_PCT=6.5; MEME_VOL_MULT=1.5; MEME_RSI_LOW=25; MEME_RSI_HIGH=75
MEME_BREAKOUT_BARS=5; MEME_TRAIL_ATR=2.0; MEME_TIME_STOP=12; MEME_MAX_HOLD=16
RISK_SIZE={"A+":0.10,"A":0.08,"B":0.05,"C":0.02}
PREDICT_BB_SQZ=0.75; PREDICT_VOL_BUILDUP=1.4; PREDICT_RSI_NEUTRAL=(42,58)
PREDICT_OI_CHANGE_MIN=3.0; PREDICT_FUNDING_EXTREME=0.05; PREDICT_TOP_N=10; PREDICT_TF="4h"
BOTTOM_MIN_DIFF=-60; BOTTOM_MAX_DIFF=-10; BOTTOM_RSI_RANGE=(25,55)
BOTTOM_MIN_VOL_RATIO=0.8; BOTTOM_MIN_REBOUND=1.5; BOTTOM_AVOID_BREAK=0.98
SR_LOOKBACK=50; SR_MERGE_PCT=0.5
FIB_LEVELS=[0.236,0.382,0.5,0.618,0.786,1.272,1.618]
MACD_FAST,MACD_SLOW,MACD_SIGNAL=12,26,9
DAILY_LONGS=2; DAILY_SHORTS=2; DAILY_TP_COUNT=3; DAILY_TOP_N=20
DAILY_MIN_CHANGE=12.0; DAILY_MIN_VOLUME=5_000_000
CROSS_EXCLUDE={"BTC","ETH","BNB"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("ryma")
# httpx يطبع رابط كل طلب وفيه التوكن كاملاً: نخفضه حتى لا يتسرب التوكن في الـlogs ولتقليل الضجيج
for _n in ("httpx","httpcore","telegram.ext.Updater"): logging.getLogger(_n).setLevel(logging.WARNING)
START_TS = time.time(); DAY = 86400
def now_s(): return int(time.time())

DEFAULT_GOLDEN_CONFIG = {"require_1d_trend":False,"require_mtf":False,"require_rsi":False,
    "require_volume":False,"require_breakout":False,"require_liquidity":False,
    "require_alpha":False,"min_score":0}

KEYS = ["users","vip","signals","history","posted","settings","portfolios"]
DEFAULTS = {"users":{},"vip":{},"signals":[],"history":[],"posted":{},"portfolios":{},
    "settings":{"maintenance":_e("MAINTENANCE","0")=="1","multi_exchange":True,
                "golden_config":dict(DEFAULT_GOLDEN_CONFIG)}}

class Store:
    def __init__(self, local_path="data_local.json"):
        self.lock=threading.RLock(); self.local_path=local_path
        self.data=json.loads(json.dumps(DEFAULTS)); self.dirty=set()
        self.fname={k:k+".json" for k in KEYS}
        self.remote_ok,self.remote_msg=False,"in-memory"; self._load()
    def _hdr(self): return {"Authorization":f"Bearer {GITHUB_TOKEN}","Accept":"application/vnd.github+json"}
    def _load(self):
        if GIST_ID and GITHUB_TOKEN:
            try:
                r=requests.get(f"https://api.github.com/gists/{GIST_ID}",headers=self._hdr(),timeout=20)
                r.raise_for_status(); files=r.json().get("files",{})
                for k in KEYS:
                    f=files.get(k+".json")
                    if not f: continue
                    txt=f.get("content")
                    if f.get("truncated"):
                        txt=requests.get(f["raw_url"],headers=self._hdr(),timeout=20).text
                    val=json.loads(txt) if txt and txt.strip() else DEFAULTS[k]
                    if type(val) is type(DEFAULTS[k]): self.data[k]=val
                self.remote_ok,self.remote_msg=True,"Gist OK"; return
            except Exception as e: self.remote_msg=f"Gist fail: {str(e)[:60]}"
        if os.path.exists(self.local_path):
            try:
                with open(self.local_path,encoding="utf-8") as fh: self.data.update(json.load(fh))
            except Exception as _ex: log.debug('ignored: %s', _ex)
    def save(self,*keys):
        with self.lock: self.dirty.update(keys)
    def flush(self):
        with self.lock:
            keys = list(self.dirty)
            if not keys: return
            if "users" in keys and len(json.dumps(self.data["users"], ensure_ascii=False)) > 900_000:
                cutoff = time.time() - 90*DAY
                vip_ids = set(self.data.get("vip", {}).keys())
                self.data["users"] = {k: v for k, v in self.data["users"].items()
                                      if (v.get("last_seen") or 0) > cutoff or k in vip_ids}
            if "history" in keys:
                self.data["history"] = self.data["history"][-500:]
            snap = {k: json.dumps(self.data[k], ensure_ascii=False) for k in keys}
            if not self.remote_ok:
                try:
                    with open(self.local_path, "w", encoding="utf-8") as fh:
                        json.dump(self.data, fh, ensure_ascii=False)
                except Exception as e:
                    self.remote_msg = f"local save fail: {str(e)[:60]}"
                self.dirty.clear(); return
        try:
            r = requests.patch(f"https://api.github.com/gists/{GIST_ID}",
                               headers=self._hdr(), timeout=25,
                               json={"files":{self.fname[k]:{"content":snap[k]} for k in keys}})
            r.raise_for_status()
            with self.lock: self.dirty.difference_update(keys)
        except Exception as e:
            self.remote_msg = f"flush fail: {str(e)[:60]}"
    def start_flusher(self,every=20):
        def loop():
            while True:
                time.sleep(every)
                try: self.flush()
                except Exception as _ex: log.debug('ignored: %s', _ex)
        threading.Thread(target=loop,daemon=True).start()
store=Store()

def detect_lang(text):
    if not text: return "ar"
    if re.search(r'[\u0600-\u06FF]',text): return "ar"
    if re.search(r'[a-zA-Z]',text): return "en"
    return "ar"

def get_user_lang(uid):
    with store.lock:
        u=store.data["users"].get(str(uid),{})
        return u.get("lang","en")

def set_user_lang(uid,lang):
    with store.lock:
        if str(uid) in store.data["users"]:
            store.data["users"][str(uid)]["lang"]=lang; store.save("users")

TEXTS = {
    "ar": {
        "analyzing": "⏳ جارٍ تحليل {sym}...",
        "not_found": "❌ لم يتم العثور على العملة: {sym}",
        "no_data": "⚠️ البيانات غير متوفرة حالياً.",
        "error": "⚠️ حدث خطأ أثناء التحليل.",
        "invalid_symbol": "📝 إدخال رمز عملة صحيح.",
        "not_supported": "⛔ هذا الرمز غير مدعوم (أسهم أو غير متاح).",
        "maintenance": "🔧 صيانة.\n📞 {contact}",
        "blocked": "⛔ انتهت التجربة.\n📞 {contact}",
        "warning_trial": "⚠️ تنبيه: تجربتك المجانية تنتهي خلال {left} أيام.\n💎 استعد للاشتراك في VIP للاستمرار دون انقطاع.",
        "wait_3s": "⏳ انتظار 3 ثوان...",
        "vip_only": "🔒 VIP فقط",
        "admin_only": "⛔ للأدمن فقط",
        "welcome": "أهلاً {name}! 👋\n\nمرحباً بك في بوت **Ryma Crypto** للتحليل الفني الذكي.\n\n📌 **طريقة الاستخدام:**\nفقط أرسل اسم أي عملة (مثل BTC أو ETH) وسأقوم بتحليلها فوراً وتزويدك بـ:\n- نقاط الدخول والخروج.\n- 4 أهداف ذكية.\n- وقف الخسارة المناسب.\n- النماذج الفنية.\n\n📊 الأزرار بالأسفل تتيح لك الوصول السريع لحاسبة المخاطر، محفظتك، والشارتات.",
        "risk_btn": "🧮 حاسبة المخاطر",
        "port_btn": "📊 محفظتي",
        "chart_btn": "📈 شارت TradingView",
        "pat_btn": "🕯️ النماذج",
        "news_btn": "📰 الأخبار",
        "refs_btn": "🌐 المنصات",
        "vip_btn": "💎 اشترك VIP",
        "free_btn": "🆓 القناة المجانية",
        "contact_btn": "📞 تواصل معنا",
        "refresh_btn": "🔄 تحديث",
        "close_btn": "❌ إغلاق",
        "calc_prompt": "🧮 **حاسبة إدارة المخاطر ({sym})**\n\nأرسل رأس مالك ونسبة المخاطرة مفصولة بمسافة.\nمثال: `1000 2`",
        "port_prompt": "📊 **إضافة {sym} للمحفظة**\n\nكم المبلغ الذي استثمرته بالدولار؟\nمثال: `100`",
        "risk_res": "🧮 **نتيجة إدارة المخاطر لـ {sym}:**\n\n💰 رأس المال: ${cap}\n🛑 المخاطرة ({risk_pct}%): ${risk_amt:.2f}\n➡️ حجم العقد: {size:.4f}\n💵 قيمة الصفقة: ${value:.2f}\n\n⚙️ **الرافعة المقترحة:** {lev:.1f}x\n<i>(استخدم دائماً العزل Isolated)</i>",
        "risk_err": "⚠️ صيغة خاطئة (مثال: 1000 2)",
        "port_added": "✅ تمت إضافة {sym} إلى محفظتك (${amount}).",
        "port_err": "⚠️ أرسل رقماً صحيحاً (مثال: 100)",
    },
    "en": {
        "analyzing": "⏳ Analyzing {sym}...",
        "not_found": "❌ Coin not found: {sym}",
        "no_data": "⚠️ Data currently unavailable.",
        "error": "⚠️ An error occurred.",
        "invalid_symbol": "📝 Please enter a valid coin symbol.",
        "not_supported": "⛔ This symbol is not supported.",
        "maintenance": "🔧 Maintenance.\n📞 {contact}",
        "blocked": "⛔ Trial expired.\n📞 {contact}",
        "warning_trial": "⚠️ Heads up: your free trial ends in {left} days.\n💎 Get ready to subscribe to VIP to keep access.",
        "wait_3s": "⏳ Please wait 3 seconds...",
        "vip_only": "🔒 VIP only",
        "admin_only": "⛔ Admin only",
        "welcome": "Welcome {name}! 👋\n\nWelcome to **Ryma Crypto Bot** for smart technical analysis.\n\n📌 **How to use:**\nJust send any coin symbol (e.g., BTC or ETH) and I will instantly analyze it for you, providing:\n- Entry and exit points.\n- 4 smart targets.\n- Proper Stop Loss.\n- Chart patterns.\n\n📊 Use the buttons below for quick access to the Risk Calculator, Portfolio, and Charts.",
        "risk_btn": "🧮 Risk Calculator",
        "port_btn": "📊 Portfolio",
        "chart_btn": "📈 TradingView Chart",
        "pat_btn": "🕯️ Patterns",
        "news_btn": "📰 News",
        "refs_btn": "🌐 Exchanges",
        "vip_btn": "💎 Subscribe VIP",
        "free_btn": "🆓 Free Channel",
        "contact_btn": "📞 Contact Us",
        "refresh_btn": "🔄 Refresh",
        "close_btn": "❌ Close",
        "calc_prompt": "🧮 **Risk Calculator ({sym})**\n\nSend your capital and risk % separated by a space.\nExample: `1000 2`",
        "port_prompt": "📊 **Add {sym} to Portfolio**\n\nHow much did you invest in USD?\nExample: `100`",
        "risk_res": "🧮 **Risk Management Result for {sym}:**\n\n💰 Capital: ${cap}\n🛑 Risk ({risk_pct}%): ${risk_amt:.2f}\n➡️ Position Size: {size:.4f}\n💵 Position Value: ${value:.2f}\n\n⚙️ **Suggested Leverage:** {lev:.1f}x\n<i>(Always use Isolated margin)</i>",
        "risk_err": "⚠️ Invalid format (e.g., 1000 2)",
        "port_added": "✅ {sym} added to your portfolio (${amount}).",
        "port_err": "⚠️ Send a valid number (e.g., 100)",
    }
}

def t(key, lang="en", **kw):
    text = TEXTS.get(lang, TEXTS["en"]).get(key, TEXTS["en"].get(key, key))
    try: return text.format(**kw)
    except Exception: return text

def is_maintenance():
    with store.lock: return bool(store.data.get("settings",{}).get("maintenance",False))
def set_maintenance(flag):
    with store.lock:
        store.data.setdefault("settings",{})["maintenance"]=bool(flag); store.save("settings")
    return bool(flag)
def is_multi_exchange():
    with store.lock: return bool(store.data.get("settings",{}).get("multi_exchange",True))
def set_multi_exchange(flag):
    with store.lock:
        store.data.setdefault("settings",{})["multi_exchange"]=bool(flag); store.save("settings")
    return bool(flag)
def get_golden_config():
    with store.lock:
        cfg=store.data.setdefault("settings",{}).get("golden_config")
        if not cfg or not isinstance(cfg,dict):
            cfg=dict(DEFAULT_GOLDEN_CONFIG); store.data["settings"]["golden_config"]=cfg; store.save("settings")
        merged=dict(DEFAULT_GOLDEN_CONFIG); merged.update(cfg)
        # فرض التخفيف: لا نترك قيم قديمة من Gist تشدّ الشروط
        merged["require_rsi"] = False
        merged["require_1d_trend"] = False
        merged["require_mtf"] = False
        merged["require_volume"] = False
        merged["require_breakout"] = False
        merged["require_liquidity"] = False
        merged["require_alpha"] = False
        if merged.get("min_score", 0) > 1:
            merged["min_score"] = 0
        return merged
def set_golden_config(key,value):
    with store.lock:
        cfg=store.data.setdefault("settings",{}).setdefault("golden_config",dict(DEFAULT_GOLDEN_CONFIG))
        cfg[key]=value; store.save("settings")

def touch_user(user,cmd="text"):
    uid=str(user.id)
    with store.lock:
        u=store.data["users"].get(uid) or dict(id=user.id,first_seen=now_s(),requests=0,cmds={},last_cmd=None,points=0)
        if "lang" not in u:
            u["lang"] = "ar" if (user.language_code or "").startswith("ar") else "en"
        last_seen_date=dt.datetime.fromtimestamp(u.get("last_seen",0)).date()
        current_date=dt.datetime.now().date()
        if current_date>last_seen_date: u["points"]=u.get("points",0)+POINTS_DAILY
        u.update(name=getattr(user,"first_name","") or "",username=getattr(user,"username","") or "",last_seen=now_s())
        u["requests"]=u.get("requests",0)+1; u["last_cmd"]=cmd
        u["cmds"]=u.get("cmds") or {}; u["cmds"][cmd]=u["cmds"].get(cmd,0)+1
        store.data["users"][uid]=u; store.save("users")
    return u

def vip_until(uid): return int(store.data["vip"].get(str(uid),{}).get("expires",0))
def is_vip(uid): return vip_until(uid)>now_s()

def user_status(uid,rec):
    if ADMIN_ID and uid==ADMIN_ID: return "admin",0
    if is_vip(uid): return "vip",0
    actual_days=(now_s()-int(rec.get("first_seen",now_s())))//DAY+1
    if actual_days > VIP_FORCE_DAY:
        grace_until = rec.get("grace_until", 0)
        if grace_until == 0:
            with store.lock:
                rec["grace_until"] = now_s() + (3 * DAY)
                store.save("users")
            return "grace", actual_days
        elif now_s() < grace_until:
            return "grace", actual_days
        return "blocked", actual_days
    if actual_days <= TRIAL_DAYS: return "trial", actual_days
    if actual_days <= VIP_FORCE_DAY: return "warning", actual_days
    return "blocked", actual_days

def add_vip(uid,days):
    with store.lock:
        exp=max(now_s(),vip_until(uid))+int(days)*DAY
        store.data["vip"][str(uid)]=dict(expires=exp,added=now_s(),days=int(days)); store.save("vip")
    return exp
def remove_vip(uid):
    with store.lock:
        ok=store.data["vip"].pop(str(uid),None) is not None; store.save("vip")
    return ok
def time_ago(ts):
    if not ts: return "—"
    d=now_s()-int(ts)
    if d<60: return "الآن"
    if d<3600: return f"{d//60} د"
    if d<86400: return f"{d//3600} س"
    if d<604800: return f"{d//86400} يوم"
    return f"{d//604800} أسبوع"

def check_points_reward(uid):
    with store.lock:
        u=store.data["users"].get(str(uid))
        if not u: return (0,None)
        pts=u.get("points",0)
        if pts>=VIP_THRESHOLD:
            u["points"]=pts-VIP_THRESHOLD; store.save("users")
            add_vip(uid,VIP_REWARD_DAYS); return (u["points"],VIP_REWARD_DAYS)
    return (pts,None)

def points_visual(pts,threshold=VIP_THRESHOLD):
    pct=(pts/threshold) if threshold>0 else 0
    if pct>=1.0: bar,status="🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢","🎉 مكتمل!"
    elif pct>=0.9: bar,status="🟢🟢🟢🟢🟢🟢🟢🟢🟢⚪","🔥 قريب جداً"
    elif pct>=0.7: bar,status="🟢🟢🟢🟢🟢🟢🟢⚪⚪⚪","💪 ممتاز"
    elif pct>=0.5: bar,status="🟡🟡🟡🟡🟡⚪⚪⚪⚪⚪","⭐ في منتصف الطريق"
    elif pct>=0.3: bar,status="🟠🟠🟠⚪⚪⚪⚪⚪⚪⚪","🌱 استمر"
    else: bar,status="🔴🔴⚪⚪⚪⚪⚪⚪⚪⚪","🚀 ابدأ الجمع"
    return f"{bar}\n<b>{pts}/{threshold}</b> — {status}"

async def send_points_flash(msg_obj,pts,uid,bot_username=None):
    base="🎯 نقاطك: {c} <b>{p}/{t}</b>"
    try:
        sent=await msg_obj.reply_text(base.format(c="🔴",p=pts,t=VIP_THRESHOLD),
                                       parse_mode=ParseMode.HTML,link_preview_options=NO_PREVIEW)
        for c in ["🟡","🔴"]:
            await asyncio.sleep(0.5)
            try: await sent.edit_text(base.format(c=c,p=pts,t=VIP_THRESHOLD),parse_mode=ParseMode.HTML)
            except Exception: break
        await asyncio.sleep(0.5)
        ref_link=f"https://t.me/{bot_username}?start=ref_{uid}" if bot_username else "—"
        final=(f"🎯 نقاطك: <b>{pts}/{VIP_THRESHOLD}</b>\n{points_visual(pts)}\n\n"
               f"📢 <b>نشر رابط القناة أو البوت = {POINTS_PROMO} نقاط</b>\n"
               f"👥 <b>دعوة مشترك جديد = {POINTS_REFERRAL} نقطة</b>\n\n"
               f"🔗 <b>رابط البوت (إحالتك):</b>\n<code>{ref_link}</code>\n\n"
               f"📢 <b>قناة التوصيات:</b>\n<code>{CHANNEL_LINK}</code>\n\n"
               f"🎁 عند الوصول لـ {VIP_THRESHOLD} = <b>VIP {VIP_REWARD_DAYS} يوم مجاناً</b>")
        try: await sent.edit_text(final,parse_mode=ParseMode.HTML,link_preview_options=NO_PREVIEW)
        except Exception as _ex: log.debug('ignored: %s', _ex)
    except Exception as e: log.warning(f"send_points_flash: {e}")
def binance_futures_klines(sym,tf,bars=500):
    interval=BINANCE_INTERVAL_MAP.get(tf)
    if not interval: return None
    try:
        r=requests.get(f"{BINANCE_FAPI_BASE}/fapi/v1/klines",
                       params={"symbol":f"{sym}USDT","interval":interval,"limit":min(bars,1500)},timeout=10)
        if r.status_code==200:
            data=r.json()
            if isinstance(data,list) and data:
                return [[int(k[0]),float(k[1]),float(k[2]),float(k[3]),float(k[4]),float(k[5])] for k in data]
    except Exception as _ex: log.debug('ignored: %s', _ex)
    return None

def binance_futures_ticker(sym):
    try:
        r=requests.get(f"{BINANCE_FAPI_BASE}/fapi/v1/ticker/24hr",params={"symbol":f"{sym}USDT"},timeout=10)
        if r.status_code==200:
            d=r.json()
            return {"last":float(d.get("lastPrice",0)),"percentage":float(d.get("priceChangePercent",0)),
                    "quoteVolume":float(d.get("quoteVolume",0)),"high":float(d.get("highPrice",0)),
                    "low":float(d.get("lowPrice",0))}
    except Exception as _ex: log.debug('ignored: %s', _ex)
    return None

def binance_futures_all_tickers():
    try:
        r=requests.get(f"{BINANCE_FAPI_BASE}/fapi/v1/ticker/24hr",timeout=15)
        if r.status_code!=200: return None
        data=r.json()
        if not isinstance(data,list): return None
        rows=[]
        for t in data:
            try:
                sym_full=t.get("symbol","")
                if not sym_full.endswith("USDT"): continue
                base_sym=sym_full[:-4]
                if base_sym in STABLES or base_sym in EXCLUDE_TICKERS: continue
                if re.search(r"(UP|DOWN|BULL|BEAR|3L|3S|USDC)$",base_sym): continue
                ch=float(t.get("priceChangePercent",0)); price=float(t.get("lastPrice",0))
                qv=float(t.get("quoteVolume",0))
                if price<=0: continue
                rows.append({"symbol":base_sym,"change":ch,"price":price,"volume":qv})
            except Exception: continue
        return rows
    except Exception: return None

def _binance_mirror_request(path,params):
    for base in BINANCE_SPOT_MIRRORS:
        try:
            r=requests.get(f"{base}{path}",params=params,timeout=10)
            if r.status_code==200: return r.json(),base
        except Exception: continue
    return None,None

def binance_mirror_klines(sym,tf,bars=500):
    interval=BINANCE_INTERVAL_MAP.get(tf)
    if not interval: return None
    data,base=_binance_mirror_request("/api/v3/klines",{"symbol":f"{sym}USDT","interval":interval,"limit":min(bars,1000)})
    if not data or not isinstance(data,list): return None
    rows=[]
    for k in data:
        try: rows.append([int(k[0]),float(k[1]),float(k[2]),float(k[3]),float(k[4]),float(k[5])])
        except Exception: continue
    return rows

class PairNotFound(Exception): pass
class NoData(Exception): pass
_exch,_ex_lock,_down={},{},{}
_ex_init=threading.Lock()

def get_ex(name):
    with _ex_init:
        if name not in _exch:
            _exch[name]=getattr(ccxt,name)({"enableRateLimit":True,"timeout":12000})
            _ex_lock[name]=threading.Lock()
    return _exch[name]

def default_tf(sym): return "4h"

def _rows_to_df(rows,tf,bars):
    cols=["t","open","high","low","close","volume"]
    if not rows: return pd.DataFrame(columns=cols)
    df=pd.DataFrame(rows,columns=cols).dropna()
    df=df.drop_duplicates("t").sort_values("t").reset_index(drop=True)
    df["t"]=df["t"].astype("int64")
    df=df[df["t"]+TF_MS[tf]<=int(time.time()*1000)]
    return df.tail(bars).reset_index(drop=True)

def _fetch_exchange(name,sym,tf,bars):
    ex=get_ex(name); tfms=TF_MS[tf]
    now=int(time.time()*1000); since=now-bars*tfms
    rows,last_ts=[],None
    with _ex_lock[name]:
        for _ in range(80):
            chunk=ex.fetch_ohlcv(f"{sym}/USDT",tf,since=since,limit=1000)
            if not chunk: break
            rows+=chunk
            if last_ts is not None and chunk[-1][0]<=last_ts: break
            last_ts=chunk[-1][0]
            if last_ts+tfms>=now: break
            since=last_ts+tfms
    return rows

_fut_syms={"t":0.0,"s":set()}
def _fut_symbol_known(sym):
    """هل الرمز موجود في عقود Binance Futures؟ (لرسالة 'لم يتم العثور' الصحيحة)"""
    if time.time()-_fut_syms["t"]>3600 or not _fut_syms["s"]:
        try:
            r=requests.get(f"{BINANCE_FAPI_BASE}/fapi/v1/exchangeInfo",timeout=15)
            if r.status_code==200:
                _fut_syms["s"]={x["symbol"] for x in r.json().get("symbols",[])}; _fut_syms["t"]=time.time()
        except Exception as _ex: log.debug('ignored: %s', _ex)
    if not _fut_syms["s"]: return True
    return f"{sym}USDT" in _fut_syms["s"]

_candle_cache={}
def get_candles(sym,tf,bars=450,ttl=CANDLE_TTL,min_bars=MIN_BARS,binance_only=False):
    key=(sym,tf,bars,min_bars,binance_only)
    if len(_candle_cache)>400: _candle_cache.clear()
    hit=_candle_cache.get(key)
    if hit and time.time()-hit[0]<ttl: return hit[1]
    try:
        rows=binance_futures_klines(sym,tf,bars)
        if rows and len(rows)>=min_bars:
            df=_rows_to_df(rows,tf,bars)
            if len(df)>=min_bars and df["close"].min()>0 and df["high"].max()>0:
                df.attrs.update(source="binance_fut",degraded=False)
                _candle_cache[key]=(time.time(),df); return df
    except Exception as _ex: log.debug('ignored: %s', _ex)
    try:
        rows=binance_mirror_klines(sym,tf,bars)
        if rows and len(rows)>=min_bars:
            df=_rows_to_df(rows,tf,bars)
            if len(df)>=min_bars and df["close"].min()>0 and df["high"].max()>0:
                df.attrs.update(source="binance",degraded=False)
                _candle_cache[key]=(time.time(),df); return df
    except Exception as _ex: log.debug('ignored: %s', _ex)
    if not _fut_symbol_known(sym): raise PairNotFound(sym)
    raise NoData(sym)

_price_cache={}
def live_price(sym):
    if len(_price_cache)>400: _price_cache.clear()
    hit=_price_cache.get(sym)
    if hit and time.time()-hit[0]<PRICE_TTL: return hit[1],hit[2]
    try:
        t=binance_futures_ticker(sym)
        if t and t["last"]>0:
            _price_cache[sym]=(time.time(),t["last"],"binance_fut"); return t["last"],"binance_fut"
    except Exception as _ex: log.debug('ignored: %s', _ex)
    return None,None

def all_prices(sym):
    def one(name):
        try:
            ex=get_ex(name)
            with _ex_lock[name]: t=ex.fetch_ticker(f"{sym}/USDT")
            p=float(t.get("last") or 0)
            return name,p if p>0 else None
        except Exception: return name,None
    with ThreadPoolExecutor(len(EXCHANGES)) as pool:
        return {n:p for n,p in pool.map(one,EXCHANGES) if p}

def ticker_change(sym):
    price,change_24h=0.0,0.0
    try:
        t=binance_futures_ticker(sym)
        if t and t["last"]>0: price=t["last"]; change_24h=t["percentage"]
    except Exception as _ex: log.debug('ignored: %s', _ex)
    change_7d=0.0
    try:
        df=get_candles(sym,"1d",10,ttl=600,min_bars=8)
        if len(df)>=8 and float(df["close"].iloc[-8])>0:
            change_7d=(float(df["close"].iloc[-1])/float(df["close"].iloc[-8])-1)*100
    except Exception as _ex: log.debug('ignored: %s', _ex)
    return price,change_24h,change_7d

_top_movers_cache=[0,[],[]]
def _movers_from_ccxt(ex_name,min_change=DAILY_MIN_CHANGE,min_vol=DAILY_MIN_VOLUME):
    try:
        if _down.get(ex_name,0)>time.time(): return []
        ex=get_ex(ex_name)
        with _ex_lock[ex_name]: tickers=ex.fetch_tickers()
        rows=[]
        for s,t in tickers.items():
            if not s.endswith("/USDT") or ":" in s: continue
            base=s.split("/")[0]
            if not base or base in DAILY_EXCLUDE or base in EXCLUDE_TICKERS: continue
            if re.search(r"(UP|DOWN|BULL|BEAR|3L|3S|USDT|USDC)$",base): continue
            try:
                ch=float(t.get("percentage") or 0); price=float(t.get("last") or 0)
                qv=float(t.get("quoteVolume") or 0)
            except Exception: continue
            if price<=0 or qv<min_vol or abs(ch)<min_change: continue
            rows.append(dict(symbol=base,change=ch,price=price,volume=qv,src=ex_name))
        return rows
    except Exception: return []

def _movers_from_binance_mirror(min_change=DAILY_MIN_CHANGE,min_vol=DAILY_MIN_VOLUME):
    try:
        rows=binance_futures_all_tickers()
        if not rows: return []
        return [dict(symbol=x["symbol"],change=x["change"],price=x["price"],volume=x["volume"],src="binance_fut")
                for x in rows if abs(x["change"])>=min_change and x["volume"]>=min_vol
                and x["symbol"] not in DAILY_EXCLUDE]
    except Exception: return []

def get_top_movers(force=False):
    if not force and time.time()-_top_movers_cache[0]<180:
        return _top_movers_cache[1],_top_movers_cache[2]
    all_rows=_movers_from_binance_mirror()
    if is_multi_exchange():
        for ex_name in ["okx","kucoin","bitget","gateio","mexc","bybit"]:
            all_rows+=_movers_from_ccxt(ex_name)
    best_by_sym={}
    for x in all_rows:
        sym=x["symbol"]
        if sym not in best_by_sym: best_by_sym[sym]=x
        else:
            if best_by_sym[sym].get("src","").startswith("binance"): continue
            if x.get("src","").startswith("binance"): best_by_sym[sym]=x
            elif abs(x["change"])>abs(best_by_sym[sym]["change"]): best_by_sym[sym]=x
    rows=list(best_by_sym.values())
    gainers=sorted([x for x in rows if x["change"]>0],key=lambda x:-x["change"])[:DAILY_TOP_N]
    losers=sorted([x for x in rows if x["change"]<0],key=lambda x:x["change"])[:DAILY_TOP_N]
    _top_movers_cache[0]=time.time(); _top_movers_cache[1]=gainers; _top_movers_cache[2]=losers
    return gainers,losers

def _bybit_get(path,params):
    try:
        r=requests.get(f"https://api.bybit.com{path}",params=params,timeout=8)
        if r.status_code==200: return r.json()
    except Exception as _ex: log.debug('ignored: %s', _ex)
    return None

def futures_oi_history(sym,period="4h",limit=20):
    try:
        bp={"15m":"15min","1h":"1h","4h":"4h","1d":"1d"}.get(period,"4h")
        d=_bybit_get("/v5/market/open-interest",{"category":"linear","symbol":f"{sym}USDT","intervalTime":bp,"limit":limit})
        if d and d.get("retCode")==0:
            lst=d.get("result",{}).get("list",[])
            if lst and len(lst)>=3:
                curr=float(lst[0].get("openInterest",0)); old=float(lst[-1].get("openInterest",0))
                if old>0 and curr>0:
                    return dict(current=curr,change_pct=round((curr/old-1)*100,2),src="bybit")
    except Exception as _ex: log.debug('ignored: %s', _ex)
    return None

def futures_funding_rate(sym):
    try:
        d=_bybit_get("/v5/market/tickers",{"category":"linear","symbol":f"{sym}USDT"})
        if d and d.get("retCode")==0:
            lst=d.get("result",{}).get("list",[])
            if lst: return float(lst[0].get("fundingRate",0))*100
    except Exception as _ex: log.debug('ignored: %s', _ex)
    return None

def futures_long_short_ratio(sym,period="4h",limit=10):
    try:
        bp={"15m":"15min","1h":"1h","4h":"4h","1d":"1d"}.get(period,"4h")
        d=_bybit_get("/v5/market/account-ratio",{"category":"linear","symbol":f"{sym}USDT","period":bp,"limit":1})
        if d and d.get("retCode")==0:
            lst=d.get("result",{}).get("list",[])
            if lst:
                buy=float(lst[0].get("buyRatio",0)); sell=float(lst[0].get("sellRatio",0))
                if sell>0: return round(buy/sell,3)
    except Exception as _ex: log.debug('ignored: %s', _ex)
    return None

def futures_taker_flow(sym,limit=500):
    try:
        r=requests.get(f"{BINANCE_FAPI_BASE}/fapi/v1/aggTrades",params={"symbol":f"{sym}USDT","limit":limit},timeout=8)
        if r.status_code!=200: return None
        buy=sell=0.0
        for t in r.json():
            q=float(t.get("q",0))
            if t.get("m"): sell+=q
            else: buy+=q
        total=buy+sell
        if total<=0: return None
        return round(buy/total,3)
    except Exception: return None

def futures_liquidations(sym,limit=100):
    try:
        r=requests.get(f"{BINANCE_FAPI_BASE}/fapi/v1/allForceOrders",params={"symbol":f"{sym}USDT","limit":limit},timeout=8)
        if r.status_code!=200: return None
        longs=shorts=0.0
        for o in r.json():
            notional=float(o.get("price",0))*float(o.get("origQty",0))
            if o.get("side")=="SELL": longs+=notional
            else: shorts+=notional
        return dict(long_liq=longs,short_liq=shorts)
    except Exception: return None

def futures_signal(sym):
    try:
        oi=futures_oi_history(sym,"4h",20); fr=futures_funding_rate(sym); ls=futures_long_short_ratio(sym)
        price,_=live_price(sym)
        if not price or price<=0: return None
        score=0; reasons=[]
        if oi and oi.get("change_pct",0)!=0:
            oc=oi["change_pct"]
            if oc>5: score+=2; reasons.append(f"OI +{oc:.1f}%")
            elif oc>2: score+=1
            elif oc<-3: score-=1
        if fr is not None:
            if fr>0.05: score-=2; reasons.append(f"Funding {fr:+.3f}%")
            elif fr<-0.05: score+=2; reasons.append(f"Funding {fr:+.3f}%")
            elif abs(fr)<0.01: score+=1
        if ls is not None:
            if ls>2.0: score-=2
            elif ls<0.67: score+=2
            elif ls<0.85: score+=1
        if score>=3: direction,conf="LONG","قوية"
        elif score>=1: direction,conf="LONG","متوسطة"
        elif score<=-3: direction,conf="SHORT","قوية"
        elif score<=-1: direction,conf="SHORT","متوسطة"
        else: direction,conf=None,"—"
        return dict(direction=direction,confidence=conf,score=score,reasons=reasons,
                    price=price,oi=oi,fr=fr,ls=ls)
    except Exception: return None

_futures_score_cache={}
def futures_score_for(sym,side):
    key=(sym,side)
    if len(_futures_score_cache)>400: _futures_score_cache.clear()
    hit=_futures_score_cache.get(key)
    if hit and time.time()-hit[0]<300: return hit[1]
    try:
        oi=futures_oi_history(sym,"4h",20); fr=futures_funding_rate(sym); ls=futures_long_short_ratio(sym)
        score=0; details=[]
        if oi and oi.get("change_pct",0)!=0:
            oc=oi["change_pct"]
            if oc>5: score+=2; details.append(f"OI+{oc:.1f}%")
            elif oc>2: score+=1
            elif oc<-3: score-=1
        if fr is not None:
            if side==1:
                if fr<-0.05: score+=2
                elif abs(fr)<0.01: score+=1
                elif fr>0.05: score-=2
            else:
                if fr>0.05: score+=2
                elif abs(fr)<0.01: score+=1
                elif fr<-0.05: score-=2
        if ls is not None:
            if side==1:
                if ls<0.67: score+=2
                elif ls<0.85: score+=1
                elif ls>2.0: score-=2
            else:
                if ls>2.0: score+=2
                elif ls>1.5: score+=1
                elif ls<0.67: score-=2
        tf_ratio=futures_taker_flow(sym)
        if tf_ratio is not None:
            if side==1 and tf_ratio>0.6: score+=1; details.append(f"Taker شراء {tf_ratio:.0%}")
            elif side==-1 and tf_ratio<0.4: score+=1; details.append(f"Taker بيع {1-tf_ratio:.0%}")
            elif side==1 and tf_ratio<0.4: score-=1
            elif side==-1 and tf_ratio>0.6: score-=1
        result=dict(score=score,details=details,oi=oi,fr=fr,ls=ls,taker=tf_ratio)
        _futures_score_cache[key]=(time.time(),result); return result
    except Exception: return dict(score=0,details=[],oi=None,fr=None,ls=None)

def detect_cross(df):
    try:
        if len(df)<210: return None
        last=df.iloc[-1]; prev=df.iloc[-2]
        if pd.isna(last["ema50"]) or pd.isna(last["ema200"]): return None
        if pd.isna(prev["ema50"]) or pd.isna(prev["ema200"]): return None
        le50=float(last["ema50"]); le200=float(last["ema200"])
        pe50=float(prev["ema50"]); pe200=float(prev["ema200"])
        if pe50<=pe200 and le50>le200: return dict(type="golden",price=float(last["close"]),ema50=le50,ema200=le200,bar_t=int(last["t"]))
        if pe50>=pe200 and le50<le200: return dict(type="death",price=float(last["close"]),ema50=le50,ema200=le200,bar_t=int(last["t"]))
        return None
    except Exception: return None

def check_liquidity(sym,min_usd=MIN_DAILY_VOLUME_USD):
    try:
        df=get_candles(sym,"1h",24,ttl=600,min_bars=20)
        if df.empty or len(df)<20: return False
        return float((df["volume"]*df["close"]).sum())>=min_usd
    except Exception: return False

def whale_radar(sym):
    try:
        for ex_name in ["binance","okx","bybit"]:
            try:
                ex=get_ex(ex_name)
                with _ex_lock[ex_name]: trades=ex.fetch_trades(f"{sym}/USDT",limit=200)
                if not trades: continue
                large,largest,buy_vol,sell_vol=0,0.0,0.0,0.0
                for t in trades:
                    notional=float(t.get("price",0))*float(t.get("amount",0))
                    largest=max(largest,notional)
                    if notional>=5000:
                        large+=1
                        if t.get("side")=="buy": buy_vol+=notional
                        elif t.get("side")=="sell": sell_vol+=notional
                if largest>0: return large,largest,buy_vol,sell_vol
            except Exception: continue
        return None,None,0,0
    except Exception: return None,None,0,0

def liquidity_radar(sym):
    try:
        for ex_name in ["okx","bybit","gateio","mexc","kucoin"]:
            try:
                ex=get_ex(ex_name)
                with _ex_lock[ex_name]: ob=ex.fetch_order_book(f"{sym}/USDT",limit=50)
                bids=sum(b[0]*b[1] for b in ob.get("bids",[]))
                asks=sum(a[0]*a[1] for a in ob.get("asks",[]))
                total=bids+asks
                if total>0: return dict(bids=bids,asks=asks,ratio=bids/total,source=ex_name)
            except Exception: continue
        return None
    except Exception: return None

def explosion_radar(sym):
    try:
        df=get_candles(sym,"1h",60,ttl=120,min_bars=30)
        if df.empty or len(df)<20: return None
        last_vol=float(df["volume"].iloc[-1]); avg_vol=float(df["volume"].iloc[-20:-1].mean())
        if avg_vol<=0: return None
        vol_ratio=round(last_vol/avg_vol,2)
        if vol_ratio>=1.2:
            return dict(vol_ratio=vol_ratio,price_change=round((float(df["close"].iloc[-1])/float(df["close"].iloc[-5])-1)*100,2))
        return None
    except Exception: return None

def pump_radar(sym):
    try:
        df=get_candles(sym,"1h",100,ttl=180,min_bars=30)
        if df.empty or len(df)<30: return None
        change_1h=(float(df["close"].iloc[-1])/float(df["close"].iloc[-2])-1)*100
        change_4h=(float(df["close"].iloc[-1])/float(df["close"].iloc[-5])-1)*100
        last_vol=float(df["volume"].iloc[-1]); avg_vol=float(df["volume"].iloc[-20:-1].mean())
        if avg_vol<=0: return None
        vol_ratio=last_vol/avg_vol
        if change_1h>0.5 and vol_ratio>1.0:
            return dict(change_1h=round(change_1h,2),change_4h=round(change_4h,2),vol_ratio=round(vol_ratio,2))
        return None
    except Exception: return None

def bottom_radar(sym):
    """قيعان مرشحة للشراء — تُعرض فقط إذا المحرك لا يعارض بشدة (ليس SHORT مؤكد)."""
    try:
        df=get_candles(sym,"4h",400,ttl=300,min_bars=200)
        if df.empty or len(df)<200: return None
        df=add_indicators(df,100); last=df.iloc[-1]
        if pd.isna(last["rsi"]) or pd.isna(last["ema200"]): return None
        price=float(last["close"]); ema200=float(last["ema200"])
        if price < 0.000001: return None
        diff_pct=(price/ema200-1)*100
        if not (BOTTOM_MIN_DIFF<=diff_pct<=BOTTOM_MAX_DIFF): return None
        rsi_v=float(last["rsi"])
        if not (BOTTOM_RSI_RANGE[0]<=rsi_v<=BOTTOM_RSI_RANGE[1]): return None
        recent_vol=float(df["volume"].tail(5).mean()); avg_vol=float(df["volume"].tail(30).mean())
        if avg_vol<=0: return None
        vol_ratio=recent_vol/avg_vol
        if vol_ratio<BOTTOM_MIN_VOL_RATIO: return None
        low20=float(df["low"].tail(20).min())
        if price<low20*BOTTOM_AVOID_BREAK: return None
        low10=float(df["low"].tail(10).min())
        rebound_pct=(price/low10-1)*100
        if rebound_pct<BOTTOM_MIN_REBOUND: return None
        eng = engine_direction(sym, price)
        if eng == -1:
            return None  # المحرك SHORT → لا قاع شراء
        if diff_pct<-40 and rsi_v<35 and vol_ratio>1.2: severity="فرصة قوية"
        elif diff_pct<-25 and rebound_pct>3: severity="فرصة جيدة"
        else: severity="مراقبة"
        return dict(diff=round(diff_pct,1),rsi=round(rsi_v,1),rebound=round(rebound_pct,2),
                    vol_ratio=round(vol_ratio,2),severity=severity,price=round(price,6),
                    side=1,engine_aligned=(eng==1))
    except Exception: return None

def short_radar(sym):
    """فرص الشورت فقط إذا المحرك يدعم SHORT (لا تناقض مع التحليل)."""
    try:
        df=get_candles(sym,"4h",200,ttl=300,min_bars=100)
        if df.empty or len(df)<100: return None
        df=add_indicators(df,100); last=df.iloc[-1]
        if pd.isna(last["rsi"]): return None
        price=float(last["close"])
        if price < 0.000001: return None
        # شرط أولي: RSI مرتفع نسبياً
        if float(last["rsi"]) <= 55: return None
        eng = engine_direction(sym, price)
        if eng != -1:
            return None  # المحرك ليس SHORT → لا تُعرض كفرصة شورت
        return dict(rsi=round(float(last["rsi"]),1),price=round(price,6),side=-1,engine_aligned=True)
    except Exception: return None

def alpha_radar(sym):
    try:
        df=get_candles(sym,"1d",100,ttl=600,min_bars=50)
        if df.empty or len(df)<50: return None
        df=add_indicators(df,100); last=df.iloc[-1]
        if pd.isna(last["rsi"]) or pd.isna(last["ema20"]) or pd.isna(last["ema50"]): return None
        change_7d=(float(df["close"].iloc[-1])/float(df["close"].iloc[-7])-1)*100
        change_30d=(float(df["close"].iloc[-1])/float(df["close"].iloc[-30])-1)*100
        if last["ema20"]>last["ema50"] and 30<last["rsi"]<75 and change_7d>3:
            return dict(change_7d=round(change_7d,2),change_30d=round(change_30d,2),rsi=round(float(last["rsi"]),1))
        return None
    except Exception: return None

def hype_radar(limit=10):
    try:
        r = requests.get("https://api.coingecko.com/api/v3/search/trending", timeout=10)
        if r.status_code != 200: return []
        coins = r.json().get("coins", [])
        results = []
        for item in coins[:limit]:
            cd = item.get("item", {})
            sym = (cd.get("symbol", "") or "").upper()
            name = cd.get("name", "")
            rank = cd.get("market_cap_rank", "N/A")
            score = cd.get("score", 0)
            if sym in ALPHA_MAJORS_EXCLUDE or sym in STABLES: continue
            pd_ = cd.get("data", {}) or {}
            ch_raw = (pd_.get("price_change_percentage_24h") or {}).get("usd", 0)
            try: change = float(ch_raw or 0)
            except Exception: change = 0.0
            price_raw = (pd_.get("price") or 0)
            try: price = float(price_raw or 0)
            except Exception: price = 0.0
            categories = pd_.get("categories") or []
            is_web3 = any(("web3" in str(c).lower() or "defi" in str(c).lower()) for c in categories)
            results.append(dict(sym=sym, name=name, rank=rank, score=score,
                                change=change, price=price, is_web3=is_web3))
        return results
    except Exception as e:
        log.warning(f"hype_radar: {e}")
        return []

def build_hype_radar_text(results):
    if not results: return "🔥 لا توجد ترندات حالياً"
    lines = ["🔥 <b>رادار الضجة (Hype Radar)</b>","━━━━━━━━━━━━━━━━",""]
    web3_count = 0
    for r in results:
        icon = "🟢" if r["change"] > 2 else ("🔴" if r["change"] < -2 else "🟡")
        w3 = " 🌐 Web3" if r["is_web3"] else ""
        lines.append(f"{icon} <b>#{r['sym']}</b> ({r['name']}){w3}")
        lines.append(f"   📊 ترتيب: #{r['rank']} | زخم: {r['score']} | {r['change']:+.2f}%")
        if r["price"]: lines.append(f"   💵 ${r['price']:,.6f}")
        lines.append("")
        if r["is_web3"]: web3_count += 1
    lines.append(f"📈 إجمالي: {len(results)} | 🌐 Web3: {web3_count}")
    return "\n".join(lines)
_top_cache=[0,[]]
def top_symbols(n=60,binance_only=False):
    if not binance_only and time.time()-_top_cache[0]<1800 and _top_cache[1]: return _top_cache[1][:n]
    try:
        rows=binance_futures_all_tickers()
        if rows:
            rows=[(x["volume"],x["symbol"]) for x in rows if x["volume"]>2e6 and x["symbol"] not in EXCLUDE_TICKERS]
            rows.sort(reverse=True)
            if rows:
                _top_cache[0],_top_cache[1]=time.time(),[b for _,b in rows]
                return _top_cache[1][:n]
    except Exception as _ex: log.debug('ignored: %s', _ex)
    if binance_only: return COINS[:n]
    if is_multi_exchange():
        for name in FAST_EXCHANGES:
            if name=="binance": continue
            if _down.get(name,0)>time.time(): continue
            try:
                ex=get_ex(name)
                with _ex_lock[name]: tk=ex.fetch_tickers()
                rows=[]
                for s,t in tk.items():
                    if not s.endswith("/USDT") or ":" in s: continue
                    base=s.split("/")[0]
                    if base in DAILY_EXCLUDE or base in EXCLUDE_TICKERS: continue
                    if re.search(r"(UP|DOWN|BULL|BEAR|3L|3S)$",base): continue
                    qv=t.get("quoteVolume") or 0
                    if qv and qv>2e6: rows.append((qv,base))
                rows.sort(reverse=True)
                if rows:
                    _top_cache[0],_top_cache[1]=time.time(),[b for _,b in rows]
                    return _top_cache[1][:n]
            except Exception: continue
    return COINS[:n]

_HOT=[0,["BTC","ETH"]]
def hot_coins_for(sym):
    if time.time()-_HOT[0]>3600:
        try:
            rows=binance_futures_all_tickers()
            if rows:
                pop=sorted([r for r in rows if r["symbol"] in ("BTC","ETH","SOL","BNB","XRP","DOGE","ADA")],
                           key=lambda x:-x["volume"])
                _HOT[0]=time.time()
                _HOT[1]=[r["symbol"] for r in pop[:2]] or ["BTC","ETH"]
        except Exception as _ex: log.debug('ignored: %s', _ex)
    others=[c for c in _HOT[1] if c!=sym]
    coins=([sym]+others)[:3]
    while len(coins)<3:
        for c in ("BTC","ETH","SOL"):
            if c not in coins: coins.append(c); break
    return coins

def wilder(s,n): return s.ewm(alpha=1/n,adjust=False,min_periods=n).mean()
def ema(s,n): return s.ewm(span=n,adjust=False,min_periods=n).mean()
def true_range(df):
    pc=df["close"].shift()
    return pd.concat([df["high"]-df["low"],(df["high"]-pc).abs(),(df["low"]-pc).abs()],axis=1).max(axis=1)
def rsi_calc(close,n=14):
    d=close.diff()
    up,dn=wilder(d.clip(lower=0),n),wilder((-d).clip(lower=0),n)
    out=100-100/(1+up/dn.replace(0,np.nan))
    out[(dn==0)&up.notna()]=100.0
    return out
def macd_calc(close,fast=MACD_FAST,slow=MACD_SLOW,sig=MACD_SIGNAL):
    efast=ema(close,fast); eslow=ema(close,slow)
    macd=efast-eslow; signal_line=ema(macd,sig); hist=macd-signal_line
    return macd,signal_line,hist

def add_indicators(df,atrp_win=100):
    df=df.copy(); c=df["close"]
    df["ema20"],df["ema50"],df["ema200"]=ema(c,20),ema(c,50),ema(c,200)
    df["rsi"]=rsi_calc(c); df["atr"]=wilder(true_range(df),14)
    df["rvol"]=df["volume"]/df["volume"].rolling(20).mean().replace(0,np.nan)
    df["macd"],df["macd_sig"],df["macd_hist"]=macd_calc(c)
    mid,sd=c.rolling(20).mean(),c.rolling(20).std()
    df["bb_mid"],df["bb_up"],df["bb_lo"]=mid,mid+2*sd,mid-2*sd
    atrp=df["atr"]/c
    df["atrp"]=atrp.rolling(atrp_win,min_periods=min(atrp_win,30)).apply(lambda x:(x[-1]>=x).mean()*100,raw=True)
    rng=(df["high"]-df["low"]).replace(0,np.nan)
    df["wick_up"]=(df["high"]-df[["close","open"]].max(axis=1))/rng
    df["wick_dn"]=(df[["close","open"]].min(axis=1)-df["low"])/rng
    df["ext_atr"]=(c-df["ema20"])/df["atr"]
    tp=(df["high"]+df["low"]+df["close"])/3
    df["vwap"]=(tp*df["volume"]).rolling(50).sum()/df["volume"].rolling(50).sum().replace(0,np.nan)
    df["highest_10"]=df["high"].rolling(10).max()
    df["lowest_10"]=df["low"].rolling(10).min()
    return df

def detect_trend(df,i):
    if i<50: return "unknown"
    r=df.iloc[i]
    if pd.isna(r["ema20"]) or pd.isna(r["ema50"]) or pd.isna(r["ema200"]): return "unknown"
    if r["ema20"]>r["ema50"]>r["ema200"] and r["close"]>r["ema20"]: return "strong_up"
    if r["ema20"]>r["ema50"] and r["close"]>r["ema200"]: return "up"
    if r["ema20"]<r["ema50"]<r["ema200"] and r["close"]<r["ema20"]: return "strong_down"
    if r["ema20"]<r["ema50"] and r["close"]<r["ema200"]: return "down"
    return "range"

def daily_bias(df_d):
    try:
        if df_d is None or len(df_d)<200: return 0
        last=df_d.iloc[-1]; c=float(last["close"])
        e20=float(last["ema20"]) if not pd.isna(last["ema20"]) else c
        e50=float(last["ema50"]) if not pd.isna(last["ema50"]) else c
        e200=float(last["ema200"]) if not pd.isna(last["ema200"]) else c
        rsi_v=float(last["rsi"]) if not pd.isna(last["rsi"]) else 50
        score=0
        if c<e50: score-=1
        if c<e200: score-=1
        if c>e50: score+=1
        if c>e200: score+=1
        if e20<e50: score-=1
        if e50<e200: score-=1
        if e20>e50: score+=1
        if e50>e200: score+=1
        if rsi_v<45: score-=1
        if rsi_v>55: score+=1
        try:
            change_7d=(c/float(df_d["close"].iloc[-8])-1)*100
            if change_7d<-5: score-=1
            if change_7d>5: score+=1
        except Exception as _ex: log.debug('ignored: %s', _ex)
        if score<=-2: return -1
        if score>=2: return 1
        return 0
    except Exception: return 0

def mtf_score(sym,side):
    try:
        if sym in MAJORS: tfs=["1d","4h"]
        elif sym in MEME_COINS: tfs=["4h","1d"]
        else: tfs=["4h","1h","1d"]
        confirms=0; total=0
        for tf in tfs:
            try:
                df=get_candles(sym,tf,250,ttl=300,min_bars=100)
                df=add_indicators(df,100); last=df.iloc[-1]
                if pd.isna(last["ema50"]) or pd.isna(last["ema200"]): continue
                total+=1
                if side==1:
                    if last["close"]>last["ema50"] and last["close"]>last["ema200"]: confirms+=1
                else:
                    if last["close"]<last["ema50"] and last["close"]<last["ema200"]: confirms+=1
            except Exception: continue
        if total==0: return 0,0
        return confirms,total
    except Exception: return 0,0

def find_pivots(df,window=PIVOT_WINDOW,lookback=PIVOT_LOOKBACK):
    try:
        n=len(df)
        if n<window+20: window=max(20,n-20)
        if window<20: return [],[]
        sub=df.tail(window).reset_index(drop=True)
        highs,lows=[],[]
        for i in range(lookback,len(sub)-lookback):
            try:
                h=float(sub["high"].iloc[i]); l=float(sub["low"].iloc[i])
                if pd.isna(h) or pd.isna(l): continue
                if all(h>=float(sub["high"].iloc[i-j]) for j in range(1,lookback+1)) and all(h>=float(sub["high"].iloc[i+j]) for j in range(1,lookback+1)): highs.append(h)
                if all(l<=float(sub["low"].iloc[i-j]) for j in range(1,lookback+1)) and all(l<=float(sub["low"].iloc[i+j]) for j in range(1,lookback+1)): lows.append(l)
            except Exception: continue
        highs.sort(reverse=True); lows.sort(reverse=True)
        return highs,lows
    except Exception: return [],[]

def select_targets(pivots,price,side,min_dist_pct=MIN_PIVOT_DIST):
    selected=[]
    ordered=sorted(set(pivots)) if side==1 else sorted(set(pivots),reverse=True)
    for p in ordered:
        if side==1:
            if p<=price*(1+min_dist_pct/100): continue
            if selected and p<=selected[-1]*(1+min_dist_pct/100): continue
            selected.append(p)
        else:
            if p>=price*(1-min_dist_pct/100): continue
            if selected and p>=selected[-1]*(1-min_dist_pct/100): continue
            selected.append(p)
        if len(selected)>=4: break
    return selected

def compute_core_levels(df, i, side, price=None):
    try:
        r = df.iloc[i]; atr = float(r["atr"]) if not pd.isna(r["atr"]) else 0
        if atr <= 0: return None
        entry = float(price) if price else float(r["close"])
        
        recent_df = df.tail(100)
        if side == 1:
            resistances = recent_df[recent_df["high"] > entry]["high"].unique()
            targets = sorted([float(x) for x in resistances if float(x) > entry + (atr*0.5)])[:4]
        else:
            supports = recent_df[recent_df["low"] < entry]["low"].unique()
            targets = sorted([float(x) for x in supports if float(x) < entry - (atr*0.5)], reverse=True)[:4]

        if len(targets) < 4:
            last_px = targets[-1] if targets else entry
            while len(targets) < 4:
                next_px = last_px + side * atr * 1.5
                targets.append(next_px)
                last_px = next_px

        targets = targets[:4]
        
        lookback_df = df.tail(15)
        swing = float(lookback_df["low"].min()) if side==1 else float(lookback_df["high"].max())
        sl_raw = swing - 0.1*atr if side==1 else swing + 0.1*atr
        dist_raw = abs(entry - sl_raw)
        
        dist = max(dist_raw, entry*CORE_SL_MIN/100)
        dist = min(dist, entry*CORE_SL_MAX/100)
        sl = entry - side * dist; risk = abs(entry - sl)
        if risk <= 0: return None

        min_tp1_dist = risk * MIN_RR_TP1
        if side == 1:
            if targets[0] < entry + min_tp1_dist:
                targets[0] = entry + min_tp1_dist
            for k in range(1, 4):
                if targets[k] <= targets[k-1]:
                    targets[k] = targets[k-1] + atr * 0.5
        else:
            if targets[0] > entry - min_tp1_dist:
                targets[0] = entry - min_tp1_dist
            for k in range(1, 4):
                if targets[k] >= targets[k-1]:
                    targets[k] = targets[k-1] - atr * 0.5
            # لا أسعار سالبة في الشورت
            targets = [max(float(x), entry * 0.05) for x in targets]

        # سقف المخاطرة 12%
        if risk > entry * 0.12:
            risk = entry * 0.12
            sl = entry - side * risk
        tps = []
        for idx, px in enumerate(targets):
            pct = 100 * side * (px - entry) / entry
            frac = CORE_TP_FRACS[idx] if idx < len(CORE_TP_FRACS) else  0
            tps.append(dict(px=float(px), pct=round(pct,2), frac=frac, hit=False, src=""))
            
        return dict(entry=entry, sl=sl, risk=risk, tps=tps, atr=atr)
    except Exception: return None

def compute_meme_levels(df,i,side,price=None):
    try:
        r=df.iloc[i]; entry=float(price) if price else float(r["close"])
        sl=entry*(1-side*MEME_SL_PCT/100); risk=abs(entry-sl)
        if risk<=0: return None
        tps=[]
        for pct,f in zip(MEME_TP_PCTS,MEME_TP_FRACS):
            px=entry*(1+side*pct/100)
            tps.append(dict(px=float(px),pct=pct,frac=f,hit=False,src=""))
        atr=float(r["atr"]) if not pd.isna(r["atr"]) else 0
        return dict(entry=entry,sl=sl,risk=risk,tps=tps,atr=atr)
    except Exception: return None

def detect_core_conditions(df,i,side):
    if i<30: return False
    r=df.iloc[i]
    if pd.isna(r.rsi) or pd.isna(r.ema20) or pd.isna(r.ema50) or pd.isna(r.rvol): return False
    if side==1:
        if not (CORE_RSI_LONG[0]<=r.rsi<=CORE_RSI_LONG[1]): return False
        if not (r.ema20>r.ema50*0.99 and r.close>r.ema50*0.99): return False
    else:
        if not (CORE_RSI_SHORT[0]<=r.rsi<=CORE_RSI_SHORT[1]): return False
        if not (r.ema20<r.ema50*1.01 and r.close<r.ema50*1.01): return False
    if r.rvol<CORE_VOL_MULT: return False
    if abs(r.ext_atr)>EXT_MAX_ATR: return False
    return True

def detect_meme_conditions(df,i,side):
    if i<MEME_BREAKOUT_BARS+5: return False
    r=df.iloc[i]
    if pd.isna(r.rsi) or pd.isna(r.rvol): return False
    if not (MEME_RSI_LOW<=r.rsi<=MEME_RSI_HIGH): return False
    if r.rvol<MEME_VOL_MULT: return False
    rh=float(df["high"].iloc[i-MEME_BREAKOUT_BARS:i].max())
    rl=float(df["low"].iloc[i-MEME_BREAKOUT_BARS:i].min())
    if side==1:
        if float(r.close)<=rh: return False
    else:
        if float(r.close)>=rl: return False
    return True

def _find_pattern_pivots(df,left=5,right=5,max_pivots=8):
    highs,lows=[],[]; n=len(df)
    if n<left+right+5: return highs,lows
    for i in range(left,n-right):
        try:
            h=float(df["high"].iloc[i]); l=float(df["low"].iloc[i])
            is_high=all(h>=float(df["high"].iloc[i-j]) for j in range(1,left+1)) and all(h>=float(df["high"].iloc[i+j]) for j in range(1,right+1))
            if is_high: highs.append((i,h))
            is_low=all(l<=float(df["low"].iloc[i-j]) for j in range(1,left+1)) and all(l<=float(df["low"].iloc[i+j]) for j in range(1,right+1))
            if is_low: lows.append((i,l))
        except Exception: continue
    return highs[-max_pivots:],lows[-max_pivots:]

def _approx_equal(a,b,tol_pct=2.0):
    if a==0 or b==0: return False
    return abs(a-b)/max(a,b)*100<=tol_pct

def _pt(df,i,price): return (int(df["t"].iloc[i]),float(price))
def _seg(p1,p2,color="neutral",ext=False):
    return dict(t1=int(p1[0]),p1=float(p1[1]),t2=int(p2[0]),p2=float(p2[1]),ext=ext,color=color)
def _mark(p,label): return dict(t=int(p[0]),p=float(p[1]),label=label)
def _fit_seg(sub,slope,icpt,color):
    last=len(sub)-1
    return _seg((int(sub["t"].iloc[0]),icpt),(int(sub["t"].iloc[-1]),icpt+slope*last),color,True)

def detect_double_top_bottom(df):
    highs,lows=_find_pattern_pivots(df,5,5,8)
    patterns=[]
    last_t=int(df["t"].iloc[-1]); cur=float(df["close"].iloc[-1])
    if len(highs)>=2:
        h1_i,h1_p=highs[-2]; h2_i,h2_p=highs[-1]
        if h2_i-h1_i>=5 and _approx_equal(h1_p,h2_p,1.5):
            neck=float(df["low"].iloc[h1_i:h2_i+1].min())
            if cur<neck:
                p1=_pt(df,h1_i,h1_p); p2=_pt(df,h2_i,h2_p)
                patterns.append(dict(name="Double Top",bias="bearish",confirmed=True,
                    pivot1=h1_p,pivot2=h2_p,neckline=neck,
                    segments=[_seg(p1,p2,"bearish"),_seg((p1[0],neck),(last_t,neck),"neck",True)],
                    marks=[_mark(p1,"T1"),_mark(p2,"T2")]))
    if len(lows)>=2:
        l1_i,l1_p=lows[-2]; l2_i,l2_p=lows[-1]
        if l2_i-l1_i>=5 and _approx_equal(l1_p,l2_p,1.5):
            neck=float(df["high"].iloc[l1_i:l2_i+1].max())
            if cur>neck:
                p1=_pt(df,l1_i,l1_p); p2=_pt(df,l2_i,l2_p)
                patterns.append(dict(name="Double Bottom",bias="bullish",confirmed=True,
                    pivot1=l1_p,pivot2=l2_p,neckline=neck,
                    segments=[_seg(p1,p2,"bullish"),_seg((p1[0],neck),(last_t,neck),"neck",True)],
                    marks=[_mark(p1,"B1"),_mark(p2,"B2")]))
    return patterns

def detect_head_shoulders(df):
    highs,lows=_find_pattern_pivots(df,5,5,10)
    patterns=[]
    last_t=int(df["t"].iloc[-1]); cur=float(df["close"].iloc[-1])
    if len(highs)>=3:
        l_sh,head,r_sh=highs[-3],highs[-2],highs[-1]
        if head[1]>l_sh[1] and head[1]>r_sh[1]:
            if _approx_equal(l_sh[1],r_sh[1],3.0) and (r_sh[0]-l_sh[0])>=15:
                neck=float(df["low"].iloc[l_sh[0]:r_sh[0]+1].min())
                if cur<neck:
                    pl=_pt(df,l_sh[0],l_sh[1]); ph=_pt(df,head[0],head[1]); pr=_pt(df,r_sh[0],r_sh[1])
                    patterns.append(dict(name="Head & Shoulders",bias="bearish",confirmed=True,
                        head=head[1],neckline=neck,
                        segments=[_seg(pl,ph,"bearish"),_seg(ph,pr,"bearish"),_seg((pl[0],neck),(last_t,neck),"neck",True)],
                        marks=[_mark(pl,"LS"),_mark(ph,"H"),_mark(pr,"RS")]))
    if len(lows)>=3:
        l_sh,head,r_sh=lows[-3],lows[-2],lows[-1]
        if head[1]<l_sh[1] and head[1]<r_sh[1]:
            if _approx_equal(l_sh[1],r_sh[1],3.0) and (r_sh[0]-l_sh[0])>=15:
                neck=float(df["high"].iloc[l_sh[0]:r_sh[0]+1].max())
                if cur>neck:
                    pl=_pt(df,l_sh[0],l_sh[1]); ph=_pt(df,head[0],head[1]); pr=_pt(df,r_sh[0],r_sh[1])
                    patterns.append(dict(name="Inverse H&S",bias="bullish",confirmed=True,
                        head=head[1],neckline=neck,
                        segments=[_seg(pl,ph,"bullish"),_seg(ph,pr,"bullish"),_seg((pl[0],neck),(last_t,neck),"neck",True)],
                        marks=[_mark(pl,"LS"),_mark(ph,"H"),_mark(pr,"RS")]))
    return patterns

def detect_triangle(df,lookback=40):
    if len(df)<lookback: return []
    sub=df.tail(lookback).reset_index(drop=True)
    x=np.arange(len(sub))
    try:
        hs,hb=np.polyfit(x,sub["high"].values.astype(float),1)
        ls,lb=np.polyfit(x,sub["low"].values.astype(float),1)
    except Exception: return []
    if abs(hs)<1e-9 or abs(ls)<1e-9: return []
    patterns=[]
    if abs(hs)/max(sub["high"].mean(),1e-9)<0.0005 and ls>0:
        patterns.append(dict(name="Ascending Triangle",bias="bullish",confirmed=True,
            segments=[_fit_seg(sub,hs,hb,"bullish"),_fit_seg(sub,ls,lb,"bullish")],marks=[]))
    elif abs(ls)/max(sub["low"].mean(),1e-9)<0.0005 and hs<0:
        patterns.append(dict(name="Descending Triangle",bias="bearish",confirmed=True,
            segments=[_fit_seg(sub,hs,hb,"bearish"),_fit_seg(sub,ls,lb,"bearish")],marks=[]))
    elif hs<0 and ls>0:
        patterns.append(dict(name="Symmetrical Triangle",bias="neutral",confirmed=False,
            segments=[_fit_seg(sub,hs,hb,"neutral"),_fit_seg(sub,ls,lb,"neutral")],marks=[]))
    return patterns

def detect_flags(df,lookback=30):
    if len(df)<lookback+10: return []
    recent=df.tail(lookback).reset_index(drop=True)
    split=int(lookback*0.3)
    imp=recent.iloc[:split]; consol=recent.iloc[split:]
    if len(imp)<5 or len(consol)<5: return []
    try:
        imp_move=(float(imp["close"].iloc[-1])/float(imp["close"].iloc[0])-1)*100
        consol_move=(float(consol["close"].iloc[-1])/float(consol["close"].iloc[0])-1)*100
        consol_range=(float(consol["high"].max())/float(consol["low"].min())-1)*100
    except Exception: return []
    t_c1=int(consol["t"].iloc[0]); t_c2=int(consol["t"].iloc[-1])
    hi_c=float(consol["high"].max()); lo_c=float(consol["low"].min())
    channel=[_seg((t_c1,hi_c),(t_c2,hi_c),"neutral",True),_seg((t_c1,lo_c),(t_c2,lo_c),"neutral",True)]
    pole_p1=(int(imp["t"].iloc[0]),float(imp["close"].iloc[0]))
    pole_p2=(int(imp["t"].iloc[-1]),float(imp["close"].iloc[-1]))
    patterns=[]
    if imp_move>5 and -5<consol_move<0 and consol_range<imp_move*0.5:
        patterns.append(dict(name="Bull Flag",bias="bullish",confirmed=True,
            segments=[_seg(pole_p1,pole_p2,"bullish")]+channel,marks=[]))
    elif imp_move<-5 and 0<consol_move<5 and consol_range<abs(imp_move)*0.5:
        patterns.append(dict(name="Bear Flag",bias="bearish",confirmed=True,
            segments=[_seg(pole_p1,pole_p2,"bearish")]+channel,marks=[]))
    return patterns

def detect_wedge(df,lookback=40):
    if len(df)<lookback: return []
    sub=df.tail(lookback).reset_index(drop=True)
    x=np.arange(len(sub))
    try:
        hs,hb=np.polyfit(x,sub["high"].values.astype(float),1)
        ls,lb=np.polyfit(x,sub["low"].values.astype(float),1)
    except Exception: return []
    patterns=[]
    if hs>0 and ls>0 and ls>hs*1.5:
        patterns.append(dict(name="Rising Wedge",bias="bearish",confirmed=True,
            segments=[_fit_seg(sub,hs,hb,"bearish"),_fit_seg(sub,ls,lb,"bearish")],marks=[]))
    elif hs<0 and ls<0 and hs<ls*1.5:
        patterns.append(dict(name="Falling Wedge",bias="bullish",confirmed=True,
            segments=[_fit_seg(sub,hs,hb,"bullish"),_fit_seg(sub,ls,lb,"bullish")],marks=[]))
    return patterns

def detect_all_patterns(df,tf=None):
    out=[]
    try: out+=detect_head_shoulders(df)
    except Exception as _ex: log.debug('ignored: %s', _ex)
    try: out+=detect_double_top_bottom(df)
    except Exception as _ex: log.debug('ignored: %s', _ex)
    try: out+=detect_triangle(df)
    except Exception as _ex: log.debug('ignored: %s', _ex)
    try: out+=detect_flags(df)
    except Exception as _ex: log.debug('ignored: %s', _ex)
    try: out+=detect_wedge(df)
    except Exception as _ex: log.debug('ignored: %s', _ex)
    confirmed=[p for p in out if p.get("confirmed")]
    if tf:
        for p in confirmed: p["tf"]=tf
    return confirmed

def grade_setup(df,i,side,futures_score=0,patterns=None):
    r=df.iloc[i]; score=0
    if side==1 and 40<=r.rsi<=65: score+=1
    elif side==-1 and 55<=r.rsi<=75: score+=1
    ema_dist=abs(r.close-r.ema20)/r.atr if r.atr>0 else 0
    if 0.3<=ema_dist<=2.5: score+=1
    if r.rvol>=1.2: score+=1
    body=abs(r.close-r.open)/r.atr if r.atr>0 else 0
    if body>=0.4: score+=1
    if futures_score>=3: score+=2
    elif futures_score>=1: score+=1
    elif futures_score<=-3: score-=2
    elif futures_score<=-1: score-=1
    vwap=float(r.get("vwap",0))
    if side==1 and r.close>vwap: score+=1
    if side==-1 and r.close<vwap: score+=1
    if patterns:
        for p in patterns:
            if p["bias"]=="bullish" and side==1: score+=2
            elif p["bias"]=="bearish" and side==-1: score+=2
            elif p["bias"]=="bullish" and side==-1: score-=1
            elif p["bias"]=="bearish" and side==1: score-=1
    if score>=5: return "A+"
    if score>=4: return "A"
    if score>=2: return "B"
    return "C"

def decide_side(df,i,trend,bias_d,long_ok,short_ok):
    r=df.iloc[i]
    try:
        c=float(r["close"]); sgn=lambda x:1 if x>0 else (-1 if x<0 else 0)
        score=(sgn(c-float(r["ema20"]))+sgn(c-float(r["ema50"]))+sgn(c-float(r["ema200"]))
               +sgn(float(r["ema20"])-float(r["ema50"]))+sgn(float(r["macd_hist"])))
        vwap=float(r.get("vwap",c))
        if c>vwap: score+=1
        elif c<vwap: score-=1
        highest_10=float(r.get("highest_10",c)); lowest_10=float(r.get("lowest_10",c))
        if c>=highest_10: score+=1
        if c<=lowest_10: score-=1
        if score<=-3: return -1
        if score>=3: return 1
        if score in (-2,2):
            d=1 if score>0 else -1
            if bias_d!=-d: return d
            return d if abs(c-float(r["ema200"]))>0 and sgn(c-float(r["ema200"]))==d else bias_d
    except Exception as _ex: log.debug('ignored: %s', _ex)
    tf_dir=1 if trend in ("strong_up","up") else (-1 if trend in ("strong_down","down") else 0)
    if tf_dir: return tf_dir
    if bias_d: return bias_d
    if long_ok!=short_ok: return 1 if long_ok else -1
    try:
        c,e50=float(r["close"]),float(r["ema50"])
        return -1 if c<e50 else 1
    except Exception: return 1

def find_sr_levels(df,lookback=SR_LOOKBACK):
    try:
        sub=df.tail(lookback).reset_index(drop=True)
        if len(sub)<20: return [],[]
        highs,lows=[],[]
        for i in range(3,len(sub)-3):
            h=float(sub["high"].iloc[i]); l=float(sub["low"].iloc[i])
            if pd.isna(h) or pd.isna(l): continue
            if all(h>=float(sub["high"].iloc[i-j]) for j in range(1,4)) and all(h>=float(sub["high"].iloc[i+j]) for j in range(1,4)): highs.append(h)
            if all(l<=float(sub["low"].iloc[i-j]) for j in range(1,4)) and all(l<=float(sub["low"].iloc[i+j]) for j in range(1,4)): lows.append(l)
        def merge(levels):
            if not levels: return []
            levels=sorted(levels); merged=[levels[0]]
            for lv in levels[1:]:
                if abs(lv-merged[-1])/merged[-1]*100<SR_MERGE_PCT: merged[-1]=(merged[-1]+lv)/2
                else: merged.append(lv)
            return merged
        return merge(highs),merge(lows)
    except Exception: return [],[]

def detect_candle_pattern(df,i):
    if i<5: return None
    r=df.iloc[i]; o,h,l,c=float(r["open"]),float(r["high"]),float(r["low"]),float(r["close"])
    body=abs(c-o); rng=h-l
    if rng==0: return None
    upper=h-max(o,c); lower=min(o,c)-l
    if lower>body*2 and upper<body*0.5 and body>0: return "مطرقة"
    if upper>body*2 and lower<body*0.5 and body>0: return "شهاب"
    if body<rng*0.1: return "دوجي"
    if i>=1:
        prev=df.iloc[i-1]
        if c>o and prev["close"]<prev["open"] and c>float(prev["open"]) and o<float(prev["close"]): return "ابتلاع صاعد"
        if c<o and prev["close"]>prev["open"] and c<float(prev["open"]) and o>float(prev["close"]): return "ابتلاع هابط"
    return None

def fib_levels(df,lookback=SR_LOOKBACK):
    try:
        sub=df.tail(lookback); hi=float(sub["high"].max()); lo=float(sub["low"].min())
        diff=hi-lo
        if diff<=0: return []
        return [round(hi-diff*f,6) for f in FIB_LEVELS]
    except Exception: return []

def _sgn_i(x): return 1 if x>0 else (-1 if x<0 else 0)

def _eng_df(d):
    return se.prepare(d[["t","open","high","low","close","volume"]])

_btc_cache=[0,0]
def btc_trend_cached():
    if time.time()-_btc_cache[0]<300: return _btc_cache[1]
    d=get_candles("BTC","4h",300,ttl=300,min_bars=100)
    e=_eng_df(d); v=_sgn_i(se.trend_state(e,len(e)-1))
    _btc_cache[0],_btc_cache[1]=time.time(),v
    return v

def _hypo_levels(price,side,atr):
    """مستويات افتراضية للعرض فقط عند حالة الانتظار (لا تُنشر ولا تُتتبع)."""
    risk = min(1.5 * atr, price * 0.08)
    risk = max(risk, price * 0.012)
    sl = price - side * risk
    tps = []
    for k, m in enumerate((1.2, 2.0, 3.0, 4.0)):
        px = price + side * risk * m
        if side == -1:
            px = max(px, price * 0.05)
        tps.append(dict(px=float(px), pct=round(100 * side * (px - price) / price, 2),
                        frac=CORE_TP_FRACS[k], hit=False, src="R"))
    return dict(entry=float(price), sl=float(sl), risk=float(risk), tps=tps, atr=float(atr))

_analysis_cache={}

def engine_direction(sym, price=None):
    """اتجاه المحرك الرئيسي فقط (بدون forced).
    يرجع: 1 LONG، -1 SHORT، 0 WAIT/محايد.
    يُستخدم لضمان توافق الرادارات مع التحليل."""
    try:
        tf = default_tf(sym)
        df = get_candles(sym, tf, 450, ttl=120, min_bars=MIN_BARS)
        df = add_indicators(df, 100)
        ed = _eng_df(df)
        ed_d = None
        try:
            df_d = get_candles(sym, "1d", 250, ttl=300, min_bars=100)
            ed_d = _eng_df(add_indicators(df_d, 100))
        except Exception:
            pass
        ed_h = None
        if tf != "1h":
            try:
                ed_h = _eng_df(get_candles(sym, "1h", 250, ttl=300, min_bars=100))
            except Exception:
                pass
        px = price
        if px is None:
            px, _ = live_price(sym)
            px = float(px or df["close"].iloc[-1])
        if px < 0.000001:
            return 0
        try:
            fut = futures_score_for(sym, _sgn_i(se.trend_state(ed, len(ed)-1)) or 1)
            fut_score = fut.get("score", 0)
        except Exception:
            fut_score = 0
        funding = oi_change = ls_ratio = liq_data = None
        try: funding = futures_funding_rate(sym)
        except Exception: pass
        try:
            oi = futures_oi_history(sym, "4h", 20)
            oi_change = oi.get("change_pct") if oi else None
        except Exception: pass
        try: ls_ratio = futures_long_short_ratio(sym)
        except Exception: pass
        try: liq_data = futures_liquidations(sym)
        except Exception: pass
        patterns = []
        btc_t = 0
        if sym != "BTC":
            try: btc_t = btc_trend_cached()
            except Exception: pass
        ev = se.evaluate(ed, ed_d, ed_h, price=px, fut_score=fut_score, btc_trend=btc_t,
                         patterns=patterns, funding=funding, oi_change=oi_change,
                         ls_ratio=ls_ratio, liq_data=liq_data)
        if ev["status"] == "WAIT":
            return 0
        # MTF hard filter (same as analyze)
        if ed_h is not None:
            try:
                h1_t = se.trend_state(ed_h, len(ed_h)-1)
                if ev["side"] == 1 and h1_t == -2: return 0
                if ev["side"] == -1 and h1_t == 2: return 0
            except Exception:
                pass
        return int(ev["side"])
    except Exception:
        return 0


def analyze(sym):
    tf=default_tf(sym); df=None; last_err=None
    fallbacks=[tf]+[x for x in ("1h","4h","1d") if x!=tf]
    for try_tf in fallbacks:
        try:
            df=get_candles(sym,try_tf,450); tf=try_tf; break
        except PairNotFound: raise
        except NoData as e: last_err=e; continue
    if df is None: raise last_err or NoData(sym)
    degraded=bool(df.attrs.get("degraded"))
    df=add_indicators(df,100); i=len(df)-1
    is_meme=sym in MEME_COINS
    if is_meme:
        long_ok=detect_meme_conditions(df,i,1); short_ok=detect_meme_conditions(df,i,-1)
        strategy="Meme"
    else:
        long_ok=detect_core_conditions(df,i,1); short_ok=detect_core_conditions(df,i,-1)
        strategy="Core"
    price,_=live_price(sym); price=float(price or df["close"].iloc[-1])
    # حماية من العملات الرخيصة جداً (shitcoins) — انزلاق سعري كارثي
    if price < 0.000001:
        raise NoData(sym)
    trend=detect_trend(df,i); df_d=None
    try:
        df_d=get_candles(sym,"1d",250,ttl=300,min_bars=100); df_d=add_indicators(df_d,100)
    except Exception as _ex: log.debug('ignored: %s', _ex)
    bias_d=daily_bias(df_d) if df_d is not None else 0
    # ---------- المحرك الدقيق (signal_engine) ----------
    ed=_eng_df(df); ed_d=_eng_df(df_d) if df_d is not None else None
    ed_h=None
    if tf!="1h":
        try: ed_h=_eng_df(get_candles(sym,"1h",250,ttl=300,min_bars=100))
        except Exception: ed_h=None
    cand=_sgn_i(se.trend_state(ed,len(ed)-1))
    try: fut=futures_score_for(sym,cand) if cand else dict(score=0,details=[])
    except Exception: fut=dict(score=0,details=[])
    fut_score=fut.get("score",0)
    funding=oi_change=ls_ratio=liq_data=None
    try: funding=futures_funding_rate(sym)
    except Exception: pass
    try:
        oi_data=futures_oi_history(sym,"4h",20)
        oi_change=oi_data.get("change_pct") if oi_data else None
    except Exception: pass
    try: ls_ratio=futures_long_short_ratio(sym)
    except Exception: pass
    try: liq_data=futures_liquidations(sym)
    except Exception: pass
    try:
        all_pats=[]; seen_names=set()
        for pat_tf in ["1d","4h","1h"]:
            try:
                if pat_tf==tf: p_df=df
                else:
                    p_df=get_candles(sym,pat_tf,300,ttl=600,min_bars=100); p_df=add_indicators(p_df,100)
                pats=detect_all_patterns(p_df,pat_tf)
                for p in pats:
                    key=f"{p['name']}"
                    if key not in seen_names: seen_names.add(key); all_pats.append(p)
            except Exception: continue
        patterns=all_pats
    except Exception: patterns=[]
    btc_t=0
    if sym!="BTC":
        try: btc_t=btc_trend_cached()
        except Exception: btc_t=0
    ev=se.evaluate(ed,ed_d,ed_h,price=price,fut_score=fut_score,btc_trend=btc_t,patterns=patterns,
                   funding=funding,oi_change=oi_change,ls_ratio=ls_ratio,liq_data=liq_data)
    wait=(ev["status"]=="WAIT")
    # ---- فلتر تعدد الأطر الزمنية الصارم (MTF)
    # قبل قبول LONG على 4س: فريم الساعة يجب ألا يكون هابطاً قوياً (-2)
    # قبل قبول SHORT: فريم الساعة يجب ألا يكون صاعداً قوياً (+2)
    if not wait and ed_h is not None:
        try:
            h1_t = se.trend_state(ed_h, len(ed_h)-1)
            if ev["side"] == 1 and h1_t == -2:
                wait = True
                ev = dict(ev)
                ev["status"] = "WAIT"
                ev.setdefault("wait_reasons", []).append("MTF: فريم الساعة هابط قوي (−2) — إلغاء LONG")
            elif ev["side"] == -1 and h1_t == 2:
                wait = True
                ev = dict(ev)
                ev["status"] = "WAIT"
                ev.setdefault("wait_reasons", []).append("MTF: فريم الساعة صاعد قوي (+2) — إلغاء SHORT")
        except Exception:
            pass
    atr_now=float(ed["atr"].iloc[-1]) if not pd.isna(ed["atr"].iloc[-1]) else price*0.02
    forced=False
    if wait:
        # البوت يعطي صفقة دائماً: إن لم تتحقق شروط الجودة تُعرض بدرجة C مع تحذير واضح
        # (forced=True تمنع نشرها تلقائياً في القنوات وفي الفرصة الذهبية)
        forced=True
        side=cand
        if not side and ed_d is not None: side=_sgn_i(se.trend_state(ed_d,len(ed_d)-1))
        if not side: side=1 if price>=float(ed["ema50"].iloc[-1]) else -1
        lv=None
        try:
            lv,_w=se.build_levels(ed,len(ed)-1,side,price,atr_now,ed_d,
                                  (len(ed_d)-1) if ed_d is not None else None,
                                  dict(se.CFG,max_sl_atr=4.0,min_rr_tp1=0.8))
        except Exception: lv=None
        if lv is None: lv=_hypo_levels(price,side,atr_now)
        grade="C"; wait=False
    else:
        side=ev["side"]; grade=ev["grade"]; lv=ev["lv"]
    if lv is None: raise NoData(sym)
    candle=detect_candle_pattern(df,i); sr_high,sr_low=find_sr_levels(df); fibs=fib_levels(df)
    last=df.iloc[-1]
    return dict(sym=sym,tf=tf,df=df,side=side,strategy=strategy,
                rank={"A+":3,"A":2,"B":1,"C":0}.get(grade,-1),grade=grade,setup=strategy,
                ok=1 if grade in ("A+","A","B") else 0,total=4,
                wait=wait,forced=forced,wait_reasons=ev.get("wait_reasons",[]),reasons=ev.get("reasons",[]),
                warnings=ev.get("warnings",[]),score=ev.get("score",0.0),
                confidence=ev.get("confidence",0),watch=ev.get("watch"),trends=ev.get("trends",{}),
                lv=lv,price=price,degraded=degraded,source=df.attrs.get("source","binance_fut"),
                bar_t=int(last["t"]),rsi=float(last["rsi"]) if not pd.isna(last["rsi"]) else 50.0,
                macd=float(last["macd"]) if not pd.isna(last["macd"]) else 0.0,
                candle=candle,trend=trend,trend_d=("down" if bias_d==-1 else ("up" if bias_d==1 else "range")),
                sr_high=sr_high[:3],sr_low=sr_low[:3],fibs=fibs,
                fut_score=fut_score,fut_details=fut.get("details",[]),patterns=patterns)

def get_analysis(sym,force=False):
    if len(_analysis_cache)>300: _analysis_cache.clear()
    hit=_analysis_cache.get(sym)
    if hit and not force and time.time()-hit[0]<45: return hit[1]
    res=analyze(sym); _analysis_cache[sym]=(time.time(),res); return res

def scan_sync(symbols,workers=SCAN_WORKERS):
    def one(s):
        try: return get_analysis(s)
        except Exception: return None
    with ThreadPoolExecutor(workers) as ex:
        return [r for r in ex.map(one,symbols) if r]

def new_trade(sym,tf,side,lv,bar_t,grade,setup,size_pct=0.05):
    return dict(id=f"{sym}-{tf}-{bar_t}",coin=sym,tf=tf,side=side,grade=grade,setup=setup,
                entry=lv["entry"],sl=lv["sl"],original_sl=lv["sl"],risk=lv["risk"],
                size_pct=size_pct,tps=[dict(t) for t in lv["tps"]],state="open",
                wait=0,fill=lv["entry"],ext=lv["entry"],remaining=1.0,realized=0.0,
                bars=0,be=False,trail_on=False,mfe=0.0,mae=0.0,
                created=int(time.time()*1000),opened=int(time.time()*1000),
                closed=None,next_t=int(bar_t)+TF_MS[tf],R=None,result=None)

def _tighten(side,cur,new): return max(cur,new) if side==1 else min(cur,new)
def _finish(p,ev,kind):
    p["remaining"],p["state"],p["result"]=0.0,"closed",kind
    p["closed"]=int(time.time()*1000)
    cost=2*(FEE+SLIP)*p["fill"]/p["risk"]
    p["R"]=round(p["realized"]-cost,3); ev.append(dict(kind="DONE")); return ev
def close_at(p,price,kind,ev=None):
    ev=[] if ev is None else ev
    p["realized"]+=p["remaining"]*p["side"]*(price-p["fill"])/p["risk"]
    ev.append(dict(kind=kind,px=price)); return _finish(p,ev,kind)

def trade_step(p,o,h,l,c,atr):
    ev,s=[],p["side"]
    if p["state"] not in ("pending","open"): return ev
    p["bars"]+=1; fill,risk=p["fill"],p["risk"]
    fav,adv=(h,l) if s==1 else (l,h)
    p["mfe"]=max(p["mfe"],s*(fav-fill)/risk); p["mae"]=max(p["mae"],s*(fill-adv)/risk)
    if (s==1 and l<=p["sl"]) or (s==-1 and h>=p["sl"]):
        kind="TRAIL" if p["trail_on"] else ("BE" if p["be"] else "SL")
        return close_at(p,p["sl"],kind,ev)
    for j,tp in enumerate(p["tps"]):
        if tp["hit"]: continue
        if not ((s==1 and h>=tp["px"]) or (s==-1 and l<=tp["px"])): break
        tp["hit"]=True
        p["realized"]+=tp["frac"]*s*(tp["px"]-fill)/risk
        p["remaining"]-=tp["frac"]
        ev.append(dict(kind="TP",j=j+1,px=tp["px"],pct=tp["pct"]))
    if p["remaining"]<=1e-9: return _finish(p,ev,"TP")
    if p["tps"][0]["hit"] and not p["be"]:
        p["sl"]=_tighten(s,p["sl"],fill+s*BE_OFFSET_R*risk); p["be"]=True
        ev.append(dict(kind="BE_MOVED",px=p["sl"]))
    p["ext"]=max(p["ext"],h) if s==1 else min(p["ext"],l)
    if sum(t["hit"] for t in p["tps"])>=2:
        p["trail_on"]=True
        if atr and atr>0: p["sl"]=_tighten(s,p["sl"],p["ext"]-s*TRAIL_ATR*atr)
    # حد زمني ليوم التداول (~24 ساعة) بغض النظر عن الفريم
    age_ms = int(time.time()*1000) - int(p.get("opened") or p.get("created") or 0)
    tf_hold = {"15m": 96, "1h": 24, "4h": 6, "1d": 1}.get(p.get("tf"), MAX_HOLD)
    if p["bars"] >= tf_hold or age_ms >= 24 * 3600 * 1000:
        return close_at(p, c, "TIME", ev)
    return ev

def fmt(x):
    x=float(x)
    if x>=1000: return f"{x:,.2f}"
    if x>=1: return f"{x:.4f}".rstrip("0").rstrip(".")
    if x>=0.01: return f"{x:.5f}"
    return f"{x:.8f}"

def smart_cleanup_trades():
    now_ms=int(time.time()*1000); act=active_signals()
    fill_ratio=len(act)/MAX_OPEN if MAX_OPEN>0 else 0
    closed_profit=closed_age=closed_forced=0
    with store.lock:
        for p in list(store.data["signals"]):
            if p.get("state") not in ("pending","open"): continue
            age_days=(now_ms-p.get("created",now_ms))/(DAY*1000)
            cur_r=p.get("realized",0.0)
            if age_days>SMART_MAX_AGE_DAYS:
                p["state"]="closed"; p["result"]="AGE_CLOSE"
                p["R"]=p.get("R") or round(cur_r,3); p["closed"]=now_ms; closed_age+=1; continue
            if age_days>SMART_PROFIT_DAYS and cur_r>2.0:
                p["state"]="closed"; p["result"]="PROFIT_TAKE"
                p["R"]=round(cur_r,3); p["closed"]=now_ms; closed_profit+=1; continue
            if fill_ratio>SMART_FILL_TRIGGER and age_days>SMART_FORCE_DAYS and cur_r<0:
                p["state"]="closed"; p["result"]="SMART_EXIT"
                p["R"]=round(cur_r,3); p["closed"]=now_ms; closed_forced+=1; continue
        if closed_profit+closed_age+closed_forced: store.save("signals")
    return closed_profit+closed_age+closed_forced

def check_signals_sync():
    notes=[]; by={}
    for p in active_signals(): by.setdefault((p["coin"],p["tf"]),[]).append(p)
    for (coin,tf),plist in by.items():
        try:
            raw=get_candles(coin,tf,450,min_bars=60); d=add_indicators(raw,100)
        except Exception: continue
        for p in list(plist):
            try:
                with store.lock:
                    if p.get("state") not in ("pending","open"): continue
                    rows=d[d["t"]>=p["next_t"]]
                    for row in rows.itertuples():
                        if p.get("state") not in ("pending","open"): break
                        ev=trade_step(p,row.open,row.high,row.low,row.close,row.atr)
                        p["next_t"]=int(row.t)+TF_MS[tf]
                        notes+=[(coin,tf,p["side"],e,p.get("R"),p["id"]) for e in ev if e["kind"]!="DONE"]
                        if p["state"] in ("closed","expired"): break
                    store.save("signals")
                if p["state"]=="closed": _archive(p)
            except Exception: continue
    return notes

def _archive(p):
    with store.lock:
        store.data["history"].append(dict(
            coin=p["coin"],tf=p["tf"],side=p["side"],grade=p["grade"],setup=p["setup"],
            R=p["R"],result=p["result"],opened=p["opened"],closed=p["closed"],
            bars=p["bars"],tps=sum(1 for t in p["tps"] if t["hit"]),
            mfe=round(p["mfe"],2),mae=round(p["mae"],2),fill=p.get("fill"),sl=p.get("sl"),
            tps_list=[dict(t) for t in p["tps"]],realized=p.get("realized",0.0)))
        del store.data["history"][:-1500]
        cutoff=time.time()*1000-3*DAY*1000
        store.data["signals"]=[q for q in store.data["signals"]
                               if q.get("state","open") in ("pending","open") or
                                  (q.get("closed") or q.get("created",0))>cutoff]
        store.save("signals","history")


def grade_history_stats(grade=None, limit=500):
    """نسبة نجاح تاريخية من سجل الصفقات المغلقة (للأدمن/الشفافية)."""
    try:
        with store.lock:
            h = list(store.data.get("history") or [])[-limit:]
        if grade:
            h = [x for x in h if x.get("grade") == grade]
        if not h:
            return None
        wins = sum(1 for x in h if float(x.get("R") or 0) > 0)
        n = len(h)
        rate = 100.0 * wins / n if n else 0.0
        return dict(n=n, wins=wins, rate=round(rate, 1), grade=grade)
    except Exception:
        return None


def format_trade_card(p,event_kind=None,event_data=None):
    side = p.get("side", 1)
    side_lbl = "LONG" if side == 1 else "SHORT"
    dot = "🔵" if side == 1 else "🔴"
    coin = p.get("coin") or p.get("sym") or "?"
    lines = [
        f"{dot} <b>{coin}/USDT {side_lbl}</b>",
        "",
        f"{dot} Entry: {fmt(p.get('fill') or p.get('entry') or 0)}",
    ]
    for i, tp in enumerate(p.get("tps") or [], 1):
        check = "✅" if tp.get("hit") else ""
        lines.append(f"🎯 TP{i}: {fmt(tp['px'])}{check}")
    if p.get("be") and p.get("trail_on"):
        lines.append(f"🛑 Trail SL: {fmt(p['sl'])}")
    elif p.get("be"):
        lines.append(f"🛑 BE: {fmt(p['sl'])}")
    else:
        lines.append(f"🛑 SL: {fmt(p['sl'])}")
    tp_hits = sum(1 for t in (p.get("tps") or []) if t.get("hit"))
    lines += ["", f"📊 R: {p.get('realized', 0.0):+.2f} | TP: {tp_hits}/{len(p.get('tps') or [])}"]
    # شفافية تاريخية حسب الدرجة (مفيد للأدمن والمشترك)
    st = grade_history_stats(p.get("grade"))
    if st and st["n"] >= 5:
        lines.append(f"📊 درجة {p.get('grade','?')}: نجاح تاريخي {st['rate']}% من {st['n']} صفقة")
    if p.get("state") == "closed":
        labels = {"TP": "✅ صفقة ناجحة بالكامل", "SL": "🛑 ضرب وقف الخسارة", "BE": "🔒 خروج على التعادل",
                  "TRAIL": "🔁 خروج بالوقف المتحرك", "TIME": "⏱ انتهت المدة", "PROFIT_TAKE": "💰 أخذ الربح الذكي",
                  "SMART_EXIT": "🧹 خروج ذكي", "AGE_CLOSE": "📅 انتهاء العمر"}
        lines += ["", f"{labels.get(p.get('result', ''), '🏁 انتهت')} | {p.get('R', 0):+.2f}R"]
    lines += ["", "📋 تنبيه معلومات — البوت لا ينفّذ صفقات"]
    return "\n".join(lines)


def event_text(coin,tf,side,e,R,tier="free",trade=None):
    k=e["kind"]
    if trade is not None:
        card=format_trade_card(trade,k,e)
        headers={"TP":f"🎯 <b>تحقق الهدف {e['j']}</b>\n\n","SL":"🛑 <b>ضرب وقف الخسارة</b>\n\n",
                 "BE":"🔒 <b>خروج على التعادل</b>\n\n","TRAIL":"🔁 <b>خروج بالوقف المتحرك</b>\n\n",
                 "TIME":"⏱ <b>انتهت المدة</b>\n\n","BE_MOVED":"🔒 <b>نُقل الوقف للتعادل</b>\n\n",
                 "ENTRY":"✅ <b>تفعّل الدخول</b>\n\n"}
        return headers.get(k,"")+card
    return None

def _build_daily_lists(force_side=None):
    """يبني دائماً حتى 2 LONG و 2 SHORT قدر الإمكان (A+/A/B أولاً ثم C كاحتياطي)."""
    gainers, losers = get_top_movers(force=True)
    all_movers = gainers + losers
    top_syms = [x["symbol"] for x in all_movers[:40]]
    if len(top_syms) < 20:
        top_syms.extend(top_symbols(30))
    # أضف أقوى الرابحين/الخاسرين صراحةً لضمان توازن الاتجاه
    top_syms = list(dict.fromkeys(
        [x["symbol"] for x in gainers[:15]] +
        [x["symbol"] for x in losers[:15]] +
        top_syms + top_symbols(40)
    ))
    results = scan_sync(top_syms[:50])
    longs, shorts = [], []

    def _add(pool, r, mv):
        if any(x["res"]["sym"] == r["sym"] for x in pool):
            return
        pool.append(dict(res=r, change=mv.get("change", 0), volume=mv.get("volume", 0)))

    # مرور 1: درجات جيدة
    for r in sorted(results, key=lambda x: -{"A+":3,"A":2,"B":1,"C":0}.get(x.get("grade"), -1)):
        if r.get("grade") not in ("A+", "A", "B"):
            continue
        if r["sym"] in EXCLUDE_TICKERS:
            continue
        mv = next((x for x in all_movers if x["symbol"] == r["sym"]), {"change": 0, "volume": 0})
        if r["side"] == 1 and len(longs) < 2:
            _add(longs, r, mv)
        elif r["side"] == -1 and len(shorts) < 2:
            _add(shorts, r, mv)

    # مرور 2: أي درجة (بما فيها C) لملء الناقص
    if len(longs) < 2 or len(shorts) < 2:
        for r in results:
            if r["sym"] in EXCLUDE_TICKERS:
                continue
            mv = next((x for x in all_movers if x["symbol"] == r["sym"]), {"change": 0, "volume": 0})
            if r["side"] == 1 and len(longs) < 2:
                _add(longs, r, mv)
            elif r["side"] == -1 and len(shorts) < 2:
                _add(shorts, r, mv)

    # مرور 3: من أقوى الرابحين/الخاسرين مع قبول أي اتجاه محلل
    if len(longs) < 2:
        for g in gainers:
            if len(longs) >= 2: break
            if g["symbol"] in EXCLUDE_TICKERS: continue
            if any(x["res"]["sym"] == g["symbol"] for x in longs + shorts): continue
            try:
                r = get_analysis(g["symbol"], force=True)
                if not r or not r.get("lv"): continue
                if r.get("wait") or r.get("side") != 1: continue
                _add(longs, r, g)
            except Exception:
                continue
    if len(shorts) < 2:
        for g in losers:
            if len(shorts) >= 2: break
            if g["symbol"] in EXCLUDE_TICKERS: continue
            if any(x["res"]["sym"] == g["symbol"] for x in longs + shorts): continue
            try:
                r = get_analysis(g["symbol"], force=True)
                if not r or not r.get("lv"): continue
                if r.get("wait") or r.get("side") != -1: continue
                _add(shorts, r, g)
            except Exception:
                continue
    return longs[:2], shorts[:2]

def _format_daily_card(res,side_label,change,volume):
    lv=res["lv"]; decision="🟢 شراء" if res["side"]==1 else "🔴 بيع"
    cl=coin_link(res['sym'],res.get("source","binance_fut"))
    lines=[side_label,"",
           f"📌 <a href='{cl}'>#{res['sym']}/USDT</a> 👆 اضغط هنا",
           f"📡 {display_source(res.get('source','binance_fut'))}",
           f"➡️ Entry: {fmt(lv['entry'])}"]
    for i,tp in enumerate(lv["tps"][:DAILY_TP_COUNT],1):
        lines.append(f"🎯 Target {i}: {fmt(tp['px'])}")
    lines+=[f"🛑 Stop loss: {fmt(lv['sl'])}",""]
    pats=res.get("patterns",[])
    if pats:
        pat_names=" | ".join(
            f"{p['name']}{'[' + p.get('tf','').upper() + ']' if p.get('tf') else ''} {'🟢' if p['bias']=='bullish' else ('🔴' if p['bias']=='bearish' else '⚪')}"
            for p in pats[:2])
        lines.append(f"🕯 النموذج: {pat_names}"); lines.append("")
    lines+=[f"🎯 <b>القرار المتوقع: {decision}</b>",
            f"📊 {'🔺' if change >= 0 else '🔻'} {change:+.1f}% | 💰 ${volume/1_000_000:.1f}M"]
    return "\n".join(lines)

def golden_score(sym,debug=False,relaxed=False):
    try:
        res=get_analysis(sym)
        if not res: return None
        if res.get("forced") or res.get("wait"): return None
        if not relaxed:
            if res["grade"] not in ("A+","A"):
                if debug: return dict(sym=sym,score=0,reasons=[f"الدرجة {res['grade']} غير كافية"])
                return None
        else:
            if res["grade"] not in ("A+","A","B"):
                if debug: return dict(sym=sym,score=0,reasons=[f"الدرجة {res['grade']} غير كافية (relaxed)"])
                return None
        side=res["side"]; price=res["price"]; atr=res["lv"]["atr"]
        pats=res.get("patterns",[])
        # لا ترفض بسبب نموذج معاكس في الوضع relaxed (فقط تحذير داخل النقاط)
        if not relaxed:
            for p in pats:
                if p["bias"]=="bearish" and side==1: return None
                if p["bias"]=="bullish" and side==-1: return None
        cfg=get_golden_config()
        df=res["df"]; last=df.iloc[-1]
        rsi_v=float(res["rsi"])
        rvol=float(last["rvol"]) if not pd.isna(last["rvol"]) else 0.0
        hi10=float(df["highest_10"].iloc[-2]); lo10=float(df["lowest_10"].iloc[-2])
        mtf_c,mtf_t=mtf_score(sym,side)
        checks={"require_1d_trend":res["trend_d"]==("up" if side==1 else "down"),
                "require_mtf":mtf_t>0 and mtf_c==mtf_t,
                "require_rsi":(40<=rsi_v<=70) if side==1 else (30<=rsi_v<=60),
                "require_volume":rvol>=1.2,
                "require_breakout":(float(last["close"])>hi10) if side==1 else (float(last["close"])<lo10),
                "require_liquidity":check_liquidity(sym,1_000_000),
                "require_alpha":sym in ALPHA_COINS}
        failed=[k for k,v in checks.items() if cfg.get(k) and not v]
        if failed:
            if debug: return dict(sym=sym,score=0,reasons=[f"شروط غير متحققة: {', '.join(failed)}"])
            return None
        score=(3 if res["grade"]=="A+" else 2)+sum(1 for v in checks.values() if v)
        if score<cfg.get("min_score",1):
            if debug: return dict(sym=sym,score=score,reasons=["النقاط أقل من الحد الأدنى"])
            return None
        reasons=[f"الدرجة: {res['grade']}",f"اتجاه: {'صاعد' if side==1 else 'هابط'}"]
        reasons+=[f"✅ {k.replace('require_','')}" for k,v in checks.items() if v]
        for p in pats[:2]:
            emoji="🟢" if p["bias"]=="bullish" else ("🔴" if p["bias"]=="bearish" else "⚪")
            tf_lbl=f"[{p.get('tf','').upper()}]" if p.get("tf") else ""
            reasons.append(f"{emoji} نموذج {p['name']} {tf_lbl}")
        sl=res["lv"]["sl"]; tps=[t["px"] for t in res["lv"]["tps"]]
        return dict(sym=sym,side=side,score=score,reasons=reasons,
            price=price,atr=atr,sl=sl,tps=tps,
            risk=abs(price-sl),trend_1d=res["trend_d"],trend_4h=res["trend"],
            change_7d=0,rsi=rsi_v,rvol=rvol,source=res.get("source","binance_fut"),
            patterns=pats)
    except (PairNotFound, NoData) as e:
        return None
    except Exception as e:
        log.debug(f"golden_score err {sym}: {e}")
        return None

def _run_golden_scan(syms,max_results,progress_cb,relaxed=False):
    results=[]; total=len(syms)
    def scan_one(sym):
        try:
            time.sleep(0.05)
            return golden_score(sym,debug=False,relaxed=relaxed)
        except Exception: return None
    processed=0
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        futures={executor.submit(scan_one,s):s for s in syms}
        for future in concurrent.futures.as_completed(futures):
            processed+=1
            res=future.result()
            if res: results.append(res)
            if progress_cb and processed%50==0:
                try: progress_cb(processed,total,len(results))
                except Exception as _ex: log.debug('ignored: %s', _ex)
    results.sort(key=lambda x:-x["score"])
    return results[:max_results]

def golden_scan(max_results=2,progress_cb=None):
    try:
        rows=binance_futures_all_tickers()
        if rows:
            rows_sorted=sorted(rows,key=lambda x:-x["volume"])
            syms=[x["symbol"] for x in rows_sorted if x["volume"]>2_000_000 and x["symbol"] not in EXCLUDE_TICKERS][:150]
        else: syms=top_symbols(600,binance_only=True)
    except Exception: syms=top_symbols(600,binance_only=True)
    if not syms: return []
    results=_run_golden_scan(syms,max_results,progress_cb,relaxed=False)
    if not results:
        log.info("golden_scan: no strict results, trying relaxed mode on top 200")
        results=_run_golden_scan(syms[:150],max_results,progress_cb,relaxed=True)
    return results

def format_golden_post(g,tier="vip"):
    side_word="🟢 شراء (LONG)" if g["side"]==1 else "🔴 بيع (SHORT)"
    decision="🟢 شراء" if g["side"]==1 else "🔴 بيع"
    cl=coin_link(g['sym'],g.get("source","binance_fut"))
    lines=["🏆✨ <b>الفرصة الذهبية</b> ✨🏆","━━━━━━━━━━━━━━━━━━━━━",
        f"📌 <a href='{cl}'>#{g['sym']}/USDT</a> 👆 اضغط هنا",
        f"📡 المصدر: <b>{display_source(g.get('source','binance_fut'))}</b>",
        f"🏆 التقييم: <b>{g['score']} نقطة</b>","━━━━━━━━━━━━━━━━━━━━━","",
        f"🎯 الاتجاه: <b>{side_word}</b>",
        f"💵 الدخول: <code>{fmt(g['price'])}</code>",
        f"🛑 وقف الخسارة: <code>{fmt(g['sl'])}</code>","",
        "🎯 <b>الأهداف (الأسعار):</b>"]
    for i,tp in enumerate(g["tps"],1):
        emoji=["🥇","🥈","🥉","🏅"][i-1]
        lines.append(f"{emoji} هدف {i}: <code>{fmt(tp)}</code>")
    lines+=["","━━━━━━━━━━━━━━━━━━━━━",f"🎯 <b>القرار المتوقع: {decision}</b>",
            "","🔍 <b>لماذا الفرصة الذهبية؟</b>"]
    for r in g["reasons"][:8]: lines.append(f"  • {r}")
    if g.get("patterns"):
        lines.append(""); lines.append("🕯 <b>النماذج الفنية:</b>")
        for p in g["patterns"][:3]:
            emoji="🟢" if p["bias"]=="bullish" else ("🔴" if p["bias"]=="bearish" else "⚪")
            tf_lbl=f" [{p.get('tf','').upper()}]" if p.get('tf') else ""
            lines.append(f"  {emoji} {p['name']}{tf_lbl}")
    lines+=["","━━━━━━━━━━━━━━━━━━━━━",RISK_WARNING_AR,f"👤 {BRAND}"]
    return "\n".join(lines)

def build_whale_tweet(sym,side,notional,price):
    side_emoji="🟢 شراء" if side=="buy" else "🔴 بيع"
    name,ref=referral_for("binance_fut")
    text=(f"📢 صفقة كبيرة مرصودة على Binance\n\n"
          f"💰 صفقة {side_emoji} ضخمة على #{sym}/USDT\n"
          f"💵 بقيمة: ${notional:,.0f}\n📊 السعر: {fmt(price)}\n\n"
          f"⚡️ الصفقات الكبيرة قد تؤثر على السعر\n"
          f"🔗 للتداول على المنصة:\n{ref}\n\n👤 Ryma Crypto")
    share_url=f"https://twitter.com/intent/tweet?text={urllib.parse.quote(text)}"
    return text,share_url

def build_social_text(res,platform="binance"):
    side=res["side"]; lv=res["lv"]; sym=res["sym"]
    side_word="LONG 🟢" if side==1 else "SHORT 🔴"
    meta=PLATFORM_META.get(platform,PLATFORM_META["binance"])
    emoji=meta["emoji"]; trade_link=coin_link(sym,"binance_fut")
    tag=meta.get("tags","#Binance")
    hashtags=f"#{sym}USDT {tag} #CryptoTrading"
    return (f"{emoji} {sym}/USDT — {side_word}\n\n"
            f"💵 Entry: {fmt(lv['entry'])}\n"
            f"🎯 TP1: {fmt(lv['tps'][0]['px'])}\n"
            f"🎯 TP2: {fmt(lv['tps'][1]['px'])}\n"
            f"🎯 TP3: {fmt(lv['tps'][2]['px'])}\n"
            f"🛑 SL: {fmt(lv['sl'])}\n\n"
            f"👉 {trade_link}\n\n"
            f"{hashtags}")

def build_copy_post(res):
    side,lv,sym=res["side"],res["lv"],res["sym"]
    side_word="LONG" if side==1 else "SHORT"
    dot="🔵" if side==1 else "🔴"
    lines=[f"{dot} {sym}/USDT {side_word}","",f"{dot} Entry: {fmt(lv['entry'])}"]
    for i,tp in enumerate(lv["tps"][:4],1):
        lines.append(f"🎯 TP{i}: {fmt(tp['px'])}")
    lines.append(f"🛑 SL: {fmt(lv['sl'])}")
    grade=res.get("grade","?")
    lines+=["",f"🏆 الدرجة: {grade}"]
    st=grade_history_stats(grade)
    if st and st["n"]>=5:
        lines.append(f"📊 درجة {grade}: نجاح تاريخي {st['rate']}% من {st['n']} صفقة")
    return chr(10).join(lines)


async def send_admin_copy(msg_obj,res):
    try:
        await msg_obj.reply_text(build_copy_post(res)+"\n\n"+admin_stats_line(res),
                                  parse_mode=None,link_preview_options=NO_PREVIEW)
    except Exception as e: log.warning(f"send_admin_copy: {e}")

def keyboard_share_post(res):
    trade_meta=[("binance","🟡","Binance Square"),("bybit","🔵","Bybit"),("okx","⚫","OKX"),
                ("bitget","🟢","Bitget"),("kucoin","🟢","KuCoin"),("gateio","⚪","Gate"),
                ("twitter","🐦","Twitter (X)"),("telegram","📱","Telegram"),("facebook","📘","Facebook"),
                ("whatsapp","💬","WhatsApp"),("reddit","🔴","Reddit"),("discord","🎮","Discord")]
    trade_rows=[]; general_text=build_copy_post(res)
    trade_rows.append([_btn("📋 نسخ الصفقة",copy_text=CopyTextButton(text=general_text[:250]))])
    for key,emoji,name in trade_meta:
        short_text=build_social_text(res,key)
        trade_rows.append([_btn(f"📋 نسخ لـ {emoji} {name}",copy_text=CopyTextButton(text=short_text[:250]))])
    return InlineKeyboardMarkup(trade_rows+[[_btn("❌ إغلاق",callback_data="close_menu")]])

async def notify_both(bot,text,protect=True):
    for chat in (VIP_CHANNEL_ID,ADMIN_ID or None):
        if not chat: continue
        try:
            await bot.send_message(chat,text,parse_mode=ParseMode.HTML,
                                   protect_content=PROTECT if chat!=ADMIN_ID else False,
                                   link_preview_options=NO_PREVIEW)
        except Exception as e: log.warning(f"notify_both {chat}: {str(e)[:80]}")

async def notify_free(bot,text):
    if CHANNEL_ID:
        try:
            await bot.send_message(CHANNEL_ID,text,parse_mode=ParseMode.HTML,
                                   protect_content=PROTECT,link_preview_options=NO_PREVIEW)
        except Exception as e: log.warning(f"notify_free: {str(e)[:80]}")
def _new_fig(w=12,h=8,bg="#ffffff"):
    fig=Figure(figsize=(w,h),facecolor=bg,dpi=200)
    FigureCanvasAgg(fig); return fig
def _to_png(fig):
    buf=io.BytesIO(); fig.savefig(buf,format="png",dpi=200,facecolor=fig.get_facecolor())
    buf.seek(0); return buf
def _draw_candles(ax,df,up,down,width=0.62):
    x=np.arange(len(df))
    o=df["open"].values.astype(float); h=df["high"].values.astype(float)
    l=df["low"].values.astype(float); c=df["close"].values.astype(float)
    colors=np.where(c>=o,up,down)
    ax.vlines(x,l,h,colors=colors,linewidth=1.1,zorder=2)
    body=np.maximum(np.abs(c-o),(h.max()-l.min())*0.0008)
    ax.bar(x,body,bottom=np.minimum(o,c),color=colors,width=width,linewidth=0,zorder=3)
    return x
def _style_axes(axes,bg,fg,grid):
    for a in axes:
        a.set_facecolor(bg); a.tick_params(colors=fg,labelsize=9)
        a.grid(color=grid,lw=0.6,alpha=0.8)
        for sp in a.spines.values(): sp.set_color(grid)
        a.yaxis.tick_right()
def _time_ticks(ax,df,n_ticks=7):
    ts=pd.to_datetime(df["t"],unit="ms")
    step=max(len(df)//n_ticks,1); idx=list(range(0,len(df),step))
    daily=ts.diff().dropna().median()>=pd.Timedelta(hours=24)
    fmt_="%Y-%m-%d" if daily else "%m-%d %H:%M"
    ax.set_xticks(idx)
    ax.set_xticklabels([ts.iloc[i].strftime(fmt_) for i in idx],fontsize=9)
def _right_labels(ax,levels,n,ylo,yhi,fs=9):
    gap=(yhi-ylo)*0.042
    items=sorted([dict(y=px,text=f"{name}  {fmt(px)}",col=col) for name,px,col,_ls in levels],key=lambda z:z["y"])
    for i in range(1,len(items)):
        if items[i]["y"]-items[i-1]["y"]<gap: items[i]["y"]=items[i-1]["y"]+gap
    for it in items:
        ax.text(n+0.8,it["y"]," "+it["text"]+" ",color="white",fontsize=fs,fontweight="bold",
                va="center",ha="left",clip_on=False,zorder=9,
                bbox=dict(boxstyle="round,pad=0.3",fc=it["col"],ec="none"))
def _x_index(t_arr,t_ms): return float(np.clip(np.searchsorted(t_arr,t_ms),0,len(t_arr)-1))

# ======================= شارت TradingView الاحترافي (نماذج + خطوط مرسومة) =======================
TV_BARS=120
TV_BG="#131722"; TV_GRID="#1c2230"; TV_TXT="#b2b5be"; TV_UP="#26a69a"; TV_DN="#ef5350"
TV_COL={"bullish":"#2ebd85","bearish":"#f6465d","neutral":"#f0b90b"}
TV_IV={"15m":"15","1h":"60","4h":"240","1d":"D"}

def _tv_pivots(df,w=4):
    H=df["high"].to_numpy(float); L=df["low"].to_numpy(float); n=len(df); hs=[]; ls=[]
    for k in range(w,n-w):
        if H[k]>=H[k-w:k+w+1].max(): hs.append((k,float(H[k])))
        if L[k]<=L[k-w:k+w+1].min(): ls.append((k,float(L[k])))
    def dedupe(p,keep_max):
        out=[]
        for k,v in p:
            if out and k-out[-1][0]<w:
                if (v>out[-1][1])==keep_max: out[-1]=(k,v)
            else: out.append((k,v))
        return out
    return dedupe(hs,True),dedupe(ls,False)

def _tv_classify(mh,ml,conv,L,pole,prior):
    """mh/ml: ميل الخطين بوحدات ATR على كامل النافذة. conv: عرض النهاية/عرض البداية."""
    flat,_ = 1.2,0
    h_flat=abs(mh)<flat; l_flat=abs(ml)<flat
    h_up=mh>=flat; h_dn=mh<=-flat; l_up=ml>=flat; l_dn=ml<=-flat
    pb="bullish" if prior>0 else ("bearish" if prior<0 else "neutral")
    if conv<0.78:
        if L<=35 and conv<0.65 and abs(pole)>=5: return "Pennant","bullish" if pole>0 else "bearish"
        if h_flat and l_up: return "Ascending Triangle","bullish"
        if l_flat and h_dn: return "Descending Triangle","bearish"
        if h_dn and l_up: return "Symmetrical Triangle",pb
        if h_up and l_up: return "Rising Wedge","bearish"
        if h_dn and l_dn: return "Falling Wedge","bullish"
        return None
    if conv<=1.35:
        if abs(pole)>=5 and L<=45:
            if pole>0 and mh<=0.5 and ml<=0.5: return "Bull Flag","bullish"
            if pole<0 and mh>=-0.5 and ml>=-0.5: return "Bear Flag","bearish"
        if h_flat and l_flat: return "Rectangle",pb
        if h_up and l_up: return "Ascending Channel","bullish"
        if h_dn and l_dn: return "Descending Channel","bearish"
    return None

def _tv_channel_patterns(df,hs,ls):
    """مثلثات/أوتاد/أعلام/قنوات/مستطيل: خطوط مرسومة على القمم والقيعان الفعلية."""
    H=df["high"].to_numpy(float); L=df["low"].to_numpy(float); C=df["close"].to_numpy(float)
    n=len(df); atr=float(df["atr"].iloc[-1]) if "atr" in df and not pd.isna(df["atr"].iloc[-1]) else float(np.mean(H-L))
    if atr<=0: return []
    e=n-1; best=None
    for Lw in (25,35,50,70,95):
        s=max(0,e-Lw)
        if e-s<20: continue
        ph=[(k,v) for k,v in hs if s<=k<=e]; pl=[(k,v) for k,v in ls if s<=k<=e]
        if len(ph)<2 or len(pl)<2: continue
        xh=np.array([k for k,_ in ph],float); yh=np.array([v for _,v in ph])
        xl=np.array([k for k,_ in pl],float); yl=np.array([v for _,v in pl])
        mh=float(np.polyfit(xh,yh,1)[0]); ml=float(np.polyfit(xl,yl,1)[0])
        bh=float(np.max(yh-mh*xh)); bl=float(np.min(yl-ml*xl))
        up=lambda x:mh*x+bh; lo=lambda x:ml*x+bl
        span=e-s; w0=up(s)-lo(s); w1=up(e)-lo(e)
        if w0<=0.5*atr or w1<=0.3*atr: continue
        th=sum(1 for k,v in ph if abs(v-up(k))<=0.45*atr); tl=sum(1 for k,v in pl if abs(v-lo(k))<=0.45*atr)
        if th<2 or tl<2: continue
        xs=np.arange(s,e+1)
        out_frac=float(np.mean((C[s:e+1]>up(xs)+0.5*atr)|(C[s:e+1]<lo(xs)-0.5*atr)))
        if out_frac>0.10: continue
        pole=(C[s]-C[max(0,s-12)])/atr; prior=C[s]-C[max(0,s-30)]
        prior=1 if prior>2*atr else (-1 if prior<-2*atr else 0)
        cl=_tv_classify(mh*span/atr,ml*span/atr,w1/w0,span,pole,prior)
        if not cl: continue
        name,bias=cl; score=th+tl+min(span,60)/30
        if best is None or score>best["score"]:
            best=dict(score=score,s=s,e=e,up=up,lo=lo,name=name,bias=bias,w0=w0,pole=abs(pole)*atr,th=ph,tl=pl)
    if best is None:
        return []
    s,e,up,lo,name,bias=best["s"],best["e"],best["up"],best["lo"],best["name"],best["bias"]
    ext=10 if "Channel" not in name and name!="Rectangle" else 8
    c_last=C[-1]
    status="forming"; target=None
    if c_last>up(e)+0.1*atr: status="breakout ↑"
    elif c_last<lo(e)-0.1*atr: status="breakout ↓"
    if name not in ("Ascending Channel","Descending Channel","Rectangle"):
        h=best["pole"] if name in ("Bull Flag","Bear Flag","Pennant") and best["pole"]>0 else best["w0"]
        if bias=="bullish" or status=="breakout ↑": target=up(e)+h
        elif bias=="bearish" or status=="breakout ↓": target=lo(e)-h
        else: target=None
    lines=[dict(i1=s,p1=up(s),i2=e+ext,p2=up(e+ext),role="upper"),
           dict(i1=s,p1=lo(s),i2=e+ext,p2=lo(e+ext),role="lower")]
    poly=[(s,up(s)),(e,up(e)),(e,lo(e)),(s,lo(s))]
    marks=[(k,v,"") for k,v in best["th"]+best["tl"]]
    return [dict(name=name,bias=bias,status=status,lines=lines,poly=poly,marks=marks,
                 target=target,anchor=(s,up(s)),tf_span=e-s,kind="channel",e=e)]

def _tv_pivot_patterns(df,hs,ls):
    """قمة/قاع مزدوج وثلاثي + رأس وكتفين (ومقلوب) مع خط العنق."""
    H=df["high"].to_numpy(float); L=df["low"].to_numpy(float); C=df["close"].to_numpy(float); n=len(df)
    out=[]; last=C[-1]
    def near(a,b,tol): return abs(a-b)/max(a,b)*100<=tol
    def add(name,bias,conf,lines,marks,target,anchor,e):
        out.append(dict(name=name,bias=bias,status="confirmed" if conf else "forming",lines=lines,poly=None,
                        marks=marks,target=target,anchor=anchor,kind="pivot",e=e))
    # H&S
    if len(hs)>=3 and n-hs[-1][0]<=45 and len(ls)>=2:
        a,b,c=hs[-3],hs[-2],hs[-1]
        if b[1]>a[1] and b[1]>c[1] and near(a[1],c[1],4.0) and c[0]-a[0]>=14:
            t1=[x for x in ls if a[0]<x[0]<b[0]]; t2=[x for x in ls if b[0]<x[0]<c[0]]
            if t1 and t2:
                n1=min(t1,key=lambda x:x[1]); n2=min(t2,key=lambda x:x[1])
                m=(n2[1]-n1[1])/(n2[0]-n1[0]); neck=lambda x:n1[1]+m*(x-n1[0])
                conf=last<neck(n-1)
                add("Head & Shoulders","bearish",conf,
                    [dict(i1=a[0],p1=a[1],i2=b[0],p2=b[1],role="pattern"),dict(i1=b[0],p1=b[1],i2=c[0],p2=c[1],role="pattern"),
                     dict(i1=n1[0],p1=n1[1],i2=n-1+8,p2=neck(n-1+8),role="neck")],
                    [(a[0],a[1],"LS"),(b[0],b[1],"H"),(c[0],c[1],"RS"),(n1[0],n1[1],""),(n2[0],n2[1],"")],
                    neck(c[0])-(b[1]-neck(b[0])),(a[0],a[1]),n-1)
    if len(ls)>=3 and n-ls[-1][0]<=45 and len(hs)>=2:
        a,b,c=ls[-3],ls[-2],ls[-1]
        if b[1]<a[1] and b[1]<c[1] and near(a[1],c[1],4.0) and c[0]-a[0]>=14:
            t1=[x for x in hs if a[0]<x[0]<b[0]]; t2=[x for x in hs if b[0]<x[0]<c[0]]
            if t1 and t2:
                n1=max(t1,key=lambda x:x[1]); n2=max(t2,key=lambda x:x[1])
                m=(n2[1]-n1[1])/(n2[0]-n1[0]); neck=lambda x:n1[1]+m*(x-n1[0])
                conf=last>neck(n-1)
                add("Inverse Head & Shoulders","bullish",conf,
                    [dict(i1=a[0],p1=a[1],i2=b[0],p2=b[1],role="pattern"),dict(i1=b[0],p1=b[1],i2=c[0],p2=c[1],role="pattern"),
                     dict(i1=n1[0],p1=n1[1],i2=n-1+8,p2=neck(n-1+8),role="neck")],
                    [(a[0],a[1],"LS"),(b[0],b[1],"H"),(c[0],c[1],"RS"),(n1[0],n1[1],""),(n2[0],n2[1],"")],
                    neck(c[0])+(neck(b[0])-b[1]),(a[0],a[1]),n-1)
    # Triple / Double top
    if len(hs)>=3 and n-hs[-1][0]<=40 and near(hs[-1][1],hs[-2][1],1.6) and near(hs[-2][1],hs[-3][1],1.6) \
       and hs[-1][0]-hs[-3][0]>=12:
        a,b,c=hs[-3],hs[-2],hs[-1]; neck=float(L[a[0]:c[0]+1].min()); conf=last<neck
        top=(a[1]+b[1]+c[1])/3
        add("Triple Top","bearish",conf,
            [dict(i1=a[0],p1=top,i2=n-1+8,p2=top,role="pattern"),dict(i1=a[0],p1=neck,i2=n-1+8,p2=neck,role="neck")],
            [(a[0],a[1],"1"),(b[0],b[1],"2"),(c[0],c[1],"3")],neck-(top-neck),(a[0],top),n-1)
    elif len(hs)>=2 and n-hs[-1][0]<=40 and near(hs[-1][1],hs[-2][1],1.5) and hs[-1][0]-hs[-2][0]>=6:
        a,b=hs[-2],hs[-1]; neck=float(L[a[0]:b[0]+1].min()); conf=last<neck; top=(a[1]+b[1])/2
        add("Double Top","bearish",conf,
            [dict(i1=a[0],p1=top,i2=b[0]+6,p2=top,role="pattern"),dict(i1=a[0],p1=neck,i2=n-1+8,p2=neck,role="neck")],
            [(a[0],a[1],"1"),(b[0],b[1],"2")],neck-(top-neck),(a[0],top),n-1)
    if len(ls)>=3 and n-ls[-1][0]<=40 and near(ls[-1][1],ls[-2][1],1.6) and near(ls[-2][1],ls[-3][1],1.6) \
       and ls[-1][0]-ls[-3][0]>=12:
        a,b,c=ls[-3],ls[-2],ls[-1]; neck=float(H[a[0]:c[0]+1].max()); conf=last>neck
        bot=(a[1]+b[1]+c[1])/3
        add("Triple Bottom","bullish",conf,
            [dict(i1=a[0],p1=bot,i2=n-1+8,p2=bot,role="pattern"),dict(i1=a[0],p1=neck,i2=n-1+8,p2=neck,role="neck")],
            [(a[0],a[1],"1"),(b[0],b[1],"2"),(c[0],c[1],"3")],neck+(neck-bot),(a[0],bot),n-1)
    elif len(ls)>=2 and n-ls[-1][0]<=40 and near(ls[-1][1],ls[-2][1],1.5) and ls[-1][0]-ls[-2][0]>=6:
        a,b=ls[-2],ls[-1]; neck=float(H[a[0]:b[0]+1].max()); conf=last>neck; bot=(a[1]+b[1])/2
        add("Double Bottom","bullish",conf,
            [dict(i1=a[0],p1=bot,i2=b[0]+6,p2=bot,role="pattern"),dict(i1=a[0],p1=neck,i2=n-1+8,p2=neck,role="neck")],
            [(a[0],a[1],"1"),(b[0],b[1],"2")],neck+(neck-bot),(a[0],bot),n-1)
    return out

def tv_detect(df):
    """يرجع قائمة نماذج جاهزة للرسم (بإحداثيات رقم الشمعة داخل df). لا يرجع قائمة فارغة أبداً:
    إذا لم يوجد نموذج يرسم قناة الاتجاه (Trend Channel) كي يكون هناك دائماً خطوط واضحة."""
    df=df.reset_index(drop=True)
    hs,ls=_tv_pivots(df,4)
    pats=_tv_channel_patterns(df,hs,ls)+_tv_pivot_patterns(df,hs,ls)
    if not pats:
        n=len(df); k=min(60,n); x=np.arange(n-k,n,dtype=float); C=df["close"].to_numpy(float)[n-k:]
        m,b=np.polyfit(x,C,1); res_=C-(m*x+b); sd=float(res_.std()) or 1e-9
        top=float(np.max(df["high"].to_numpy(float)[n-k:]-(m*x+b))); bot=float(np.min(df["low"].to_numpy(float)[n-k:]-(m*x+b)))
        atr=float(df["atr"].iloc[-1]) if "atr" in df and not pd.isna(df["atr"].iloc[-1]) else sd
        slope=m*k/atr
        name="Ascending Channel" if slope>1.5 else ("Descending Channel" if slope<-1.5 else "Range / Sideways")
        bias="bullish" if slope>1.5 else ("bearish" if slope<-1.5 else "neutral")
        s,e=n-k,n-1; up=lambda i:m*i+b+top; lo=lambda i:m*i+b+bot
        pats=[dict(name="Trend "+name if "Channel" in name else name,bias=bias,status="forming",
                   lines=[dict(i1=s,p1=up(s),i2=e+8,p2=up(e+8),role="upper"),dict(i1=s,p1=lo(s),i2=e+8,p2=lo(e+8),role="lower")],
                   poly=[(s,up(s)),(e,up(e)),(e,lo(e)),(s,lo(s))],marks=[],target=None,anchor=(s,up(s)),kind="channel",e=e)]
    return pats[:3]

def render_tv_chart(df,sym,tf,lv=None,side=0,sr_high=(),sr_low=(),src="Binance Futures"):
    """شارت بنمط TradingView الداكن: شموع + خطوط النماذج مرسومة كأنها يدوية + أداة الصفقة (Long/Short position)."""
    d=df.tail(TV_BARS).reset_index(drop=True).copy()
    c_=d["close"]; d["ema20"]=c_.ewm(span=20,adjust=False).mean(); d["ema50"]=c_.ewm(span=50,adjust=False).mean()
    if "atr" not in d or d["atr"].isna().all():
        pc=c_.shift(); tr=pd.concat([d["high"]-d["low"],(d["high"]-pc).abs(),(d["low"]-pc).abs()],axis=1).max(axis=1)
        d["atr"]=tr.ewm(alpha=1/14,adjust=False).mean()
    n=len(d); pats=tv_detect(d)
    fig=_new_fig(14,9,TV_BG)
    ax=fig.add_axes([0.035,0.24,0.875,0.66]); axv=fig.add_axes([0.035,0.075,0.875,0.15],sharex=ax)
    _draw_candles(ax,d,TV_UP,TV_DN,0.7)
    o=d["open"].to_numpy(float); cl=d["close"].to_numpy(float); v=d["volume"].to_numpy(float)
    axv.bar(np.arange(n),v,color=np.where(cl>=o,TV_UP,TV_DN),alpha=0.55,width=0.7,linewidth=0)
    ax.plot(np.arange(n),d["ema20"],color="#f7a600",lw=1.8,alpha=0.9,zorder=4)
    ax.plot(np.arange(n),d["ema50"],color="#2962ff",lw=1.8,alpha=0.9,zorder=4)
    if "vwap" in d:
        ax.plot(np.arange(n),d["vwap"],color="#ff6d00",lw=1.5,ls="--",alpha=0.9,zorder=4)
    ys=[float(d["low"].min()),float(d["high"].max())]
    for lvl in list(sr_high)[:2]:
        ax.axhline(lvl,color="#787b86",lw=1.0,ls=(0,(6,4)),alpha=0.8,zorder=2)
    for lvl in list(sr_low)[:2]:
        ax.axhline(lvl,color="#787b86",lw=1.0,ls=(0,(6,4)),alpha=0.8,zorder=2)
    shadow=[pe.withStroke(linewidth=6,foreground="black",alpha=0.25),pe.Normal()]
    for pi,p in enumerate(pats):
        col=TV_COL.get(p["bias"],"#f0b90b")
        if p.get("poly"):
            ax.add_patch(Polygon(p["poly"],closed=True,facecolor=col,alpha=0.12,edgecolor="none",zorder=3))
        for ln in p["lines"]:
            ls_="-" if ln["role"] in ("upper","lower","pattern") else (0,(8,4))
            lw=3.0 if ln["role"]!="neck" else 2.0
            ax.plot([ln["i1"],ln["i2"]],[ln["p1"],ln["p2"]],color=col if ln["role"]!="neck" else "#f0b90b",
                    lw=lw,ls=ls_,solid_capstyle="round",zorder=6,path_effects=shadow)
            ys+= [ln["p1"],ln["p2"]]
        for (i,pv,lab) in p["marks"]:
            ax.scatter([i],[pv],s=46,color=TV_BG,edgecolor=col,linewidth=2.0,zorder=8)
            if lab and pi==0:
                ax.annotate(lab,(i,pv),xytext=(0,12 if p["bias"]!="bullish" else -16),textcoords="offset points",
                            ha="center",fontsize=10,fontweight="bold",color="white",zorder=9,
                            bbox=dict(boxstyle="round,pad=0.25",fc=col,ec="none"))
        if p.get("target") and pi==0:
            tgt=float(p["target"]); e=p["e"]; ys.append(tgt)
            ax.annotate("",xy=(e+11,tgt),xytext=(e,cl[-1]),zorder=7,
                        arrowprops=dict(arrowstyle="-|>",color=col,lw=2.0,ls=(0,(5,3)),shrinkA=0,shrinkB=0))
            ax.text(e+11.4,tgt,f" TARGET {fmt(tgt)} ",color="white",fontsize=9,fontweight="bold",va="center",ha="left",
                    zorder=9,bbox=dict(boxstyle="round,pad=0.3",fc=col,ec="none"),clip_on=False)
        ax_,ay_=p["anchor"]
        arrow="▲" if p["bias"]=="bullish" else ("▼" if p["bias"]=="bearish" else "◆")
        ax.text(ax_+0.5,ay_,f" {p['name']}  {arrow} {p['bias'].capitalize()}  ·  {p['status']} ",color="white",
                fontsize=11 if pi==0 else 9.5,fontweight="bold",va="bottom" if pi==0 else "top",ha="left",zorder=10,
                bbox=dict(boxstyle="round,pad=0.35",fc=col,ec="none",alpha=0.95))
    levels=[]
    if lv:
        e0,sl=lv["entry"],lv["sl"]; tps=[t["px"] for t in lv["tps"]]
        gcol,rcol=TV_UP,TV_DN
        zx=n-1
        ax.add_patch(Rectangle((zx,min(e0,tps[0])),12,abs(tps[0]-e0),facecolor=gcol,alpha=0.16,edgecolor="none",zorder=2))
        ax.add_patch(Rectangle((zx,min(e0,sl)),12,abs(sl-e0),facecolor=rcol,alpha=0.16,edgecolor="none",zorder=2))
        levels=[("ENTRY",e0,"#2962ff","-."),("STOP",sl,rcol,"--")]+[(f"TP{j}",px,gcol,"--") for j,px in enumerate(tps[:4],1)]
        for name,px,col,ls_ in levels:
            ax.plot([zx-6,zx+12],[px,px],color=col,lw=1.4,ls=ls_,zorder=5); ys.append(px)
    lo_,hi_=min(ys),max(ys)
    base_lo,base_hi=float(d["low"].min()),float(d["high"].max()); rng=base_hi-base_lo
    lo_=max(lo_,base_lo-0.9*rng); hi_=min(hi_,base_hi+0.9*rng)
    pad=(hi_-lo_)*0.05; ylo,yhi=lo_-pad,hi_+pad
    ax.set_ylim(ylo,yhi); ax.set_xlim(-1,n+16)
    if levels: _right_labels(ax,levels,n+12,ylo,yhi,fs=8.5)
    last=float(cl[-1]); up=cl[-1]>=o[-1]
    ax.text(n+0.3,last,f" {fmt(last)} ",color="white",fontsize=9.5,fontweight="bold",va="center",ha="left",clip_on=False,zorder=11,
            bbox=dict(boxstyle="square,pad=0.25",fc=TV_UP if up else TV_DN,ec="none"))
    ax.axhline(last,color=TV_UP if up else TV_DN,lw=0.8,ls=(0,(2,3)),alpha=0.8,zorder=1)
    _style_axes([ax,axv],TV_BG,TV_TXT,TV_GRID)
    for a in (ax,axv):
        for sp in a.spines.values(): sp.set_visible(False)
        a.grid(color=TV_GRID,lw=0.7,alpha=1)
    ax.tick_params(labelbottom=False); axv.set_ylim(0,float(v.max())*1.15 if v.max()>0 else 1)
    axv.tick_params(labelleft=False,labelright=False); _time_ticks(axv,d)
    chg=(cl[-1]/o[0]-1)*100
    fig.text(0.04,0.955,f"{sym}/USDT · {tf.upper()} · {src}",fontsize=16,fontweight="bold",color="white",ha="left")
    fig.text(0.04,0.925,f"O {fmt(o[-1])}   H {fmt(d['high'].iloc[-1])}   L {fmt(d['low'].iloc[-1])}   C {fmt(cl[-1])}   {chg:+.2f}%",
             fontsize=10.5,color=TV_UP if chg>=0 else TV_DN,ha="left")
    fig.text(0.62,0.925,"━ EMA20",fontsize=10,color="#f7a600",ha="left"); fig.text(0.70,0.925,"━ EMA50",fontsize=10,color="#2962ff",ha="left"); fig.text(0.78,0.925,"╌ VWAP",fontsize=10,color="#ff6d00",ha="left")
    fig.text(0.5,0.5,"Ryma Crypto",fontsize=70,color="white",alpha=0.03,ha="center",va="center",rotation=18,fontweight="bold")
    fig.text(0.04,0.012,"Auto-drawn patterns · Not financial advice",fontsize=9,color="#787b86",ha="left")
    fig.text(0.965,0.012,"Ryma Crypto",fontsize=11,color="#c9a227",ha="right",fontweight="bold")
    return _to_png(fig),pats

def tv_caption(sym,tf,pats,lang="ar"):
    ico={"bullish":"🟢","bearish":"🔴","neutral":"🟡"}
    if lang=="en":
        lines=[f"📈 <b>#{sym}/USDT · {tf.upper()} — TradingView style</b>",""]
        lines+=[f"{ico.get(p['bias'],'⚪')} <b>{p['name']}</b> — {p['bias']} ({p['status']})"
                +(f" | 🎯 {fmt(p['target'])}" if p.get('target') else "") for p in pats]
        lines+=["","ℹ️ Lines are auto-drawn on real swing highs/lows. Wait for a candle close beyond the line before entering.",RISK_WARNING_EN]
    else:
        lines=[f"📈 <b>#{sym}/USDT · {tf.upper()} — شارت بنمط TradingView</b>",""]
        lines+=[f"{ico.get(p['bias'],'⚪')} <b>{p['name']}</b> — {p['bias']} ({p['status']})"
                +(f" | 🎯 {fmt(p['target'])}" if p.get('target') else "") for p in pats]
        lines+=["","ℹ️ الخطوط مرسومة تلقائياً على القمم والقيعان الحقيقية. لا تدخل قبل إغلاق شمعة خارج الخط (اختراق مؤكد).",RISK_WARNING_AR]
    return "\n".join(lines)

def tv_keyboard(sym,tf,lang="ar"):
    ar=(lang=="ar"); iv=TV_IV.get(tf,"240")
    url=f"https://www.tradingview.com/chart/?symbol=BINANCE%3A{sym}USDT.P&interval={iv}"
    tfs=[("15m","15m"),("1h","1H"),("4h","4H"),("1d","1D")]
    row=[_btn(("✅ " if k==tf else "")+lab,callback_data=f"tv:{sym}:{k}",style="primary" if k==tf else None) for k,lab in tfs]
    return InlineKeyboardMarkup([row,
        [_btn("🔗 افتح في TradingView" if ar else "🔗 Open in TradingView",url=url)],
        [_btn("🔄 تحديث" if ar else "🔄 Refresh",callback_data=f"tv:{sym}:{tf}"),
         _btn("❌ إغلاق" if ar else "❌ Close",callback_data="close_menu")]])

def build_tv_for(sym,tf,res):
    if res and tf==res.get("tf"):
        return render_tv_chart(res["df"],sym,tf,lv=res.get("lv"),side=res.get("side",0),
                               sr_high=res.get("sr_high",[]),sr_low=res.get("sr_low",[]),
                               src=display_source(res.get("source","binance_fut")).title())
    df=get_candles(sym,tf,300,ttl=120,min_bars=100); df=add_indicators(df,100)
    sh,sl=find_sr_levels(df)
    return render_tv_chart(df,sym,tf,lv=None,sr_high=sh[:2],sr_low=sl[:2],
                           src=display_source(df.attrs.get("source","binance_fut")).title())


def render_chart(res,tier="free",published=False):
    df=res["df"].tail(CHART_BARS).reset_index(drop=True)
    lv,side,sym,tf=res["lv"],res["side"],res["sym"],res["tf"]
    n=len(df); x=np.arange(n)
    bg,fg,grid="#ffffff","#0f172a","#e2e8f0"
    fig=_new_fig(12,8.2,bg)
    ax=fig.add_axes([0.065,0.36,0.80,0.54])
    axr=fig.add_axes([0.065,0.10,0.80,0.17],sharex=ax)
    close=df["close"].values.astype(float)
    ax.fill_between(x,df["bb_lo"].values,df["bb_up"].values,color="#1e88e5",alpha=0.07,lw=0,zorder=1)
    ax.plot(x,df["bb_up"],color="#1e88e5",lw=1.0,ls="--",alpha=0.6,zorder=2)
    ax.plot(x,df["bb_lo"],color="#1e88e5",lw=1.0,ls="--",alpha=0.6,zorder=2)
    ax.plot(x,df["ema20"],color="#f59e0b",lw=2.0,label="EMA 20",zorder=3)
    ax.plot(x,df["ema50"],color="#7c3aed",lw=2.0,label="EMA 50",zorder=3)
    ax.plot(x,close,color="#1565c0",lw=2.6,label="Price",zorder=4,solid_capstyle="round")
    ax.scatter([n-1],[close[-1]],s=60,color="#1565c0",edgecolor="white",linewidth=1.2,zorder=5)
    n_tps=4 if tier in ("admin","vip") else 2
    levels=[("ENTRY",lv["entry"],"#1d4ed8","-."),("STOP",lv["sl"],"#dc2626","--")]
    for j,tp in enumerate(lv["tps"][:n_tps],1):
        levels.append((f"TP{j}",tp["px"],"#15803d","--"))
    if res.get("wait"): levels=[]
    for name,px,col,ls in levels: ax.axhline(px,color=col,lw=1.7,ls=ls,zorder=3)
    lo=min([float(df["low"].min())]+[v[1] for v in levels])
    hi=max([float(df["high"].max())]+[v[1] for v in levels])
    if hi<=lo: hi=lo+1e-6
    pad=(hi-lo)*0.06
    ylo,yhi=lo-pad,hi+pad
    ax.set_ylim(ylo,yhi); ax.set_xlim(-1,n+12)
    _right_labels(ax,levels,n,ylo,yhi)
    ax.legend(loc="upper left",fontsize=9,frameon=True,facecolor="white",edgecolor=grid)
    ax.tick_params(labelbottom=False)
    axr.plot(x,df["rsi"],color="#7c3aed",lw=1.8,zorder=3)
    axr.axhline(70,color="#dc2626",lw=0.9,ls=":")
    axr.axhline(30,color="#15803d",lw=0.9,ls=":")
    axr.fill_between(x,30,70,color="#7c3aed",alpha=0.05,lw=0)
    axr.set_ylim(10,90); axr.set_xlim(-1,n+12)
    last_rsi=float(df["rsi"].iloc[-1])
    axr.text(n+0.8,last_rsi,f" RSI {last_rsi:.1f} ",color="white",fontsize=9,fontweight="bold",
             va="center",ha="left",clip_on=False,bbox=dict(boxstyle="round,pad=0.3",fc="#7c3aed",ec="none"))
    _style_axes([ax,axr],bg,fg,grid); _time_ticks(axr,df)
    fig.text(0.065,0.945,f"{sym}/USDT  ·  Binance Futures  ·  {tf.upper()}",fontsize=17,fontweight="bold",color=fg,ha="left")
    fig.text(0.065,0.915,f"Direction: {'WAIT' if res.get('wait') else ('LONG' if side == 1 else 'SHORT')}   |   EMA 20 / EMA 50   |   Bollinger (20, 2)   |   RSI (14)",fontsize=10,color="#475569",ha="left")
    fig.text(0.065,0.012,"Source: Binance Live Data",fontsize=9,color="#2563eb",ha="left")
    fig.text(0.985,0.012,"Ryma Crypto",fontsize=11,color="#b8860b",ha="right",fontweight="bold")
    return _to_png(fig)

def render_pattern_chart(res, tier="free"):
    df=res["df"].tail(CHART_BARS).reset_index(drop=True)
    lv,side,sym,tf=res["lv"],res["side"],res["sym"],res["tf"]
    n=len(df); t_arr=df["t"].values.astype("int64")
    bg,fg,grid="#131722","#d1d4dc","#1e222d"
    fig=_new_fig(12,8,bg)
    ax=fig.add_axes([0.065,0.10,0.80,0.80])
    _draw_candles(ax,df,"#26a69a","#ef5350")
    
    sr_high = res.get("sr_high", [])
    sr_low = res.get("sr_low", [])
    for lvl in sr_high:
        ax.axhline(lvl, color="#ef5350", lw=1.5, ls="--", alpha=0.7, zorder=4)
        ax.text(n+1, lvl, f" RES {fmt(lvl)}", color="#ef5350", va="center", fontsize=9, fontweight="bold")
    for lvl in sr_low:
        ax.axhline(lvl, color="#2962ff", lw=1.5, ls="--", alpha=0.7, zorder=4)
        ax.text(n+1, lvl, f" SUP {fmt(lvl)}", color="#2962ff", va="center", fontsize=9, fontweight="bold")

    col_map={"bullish":"#22c55e","bearish":"#facc15","neck":"#facc15","neutral":"#facc15"}
    ys=[float(df["low"].min()),float(df["high"].max())]
    pats=res.get("patterns",[])
    
    for p in pats:
        pname = p.get('name', '')
        pseg = p.get("segments", [])
        
        if pname in ["Ascending Triangle", "Descending Triangle", "Symmetrical Triangle",
                     "Rising Wedge", "Falling Wedge"] and len(pseg) >= 2:
            top = pseg[0]; bot = pseg[1]
            x1_top = _x_index(t_arr, top["t1"]); x2_top = _x_index(t_arr, top["t2"])
            x1_bot = _x_index(t_arr, bot["t1"]); x2_bot = _x_index(t_arr, bot["t2"])
            poly_x = [x1_top, x2_top, x2_bot, x1_bot]
            poly_y = [top["p1"], top["p2"], bot["p2"], bot["p1"]]
            fill_col = col_map.get(p["bias"], "#facc15")
            ax.add_patch(Polygon(list(zip(poly_x, poly_y)), closed=True,
                                 facecolor=fill_col, alpha=0.18, edgecolor=fill_col,
                                 linewidth=2.2, zorder=5))
            ax.plot([x1_top, x2_top], [top["p1"], top["p2"]], color=fill_col,
                    lw=2.5, zorder=6, solid_capstyle="round")
            ax.plot([x1_bot, x2_bot], [bot["p1"], bot["p2"]], color=fill_col,
                    lw=2.5, zorder=6, solid_capstyle="round")
            ys += [top["p1"], top["p2"], bot["p1"], bot["p2"]]
        else:
            for s in pseg:
                x1 = _x_index(t_arr, s["t1"])
                x2 = (n+5) if s["ext"] else _x_index(t_arr, s["t2"])
                seg_col = col_map.get(s["color"], "#facc15")
                ax.plot([x1,x2],[s["p1"],s["p2"]], color=seg_col, lw=2.5,
                        solid_capstyle="round", zorder=6,
                        path_effects=[pe.withStroke(linewidth=6, foreground=seg_col, alpha=0.2), pe.Normal()])
                ys += [s["p1"], s["p2"]]
        
        for m in p.get("marks", []):
            xm = _x_index(t_arr, m["t"]); c = col_map.get(p["bias"], "#facc15")
            ax.scatter([xm], [m["p"]], s=70, color=c, edgecolor="white", linewidth=1.5, zorder=7)
            ax.annotate(m["label"], (xm, m["p"]), xytext=(0, 12 if p["bias"]=="bearish" else -15),
                        textcoords="offset points", ha="center", fontsize=10, color="white",
                        fontweight="bold",
                        bbox=dict(boxstyle="round,pad=0.3", fc="#1c2230", ec=c, lw=1.2), zorder=8)
                        
    n_tps=4 if tier in ("admin","vip") else 2
    levels=[("ENTRY",lv["entry"],"#5c9dff","-."),("STOP",lv["sl"],"#ff5252","--")]
    for j,tp in enumerate(lv["tps"][:n_tps],1):
        levels.append((f"TP{j}",tp["px"],"#26a69a","--"))
    for name,px,col,ls in levels:
        ax.axhline(px,color=col,lw=1.8,ls=ls,zorder=5); ys.append(px)
    lo,hi=min(ys),max(ys)
    if hi<=lo: hi=lo+1e-6
    pad=(hi-lo)*0.07
    ylo,yhi=lo-pad,hi+pad
    ax.set_ylim(ylo,yhi); ax.set_xlim(-1,n+12)
    _right_labels(ax,levels,n,ylo,yhi)
    _style_axes([ax],bg,fg,grid); _time_ticks(ax,df)
    if pats:
        legend_lines=[f"● {p['name']}  [{p.get('tf','').upper()}]  {p['bias'].capitalize()}" for p in pats[:4]]
        ax.text(0.01,0.90,"\n".join(legend_lines),transform=ax.transAxes,va="top",ha="left",
                fontsize=10,color=fg,bbox=dict(boxstyle="round,pad=0.4",fc="#1c2230",ec=grid))
    fig.text(0.065,0.945,f"{sym}/USDT  ·  {tf.upper()}  ·  {'LONG' if side == 1 else 'SHORT'}",
             fontsize=18,fontweight="bold",color="#26a69a" if side==1 else "#ff5252",ha="left")
    fig.text(0.5,0.5,"Ryma Crypto",fontsize=64,color="#ffffff",alpha=0.035,ha="center",va="center",rotation=20,fontweight="bold")
    fig.text(0.065,0.012,"Automated analysis  ·  Source: Binance Live Data",fontsize=9,color="#94a3b8",ha="left")
    fig.text(0.985,0.012,"Ryma Crypto",fontsize=11,color="#c9a227",ha="right",fontweight="bold")
    return _to_png(fig)

def render_trade_card(res,tier="free"):
    lv,side,sym,tf=res["lv"],res["side"],res["sym"],res["tf"]
    grade=res.get("grade","?")
    is_long=side==1
    accent="#00ff9c" if is_long else "#ff4d6d"
    entry,sl=lv["entry"],lv["sl"]; tps=lv["tps"]
    risk=abs(entry-sl)
    rr=abs(tps[0]["px"]-entry)/risk if (risk>0 and tps) else 0.0
    fig=_new_fig(12,7.2,"#050a10")
    ax=fig.add_axes([0,0,1,1]); ax.set_xlim(0,1); ax.set_ylim(0,1); ax.axis("off")
    grad=LinearSegmentedColormap.from_list("card",["#0a1d15" if is_long else "#1d0a11","#050a10"])
    ax.imshow(np.linspace(0,1,256).reshape(-1,1),extent=[0,1,0,1],aspect="auto",cmap=grad,zorder=0)
    ax.add_patch(FancyBboxPatch((0.012,0.02),0.976,0.96,boxstyle="round,pad=0,rounding_size=0.03",fc="none",ec=accent,lw=2.2,zorder=1))
    glow=[pe.withStroke(linewidth=6,foreground=accent,alpha=0.18),pe.Normal()]
    ax.text(0.5,0.925,"RYMA CRYPTO",ha="center",va="center",color=accent,fontsize=26,fontweight="bold",zorder=4,path_effects=glow)
    ax.text(0.5,0.878,"Automated Signal Card  ·  Binance Futures",ha="center",va="center",color="#94a3b8",fontsize=12,zorder=4)
    ax.text(0.5,0.79,f"{sym}",ha="center",va="center",color="#ffffff",fontsize=60,fontweight="bold",zorder=4)
    ax.text(0.5,0.725,"/USDT PERPETUAL",ha="center",va="center",color="#94a3b8",fontsize=14,zorder=4)
    ax.add_patch(FancyBboxPatch((0.36,0.615),0.28,0.07,boxstyle="round,pad=0,rounding_size=0.015",
                                fc="#06150f" if is_long else "#150609",ec=accent,lw=2.0,zorder=3))
    ax.text(0.5,0.65,"▲ LONG" if is_long else "▼ SHORT",ha="center",va="center",color=accent,
            fontsize=24,fontweight="bold",zorder=4,path_effects=glow)
    def box(x,y,w,h,label,value,col,dim=False):
        ax.add_patch(FancyBboxPatch((x,y),w,h,boxstyle="round,pad=0,rounding_size=0.012",
                                    fc="#0b1220",ec=col if not dim else "#334155",lw=1.6,zorder=2))
        if not dim: ax.add_patch(Rectangle((x+0.012,y+h-0.01),w-0.024,0.008,color=col,zorder=3))
        ax.text(x+w/2,y+h*0.72,label,ha="center",va="center",color="#94a3b8" if not dim else "#475569",fontsize=12,zorder=4)
        tt=ax.text(x+w/2,y+h*0.33,value,ha="center",va="center",color=col if not dim else "#475569",fontsize=24,fontweight="bold",zorder=4)
        if not dim: tt.set_path_effects([pe.withStroke(linewidth=5,foreground=col,alpha=0.2),pe.Normal()])
    w,h=0.27,0.15; xs=[0.07,0.365,0.66]
    box(xs[0],0.40,w,h,"ENTRY",fmt(entry),"#60a5fa")
    box(xs[1],0.40,w,h,"STOP LOSS",fmt(sl),"#ff4d6d")
    box(xs[2],0.40,w,h,"TARGET 1",fmt(tps[0]["px"]),accent)
    box(xs[0],0.20,w,h,"TARGET 2",fmt(tps[1]["px"]),accent)
    if tier in ("admin","vip") and len(tps)>=4:
        box(xs[1],0.20,w,h,"TARGET 3",fmt(tps[2]["px"]),accent)
        box(xs[2],0.20,w,h,"TARGET 4",fmt(tps[3]["px"]),accent)
    else:
        box(xs[1],0.20,w,h,"TARGET 3","VIP","#334155",dim=True)
        box(xs[2],0.20,w,h,"TARGET 4","VIP","#334155",dim=True)
    ax.text(0.5,0.10,f"R:R  {rr:.1f}:1     ·     GRADE  {grade}     ·     TIMEFRAME  {tf.upper()}",
            ha="center",va="center",color="#e2e8f0",fontsize=14,fontweight="bold",zorder=4)
    ax.text(0.5,0.045,"Source: Binance Live Data  ·  Trading involves risk  ·  Not financial advice",
            ha="center",va="center",color="#64748b",fontsize=10,zorder=4)
    return _to_png(fig)

def caption_wait(res,lang="ar"):
    sym=res["sym"]; tr=res.get("trends") or {}
    nm={2:"صاعد قوي",1:"صاعد",0:"عرضي",-1:"هابط",-2:"هابط قوي"}
    nm_en={2:"Strong up",1:"Up",0:"Range",-1:"Down",-2:"Strong down"}
    N=nm if lang=="ar" else nm_en
    lines=[]
    if lang=="ar":
        lines+=[f"⏳ <b>#{sym}/USDT — لا توجد صفقة مناسبة الآن</b>","",
                f"💵 السعر: <code>{fmt(res['price'])}</code>",
                f"🧭 يومي: {N.get(tr.get('d1',0))} | 4س: {N.get(tr.get('h4',0))} | 1س: {N.get(tr.get('h1',0))}","",
                "📌 <b>لماذا الانتظار؟</b>"]
        lines+=[f"  • {r}" for r in res.get("wait_reasons",[])[:3]] or ["  • الشروط غير مكتملة"]
        if res.get("watch"): lines+=["",f"👀 منطقة المراقبة للدخول: <code>{fmt(res['watch'])}</code>"]
        lines+=["","💡 الدقة أهم من عدد الصفقات: البوت لا يعطيك صفقة إلا إذا توافقت الشروط (اتجاه يومي + 4س، ستوب هيكلي، مساحة للأهداف).",
                "",RISK_WARNING_AR,f"👤 {BRAND}"]
    else:
        lines+=[f"⏳ <b>#{sym}/USDT — No quality setup right now</b>","",
                f"💵 Price: <code>{fmt(res['price'])}</code>",
                f"🧭 1D: {N.get(tr.get('d1',0))} | 4H: {N.get(tr.get('h4',0))} | 1H: {N.get(tr.get('h1',0))}","",
                "📌 <b>Why wait?</b>"]
        lines+=[f"  • {r}" for r in res.get("wait_reasons",[])[:3]] or ["  • Conditions incomplete"]
        if res.get("watch"): lines+=["",f"👀 Watch entry zone: <code>{fmt(res['watch'])}</code>"]
        lines+=["","💡 Accuracy over quantity: a trade is issued only when trend (1D+4H), structural stop and room-to-target all agree.",
                "",RISK_WARNING_EN,f"👤 {BRAND}"]
    return "\n".join(lines)

def caption_core(res,tier="free",lang="ar"):
    if res.get("wait"): return caption_wait(res,lang)
    side,lv,sym=res["side"],res["lv"],res["sym"]
    if lang=="en":
        side_word="🟢 LONG" if side==1 else "🔴 SHORT"
        decision="🟢 Buy" if side==1 else "🔴 Sell"
        now_ts=dt.datetime.now().strftime("%Y-%m-%d %H:%M")
        source=display_source(res.get("source","binance_fut"))
        cl=coin_link(sym,res.get("source","binance_fut"))
        grade=res.get("grade","?")
        lines=["╭━━━ 💎 RYMA CRYPTO 💎 ━━━╮",f"┃ 🎯 Signal: {side_word}",
            f"┃ 📌 <a href='{cl}'>#{sym}/USDT</a> 👆 Click here",
            f"┃ 📡 Source: {source}",f"┃ 🏆 Grade: {grade}",
            "╰━━━━━━━━━━━━━━━━━━━━━━━╯","",f"💵 Entry: {fmt(lv['entry'])}",
            f"🛑 Stop Loss: {fmt(lv['sl'])}",""]
        n_tps=4 if tier in ("admin","vip") else 2
        for i,tp in enumerate(lv["tps"][:n_tps],1):
            emoji="🎯" if i<=2 else "🏆"; lines.append(f"{emoji} Target {i}: {fmt(tp['px'])}")
        if tier=="free" and len(lv["tps"])>2: lines+=["","💎 For more targets → VIP"]
        pats=res.get("patterns",[])
        if pats:
            lines.append("")
            pat_names=" | ".join(f"{p['name']}{'[' + p.get('tf','').upper() + ']' if p.get('tf') else ''}" for p in pats[:2])
            lines.append(f"🕯 Pattern: {pat_names}")
        if grade in ("B","C"): lines+=["","👁 <b>Watch setup:</b> enter only after breakout/breakdown is confirmed"]
        if res.get("forced") or grade == "C":
            lines += ["",
                      "⚠️ <b>Grade C warning — low quality setup:</b>",
                      "• Full engine conditions not met",
                      "• Shown for analysis only (not auto-posted)",
                      "• Use half normal risk size or less",
                      "• Invalidate immediately if stop is broken"]
            for r in res.get("wait_reasons", [])[:3]:
                lines.append(f"  • {r}")
            for w in res.get("warnings", [])[:2]:
                lines.append(f"  ⚡ {w}")
        lines+=["","━━━━━━━━━━━━━━━━",f"🎯 <b>Expected Decision: {decision}</b>"]
        lines+=["",RISK_WARNING_EN,f"👤 {BRAND}"]
        return "\n".join(lines)
        
    side_word="🟢 شراء (LONG)" if side==1 else "🔴 بيع (SHORT)"
    decision="🟢 شراء" if side==1 else "🔴 بيع"
    now_ts=dt.datetime.now().strftime("%Y-%m-%d %H:%M")
    source=display_source(res.get("source","binance_fut"))
    cl=coin_link(sym,res.get("source","binance_fut"))
    grade=res.get("grade","?")
    lines=["╭━━━ 💎 RYMA CRYPTO 💎 ━━━╮",f"┃ 🎯 التوصية: {side_word}",
        f"┃ 📌 <a href='{cl}'>#{sym}/USDT</a> 👆 اضغط هنا",
        f"┃ 📡 المصدر: {source}",f"┃ 🏆 الدرجة: {grade}",
        "╰━━━━━━━━━━━━━━━━━━━━━━━╯","",f"💵 الدخول: {fmt(lv['entry'])}",
        f"🛑 وقف الخسارة: {fmt(lv['sl'])}",""]
    n_tps=4 if tier in ("admin","vip") else 2
    for i,tp in enumerate(lv["tps"][:n_tps],1):
        emoji="🎯" if i<=2 else "🏆"; lines.append(f"{emoji} الهدف {i}: {fmt(tp['px'])}")
    if tier=="free" and len(lv["tps"])>2: lines+=["","💎 للأهداف الإضافية → VIP"]
    pats=res.get("patterns",[])
    if pats:
        lines.append("")
        pat_names=" | ".join(f"{p['name']}{'[' + p.get('tf','').upper() + ']' if p.get('tf') else ''}" for p in pats[:2])
        lines.append(f"🕯 النموذج: {pat_names}")
    if grade in ("B","C"): lines+=["","👁 <b>صفقة مراقبة:</b> ادخل فقط بعد تأكيد الاختراق أو الكسر"]
    if res.get("forced") or grade == "C":
        lines += ["",
                  "⚠️ <b>تحذير درجة C — صفقة منخفضة الجودة:</b>",
                  "• الشروط الكاملة للمحرك غير متحققة",
                  "• تُعرض للتحليل فقط وليست توصية نشر تلقائي",
                  "• استخدم نصف حجم المخاطرة المعتاد أو أقل",
                  "• راقب الإبطال فوراً إذا كسر الوقف"]
        for r in res.get("wait_reasons", [])[:3]:
            lines.append(f"  • {r}")
        for w in res.get("warnings", [])[:2]:
            lines.append(f"  ⚡ {w}")
    lines+=["","━━━━━━━━━━━━━━━━",f"🎯 <b>القرار المتوقع: {decision}</b>"]
    lines+=["",RISK_WARNING_AR,f"👤 {BRAND}"]
    return "\n".join(lines)

def keyboard_free(sym,tf,source="binance_fut",lang="en"):
    return InlineKeyboardMarkup([
        [_btn(t("risk_btn",lang),callback_data=f"calc_risk:{sym}"),_btn(t("port_btn",lang),callback_data=f"add_port:{sym}")],
        [_btn(t("chart_btn",lang),callback_data=f"chart:{sym}"), _btn(t("refresh_btn",lang),callback_data=f"tf:{sym}:{tf}")],
        [_btn(t("news_btn",lang),callback_data=f"news:{sym}")],
        [_btn(t("refs_btn",lang),callback_data=f"refs:{source}")],
        [_btn(t("vip_btn",lang),url=VIP_INVITE_LINK)],
        [_btn(t("free_btn",lang),url=CHANNEL_LINK)],
        [_btn(t("contact_btn",lang),url=CONTACT_URL)]
    ])

def keyboard_vip(sym,tf,source="binance_fut",lang="en"):
    return InlineKeyboardMarkup([
        [_btn(t("risk_btn",lang),callback_data=f"calc_risk:{sym}"),_btn(t("port_btn",lang),callback_data=f"add_port:{sym}")],
        [_btn("📤 مشاركة الصفقة" if lang=="ar" else "📤 Share Trade",callback_data=f"share:{sym}"),_btn(t("news_btn",lang),callback_data=f"news:{sym}")],
        [_btn(t("refs_btn",lang),callback_data=f"refs:{source}")],
        [_btn("📋 القائمة" if lang=="ar" else "📋 Menu",callback_data="vip_menu")],
        [_btn(t("chart_btn",lang),callback_data=f"chart:{sym}")],
        [_btn(t("refresh_btn",lang),callback_data=f"tf:{sym}:{tf}")]])

def keyboard_admin(sym,tf,source="binance_fut",lang="en"):
    ar = (lang == "ar")
    return InlineKeyboardMarkup([
        [_btn("🧮 حاسبة المخاطر" if ar else "🧮 Risk Calc",callback_data=f"calc_risk:{sym}"),
         _btn("📊 محفظتي" if ar else "📊 Portfolio",callback_data="my_portfolio")],
        [_btn("📈 شارت TradingView" if ar else "📈 TradingView Chart",callback_data=f"tv:{sym}:{tf}")],
        [_btn("🌐 جميع المنصات" if ar else "🌐 All Platforms",callback_data=f"refs:{source}")],
        [_btn("📊 المؤشرات" if ar else "📊 Indicators",callback_data=f"ind:{sym}:{tf}"),
         _btn("💧 السيولة" if ar else "💧 Liquidity",callback_data=f"liq:{sym}")],
        [_btn("🐋 الحيتان" if ar else "🐋 Whales",callback_data=f"whale:{sym}"),
         _btn("🕯️ النماذج" if ar else "🕯️ Patterns",callback_data=f"pat:{sym}")],
        [_btn("🖼️ شارت النماذج (أدمن)" if ar else "🖼️ Patterns Chart (Admin)",callback_data=f"pchart:{sym}:{tf}")],
        [_btn("💳 بطاقة النيون" if ar else "💳 Neon Card",callback_data=f"neon:{sym}")],
        [_btn("🔥 رادار الضجة" if ar else "🔥 Hype Radar",callback_data="hype_radar")],
        [_btn("🧪 Backtest",callback_data=f"btcb:{sym}")],
        [_btn("📋 قائمة الأوامر الكاملة" if ar else "📋 Full Admin Menu",callback_data="admin_menu")],
        [_btn("🎯 صفقة اليوم" if ar else "🎯 Daily Trade",callback_data="adm:/daily")],
        [_btn("🔄 تحديث" if ar else "🔄 Refresh",callback_data=f"tf:{sym}:{tf}")]])

def keyboard(sym,tf,tier,source="binance_fut",lang="en"):
    if tier=="admin": return keyboard_admin(sym,tf,source,lang)
    if tier=="vip": return keyboard_vip(sym,tf,source,lang)
    return keyboard_free(sym,tf,source,lang)

def keyboard_refs(current="binance_fut"):
    cur=(current or "binance").lower()
    if cur.startswith("binance"): cur="binance"
    rows=[]
    name,url=REFERRAL_LINKS["binance"]
    rows.append([_btn(f"🚀 {name} Futures (رسوم أقل)",url=url)])
    for key,(name,url) in REFERRAL_LINKS.items():
        if key=="binance": continue
        label=f"✅ {name}" if key==cur else f"🔗 {name}"
        rows.append([_btn(label,url=url)])
    rows.append([_btn("❌ إغلاق",callback_data="close_menu")])
    return InlineKeyboardMarkup(rows)

def keyboard_golden_config():
    cfg=get_golden_config()
    def ic(b): return "✅" if b else "⬜"
    return InlineKeyboardMarkup([
        [_btn(f"{ic(cfg.get('require_1d_trend',False))} اتجاه 1D",callback_data="gc:require_1d_trend")],
        [_btn(f"{ic(cfg.get('require_mtf',False))} توافق MTF",callback_data="gc:require_mtf")],
        [_btn(f"{ic(cfg.get('require_rsi',True))} RSI صحي",callback_data="gc:require_rsi")],
        [_btn(f"{ic(cfg.get('require_volume',False))} حجم قوي",callback_data="gc:require_volume")],
        [_btn(f"{ic(cfg.get('require_breakout',False))} اختراق/كسر",callback_data="gc:require_breakout")],
        [_btn(f"{ic(cfg.get('require_liquidity',False))} سيولة >1M$",callback_data="gc:require_liquidity")],
        [_btn(f"{ic(cfg.get('require_alpha',False))} عملة Alpha فقط",callback_data="gc:require_alpha")],
        [_btn(f"📊 الحد الأدنى للنقاط: {cfg.get('min_score',1)}",callback_data="gc:min_score")],
        [_btn("🔄 تحديث",callback_data="gc:refresh")],
        [_btn("❌ إغلاق",callback_data="close_menu")]])

def build_golden_config_text():
    cfg=get_golden_config()
    return ("💎✨ <b>إعدادات الفرصة الذهبية</b> ✨💎\n━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🔧 اضغط على أي شرط لتفعيله/تعطيله\n\n✅ = مطلوب اجتيازه\n⬜ = غير مطلوب\n\n"
        f"📊 الحد الأدنى للنقاط: <b>{cfg.get('min_score',1)}</b>\n"
        f"⏱ الدورة: كل 5 ساعات\n🔄 وضع Relaxed تلقائي عند عدم وجود نتائج\n"
        f"📌 ستظهر الفرصة عند تحقيق الشروط المفعّلة")

def keyboard_vip_menu():
    return InlineKeyboardMarkup([
        [_btn("💎✨ الفرصة الذهبية",callback_data="vip_cmd:/golden")],
        [_btn("🎯 صفقة اليوم",callback_data="vip_cmd:/daily")],
        [_btn("⚡ رادار الفرص المبكرة",callback_data="vip_cmd:/radar")],
        [_btn("🚀 رادار النخبة (Alpha/Web3)",callback_data="vip_cmd:/alpha_v2")],
        [_btn("🔮 التنبؤ بالانفجارات",callback_data="vip_cmd:/predict")],
        [_btn("🐋 رادار الصفقات الكبيرة",callback_data="vip_cmd:/whales")],
        [_btn("💥 رادار الانفجارات",callback_data="vip_cmd:/explosions")],
        [_btn("🚀 عملات صاعدة",callback_data="vip_cmd:/pump")],
        [_btn("📉 رادار القيعان الذكي",callback_data="vip_cmd:/bottom")],
        [_btn("🌱 فرص السبوت (1–2 شهر)",callback_data="vip_cmd:/spot")],
        [_btn("🔻 رادار فرص الشورت",callback_data="vip_cmd:/shortsradar")],
        [_btn("⭐ رادار Alpha الأسبوعي",callback_data="vip_cmd:/alpha")],
        [_btn("💧 رادار السيولة",callback_data="vip_cmd_args:liquidity")],
        [_btn("📊 بيانات الفيوتشر",callback_data="vip_cmd_args:futures")],
        [_btn("📈 الصفقات النشطة",callback_data="vip_cmd:/active")],
        [_btn("🔗 رابط الإحالة",callback_data="my_ref"),_btn("🎯 نقاطي",callback_data="my_points")],
        [_btn("🌐 منصات التداول",callback_data="refs:binance_fut")],
        [_btn("🆓 قناة التوصيات المجانية",url=CHANNEL_LINK)],
        [_btn("❌ إغلاق القائمة",callback_data="close_menu")]])

def build_vip_menu_text():
    return ("╭━━━ 💎 قائمة VIP الكاملة 💎 ━━━╮\n┃ 🔥 كل الخدمات متاحة 🔥\n╰━━━━━━━━━━━━━━━━━━━━━━━━━━╯\n\n"
        "🎯 الأزرار متاحة للوصول للخدمة مباشرة\n\n📌 ملاحظات:\n"
        "• 💧 السيولة و 📊 الفيوتشر يتطلبان كتابة الرمز\n"
        "• 📈 الصفقات النشطة تعرض الصفقات الحالية\n"
        "• 🔗 رابط الإحالة = +20 نقطة لكل مشترك\n"
        "• 📢 القناة المجانية للتنبيهات العامة\n\n"
        "╰━━━━━━━━━━━━━━━━━━━━━━━━━━╯")

def build_vip_page():
    return ("╭━━━ 💎 RYMA CRYPTO VIP 💎 ━━━╮\n┃ 🔥 حصري · جودة · أرباح 🔥\n╰━━━━━━━━━━━━━━━━━━━━━━━━━━╯\n\n"
        "✨ لماذا VIP؟\n\n🔥 💎✨ الفرصة الذهبية\n🔥 🎯 صفقة اليوم\n🔥 🎯 4 أهداف كاملة (بدل 2)\n"
        "🔥 📡 تتبع لحظي لـ TP + SL\n🔥 ⚡ تنبيهات فورية\n🔥 🚀 رادار النخبة (Alpha/Web3/DEX)\n"
        "🔥 🔮 رادار تنبؤي للانفجارات\n🔥 🐋 رادار الحيتان\n🔥 💧 رادار السيولة\n"
        "🔥 💥 رادار الانفجارات\n🔥 📉 رادار القيعان\n🔥 🔻 رادار الفرص الشورت\n"
        "🔥 ⭐ رادار عملات Alpha\n🔥 🕯️ كشف النماذج الفنية (MTF)\n🔥 🌟 تنبيهات التقاطع الذهبي\n\n"
        "💎 3 باقات VIP متاحة\n💰 الأسعار عند الطلب\n\n"
        f"📞 للاستفسار والاشتراك: {CONTACT_LINK or CHANNEL_LINK}\n\n👇 للانضمام")

def build_admin_menu_text():
    return "👑 <b>قائمة أوامر الأدمن الكاملة</b>\n\nالأزرار متاحة للتنفيذ المباشر.\n"

def keyboard_admin_menu():
    multi_icon="✅" if is_multi_exchange() else "❌"
    return InlineKeyboardMarkup([
        [_btn("⚡ تشغيل النشر",callback_data="adm:/forcepost"),_btn("🧹 تفريغ الصفقات",callback_data="adm:/clearsignals")],
        [_btn("🛠 وضع الصيانة",callback_data="toggle_maintenance"),_btn(f"🌐 تعدد المنصات: {multi_icon}",callback_data="toggle_multi_exchange")],
        [_btn("💎 إعدادات الذهبية",callback_data="adm:/goldenconfig")],
        [_btn("💳 بطاقة النيون (يدوي)",callback_data="neon_card_manual")],
        [_btn("🔥 رادار الضجة",callback_data="hype_radar")],
        [_btn("🖼️ شارت النماذج (يدوي)",callback_data="patterns_chart_manual")],
        [_btn("📤 مشاركة صفقة اجتماعياً",callback_data="adm_args:share")],
        [_btn("💎✨ الفرصة الذهبية",callback_data="adm:/golden"),_btn("🎯 صفقة اليوم",callback_data="adm:/daily")],
        [_btn("⚡ رادار الفرص المبكرة",callback_data="adm:/radar")],
        [_btn("🌟 التقاطعات",callback_data="adm:/cross"),_btn("🔍 تشخيص",callback_data="adm:/debug")],
        [_btn("📊 فحص السوق",callback_data="adm:/scan"),_btn("🔻 فرص شورت",callback_data="adm:/short")],
        [_btn("🔮 التنبؤ",callback_data="adm:/predict"),_btn("🐋 الحيتان",callback_data="adm:/whales")],
        [_btn("💥 الانفجارات",callback_data="adm:/explosions"),_btn("🚀 الصعود",callback_data="adm:/pump")],
        [_btn("📉 القيعان",callback_data="adm:/bottom"),_btn("🌱 فرص السبوت",callback_data="adm:/spot")],
        [_btn("🔻 شورت رادار",callback_data="adm:/shortsradar"),_btn("📋 خطة DCA/Grid",callback_data="adm:/plan")],
        [_btn("⭐ Alpha",callback_data="adm:/alpha"),_btn("🚀 رادار النخبة (Alpha Pro)",callback_data="adm:/alpha_v2")],
        [_btn("🕯️ فحص النماذج (تلقائي)",callback_data="adm:/patterns"),_btn("🧪 Backtest",callback_data="adm:/bt")],
        [_btn("🧪 Backtest TOP50",callback_data="adm:/bt50"),_btn("👥 المستخدمون",callback_data="adm:/users")],
        [_btn("💎 قائمة VIP",callback_data="adm:/viplist"),_btn("📈 إحصائيات",callback_data="adm:/stats")],
        [_btn("🗂 السجل",callback_data="adm:/history"),_btn("🎛 لوحة التحكم",callback_data="adm:/dashboard")],
        [_btn("📢 اختبار القنوات",callback_data="adm:/testchannels"),_btn("🗑 قائمة الحذف",callback_data="adm:/delist")],
        [_btn("🎁 إضافة نقاط",callback_data="adm_args:addpoints")],
        [_btn("💧 السيولة",callback_data="adm_args:liquidity"),_btn("📊 فيوتشر",callback_data="adm_args:futures")],
        [_btn("💲 أسعار",callback_data="adm_args:price"),_btn("🔎 لماذا",callback_data="adm_args:why")],
        [_btn("📝 نشر توصية",callback_data="adm_args:post"),_btn("➕ إضافة VIP",callback_data="adm_args:addvip")],
        [_btn("➖ إزالة VIP",callback_data="adm_args:removevip"),_btn("❌ إغلاق القائمة",callback_data="close_menu")]])

def build_admin_extras(res):
    trend_ar={"strong_up":"صاعد قوي","up":"صاعد","strong_down":"هابط قوي","down":"هابط","range":"عرضي","unknown":"—"}.get(res.get("trend",""),"—")
    lines=[f"📊 المؤشرات - {res['sym']}/{res['tf']}","",
        f"📈 RSI: {res['rsi']:.1f}",f"📊 ATR: {res['lv']['atr']:.4f}",
        f"📉 MACD: {res['macd']:.4f}",f"🎯 الاستراتيجية: {res['strategy']}",
        f"🏆 الدرجة: {res['grade']}",f"📊 اتجاه: {trend_ar}"]
    if res.get("candle"): lines.append(f"🕯 الشمعة: {res['candle']}")
    pats=res.get("patterns",[])
    if pats:
        lines.append(""); lines.append("🕯️ <b>النماذج الفنية:</b>")
        for p in pats[:4]:
            emoji="🟢" if p["bias"]=="bullish" else ("🔴" if p["bias"]=="bearish" else "⚪")
            tf_lbl=f" [{p.get('tf','').upper()}]" if p.get('tf') else ""
            lines.append(f"  {emoji} {p['name']}{tf_lbl}")
    lines.append("")
    for i,tp in enumerate(res['lv']['tps'],1): lines.append(f"🎯 هدف {i}: {tp['pct']}%")
    liq=futures_liquidations(res['sym'])
    if liq: lines.append(f"💥 تصفيات: شراء ${liq['long_liq']:,.0f} | بيع ${liq['short_liq']:,.0f}")
    return "\n".join(lines)

def active_signals():
    with store.lock: return [p for p in store.data["signals"] if p.get("state","open") in ("pending","open")]

def can_track(res):
    if res["sym"] in WEAK_COINS: return False
    act=active_signals()
    if len(act)>=MAX_OPEN: return False
    if any(p["coin"]==res["sym"] and p["tf"]==res["tf"] for p in act): return False
    return True

def track_signal(res):
    sz=RISK_SIZE.get(res["grade"],0.02)
    p=new_trade(res["sym"],res["tf"],res["side"],res["lv"],res["bar_t"],res["grade"],res["setup"],size_pct=sz)
    p["source"]=res.get("source","binance_fut")
    with store.lock: store.data["signals"].append(p); store.save("signals")
    return p

async def publish(bot,res,vip=True,free=False,admin_copy=False):
    """ينشر نص الصفقة أولاً (مهم للقنوات) ثم الشارت إن أمكن — بدون تعطيل عند فشل الصورة."""
    if res.get("wait"): return []
    sent=[]; source=res.get("source","binance_fut")
    text_vip = caption_core(res, tier="vip", lang="ar")
    text_free = caption_core(res, tier="free", lang="ar")
    copy_text = build_copy_post(res)
    # شفافية تاريخية للأدمن
    st = grade_history_stats(res.get("grade"))
    admin_extra = ""
    if st and st["n"] >= 5:
        admin_extra = f"\n📊 [أدمن] درجة {res.get('grade')}: نجاح تاريخي {st['rate']}% من {st['n']} صفقة"
    try:
        kb_v = keyboard_vip(res["sym"], res["tf"], source, "ar")
        kb_f = keyboard_free(res["sym"], res["tf"], source, "ar")
        if vip and VIP_CHANNEL_ID:
            try:
                await bot.send_message(VIP_CHANNEL_ID, text_vip[:4000], parse_mode=ParseMode.HTML,
                                       protect_content=PROTECT, reply_markup=kb_v,
                                       link_preview_options=NO_PREVIEW)
                sent.append("VIP_TEXT")
            except Exception as e:
                log.warning("vip text: %s", e)
            try:
                img = await asyncio.to_thread(render_chart, res, "vip", True)
                await bot.send_photo(VIP_CHANNEL_ID, img, protect_content=PROTECT, reply_markup=kb_v)
                sent.append("VIP_CHART")
            except Exception as e:
                log.debug("vip chart skip: %s", e)
        if free and CHANNEL_ID:
            try:
                await bot.send_message(CHANNEL_ID, text_free[:4000], parse_mode=ParseMode.HTML,
                                       protect_content=PROTECT, reply_markup=kb_f,
                                       link_preview_options=NO_PREVIEW)
                sent.append("FREE_TEXT")
            except Exception as e:
                log.warning("free text: %s", e)
            try:
                img = await asyncio.to_thread(render_chart, res, "free", True)
                await bot.send_photo(CHANNEL_ID, img, protect_content=PROTECT, reply_markup=kb_f)
                sent.append("FREE_CHART")
            except Exception as e:
                log.debug("free chart skip: %s", e)
        if admin_copy and ADMIN_ID:
            try:
                await bot.send_message(ADMIN_ID, (copy_text + admin_extra)[:4000], parse_mode=ParseMode.HTML,
                                       link_preview_options=NO_PREVIEW)
            except Exception as _ex:
                log.debug("admin copy: %s", _ex)
            try:
                card = await asyncio.to_thread(render_trade_card, res, "admin")
                await bot.send_photo(ADMIN_ID, card)
            except Exception as _ex:
                log.debug("admin card: %s", _ex)
    except Exception as e:
        sent.append(f"err:{str(e)[:40]}")
    return sent

def build_market_summary(symbols):
    now_ts=dt.datetime.now().strftime("%Y-%m-%d %H:%M")
    lines=["📊 تنبيهات السوق",f"🕐 {now_ts}","━━━━━━━━━━━━━━━━",""]
    ok_count=0
    for s in symbols:
        try:
            price,ch24,ch7=ticker_change(s)
            if price<=0: continue
            lines.append(f"💠 {s} — {fmt(price)}")
            lines.append(f"{'🔺' if ch24 >= 0 else '🔻'} 24h: {ch24:+.2f}%")
            lines.append(f"{'🔺' if ch7 >= 0 else '🔻'} 7d: {ch7:+.2f}%")
            lines.append(""); ok_count+=1
        except Exception: continue
    if ok_count==0: return None
    lines.append(f"📣 {CHANNEL_LINK}")
    return "\n".join(lines)

def pick_short_opportunities(n=5):
    syms=top_symbols(50); rows=[]
    for s in syms[:40]:
        r=short_radar(s)
        if r: rows.append((s,r))
    rows.sort(key=lambda x:-x[1]["rsi"])
    return rows[:n]

_free_day={"d":"","n":0}

def _can_post(r):
    key=f"{r['sym']}:{r['tf']}:{r['bar_t']}"
    if key in store.data["posted"]: return False
    if recently_posted(r["sym"],6): return False
    act=active_signals()
    if len(act)>=MAX_OPEN: return False
    return not any(p["coin"]==r["sym"] and p["tf"]==r["tf"] for p in act)

def _mark_posted(r):
    key=f"{r['sym']}:{r['tf']}:{r['bar_t']}"
    track_signal(r)
    with store.lock:
        store.data["posted"][key]=now_s()
        cut=now_s()-14*DAY
        store.data["posted"]={k:v for k,v in store.data["posted"].items() if v>cut}
        store.save("posted")

async def run_autopost(bot):
    log.info("run_autopost: بدء المسح")
    syms=await asyncio.to_thread(top_symbols,60)
    results=await asyncio.to_thread(scan_sync,syms[:40])
    cands=[r for r in results if r["rank"]>=0 and not r.get("forced") and r["sym"] not in WEAK_COINS and r["sym"] not in EXCLUDE_TICKERS]
    cands.sort(key=lambda r:(-r["rank"],-r.get("ok",0)))
    today=dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d")
    if _free_day["d"]!=today: _free_day.update(d=today,n=0)
    a_candidates=[r for r in cands if r["grade"] in ("A+","A")]
    a_candidates.sort(key=lambda r:(-r.get("fut_score",0),-r["rank"]))
    b_candidates=[r for r in cands if r["grade"]=="B"]
    vip_pool=a_candidates if a_candidates else b_candidates
    if len(active_signals())>=MAX_OPEN:
        log.warning(f"MAX_OPEN {len(active_signals())}/{MAX_OPEN}"); return dict(vip=0,free=0,total=0)
    vip_n=free_n=0; used=set()
    for r in vip_pool:
        if vip_n>=VIP_POSTS_PER_CYCLE: break
        if not _can_post(r): continue
        if not check_liquidity(r["sym"],min_usd=VIP_MIN_LIQUIDITY): continue
        if REQUIRE_MTF_FOR_SMALL and r["sym"] not in MAJORS and r["sym"] not in MEME_COINS:
            mtf_c,mtf_t=mtf_score(r["sym"],r["side"])
            if mtf_t>=2 and mtf_c<mtf_t: continue
        try: sent=await publish(bot,r,vip=True,free=False,admin_copy=False)
        except Exception: continue
        if any(x in ("VIP","FREE") for x in sent):
            _mark_posted(r); vip_n+=1; used.add(r["sym"]); await asyncio.sleep(1.0)
    if vip_n==0:
        for r in [x for x in b_candidates if x["sym"] not in used]:
            if vip_n>=2: break
            if not _can_post(r): continue
            try: sent=await publish(bot,r,vip=True,free=False,admin_copy=False)
            except Exception: continue
            if any(x in ("VIP","FREE") for x in sent):
                _mark_posted(r); vip_n+=1; used.add(r["sym"]); await asyncio.sleep(1.0)
    for r in [x for x in b_candidates if x["sym"] not in used]:
        if free_n>=FREE_POSTS_PER_CYCLE or _free_day["n"]>=FREE_MAX_PER_DAY: break
        if not _can_post(r): continue
        try: sent=await publish(bot,r,vip=False,free=True,admin_copy=False)
        except Exception: continue
        if "FREE" in sent:
            _mark_posted(r); free_n+=1; _free_day["n"]+=1; used.add(r["sym"]); await asyncio.sleep(1.0)
    log.info(f"run_autopost: VIP={vip_n}, FREE={free_n}")
    return dict(vip=vip_n,free=free_n,total=vip_n+free_n)

def build_promo_post():
    lines=["💎✨ <b>لماذا نتداول على هذه المنصات؟</b> ✨💎","━━━━━━━━━━━━━━━━━━━━━","","🚀 <b>مزايا المنصات المعتمدة:</b>",""]
    for key,(name,url) in REFERRAL_LINKS.items():
        lines+=[f"🔗 <b>{name}</b>",f"   <a href='{url}'>سجّل الآن واحصل على مكافآت</a>",""]
    lines+=["━━━━━━━━━━━━━━━━━━━━━","✅ رسوم منخفضة","✅ سيولة عالية",
        "✅ عقود فيوتشر احترافية","✅ دعم فني 24/7","",
        "🎁 <b>للحصول على تجربة مجانية:</b> اكتب كلمة (تجربة) في الردود","",
        RISK_WARNING_AR,f"👤 {BRAND}"]
    return "\n".join(lines)

async def autopost_loop(app):
    await asyncio.sleep(20)
    while True:
        try:
            now=time.time()
            nxt=(int(now//CYCLE_AUTOPOST)+1)*CYCLE_AUTOPOST+60
            await asyncio.sleep(max(5,nxt-now))
            await run_autopost(app.bot)
        except asyncio.CancelledError: raise
        except Exception: log.exception("autopost")
        await asyncio.sleep(60)

def render_result_card(coin,tf,side,e,trade):
    kind=e["kind"]
    titles={"TP":f"TARGET {e.get('j',1)} HIT","SL":"STOP LOSS HIT","BE":"BREAKEVEN EXIT",
            "TRAIL":"TRAILING EXIT","TIME":"TIME EXIT"}
    rr=trade.get("R"); closed=trade.get("state")=="closed" and rr is not None
    good=(kind=="TP") or (closed and rr>0)
    accent="#00ff9c" if good else "#ff4d6d"
    fig=_new_fig(12,6.4,"#050a10")
    ax=fig.add_axes([0,0,1,1]); ax.set_xlim(0,1); ax.set_ylim(0,1); ax.axis("off")
    ax.add_patch(FancyBboxPatch((0.012,0.02),0.976,0.96,boxstyle="round,pad=0,rounding_size=0.03",
                                fc="#0b1220",ec=accent,lw=2.2,zorder=1))
    glow=[pe.withStroke(linewidth=6,foreground=accent,alpha=0.18),pe.Normal()]
    ax.text(0.5,0.84,"RYMA CRYPTO  ·  RESULT",ha="center",va="center",color=accent,fontsize=22,fontweight="bold",zorder=4,path_effects=glow)
    ax.text(0.5,0.68,f"{coin}/USDT  ·  {'LONG' if side==1 else 'SHORT'}  ·  {tf.upper()}",ha="center",va="center",color="#e2e8f0",fontsize=20,fontweight="bold",zorder=4)
    ax.text(0.5,0.50,titles.get(kind,kind),ha="center",va="center",color=accent,fontsize=36,fontweight="bold",zorder=4,path_effects=glow)
    lines=[]
    if kind=="TP": lines.append(f"Move: {float(e.get('pct',0)):+.2f}%  (partial exit)")
    if closed: lines.append(f"Result: {rr:+.2f} R")
    for k,line in enumerate(lines):
        ax.text(0.5,0.32-0.08*k,line,ha="center",va="center",color="#e2e8f0",fontsize=18,zorder=4)
    ax.text(0.5,0.08,"Source: Binance Live Data  ·  Trading involves risk  ·  Not financial advice",
            ha="center",va="center",color="#64748b",fontsize=10,zorder=4)
    return _to_png(fig)

async def send_result_image(bot,img_buf,free_ok):
    data=img_buf.getvalue()
    if VIP_CHANNEL_ID:
        try: await bot.send_photo(VIP_CHANNEL_ID,io.BytesIO(data),protect_content=PROTECT)
        except Exception as ex: log.warning(f"result vip: {str(ex)[:80]}")
    if free_ok and CHANNEL_ID:
        try: await bot.send_photo(CHANNEL_ID,io.BytesIO(data),protect_content=PROTECT)
        except Exception as ex: log.warning(f"result free: {str(ex)[:80]}")

async def trial_notice_loop(app):
    await asyncio.sleep(120)
    while True:
        try:
            today=dt.datetime.now().strftime("%Y%m%d")
            with store.lock: users=list(store.data["users"].items())
            for uid,rec in users:
                try: st,day=user_status(int(uid),rec)
                except Exception: continue
                if st!="warning" or rec.get("trial_notice")==today: continue
                lang=get_user_lang(int(uid)); left=VIP_FORCE_DAY-day
                kb=InlineKeyboardMarkup([[_btn("💎 اشترك في VIP" if lang=="ar" else "💎 Subscribe to VIP",url=VIP_INVITE_LINK)]])
                try: await app.bot.send_message(int(uid),t("warning_trial",lang,left=left),reply_markup=kb)
                except Exception: continue
                with store.lock:
                    store.data["users"][uid]["trial_notice"]=today; store.save("users")
                await asyncio.sleep(0.5)
        except asyncio.CancelledError: raise
        except Exception: log.exception("trial_notice")
        await asyncio.sleep(3600)

ALG_TZ=dt.timezone(dt.timedelta(hours=1))
MORNING_HOUR=8
async def morning_brief_loop(app):
    await asyncio.sleep(60)
    while True:
        try:
            now=dt.datetime.now(ALG_TZ)
            nxt=now.replace(hour=MORNING_HOUR,minute=0,second=0,microsecond=0)
            if nxt<=now: nxt+=dt.timedelta(days=1)
            await asyncio.sleep((nxt-now).total_seconds())
            txt=await asyncio.to_thread(build_market_summary,["BTC","ETH","BNB","SOL","XRP"])
            if txt:
                txt=txt.replace("📊 تنبيهات السوق","🌅 <b>الإحاطة الصباحية</b>",1)
                await notify_both(app.bot,txt,protect=True)
                await notify_free(app.bot,txt)
        except asyncio.CancelledError: raise
        except Exception: log.exception("morning_brief")
        await asyncio.sleep(60)

async def tracker_loop(app):
    await asyncio.sleep(30); cycle=0
    while True:
        try:
            cycle+=1
            if cycle%CLEANUP_EVERY_CYCLES==0: await asyncio.to_thread(smart_cleanup_trades)
            notes=await asyncio.to_thread(check_signals_sync)
            for coin,tf,side,e,R,trade_id in notes:
                with store.lock:
                    found=next((dict(p) for p in store.data["signals"] if p.get("id")==trade_id),None)
                if not found: continue
                if e["kind"] in ("TP","SL","BE","TRAIL","TIME"):
                    try:
                        free_ok=not (e["kind"]=="TP" and e.get("j",99)>2)
                        img=await asyncio.to_thread(render_result_card,coin,tf,side,e,found)
                        await send_result_image(app.bot,img,free_ok)
                    except Exception: log.exception("result card")
                vip_msg=event_text(coin,tf,side,e,R,"vip",trade=found)
                if vip_msg: await notify_both(app.bot,vip_msg,protect=True)
                free_msg=free_event_text(coin,tf,side,e,R,found)
                if free_msg and CHANNEL_ID: await notify_free(app.bot,free_msg)
        except asyncio.CancelledError: raise
        except Exception: log.exception("tracker")
        await asyncio.sleep(300)

async def cross_loop(app):
    await asyncio.sleep(300)
    while True:
        try:
            syms=await asyncio.to_thread(top_symbols,60)
            syms=[s for s in syms if s not in CROSS_EXCLUDE and s not in WEAK_COINS and s not in EXCLUDE_TICKERS]
            for sym in syms[:50]:
                try:
                    df=await asyncio.to_thread(get_candles,sym,"4h",450,600,200)
                    if df.empty or len(df)<210: continue
                    df=add_indicators(df,100); cross=detect_cross(df)
                    if not cross: continue
                    key=f"cross|{sym}|{cross['type']}|{cross['bar_t']}"
                    with store.lock:
                        if key in store.data["posted"]: continue
                        store.data["posted"][key]=now_s(); store.save("posted")
                    if cross["type"]=="golden": emoji="🌟"; title="تقاطع ذهبي (Golden Cross)"; side_word="🟢 إشارة شراء قوية"; decision="🟢 شراء"
                    else: emoji="💀"; title="تقاطع الموت (Death Cross)"; side_word="🔴 إشارة بيع قوية"; decision="🔴 بيع"
                    now_ts=dt.datetime.now().strftime("%Y-%m-%d %H:%M")
                    cl=coin_link(sym,"binance_fut")
                    msg=(f"{emoji} <b>{title}</b>\n━━━━━━━━━━━━━━━━━━━\n\n"
                         f"📌 <a href='{cl}'>#{sym}/USDT</a> 👆\n⏱ الفريم: <b>4H</b>\n\n"
                         f"💵 السعر: <code>{fmt(cross['price'])}</code>\n"
                         f"📈 EMA 50: <code>{fmt(cross['ema50'])}</code>\n"
                         f"📉 EMA 200: <code>{fmt(cross['ema200'])}</code>\n\n"
                         f"{side_word}\n🎯 <b>القرار المتوقع: {decision}</b>\n\n"
                         f"📅 {now_ts}\n👤 {BRAND}")
                    try: await notify_both(app.bot,msg,protect=True)
                    except Exception as _ex: log.debug('ignored: %s', _ex)
                    try: await notify_free(app.bot,msg)
                    except Exception as _ex: log.debug('ignored: %s', _ex)
                    await asyncio.sleep(3)
                except Exception: continue
        except asyncio.CancelledError: raise
        except Exception: log.exception("cross_loop")
        await asyncio.sleep(CROSS_CHECK_INTERVAL)

async def short_free_loop(app):
    await asyncio.sleep(300)
    while True:
        try:
            if CHANNEL_ID:
                rows=await asyncio.to_thread(pick_short_opportunities,5)
                if rows:
                    lines=["🔻 فرص الشورت المجانية",f"🕐 {dt.datetime.now().strftime('%Y-%m-%d %H:%M')}","━━━━━━━━━━━━━━━━",""]
                    for s,r in rows:
                        cl=coin_link(s,"binance_fut")
                        lines+=[f"💠 <a href='{cl}'>#{s}</a> 👆 — RSI {r['rsi']}",f"💵 {fmt(r['price'])}",f"🎯 القرار المتوقع: 🔴 بيع",""]
                    lines.append("💎 للأدق → VIP"); lines.append(RISK_WARNING_AR)
                    try: await app.bot.send_message(CHANNEL_ID,"\n".join(lines),protect_content=PROTECT,link_preview_options=NO_PREVIEW)
                    except Exception as _ex: log.debug('ignored: %s', _ex)
        except asyncio.CancelledError: raise
        except Exception: log.exception("short_free")
        await asyncio.sleep(CYCLE_SHORT_FREE)

async def market_summary_loop(app):
    await asyncio.sleep(180)
    FREE_SYMS=["BTC","ETH","BNB","SOL","XRP","ADA","DOGE","DOT","LINK","AVAX"]
    while True:
        try:
            if CHANNEL_ID:
                txt=await asyncio.to_thread(build_market_summary,FREE_SYMS)
                if txt:
                    try: await app.bot.send_message(CHANNEL_ID,txt,protect_content=PROTECT,link_preview_options=NO_PREVIEW)
                    except Exception as _ex: log.debug('ignored: %s', _ex)
            if VIP_CHANNEL_ID:
                txt=await asyncio.to_thread(build_market_summary,ALPHA_COINS[:10])
                if txt:
                    try: await app.bot.send_message(VIP_CHANNEL_ID,txt,protect_content=PROTECT,link_preview_options=NO_PREVIEW)
                    except Exception as _ex: log.debug('ignored: %s', _ex)
        except asyncio.CancelledError: raise
        except Exception: log.exception("market_summary")
        await asyncio.sleep(CYCLE_MARKET_SUMMARY)

async def predict_loop(app):
    await asyncio.sleep(120)
    while True:
        try:
            if VIP_CHANNEL_ID:
                syms=await asyncio.to_thread(top_symbols,40)
                preds=await asyncio.to_thread(predictive_scan,syms[:30])
                if preds:
                    lines=["🔮 رادار التنبؤ بالانفجارات","━━━━━━━━━━━━━━━━",""]
                    for p in preds[:7]:
                        icon="🟢" if p["direction"]=="LONG" else "🔴"
                        decision="🟢 شراء" if p["direction"]=="LONG" else "🔴 بيع"
                        reasons=" | ".join(p["reasons"][:3])
                        lines+=[f"{icon} #{p['sym']} ({p['score']}/15)",f"   {reasons}",f"   🎯 القرار المتوقع: {decision}"]
                    await app.bot.send_message(VIP_CHANNEL_ID,"\n".join(lines),protect_content=PROTECT,link_preview_options=NO_PREVIEW)
        except asyncio.CancelledError: raise
        except Exception: log.exception("predict")
        await asyncio.sleep(CYCLE_PREDICT)

async def alpha_summary_loop(app):
    await asyncio.sleep(240)
    while True:
        try:
            if VIP_CHANNEL_ID:
                syms=await asyncio.to_thread(top_symbols,60); rows=[]
                for s in syms[:50]:
                    r=await asyncio.to_thread(alpha_radar,s)
                    if r: rows.append((s,r))
                if rows:
                    rows.sort(key=lambda x:-x[1]["change_7d"])
                    lines=["⭐ رادار عملات Alpha",f"🕐 {dt.datetime.now().strftime('%Y-%m-%d %H:%M')}","━━━━━━━━━━━━━━━━",""]
                    for s,r in rows[:10]:
                        lines+=[f"⭐ #{s}",f"   7ي {r['change_7d']:+.2f}% | 30ي {r['change_30d']:+.2f}%",f"   RSI {r['rsi']}",f"   🎯 القرار المتوقع: 🟢 شراء"]
                    await app.bot.send_message(VIP_CHANNEL_ID,"\n".join(lines),protect_content=PROTECT,link_preview_options=NO_PREVIEW)
        except asyncio.CancelledError: raise
        except Exception: log.exception("alpha_summary")
        await asyncio.sleep(CYCLE_ALPHA_SUMMARY)

async def bottoms_auto_loop(app):
    await asyncio.sleep(300)
    while True:
        try:
            if VIP_CHANNEL_ID:
                syms=await asyncio.to_thread(top_symbols,70); rows=[]
                for s in syms[:55]:
                    r=await asyncio.to_thread(bottom_radar,s)
                    if r: rows.append((s,r))
                if rows:
                    rows.sort(key=lambda x:x[1]["diff"])
                    lines=["📉 رادار القيعان الذكي",f"🕐 {dt.datetime.now().strftime('%Y-%m-%d %H:%M')}","━━━━━━━━━━━━━━━━",""]
                    for s,r in rows[:10]:
                        lines+=[f"{r['severity']} #{s}",f"   {r['diff']}% تحت EMA200 | RSI {r['rsi']}",
                                f"   تعافي: +{r['rebound']}% | حجم: x{r['vol_ratio']}",f"   💵 {fmt(r['price'])}",f"   🎯 القرار المتوقع: 🟢 شراء"]
                    await app.bot.send_message(VIP_CHANNEL_ID,"\n".join(lines),protect_content=PROTECT,link_preview_options=NO_PREVIEW)
        except asyncio.CancelledError: raise
        except Exception: log.exception("bottoms_auto")
        await asyncio.sleep(CYCLE_BOTTOMS_AUTO)

async def golden_loop(app):
    await asyncio.sleep(600); _golden_posted={}
    while True:
        try:
            if len(_golden_posted)>500:
                cutoff=time.time()-14*DAY
                _golden_posted={k:v for k,v in _golden_posted.items() if v>cutoff}
            log.info("golden_loop: بدء البحث الشامل")
            results=await asyncio.to_thread(golden_scan,2,None)
            for g in results:
                key=f"{g['sym']}:{g['side']}"
                if key in _golden_posted and time.time()-_golden_posted[key]<48*3600: continue
                _golden_posted[key]=time.time()
                txt=format_golden_post(g,"vip")
                try: await notify_both(app.bot,txt,protect=True)
                except Exception as _ex: log.debug('ignored: %s', _ex)
                log.info(f"الفرصة الذهبية: {g['sym']} (score={g['score']}) - VIP فقط")
                await asyncio.sleep(3)
        except asyncio.CancelledError: raise
        except Exception: log.exception("golden_loop")
        await asyncio.sleep(CYCLE_GOLDEN)

_whale_day = {"date": "", "count": 0}
async def whale_hunter_loop(app):
    await asyncio.sleep(60)
    log.info("whale_hunter_loop: بدء مراقبة الحيتان")
    _seen_whales=set()
    while True:
        try:
            today = dt.datetime.now().strftime("%Y%m%d")
            if _whale_day["date"] != today:
                _whale_day["date"] = today
                _whale_day["count"] = 0
            
            if _whale_day["count"] >= 5:
                await asyncio.sleep(300)
                continue

            syms=await asyncio.to_thread(top_symbols,20)
            for s in syms[:10]:
                if _whale_day["count"] >= 5: break
                try:
                    ex=get_ex("binance")
                    with _ex_lock["binance"]: trades=ex.fetch_trades(f"{s}/USDT",limit=50)
                    for tr in trades:
                        if _whale_day["count"] >= 5: break
                        notional=float(tr.get("price",0))*float(tr.get("amount",0))
                        if notional >= WHALE_MIN_USD:
                            trade_id=f"{s}_{tr.get('id')}"
                            if trade_id in _seen_whales: continue
                            _seen_whales.add(trade_id)
                            if len(_seen_whales)>2000:
                                _seen_whales=set(list(_seen_whales)[-1000:])
                            side=tr.get("side","buy"); price=float(tr.get("price",0))
                            text,share_url=build_whale_tweet(s,side,notional,price)
                            kb=InlineKeyboardMarkup([
                                [_btn("🚀 انشر على تويتر (X)",url=share_url)],
                                [_btn("📋 رابط التسجيل فقط",url=REFERRAL_LINKS["binance"][1])]])
                            await app.bot.send_message(ADMIN_ID,f"🚨 <b>صيد جديد (جاهز للنشر):</b>\n\n{text}",
                                parse_mode=ParseMode.HTML,reply_markup=kb,link_preview_options=NO_PREVIEW)
                            _whale_day["count"] += 1
                except Exception: continue
        except asyncio.CancelledError: raise
        except Exception: log.exception("whale_hunter")
        await asyncio.sleep(120)

async def promo_loop(app):
    await asyncio.sleep(3600)
    while True:
        try:
            txt=build_promo_post()
            try: await notify_both(app.bot,txt)
            except Exception as _ex: log.debug('ignored: %s', _ex)
            try: await notify_free(app.bot,txt)
            except Exception as _ex: log.debug('ignored: %s', _ex)
            log.info("promo_loop: نشر المنشور الترويجي")
        except asyncio.CancelledError: raise
        except Exception: log.exception("promo")
        await asyncio.sleep(CYCLE_PROMO)

def build_weekly_report():
    since = (time.time() - 7*DAY) * 1000
    with store.lock:
        h = [x for x in store.data["history"] if (x.get("closed") or 0) >= since]
    if len(h) < 5: return None
    R = np.array([float(x.get("R") or 0) for x in h], dtype=float)
    wins = int((R > 0).sum()); g = R[R > 0].sum(); l = -R[R <= 0].sum()
    pf = round(float(g/l), 2) if l > 0 else 999.0
    best = max(h, key=lambda x: x.get("R") or 0)
    lines = ["📊 <b>التقرير الأسبوعي</b>", f"🕐 {dt.datetime.now().strftime('%Y-%m-%d')}",
             "━━━━━━━━━━━━━━━━", "",
             f"📈 الصفقات المغلقة: <b>{len(R)}</b>",
             f"✅ نسبة النجاح: <b>{100*wins/len(R):.1f}%</b>",
             f"⚖️ معامل الربح: <b>{pf}</b>",
             f"💰 صافي النتيجة: <b>{R.sum():+.1f}R</b>", "",
             f"🏆 أفضل صفقة: #{best['coin']} {float(best.get('R') or 0):+.2f}R", "",
             "⚠️ الأرقام من الصفقات المسجلة فعلياً فقط، ونتائج الماضي لا تضمن المستقبل.",
             RISK_WARNING_AR]
    return "\n".join(lines)

async def weekly_report_loop(app):
    await asyncio.sleep(900)
    while True:
        try:
            txt = await asyncio.to_thread(build_weekly_report)
            if txt:
                await notify_both(app.bot, txt, protect=True)
                await notify_free(app.bot, txt)
        except asyncio.CancelledError: raise
        except Exception: log.exception("weekly_report")
        await asyncio.sleep(CYCLE_PROMO)

def recently_posted(sym, hours=6):
    cut = now_s() - hours*3600
    with store.lock:
        return any(k.startswith(f"{sym}:") and v > cut for k, v in store.data["posted"].items())

def predictive_scan(symbols):
    out=[]
    for s in symbols:
        try:
            df=get_candles(s,PREDICT_TF,250,ttl=600,min_bars=100)
            if df.empty or len(df)<100: continue
            df=add_indicators(df,100); last=df.iloc[-1]
            if pd.isna(last["bb_up"]) or pd.isna(last["bb_lo"]): continue
            bbw=float(last["bb_up"])-float(last["bb_lo"])
            bbw_avg=float((df["bb_up"]-df["bb_lo"]).tail(50).mean())
            if bbw_avg<=0: continue
            bb_sqz=bbw/bbw_avg
            rvol=float(last["rvol"]) if not pd.isna(last["rvol"]) else 0
            rsi_v=float(last["rsi"]) if not pd.isna(last["rsi"]) else 50
            score=0; reasons=[]
            if bb_sqz<PREDICT_BB_SQZ: score+=4; reasons.append(f"ضغط BB {bb_sqz:.2f}")
            if rvol>=PREDICT_VOL_BUILDUP: score+=3; reasons.append(f"حجم x{rvol:.2f}")
            if PREDICT_RSI_NEUTRAL[0]<=rsi_v<=PREDICT_RSI_NEUTRAL[1]: score+=2; reasons.append(f"RSI محايد {rsi_v:.0f}")
            try:
                oi=futures_oi_history(s,"4h",20)
                if oi and abs(oi.get("change_pct",0))>=PREDICT_OI_CHANGE_MIN:
                    score+=3; reasons.append(f"OI {oi['change_pct']:+.1f}%")
            except Exception as _ex: log.debug('ignored: %s', _ex)
            if score<5: continue
            direction="LONG" if float(last["ema20"])>=float(last["ema50"]) else "SHORT"
            out.append(dict(sym=s,score=score,direction=direction,reasons=reasons))
        except Exception: continue
    out.sort(key=lambda x:-x["score"])
    return out[:PREDICT_TOP_N]

def simulate(df,sym,tf,start=210):
    n=len(df); o,h,l,c=(df[k].values for k in ("open","high","low","close"))
    atr,t=df["atr"].values,df["t"].values
    trades,i=[],start; is_meme=sym in MEME_COINS
    while i<n-1:
        if is_meme: long_ok=detect_meme_conditions(df,i,1); short_ok=detect_meme_conditions(df,i,-1)
        else: long_ok=detect_core_conditions(df,i,1); short_ok=detect_core_conditions(df,i,-1)
        if long_ok and not short_ok: side=1
        elif short_ok and not long_ok: side=-1
        elif long_ok and short_ok: side=1 if df["rsi"].iloc[i]<50 else -1
        else: i+=1; continue
        grade=grade_setup(df,i,side)
        lv=compute_meme_levels(df,i,side) if is_meme else compute_core_levels(df.iloc[:i+1],i,side)
        if lv is None: i+=1; continue
        p=new_trade(sym,tf,side,lv,int(t[i]),grade,"Core" if not is_meme else "Meme")
        j=i+1
        while j<n:
            trade_step(p,o[j],h[j],l[j],c[j],atr[j])
            if p["state"] in ("closed","expired"): break
            j+=1
        if p["state"]=="open":
            ev=[]; close_at(p,float(c[n-1]),"TIME",ev); j=n-1
        if p["state"]=="closed" and p.get("R") is not None:
            trades.append(dict(coin=sym,t=int(t[min(j,n-1)]),R=p["R"],exit=p["result"],
                               setup=p["setup"],grade=p["grade"],side=side,bars=p["bars"],
                               tps=sum(1 for x in p["tps"] if x["hit"])))
        i=j+1
    return trades

def binance_futures_klines_long(sym, tf, bars):
    interval = BINANCE_INTERVAL_MAP.get(tf)
    if not interval: return None
    out, end = [], None
    try:
        while len(out) < bars:
            params = {"symbol": f"{sym}USDT", "interval": interval, "limit": 1500}
            if end: params["endTime"] = end
            r = requests.get(f"{BINANCE_FAPI_BASE}/fapi/v1/klines", params=params, timeout=10)
            if r.status_code != 200: break
            data = r.json()
            if not isinstance(data, list) or not data: break
            out = [[int(k[0]), float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5])] for k in data] + out
            end = int(data[0][0]) - 1
            if len(data) < 1500: break
    except Exception as _ex: log.debug('ignored: %s', _ex)
    return out[-bars:] if out else None

def backtest_symbol(sym, years=3.0):
    """يرجع (الصفقات، بداية فترة التقييم، نهايتها). لا يفترض أن مصدر البيانات نجح."""
    try:
        years = float(years)
    except (TypeError, ValueError):
        years = 3.0
    if not np.isfinite(years) or years <= 0:
        raise ValueError("مدة الباك تست يجب أن تكون أكبر من صفر")
    years = min(years, 10.0)
    need = int(years * 6 * 365) + 300
    rows = binance_futures_klines_long(sym, "4h", need)
    if rows and len(rows) >= 400:
        df = _rows_to_df(rows, "4h", need)
    else:
        df = get_candles(sym, "4h", need, ttl=900)
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        raise NoData(f"{sym}: لم تُرجع مصادر البيانات شموعاً")
    if not {"t", "open", "high", "low", "close", "volume"}.issubset(df.columns):
        raise ValueError(f"{sym}: أعمدة الشموع غير مكتملة")
    df = df.dropna(subset=["t", "open", "high", "low", "close", "volume"]).copy()
    if len(df) < 400:
        if len(df) >= 2:
            return [], int(df["t"].iloc[0]), int(df["t"].iloc[-1])
        raise NoData(f"{sym}: شموع غير كافية ({len(df)}/400)")
    raw = df[["t", "open", "high", "low", "close", "volume"]].reset_index(drop=True)
    trs = se.simulate(raw, None, start=260)
    for x in trs:
        x.update(coin=sym, setup="Engine")
    return trs, int(df["t"].iloc[260]), int(df["t"].iloc[-1])

def metrics(trs):
    if not trs: return dict(n=0,wr=0.0,pf=0.0,total=0.0,avg=0.0,dd=0.0,tp1=0.0)
    srt=sorted(trs,key=lambda z:z["t"]); R=np.array([x["R"] for x in srt],dtype=float)
    g,l=R[R>0].sum(),-R[R<=0].sum(); eq=np.cumsum(R)
    return dict(n=len(R),wr=round(100*float((R>0).mean()),1),pf=round(float(g/l),2) if l>0 else 999.0,
                total=round(float(R.sum()),1),avg=round(float(R.mean()),3),
                dd=round(float((np.maximum.accumulate(eq)-eq).max()),1),
                tp1=round(100*float(np.mean([x.get("tps",0)>=1 for x in srt])),1))

def bt_report(per,tmin,tmax,label,errors=None):
    errors = errors or {}
    allt = [x for tr in (per or {}).values() if isinstance(tr, list) for x in tr
            if isinstance(x, dict) and isinstance(x.get("t"), (int, float)) and isinstance(x.get("R"), (int, float))]
    valid_range = (isinstance(tmin, (int, float)) and isinstance(tmax, (int, float)) and tmax > tmin)
    lines = [f"📊 Backtest {label} ({len(per or {})} عملة صالحة) · {BOT_VERSION}",
             f"📥 العملات التي فشل تحميلها/اختبارها: {len(errors)}"]
    if not valid_range:
        lines += ["", "⚠️ لا توجد فترة زمنية صالحة لتقسيم TRAIN وTEST.",
                  "لم يتم حساب نسب نجاح أو ربح وهمية."]
    elif not allt:
        lines += ["", "⚠️ تم تحميل البيانات لكن لم ينتج محرك الاختبار أي صفقة.",
                  "لا يمكن الحكم على الاستراتيجية من عينة بلا صفقات."]
    else:
        cut = tmin + 0.65 * (tmax - tmin)
        tr = [x for x in allt if x["t"] < cut]
        te = [x for x in allt if x["t"] >= cut]
        mt, me = metrics(tr), metrics(te)
        lines += ["", f"📈 TRAIN: n={mt['n']} | WR {mt['wr']}% | PF {mt['pf']} | {mt['total']:+}R",
                  f"📊 TEST : n={me['n']} | WR {me['wr']}% | PF {me['pf']} | {me['total']:+}R",
                  f"🎯 وصول TP1: {me['tp1']}%", ""]
        if me["n"] < 30:
            verdict = "⚪ عينة الاختبار صغيرة — لا يعتمد عليها للحكم"
        else:
            verdict = "🔴 ضعيف" if me["pf"] < 1.1 else ("🟡 مقبول" if me["pf"] < 1.5 else ("✅ جيد" if me["pf"] < 1.8 else "🏆 ممتاز"))
        lines.append(f"الحكم: {verdict}")
    if errors:
        lines += ["", "⚠️ أمثلة على أسباب فشل العملات:"]
        for sym, err in list(errors.items())[:5]:
            lines.append(f"• {sym}: {str(err)[:110]}")
    return "\n".join(lines), ""

def backtest_many(symbols, years, max_workers=4):
    """تحميل/اختبار العملات بالتوازي المحدود لتقليل وقت TOP50 مع حفظ أخطاء كل عملة."""
    per, tmin, tmax, errors = {}, None, None, {}
    symbols = list(dict.fromkeys(s.strip().upper() for s in (symbols or [])
                                 if isinstance(s, str) and s.strip()))
    if not symbols:
        return per, tmin, tmax, errors

    def _one(sym):
        try:
            trs, a, b = backtest_symbol(sym, years)
            if a is None or b is None:
                return sym, None, "لا توجد فترة بيانات صالحة"
            if not isinstance(trs, list):
                return sym, None, "محرك الاختبار لم يُرجع قائمة صفقات صالحة"
            return sym, (trs, int(a), int(b)), None
        except Exception as ex:
            log.exception("backtest_symbol failed for %s", sym)
            return sym, None, f"{type(ex).__name__}: {ex}"

    workers = max(1, min(int(max_workers or 1), 4, len(symbols)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        # executor.map يحافظ على ترتيب الرموز، بينما ينفذ الطلبات بالتوازي.
        for sym, result, error in pool.map(_one, symbols):
            if error:
                errors[sym] = error
                continue
            trs, a, b = result
            per[sym] = trs
            tmin = a if tmin is None else min(tmin, a)
            tmax = b if tmax is None else max(tmax, b)
    return per, tmin, tmax, errors

def admin_only(fn):
    @functools.wraps(fn)
    async def wrapper(update,ctx):
        if not ADMIN_ID or ADMIN_ID == 0 or not update.effective_user or update.effective_user.id!=ADMIN_ID:
            lang=get_user_lang(update.effective_user.id) if update.effective_user else "ar"
            await update.message.reply_text(t("admin_only",lang)); return
        try: return await fn(update,ctx)
        except Exception as e:
            log.exception("admin"); await update.message.reply_text(f"⚠️ {str(e)[:200]}")
    return wrapper

def vip_only(fn):
    @functools.wraps(fn)
    async def wrapper(update,ctx):
        u=update.effective_user
        if not u: return
        lang=get_user_lang(u.id)
        rec=store.data["users"].get(str(u.id),{})
        st,_=user_status(u.id,rec)
        if st not in ("admin","vip","trial","warning"):
            kb=InlineKeyboardMarkup([[_btn("💎 اشترك في VIP" if lang=="ar" else "💎 Subscribe to VIP",url=VIP_INVITE_LINK)]])
            await update.message.reply_text(t("vip_only",lang),reply_markup=kb); return
        try: return await fn(update,ctx)
        except Exception as e:
            log.exception("vip"); await update.message.reply_text(f"⚠️ {str(e)[:200]}")
    return wrapper

_cb_last={}
def cb_throttled(uid,secs=5):
    now=time.time()
    if now-_cb_last.get(uid,0)<secs: return True
    _cb_last[uid]=now; return False

def clean_symbol(text):
    s=(text or "").upper().strip().replace("$","").replace("/","").replace("-","")
    for suf in ("USDT","USDC","PERP","USD"):
        if s.endswith(suf) and len(s)>len(suf): s=s[:-len(suf)]
    return s if re.fullmatch(r"[A-Z0-9]{1,12}",s) else ""

_cooldown={}

async def gate(update):
    user=update.effective_user
    lang=get_user_lang(user.id)
    if is_maintenance() and user.id!=ADMIN_ID:
        await update.message.reply_text(t("maintenance",lang,contact=(CONTACT_LINK or CHANNEL_LINK))); return "maintenance",False
    rec=touch_user(user); st,day=user_status(user.id,rec)
    if st=="blocked":
        await update.message.reply_text(t("blocked",lang,contact=(CONTACT_LINK or CHANNEL_LINK))); return st,False
    if st=="warning":
        left=VIP_FORCE_DAY-day
        await update.message.reply_text(t("warning_trial",lang,left=left))
    if st not in ("admin", "grace"):
        if time.time()-_cooldown.get(user.id,0)<3:
            await update.message.reply_text(t("wait_3s",lang)); return st,False
        _cooldown[user.id]=time.time()
    return st,True

async def cmd_start(update,ctx):
    if is_maintenance() and update.effective_user.id!=ADMIN_ID:
        lang=get_user_lang(update.effective_user.id)
        await update.message.reply_text(t("maintenance",lang,contact=(CONTACT_LINK or CHANNEL_LINK))); return
    u=update.effective_user; uid=str(u.id); lang=get_user_lang(u.id)
    if ctx.args and len(ctx.args)>0 and ctx.args[0].startswith("ref_"):
        try:
            referrer_id=int(ctx.args[0][4:])
            if referrer_id!=u.id:
                is_new=uid not in store.data["users"]
                if is_new:
                    awarded=False
                    with store.lock:
                        ref_uid=str(referrer_id)
                        if ref_uid in store.data["users"]:
                            store.data["users"][ref_uid]["points"]=store.data["users"][ref_uid].get("points",0)+POINTS_REFERRAL
                            store.save("users"); awarded=True
                    if awarded:
                        new_pts,reward=check_points_reward(referrer_id)
                        try:
                            ref_lang=get_user_lang(referrer_id)
                            if ref_lang=="en":
                                msg=f"🎉 Someone joined via your link!\n💰 +{POINTS_REFERRAL} points\n📊 Balance: {new_pts}/{VIP_THRESHOLD}"
                                if reward: msg+=f"\n\n🎁 <b>Congrats!</b> Reached {VIP_THRESHOLD} → VIP {reward} day free!"
                            else:
                                msg=f"🎉 شخص جديد انضم عبر رابطك!\n💰 +{POINTS_REFERRAL} نقطة\n📊 رصيدك: {new_pts}/{VIP_THRESHOLD}"
                                if reward: msg+=f"\n\n🎁 <b>مبروك!</b> وصلت {VIP_THRESHOLD} → VIP {reward} يوم مجاناً!"
                            await ctx.bot.send_message(referrer_id,msg,parse_mode=ParseMode.HTML)
                        except Exception as _ex: log.debug('ignored: %s', _ex)
        except Exception as _ex: log.debug('ignored: %s', _ex)
    rec=touch_user(u,"/start"); st,day=user_status(u.id,rec)
    reward_pts,reward_days=check_points_reward(u.id)
    if reward_days:
        try:
            if lang=="en": reward_msg=f"🎉 <b>Congrats!</b> Reached {VIP_THRESHOLD} points\n✅ You got <b>VIP {reward_days} day free</b>!"
            else: reward_msg=f"🎉 <b>مبروك!</b> وصلت {VIP_THRESHOLD} نقطة\n✅ حصلت على <b>VIP {reward_days} يوم مجاناً</b>!"
            await update.message.reply_text(reward_msg,parse_mode=ParseMode.HTML)
        except Exception as _ex: log.debug('ignored: %s', _ex)
        rec["points"]=reward_pts
    bot_username=ctx.application.bot_data.get("username","")
    ref_link=f"https://t.me/{bot_username}?start=ref_{u.id}" if bot_username else None
    if lang=="en":
        rows=[[_btn("📊 My Portfolio",callback_data="my_portfolio")]]
        if ref_link: rows.append([_btn("🔗 Referral Link",copy_text=CopyTextButton(text=ref_link))])
        rows+=[ [_btn("💎 Subscribe to VIP ✨",url=VIP_INVITE_LINK)],
            [_btn("🆓 Signals Channel",url=CHANNEL_LINK)],
            [_btn("📞 Contact",url=CONTACT_URL)]]
        ref_text=f"\n\n🔗 Your link:\n<code>{ref_link}</code>" if ref_link else ""
        welcome=(f"Welcome {u.first_name}! 👋\n\nSend any coin symbol to analyze.\n\n"
            f"{points_visual(rec.get('points',0))}\n\n📌 <b>Ways to earn points:</b>\n"
            f"• Daily login = <b>{POINTS_DAILY}</b>\n• Share channel/bot link = <b>{POINTS_PROMO} points</b>\n"
            f"• Invite new subscriber = <b>{POINTS_REFERRAL} points</b>\n\n"
            f"🎁 {VIP_THRESHOLD} points = VIP {VIP_REWARD_DAYS} day free{ref_text}")
    else:
        rows=[[_btn("📊 محفظتي",callback_data="my_portfolio")]]
        if ref_link: rows.append([_btn("🔗 رابط الإحالة",copy_text=CopyTextButton(text=ref_link))])
        rows+=[ [_btn("💎 اشترك في VIP ✨",url=VIP_INVITE_LINK)],
            [_btn("🆓 قناة التوصيات",url=CHANNEL_LINK)],
            [_btn("📞 تواصل",url=CONTACT_URL)]]
        ref_text=f"\n\n🔗 رابطك:\n<code>{ref_link}</code>" if ref_link else ""
        welcome=(f"أهلاً {u.first_name}! 👋\n\nأرسل اسم أي عملة لتحليلها.\n\n"
            f"{points_visual(rec.get('points',0))}\n\n📌 <b>طرق جمع النقاط:</b>\n"
            f"• دخول يومي = <b>{POINTS_DAILY}</b>\n• نشر رابط القناة أو البوت = <b>{POINTS_PROMO} نقاط</b>\n"
            f"• دعوة مشترك جديد = <b>{POINTS_REFERRAL} نقطة</b>\n\n"
            f"🎁 {VIP_THRESHOLD} نقطة = VIP {VIP_REWARD_DAYS} يوم مجاناً{ref_text}")
    await update.message.reply_text(welcome,reply_markup=InlineKeyboardMarkup(rows),
                                    parse_mode=ParseMode.HTML,link_preview_options=NO_PREVIEW)

async def cmd_help(update,ctx):
    touch_user(update.effective_user,"/help")
    u=update.effective_user; lang=get_user_lang(u.id)
    rec=store.data["users"].get(str(u.id),{})
    st,_=user_status(u.id,rec)
    if st in ("admin","vip"):
        await update.message.reply_text(
            "ℹ️ <b>مساعدة VIP</b>\n\n📝 إرسال رمز عملة → تحليل + 4 أهداف\n"
            "📋 /menu — قائمة VIP\n💎✨ /golden — الفرصة الذهبية\n"
            "🎯 /daily — صفقة اليوم\n📊 /futures BTC — الفيوتشر\n"
            "📈 /portfolio — محفظتي\n🎯 /points — نقاطي\n🔗 /myref — رابط الإحالة"
            if lang=="ar" else
            "ℹ️ <b>VIP Help</b>\n\n📝 Send coin symbol → analysis + 4 targets\n"
            "📋 /menu — VIP menu\n💎✨ /golden — Golden Setup\n"
            "🎯 /daily — Daily trade\n📊 /futures BTC — Futures data\n"
            "📈 /portfolio — My portfolio\n🎯 /points — My points\n🔗 /myref — Referral link",
            parse_mode=ParseMode.HTML); return
    await update.message.reply_text(
        "ℹ️ مساعدة Ryma Crypto\n\n📝 إرسال رمز عملة للتحليل\n🆔 /myid\n💎 /vip\n📈 /portfolio\n🎯 /points\n🔗 /myref"
        if lang=="ar" else
        "ℹ️ Ryma Crypto Help\n\n📝 Send coin symbol to analyze\n🆔 /myid\n💎 /vip\n📈 /portfolio\n🎯 /points\n🔗 /myref")

async def cmd_myid(update,ctx):
    touch_user(update.effective_user,"/myid")
    await update.message.reply_text(f"🆔 رقم الحساب: {update.effective_user.id}")

async def cmd_vip(update,ctx):
    u=update.effective_user; touch_user(u,"/vip")
    kb=InlineKeyboardMarkup([
        [_btn("💎 اشترك الآن ✨",url=VIP_INVITE_LINK)],
        [_btn("🆓 قناة التوصيات المجانية",url=CHANNEL_LINK)],
        [_btn("📞 تواصل معنا",url=CONTACT_URL)]])
    await update.message.reply_text(build_vip_page(),reply_markup=kb)

async def cmd_menu(update,ctx):
    touch_user(update.effective_user,"/menu")
    u=update.effective_user; rec=store.data["users"].get(str(u.id),{})
    st,_=user_status(u.id,rec)
    if st=="admin":
        await update.message.reply_text(build_admin_menu_text(),reply_markup=keyboard_admin_menu(),parse_mode=ParseMode.HTML); return
    if st not in ("vip","trial","warning"):
        kb=InlineKeyboardMarkup([[_btn("💎 اشترك في VIP",url=VIP_INVITE_LINK)]])
        await update.message.reply_text("🔒 القائمة لمشتركي VIP فقط",reply_markup=kb); return
    await update.message.reply_text(build_vip_menu_text(),reply_markup=keyboard_vip_menu())

async def cmd_daily(update,ctx):
    u=update.effective_user
    if not u: return
    rec=store.data["users"].get(str(u.id),{}); st,_=user_status(u.id,rec)
    if st not in ("admin","vip","trial","warning"):
        kb=InlineKeyboardMarkup([[_btn("💎 اشترك في VIP",url=VIP_INVITE_LINK)]])
        await update.message.reply_text("🔒 الأمر حصري للمشتركين",reply_markup=kb); return
    touch_user(u,"/daily")
    wait=await update.message.reply_text("🎯 جلب Top Movers...")
    try:
        longs,shorts=await asyncio.to_thread(_build_daily_lists)
        if not longs and not shorts:
            await wait.edit_text("🎯 لا توجد صفقات مطابقة حالياً بمعايير العقل المركزي."); return
        await wait.delete()
        for idx,item in enumerate(longs,1):
            txt=_format_daily_card(item["res"],f"🟢 LONG {idx}",item.get("change",0),item.get("volume",0))
            await update.message.reply_text(txt,parse_mode=ParseMode.HTML,link_preview_options=NO_PREVIEW)
            await asyncio.sleep(0.7)
        for idx,item in enumerate(shorts,1):
            txt=_format_daily_card(item["res"],f"🔴 SHORT {idx}",item.get("change",0),item.get("volume",0))
            await update.message.reply_text(txt,parse_mode=ParseMode.HTML,link_preview_options=NO_PREVIEW)
            await asyncio.sleep(0.7)
        await update.message.reply_text(f"📊 النتيجة: {len(longs)} LONG + {len(shorts)} SHORT\n\n"
            f"💡 الصفقات جاهزة للنشر يدوياً\n\n{RISK_WARNING_AR}",parse_mode=ParseMode.HTML)
    except Exception as e:
        log.exception("daily"); await wait.edit_text(f"⚠️ {str(e)[:200]}")

async def cmd_portfolio(update,ctx):
    u=update.effective_user; uid=str(u.id); touch_user(u,"/portfolio")
    port=store.data["portfolios"].get(uid,[])
    if not port:
        await update.message.reply_text("محفظتك فارغة. استخدم زر (إضافة للمحفظة) في أي تحليل."); return
    msg="📊 <b>محفظتك الاستثمارية:</b>\n\n"; total_pnl=0
    for item in port:
        p,_=live_price(item["sym"])
        if p:
            pnl=(p-item["entry"])*(item["amount"]/item["entry"]); total_pnl+=pnl
            emoji="🟢" if pnl>=0 else "🔴"
            msg+=f"{emoji} <b>#{item['sym']}</b> | دخل: {fmt(item['entry'])} | حالي: {fmt(p)}\n"
            msg+=f"   استثمرت: ${item['amount']} | الربح: {pnl:+.2f}$\n\n"
    msg+=f"━━━━━━━━━━━━━━━\nإجمالي الأرباح/الخسائر: <b>{total_pnl:+.2f}$</b>"
    await update.message.reply_text(msg,parse_mode=ParseMode.HTML)

async def cmd_myref(update,ctx):
    u=update.effective_user
    bot_username=ctx.application.bot_data.get("username","")
    if not bot_username:
        await update.message.reply_text("⚠️ غير متاح حالياً، جرب بعد دقيقة"); return
    ref_link=f"https://t.me/{bot_username}?start=ref_{u.id}"
    rec=store.data["users"].get(str(u.id),{}); pts=rec.get("points",0)
    await update.message.reply_text(
        f"🔗 <b>رابط الإحالة الخاص بك:</b>\n\n<code>{ref_link}</code>\n\n"
        f"💰 كل مشترك جديد = <b>{POINTS_REFERRAL} نقطة</b>\n\n"
        f"🎯 نقاطك: <b>{pts}/{VIP_THRESHOLD}</b>\n{points_visual(pts)}",
        parse_mode=ParseMode.HTML,link_preview_options=NO_PREVIEW)

async def cmd_points(update,ctx):
    u=update.effective_user; rec=store.data["users"].get(str(u.id),{}); pts=rec.get("points",0)
    bot_username=ctx.application.bot_data.get("username","")
    ref_link=f"https://t.me/{bot_username}?start=ref_{u.id}" if bot_username else "—"
    await update.message.reply_text(
        f"🎯 <b>نقاطك:</b> {pts}/{VIP_THRESHOLD}\n\n{points_visual(pts)}\n\n"
        f"📌 <b>طرق جمع النقاط:</b>\n• دخول يومي = <b>{POINTS_DAILY}</b>\n"
        f"• نشر رابط القناة أو البوت = <b>{POINTS_PROMO} نقاط</b>\n"
        f"• دعوة مشترك جديد = <b>{POINTS_REFERRAL} نقطة</b>\n\n"
        f"🔗 <b>رابط البوت (إحالتك):</b>\n<code>{ref_link}</code>\n\n"
        f"📢 <b>قناة التوصيات:</b>\n<code>{CHANNEL_LINK}</code>\n\n"
        f"🎁 عند {VIP_THRESHOLD} نقطة = <b>VIP {VIP_REWARD_DAYS} يوم مجاناً</b>",
        parse_mode=ParseMode.HTML)

@admin_only
async def cmd_addpoints(update,ctx):
    if len(ctx.args)<2:
        await update.message.reply_text(
            "📝 <b>الصيغة:</b>\n/addpoints ID POINTS [reason]\n\n"
            f"مثال: <code>/addpoints 123456789 {POINTS_PROMO} ترويج</code>",parse_mode=ParseMode.HTML); return
    try:
        target_uid=int(ctx.args[0]); pts=int(ctx.args[1])
        reason=" ".join(ctx.args[2:]) if len(ctx.args)>2 else "مكافأة"
        with store.lock:
            exists=str(target_uid) in store.data["users"]
            if exists:
                store.data["users"][str(target_uid)]["points"]=store.data["users"][str(target_uid)].get("points",0)+pts
                store.save("users"); new_pts=store.data["users"][str(target_uid)]["points"]
        if not exists:
            await update.message.reply_text("❌ المستخدم غير موجود"); return
        await update.message.reply_text(
            f"✅ +{pts} نقطة → <code>{target_uid}</code>\n📝 السبب: {reason}\n📊 رصيده: {new_pts}/{VIP_THRESHOLD}",
            parse_mode=ParseMode.HTML)
        final_pts,reward=check_points_reward(target_uid)
        try:
            msg=f"🎁 حصلت على <b>+{pts} نقطة</b> ({reason})\n📊 رصيدك: {new_pts}/{VIP_THRESHOLD}"
            if reward: msg+=f"\n\n🎉 <b>مبروك!</b> {VIP_THRESHOLD} نقطة → VIP {reward} يوم مجاناً!"
            await ctx.bot.send_message(target_uid,msg,parse_mode=ParseMode.HTML)
        except Exception as _ex: log.debug('ignored: %s', _ex)
    except ValueError: await update.message.reply_text("⚠️ صيغة خاطئة")

@admin_only
async def cmd_goldenconfig(update,ctx):
    await update.message.reply_text(build_golden_config_text(),reply_markup=keyboard_golden_config(),parse_mode=ParseMode.HTML)

@admin_only
async def cmd_cross(update,ctx):
    wait=await update.message.reply_text("🌟 جارٍ فحص التقاطعات...")
    try:
        syms=await asyncio.to_thread(top_symbols,60)
        syms=[s for s in syms if s not in CROSS_EXCLUDE and s not in WEAK_COINS and s not in EXCLUDE_TICKERS]
        found=[]
        for sym in syms[:50]:
            try:
                df=await asyncio.to_thread(get_candles,sym,"4h",450,600,200)
                if df.empty or len(df)<210: continue
                df=add_indicators(df,100); cross=detect_cross(df)
                if cross: found.append((sym,cross))
            except Exception: continue
        if not found: await wait.edit_text("🌟 لا توجد تقاطعات"); return
        lines=["🌟 التقاطعات:",""]
        for sym,c in found:
            t="🌟 ذهبي" if c["type"]=="golden" else "💀 موت"
            dec="🟢 شراء" if c["type"]=="golden" else "🔴 بيع"
            lines+=[f"{t} #{sym} — {fmt(c['price'])}",f"   🎯 القرار المتوقع: {dec}"]
        await wait.edit_text("\n".join(lines)[:4000],link_preview_options=NO_PREVIEW)
    except Exception as e:
        log.exception("cross"); await wait.edit_text(f"⚠️ {str(e)[:200]}")

@vip_only
async def cmd_active(update,ctx):
    act=active_signals()
    if not act: await update.message.reply_text("📊 لا توجد صفقات نشطة"); return
    act.sort(key=lambda p:-p.get("created",0))
    lines=[f"📊 الصفقات النشطة ({len(act)}/{MAX_OPEN})",""]
    for p in act[:20]:
        side_icon="🟢" if p["side"]==1 else "🔴"
        decision="🟢 شراء" if p["side"]==1 else "🔴 بيع"
        cur_r=p.get("realized",0.0); tp_hits=sum(1 for t in p["tps"] if t["hit"])
        lines+=[f"{side_icon} #{p['coin']}/{p['tf']} R:{cur_r:+.2f} TP:{tp_hits}/4",f"   🎯 القرار المتوقع: {decision}"]
    await update.message.reply_text("\n".join(lines)[:4000],link_preview_options=NO_PREVIEW)

@vip_only
async def cmd_predict(update,ctx):
    wait=await update.message.reply_text("🔮 جارٍ الفحص...")
    try:
        syms=await asyncio.to_thread(top_symbols,40)
        preds=await asyncio.to_thread(predictive_scan,syms[:30])
        if not preds: await wait.edit_text("🔮 لا توجد عملات"); return
        lines=["🔮 رادار التنبؤ:",""]
        for p in preds[:7]:
            icon="🟢" if p["direction"]=="LONG" else "🔴"
            decision="🟢 شراء" if p["direction"]=="LONG" else "🔴 بيع"
            lines.append(f"{icon} #{p['sym']} — {p['score']}/15")
            for r in p["reasons"][:3]: lines.append(f"   ✅ {r}")
            lines+=[f"   🎯 القرار المتوقع: {decision}",""]
        await wait.edit_text("\n".join(lines),link_preview_options=NO_PREVIEW)
    except Exception as e: await wait.edit_text(f"⚠️ {str(e)[:150]}")

@vip_only
async def cmd_whales(update,ctx):
    wait=await update.message.reply_text("🐋 فحص الصفقات الكبيرة...")
    syms=await asyncio.to_thread(top_symbols,30); rows=[]
    for s in syms[:15]:
        w,largest,bv,sv=await asyncio.to_thread(whale_radar,s)
        if w and w>=1: rows.append((s,w,largest,bv,sv))
    lines=[]
    if rows:
        rows.sort(key=lambda x:-x[2])
        lines+=["🐋 <b>رادار الصفقات الكبيرة (فوق 5 آلاف دولار)</b>","━━━━━━━━━━━━━━━━",""]
        for s,w,largest,bv,sv in rows[:10]:
            total=bv+sv; buy_pct=(bv/total*100) if total>0 else 0
            direction="🟢 شراء" if buy_pct>55 else ("🔴 بيع" if buy_pct<45 else "⚖️ متذبذب")
            lines+=[f"🐋 <b>#{s}</b>",f"   🔢 {w} صفقة كبيرة",f"   💰 أكبر صفقة: <b>${largest:,.0f}</b>",
                    f"   📊 الاتجاه: <b>{direction}</b> ({buy_pct:.0f}%)",f"   🎯 القرار المتوقع: {direction}",""]
    else:
        fallback_syms=await asyncio.to_thread(top_symbols,5)
        lines+=["🐋 <b>لا توجد صفقات كبيرة حالياً على Binance</b>","👇 إليك أنشط العملات للمراقبة:",""]
        for s in fallback_syms: lines.append(f"• #{s} — 🎯 القرار المتوقع: مراقبة")
    if ETHERSCAN_KEY and ETH_WATCH:
        eth_flows=await asyncio.to_thread(eth_exchange_flows,1)
        if eth_flows: lines+=["","🔷 <b>تدفقات ETH من عناوين البورصات (آخر ساعة):</b>"]+eth_flows
    if WHALE_ALERT_KEY:
        onchain=await asyncio.to_thread(whale_alert_recent,1_000_000)
        if onchain: lines+=["","🌍 <b>تحويلات on-chain كبيرة (آخر 60 دقيقة):</b>"]+onchain[:6]
    await wait.edit_text("\n".join(lines)[:4000],parse_mode=ParseMode.HTML,link_preview_options=NO_PREVIEW)

@vip_only
async def cmd_liquidity(update,ctx):
    if not ctx.args: await update.message.reply_text("مثال: /liquidity BTC"); return
    sym=clean_symbol(ctx.args[0]); wait=await update.message.reply_text(f"💧 تحليل {sym}...")
    liq=await asyncio.to_thread(liquidity_radar,sym)
    if not liq: await wait.edit_text("💧 لا توجد بيانات"); return
    bias="🟢 ضغط شراء" if liq["ratio"]>0.55 else ("🔴 ضغط بيع" if liq["ratio"]<0.45 else "⚖️")
    decision="🟢 شراء" if liq["ratio"]>0.55 else ("🔴 بيع" if liq["ratio"]<0.45 else "⚖️ حياد")
    await wait.edit_text(f"💧 {sym}\n🟢 ${liq['bids']:,.0f}\n🔴 ${liq['asks']:,.0f}\n"
        f"📊 {liq['ratio']*100:.1f}% | {bias}\n\n🎯 القرار المتوقع: {decision}")

@vip_only
async def cmd_futures(update,ctx):
    if not ctx.args: await update.message.reply_text("مثال: /futures BTC"); return
    sym=clean_symbol(ctx.args[0]); wait=await update.message.reply_text(f"⏳ {sym}...")
    try:
        oi=await asyncio.to_thread(futures_oi_history,sym,"4h",20)
        fr=await asyncio.to_thread(futures_funding_rate,sym)
        ls=await asyncio.to_thread(futures_long_short_ratio,sym)
        lines=[f"📊 <b>الفيوتشر - {sym}/USDT</b>","━━━━━━━━━━━━━━━━",""]
        if oi and oi.get("current",0)>0:
            lines.append(f"📈 OI: <code>{oi['current']:,.0f}</code>")
            if oi.get("change_pct",0)!=0:
                icon="🟢" if oi["change_pct"]>0 else "🔴"
                lines.append(f"{icon} تغير OI: <b>{oi['change_pct']:+.2f}%</b>")
        else: lines.append("📈 OI: <i>لا توجد بيانات</i>")
        if fr is not None:
            icon="🟢" if fr>0 else "🔴"; lines.append(f"{icon} Funding: <b>{fr:+.4f}%</b>")
        else: lines.append("💰 Funding: <i>لا توجد بيانات</i>")
        if ls is not None:
            icon="🟢" if ls>1.2 else ("🔴" if ls<0.83 else "⚖️")
            lines.append(f"{icon} L/S: <b>{ls}</b>")
        else: lines.append("⚖️ L/S: <i>لا توجد بيانات</i>")
        sig=await asyncio.to_thread(futures_signal,sym)
        if sig and sig["direction"]:
            decision="🟢 شراء" if sig['direction']=='LONG' else "🔴 بيع"
            lines+=["","━━━━━━━━━━━━━━━━",
                f"🎯 <b>الإشارة: {'🟢 شراء (LONG)' if sig['direction']=='LONG' else '🔴 بيع (SHORT)'}</b>",
                f"💪 القوة: {sig['confidence']} (Score: {sig['score']:+d})",
                f"🎯 <b>القرار المتوقع: {decision}</b>","","🔍 <b>الأسباب:</b>"]
            for r in sig["reasons"][:5]: lines.append(f"  • {r}")
        else: lines+=["","━━━━━━━━━━━━━━━━","⚖️ <b>لا توجد إشارة واضحة</b>"]
        await wait.edit_text("\n".join(lines),parse_mode=ParseMode.HTML,link_preview_options=NO_PREVIEW)
    except Exception as e:
        log.exception("futures")
        try: await wait.edit_text(f"⚠️ {str(e)[:150]}")
        except Exception as _ex: log.debug('ignored: %s', _ex)

@vip_only
async def cmd_golden(update,ctx):
    wait=await update.message.reply_text("💎 جارٍ البحث...\n⏳ فحص 500 عملة")
    try: running_loop=asyncio.get_running_loop()
    except RuntimeError: running_loop=asyncio.new_event_loop()
    async def update_progress(processed,total,found):
        try:
            await wait.edit_text(
                f"💎 <b>جارٍ المسح الشامل...</b>\n\n📊 تم فحص: <b>{processed}</b> من <b>{total}</b> عملة\n"
                f"🎯 تم إيجاد: <b>{found}</b> فرصة حتى الآن\n\n⏳ يرجى الانتظار...",parse_mode=ParseMode.HTML)
        except Exception as _ex: log.debug('ignored: %s', _ex)
    def sync_progress(proc,tot,fnd):
        try: asyncio.run_coroutine_threadsafe(update_progress(proc,tot,fnd),running_loop)
        except Exception as _ex: log.debug('ignored: %s', _ex)
    try:
        results=await asyncio.to_thread(golden_scan,2,sync_progress)
        if not results:
            syms=await asyncio.to_thread(top_symbols,20,True)
            near_misses=[]
            for s in syms[:8]:
                try:
                    r=await asyncio.to_thread(golden_score,s,True,False)
                    if r: near_misses.append(r)
                except Exception: continue
            near_misses.sort(key=lambda x:-x["score"])
            msg="💎 <b>لا توجد فرصة ذهبية حالياً</b>\n\n"
            if near_misses:
                msg+="🔍 <b>أقرب العملات للفرصة (Score):</b>\n"
                for nm in near_misses[:3]:
                    msg+=f"<b>#{nm['sym']}</b> — النقاط: {nm['score']}\n  ↳ {', '.join(nm['reasons'][:4])}\n\n"
            cfg=get_golden_config()
            msg+=f"📊 <b>الحد الأدنى المطلوب: {cfg.get('min_score',1)}</b>\n"
            msg+="💡 <b>ملاحظة:</b> البوت جرب الوضع Relaxed تلقائياً ولم يجد فرصة كافية."
            await wait.edit_text(msg,parse_mode=ParseMode.HTML,link_preview_options=NO_PREVIEW); return
        await wait.delete()
        for g in results:
            txt=format_golden_post(g,"vip")
            await update.message.reply_text(txt,parse_mode=ParseMode.HTML,link_preview_options=NO_PREVIEW)
            await asyncio.sleep(1)
    except Exception as e:
        log.exception("golden")
        try: await wait.edit_text(f"⚠️ {str(e)[:200]}")
        except Exception as _ex: log.debug('ignored: %s', _ex)

@vip_only
async def cmd_alpha_radar_v2(update,ctx):
    wait=await update.message.reply_text("🚀 <b>رادار النخبة (Alpha/Web3/DEX)</b>\n⏳ جلب الترند + فحص...",parse_mode=ParseMode.HTML)
    try:
        trending_url="https://api.coingecko.com/api/v3/search/trending"
        r=await asyncio.to_thread(requests.get,trending_url,timeout=10)
        trending_coins=r.json().get("coins",[]) if r.status_code==200 else []
        if not trending_coins:
            await wait.edit_text("⚠️ تعذر جلب بيانات الزخم"); return
        lines=["💎 <b>رادار النخبة (Alpha/Web3/DEX)</b>","━━━━━━━━━━━━━━━━",
            "🔍 <i>مستبعد: BTC, ETH, BNB, العملات الأساسية والستيبل</i>",""]
        count=0
        for item in trending_coins:
            if count>=6: break
            cd=item.get("item",{}); symbol=(cd.get("symbol","") or "").upper()
            name=cd.get("name",""); rank=cd.get("market_cap_rank","N/A"); score=cd.get("score",0)
            if symbol in ALPHA_MAJORS_EXCLUDE: continue
            pd_=cd.get("data",{}) or {}
            ch_raw=(pd_.get("price_change_percentage_24h") or {}).get("usd",0)
            try: change=float(ch_raw or 0)
            except Exception: change=0.0
            if change>5: decision,emoji="🚀 <b>صعود قوي</b>","🟢"
            elif change>2: decision,emoji="📈 <b>صعود</b>","🟢"
            elif change<-5: decision,emoji="🔻 <b>نزول قوي</b>","🔴"
            elif change<-2: decision,emoji="📉 <b>نزول</b>","🔴"
            else: decision,emoji="🟡 <b>تحرك محدود</b>","🟡"
            lines+=[f"{emoji} <b>{name} ({symbol})</b>",f"   📊 ترتيب: #{rank} | زخم: {score}",
                    f"   🎯 {decision} ({change:+.1f}%)",""]
            count+=1
        if count==0:
            await wait.edit_text("⚠️ لا توجد عملات Alpha/Web3 في الترند حالياً"); return
        lines+=["━━━━━━━━━━━━━━━━","💡 <b>استراتيجية:</b>",
            "1. هذه العملات جديدة/صغيرة (Web3, DEX, Alpha)",
            "2. ابحث عن الرمز في محفظتك (MetaMask, Trust, إلخ)",
            "3. استخدم حاسبة المخاطر لتحديد الحجم","",RISK_WARNING_AR,f"👤 {BRAND}"]
        await wait.edit_text("\n".join(lines),parse_mode=ParseMode.HTML,link_preview_options=NO_PREVIEW)
    except Exception as e:
        log.exception("alpha_radar_v2")
        try: await wait.edit_text(f"⚠️ {str(e)[:150]}")
        except Exception as _ex: log.debug('ignored: %s', _ex)

@vip_only
async def cmd_explosions(update,ctx):
    wait=await update.message.reply_text("💥 رادار الانفجارات...")
    syms=await asyncio.to_thread(top_symbols,40); rows=[]
    for s in syms[:30]:
        r=await asyncio.to_thread(explosion_radar,s)
        if r: rows.append((s,r))
    if not rows:
        fallback_syms=await asyncio.to_thread(top_symbols,5)
        lines=["💥 <b>لا توجد انفجارات حالياً (السوق هادئ)</b>","👇 إليك أنشط العملات للمراقبة:",""]
        for s in fallback_syms: lines.append(f"• #{s} — 🎯 القرار المتوقع: مراقبة")
        await wait.edit_text("\n".join(lines),parse_mode=ParseMode.HTML,link_preview_options=NO_PREVIEW); return
    rows.sort(key=lambda x:-x[1]["vol_ratio"])
    lines=["💥 رادار الانفجارات:",""]
    for s,r in rows[:10]:
        decision="🟢 شراء" if r["price_change"]>0 else "🔴 بيع"
        lines+=[f"💥 #{s}: حجم x{r['vol_ratio']} | {r['price_change']:+.2f}%",f"   🎯 القرار المتوقع: {decision}"]
    await wait.edit_text("\n".join(lines),link_preview_options=NO_PREVIEW)

@vip_only
async def cmd_pump(update,ctx):
    wait=await update.message.reply_text("🚀 عملات صاعدة...")
    syms=await asyncio.to_thread(top_symbols,40); rows=[]
    for s in syms[:30]:
        r=await asyncio.to_thread(pump_radar,s)
        if r: rows.append((s,r))
    if not rows:
        fallback_syms=await asyncio.to_thread(top_symbols,5)
        lines=["🚀 <b>لا توجد عملات صاعدة حالياً (السوق هادئ)</b>","👇 إليك أنشط العملات للمراقبة:",""]
        for s in fallback_syms: lines.append(f"• #{s} — 🎯 القرار المتوقع: مراقبة")
        await wait.edit_text("\n".join(lines),parse_mode=ParseMode.HTML,link_preview_options=NO_PREVIEW); return
    rows.sort(key=lambda x:-x[1]["change_1h"])
    lines=["🚀 عملات في صعود:",""]
    for s,r in rows[:10]:
        lines+=[f"🚀 #{s}: 1س {r['change_1h']:+.2f}% | 4س {r['change_4h']:+.2f}%",f"   🎯 القرار المتوقع: 🟢 شراء"]
    await wait.edit_text("\n".join(lines),link_preview_options=NO_PREVIEW)

@vip_only

@vip_only

async def cmd_plan(update,ctx):
    """/plan dca|grid ... معلومات فقط — بدون تنفيذ."""
    touch_user(update.effective_user,"/plan")
    args = ctx.args or []
    try:
        mode = (args[0] if args else "").lower()
        if mode not in ("dca","grid"):
            msg = (
                "📋 <b>خطط معلوماتية فقط (لا تنفيذ)</b>\n\n"
                "DCA: <code>/plan dca سعر رأس_المال [شرائح] [نسبة_الخطوة]</code>\n"
                "مثال: <code>/plan dca 0.05 500 3 5</code>\n\n"
                "Grid: <code>/plan grid منتصف رأس_المال [عدد] [تباعد%]</code>\n"
                "مثال: <code>/plan grid 65000 1000 6 1.5</code>\n\n"
                "⚠️ البوت لا يرسل أوامر للبورصة."
            )
            await update.message.reply_text(msg, parse_mode=ParseMode.HTML)
            return
        if mode == "dca":
            entry=float(args[1]); cap=float(args[2])
            slices=int(args[3]) if len(args)>3 else 3
            step=float(args[4]) if len(args)>4 else 4.0
            pl=info_dca_plan(entry,cap,slices,step)
            lines=["📥 <b>خطة DCA (معلومات فقط)</b>",f"رأس المال: ${pl['capital']}",f"متوسط دخول تقريبي: {pl['avg_entry']}",""]
            for lv in pl["levels"]:
                lines.append(f"• شريحة {lv['n']}: {lv['price']} ≈ ${lv['amount']} ({lv['qty']})")
            lines+=["", "⚠️ لا تنفيذ تلقائي — للتنبيه والتخطيط اليدوي فقط."]
            await update.message.reply_text("\n".join(lines),parse_mode=ParseMode.HTML)
        else:
            mid=float(args[1]); cap=float(args[2])
            grids=int(args[3]) if len(args)>3 else 6
            sp=float(args[4]) if len(args)>4 else 1.5
            pl=info_grid_plan(mid,cap,grids,sp)
            lines=["▦ <b>خطة Grid (معلومات فقط)</b>",f"المحور: {pl['mid']} | رأس المال: ${pl['capital']}",""]
            for lv in pl["levels"]:
                lines.append(f"• {lv['side']}: {lv['price']} ≈ ${lv['amount']}")
            lines+=["", "⚠️ لا تنفيذ تلقائي — للمستويات على الشارت يدوياً."]
            await update.message.reply_text("\n".join(lines),parse_mode=ParseMode.HTML)
    except Exception:
        await update.message.reply_text("صيغة غير صحيحة. مثال: /plan dca 0.05 500 3 5")

async def cmd_spot(update,ctx):
    wait=await update.message.reply_text("🌱 جارٍ مسح فرص السبوت (أفق 1–2 شهر)...")
    try:
        rows=await asyncio.to_thread(spot_opportunities,12)
        lang=get_user_lang(update.effective_user.id)
        txt=build_spot_text(rows,lang)
        await wait.edit_text(txt[:4000],parse_mode=ParseMode.HTML,link_preview_options=NO_PREVIEW)
    except Exception as e:
        log.exception("cmd_spot")
        try: await wait.edit_text(f"⚠️ {str(e)[:150]}")
        except Exception: pass

async def cmd_bottoms(update,ctx):
    wait=await update.message.reply_text("📉 رادار القيعان...")
    syms=await asyncio.to_thread(top_symbols,70); rows=[]
    for s in syms[:55]:
        r=await asyncio.to_thread(bottom_radar,s)
        if r: rows.append((s,r))
    if not rows: await wait.edit_text("📉 لا توجد قيعان حالياً (السوق في حالة استقرار)."); return
    rows.sort(key=lambda x:x[1]["diff"])
    lines=["📉 رادار القيعان:",""]
    for s,r in rows[:10]:
        lines+=[f"{r['severity']} #{s}",f"   {r['diff']}% تحت EMA200 | RSI {r['rsi']} | تعافي +{r['rebound']}%",
                f"   🎯 القرار المتوقع: 🟢 شراء"]
    await wait.edit_text("\n".join(lines)[:4000],link_preview_options=NO_PREVIEW)

@vip_only
async def cmd_shorts_radar(update,ctx):
    wait=await update.message.reply_text("🔻 فرص الشورت...")
    syms=await asyncio.to_thread(top_symbols,40); rows=[]
    for s in syms[:30]:
        r=await asyncio.to_thread(short_radar,s)
        if r: rows.append((s,r))
    if not rows:
        fallback_syms=await asyncio.to_thread(top_symbols,5)
        lines=["🔻 <b>لا توجد فرص شورت حالياً (السوق يميل للصعود)</b>","👇 إليك أنشط العملات للمراقبة:",""]
        for s in fallback_syms: lines.append(f"• #{s} — 🎯 القرار المتوقع: مراقبة")
        await wait.edit_text("\n".join(lines),parse_mode=ParseMode.HTML,link_preview_options=NO_PREVIEW); return
    lines=["🔻 رادار الشورت:",""]
    for s,r in rows[:10]:
        lines+=[f"🔻 #{s}: RSI {r['rsi']} | {fmt(r['price'])}",f"   🎯 القرار المتوقع: 🔴 بيع"]
    await wait.edit_text("\n".join(lines),link_preview_options=NO_PREVIEW)

@vip_only
async def cmd_alpha(update,ctx):
    wait=await update.message.reply_text("⭐ رادار Alpha...")
    syms=await asyncio.to_thread(top_symbols,50); rows=[]
    for s in syms[:35]:
        r=await asyncio.to_thread(alpha_radar,s)
        if r: rows.append((s,r))
    if not rows: await wait.edit_text("⭐ لا توجد عملات Alpha قوية حالياً."); return
    rows.sort(key=lambda x:-x[1]["change_7d"])
    lines=["⭐ رادار Alpha:",""]
    for s,r in rows[:10]:
        lines+=[f"⭐ #{s} 7ي {r['change_7d']:+.2f}% | 30ي {r['change_30d']:+.2f}%",f"   🎯 القرار المتوقع: 🟢 شراء"]
    await wait.edit_text("\n".join(lines),link_preview_options=NO_PREVIEW)

@admin_only
async def cmd_patterns(update,ctx):
    if ctx.args:
        sym=clean_symbol(ctx.args[0])
        wait=await update.message.reply_text(f"🕯️ فحص النماذج لـ {sym} على كل الفريمات...")
        try:
            all_pats=[]
            for tf in ["1d","4h","1h","15m"]:
                try:
                    df=await asyncio.to_thread(get_candles,sym,tf,300,300,100)
                    if df.empty or len(df)<100: continue
                    df=add_indicators(df,100); pats=detect_all_patterns(df,tf); all_pats.extend(pats)
                except Exception: continue
            if not all_pats:
                await wait.edit_text(f"🕯️ #{sym} — لا نماذج مؤكدة على أي فريم"); return
            lines=[f"🕯️ <b>نماذج #{sym}</b>","━━━━━━━━━━━━━━━━",""]
            for p in all_pats:
                em="🟢" if p["bias"]=="bullish" else ("🔴" if p["bias"]=="bearish" else "⚪")
                lines.append(f"{em} <b>{p['name']}</b> [{p.get('tf','?').upper()}]")
                if p.get("neckline"): lines.append(f"   📏 Neckline: {fmt(p['neckline'])}")
                lines.append("")
            await wait.edit_text("\n".join(lines),parse_mode=ParseMode.HTML,link_preview_options=NO_PREVIEW)
        except Exception as e: await wait.edit_text(f"⚠️ {str(e)[:150]}")
        return
    wait=await update.message.reply_text(
        "🕯️ <b>جاري فحص النماذج على كل العملات النشطة وكل الفريمات...</b>\n\n⏳ قد يستغرق 30-60 ثانية",parse_mode=ParseMode.HTML)
    try:
        syms=await asyncio.to_thread(top_symbols,40); all_results=[]
        for sym in syms[:25]:
            for tf in ["1d","4h","1h"]:
                try:
                    df=await asyncio.to_thread(get_candles,sym,tf,300,600,100)
                    if df.empty or len(df)<100: continue
                    df=add_indicators(df,100); pats=detect_all_patterns(df,tf)
                    for p in pats: all_results.append({**p,"sym":sym})
                except Exception: continue
        if not all_results:
            await wait.edit_text("🕯️ <b>لا نماذج مؤكدة على السوق حالياً</b>\n\nالسوق في حالة عرضية، انتظر فرصة أفضل.",parse_mode=ParseMode.HTML); return
        by_sym={}
        for p in all_results: by_sym.setdefault(p["sym"],[]).append(p)
        ordered=sorted(by_sym.items(),key=lambda x:-len(x[1]))[:15]
        lines=["🕯️ <b>النماذج المكتشفة على السوق</b>",f"📊 {len(all_results)} نموذج على {len(by_sym)} عملة","━━━━━━━━━━━━━━━━",""]
        for sym,pats in ordered:
            cl=coin_link(sym,"binance_fut"); lines.append(f"<a href='{cl}'>#{sym}</a> 👆")
            for p in pats[:3]:
                em="🟢" if p["bias"]=="bullish" else ("🔴" if p["bias"]=="bearish" else "⚪")
                lines.append(f"  {em} {p['name']} [{p.get('tf','?').upper()}]")
            lines.append("")
        lines+=["━━━━━━━━━━━━━━━━",RISK_WARNING_AR]
        await wait.edit_text("\n".join(lines)[:4000],parse_mode=ParseMode.HTML,link_preview_options=NO_PREVIEW)
    except Exception as e:
        log.exception("patterns_scan")
        try: await wait.edit_text(f"⚠️ {str(e)[:150]}")
        except Exception as _ex: log.debug('ignored: %s', _ex)

@admin_only
async def cmd_share(update,ctx):
    if not ctx.args: await update.message.reply_text("مثال: /share BTC"); return
    sym=clean_symbol(ctx.args[0]); wait=await update.message.reply_text(f"⏳ جارٍ تجهيز {sym}...")
    try:
        res=await asyncio.to_thread(get_analysis,sym,True)
        if res.get("wait"):
            await wait.edit_text("⏳ لا توجد صفقة مؤهلة لـ "+sym+" (انتظار)."); return
        await wait.delete()
        img=await asyncio.to_thread(render_chart,res,"vip")
        await update.message.reply_photo(photo=img)
        await update.message.reply_text("👇 اختر المنصة:",reply_markup=keyboard_share_post(res),link_preview_options=NO_PREVIEW)
    except Exception as e:
        log.exception("share")
        try: await wait.edit_text(f"⚠️ {str(e)[:200]}")
        except Exception as _ex: log.debug('ignored: %s', _ex)

@admin_only
async def cmd_neon(update,ctx):
    if not ctx.args:
        await update.message.reply_text("مثال: /neon BTC"); return
    sym = clean_symbol(ctx.args[0])
    wait = await update.message.reply_text(f"💳 جارٍ توليد بطاقة النيون لـ {sym}...")
    try:
        res = await asyncio.to_thread(get_analysis, sym, True)
        if res.get("wait"):
            await wait.edit_text("⏳ لا توجد صفقة مؤهلة لـ "+sym+" (انتظار)."); return
        card = await asyncio.to_thread(render_trade_card, res, "admin")
        await update.message.reply_photo(photo=card)
        await wait.delete()
    except Exception as e:
        await wait.edit_text(f"⚠️ {str(e)[:150]}")

@admin_only
async def cmd_forcepost(update,ctx):
    wait=await update.message.reply_text("⏳ جارٍ المسح والنشر...")
    try:
        res=await run_autopost(ctx.bot); stats=res or {}
        await wait.edit_text(f"✅ انتهى\n\nVIP: {stats.get('vip',0)} | FREE: {stats.get('free',0)}\nإجمالي: {stats.get('total',0)}")
    except Exception as e:
        log.exception("forcepost"); await wait.edit_text(f"⚠️ {str(e)[:200]}")

@admin_only
async def cmd_clearsignals(update,ctx):
    with store.lock:
        n=len(store.data["signals"]); store.data["signals"]=[]; store.save("signals")
    await update.message.reply_text(f"✅ تم حذف {n} صفقة")

@admin_only
async def cmd_smartcleanup(update,ctx):
    wait=await update.message.reply_text("🧹 جارٍ التنظيف...")
    n=await asyncio.to_thread(smart_cleanup_trades)
    await wait.edit_text(f"✅ تم إغلاق {n} صفقة")

@admin_only
async def cmd_debug(update,ctx):
    lines=["🔍 تشخيص","",f"💾 Storage: {store.remote_msg}",
        f"🔧 Maintenance: {'ON' if is_maintenance() else 'OFF'}",
        f"🌐 Multi-Exchange: {'ON' if is_multi_exchange() else 'OFF'}",
        f"📢 FREE: {'✅' if CHANNEL_ID else '❌'} | VIP: {'✅' if VIP_CHANNEL_ID else '❌'}",
        f"📊 MAX_OPEN: {MAX_OPEN} | نشطة: {len(active_signals())}",
        f"📝 posted: {len(store.data['posted'])}",
        f"📡 Binance Futures API: {BINANCE_FAPI_BASE}",
        f"💎 Golden: كل {CYCLE_GOLDEN//3600}س | Min score: {get_golden_config().get('min_score',1)}",
        f"🕯️ Patterns: MTF (1D+4H+1H) ✅",
        f"🎯 Points: {VIP_THRESHOLD}=VIP{VIP_REWARD_DAYS}d | +{POINTS_REFERRAL}/referral"]
    syms=await asyncio.to_thread(top_symbols,60); lines.append(f"🎯 top_symbols: {len(syms)}")
    if syms: lines.append(f"   {', '.join(syms[:10])}")
    g,l=await asyncio.to_thread(get_top_movers,True)
    lines.append(f"🟢 Gainers: {len(g)} | 🔴 Losers: {len(l)}")
    await update.message.reply_text("\n".join(lines)[:4000],link_preview_options=NO_PREVIEW)

@admin_only
async def cmd_post(update,ctx):
    if not ctx.args: await update.message.reply_text("مثال: /post BTC"); return
    sym=clean_symbol(ctx.args[0])
    res=await asyncio.to_thread(get_analysis,sym,True)
    if res.get("grade") in ("C","WAIT") or res.get("wait"):
        await update.message.reply_text(f"⛔ {sym}: لا توجد صفقة مؤهلة (انتظار) — لا تُنشر للقنوات."); return
    sent=await publish(ctx.bot,res,vip=True,free=True,admin_copy=False)
    if any(x in ("VIP","FREE") for x in sent) and res["rank"]>=0: track_signal(res)
    await update.message.reply_text(f"✅ نُشر: {', '.join(sent)}")

@admin_only
async def cmd_bt(update,ctx):
    arg=ctx.args[0].upper() if ctx.args else "ALL"
    try: years=float(ctx.args[1]) if len(ctx.args)>1 else 3.0
    except ValueError: years=3.0
    syms=COINS if arg in ("ALL","") else [clean_symbol(arg)]
    syms=[x for x in (syms or []) if x]
    wait=await update.message.reply_text("⏳ Backtest...")
    per,a,b,errors=await asyncio.to_thread(backtest_many,syms,years)
    if not per:
        text,_=bt_report(per,a,b,arg,errors)
        await wait.edit_text(text[:4000],link_preview_options=NO_PREVIEW); return
    text,_=bt_report(per,a,b,arg,errors)
    await wait.edit_text(text[:4000],link_preview_options=NO_PREVIEW)

# ======================= تشخيص الباك تست (/btdiag) =======================
_BT_DATA={"ts":0.0,"years":0.0,"syms":[],"d":{}}   # sym -> (raw, d محضّر, htf محضّر)
_BT_BUSY=[False]

def bt_load(sym,years):
    years=float(years)
    if not np.isfinite(years) or years<=0: raise ValueError("مدة غير صالحة")
    need=int(min(years,10.0)*6*365)+300
    rows=binance_futures_klines_long(sym,"4h",need)
    if rows and len(rows)>=400: df=_rows_to_df(rows,"4h",need)
    else: df=get_candles(sym,"4h",need,ttl=900)
    if df is None or not isinstance(df,pd.DataFrame) or df.empty:
        raise NoData(f"{sym}: لا توجد شموع")
    if len(df)<400: return None
    raw=df[["t","open","high","low","close","volume"]].dropna().reset_index(drop=True)
    if len(raw)<400: return None
    daily=eng_resample_daily(raw)
    return raw,eng_prepare(raw),eng_prepare(daily) if not daily.empty else pd.DataFrame()

def bt_prepare_all(syms,years):
    c=_BT_DATA
    if c["d"] and c["syms"]==list(syms) and c["years"]==years and time.time()-c["ts"]<1800: return c["d"]
    out={}
    for sy in syms:
        try:
            r=bt_load(sy,years)
            if r: out[sy]=r
        except Exception as _ex: log.debug('ignored: %s',_ex)
    c.update(ts=time.time(),years=years,syms=list(syms),d=out); return out

def bt_run(data,cfg=None):
    trades,tmin,tmax=[],None,None
    for sy,(raw,d,htf) in data.items():
        try: trs=eng_simulate(raw,None,start=260,cfg=cfg,prepared=(d,htf))
        except Exception as _ex: log.debug('ignored: %s',_ex); continue
        for x in trs: x["sym"]=sy
        trades+=trs
        a,b=int(raw["t"].iloc[260]),int(raw["t"].iloc[-1])
        tmin=a if tmin is None else min(tmin,a); tmax=b if tmax is None else max(tmax,b)
    return trades,tmin,tmax

def _bt_split(trades,tmin,tmax):
    if not isinstance(tmin,(int,float)) or not isinstance(tmax,(int,float)) or tmax<=tmin:
        return [],list(trades or [])
    cut=tmin+0.65*(tmax-tmin)   # نفس تقسيم bt_report
    return [x for x in (trades or []) if x["t"]<cut],[x for x in (trades or []) if x["t"]>=cut]

def _bt_line(name,trs):
    if not trs: return f"{name}: —"
    m=metrics(trs); return f"{name}: n={m['n']} | WR {m['wr']}% | PF {m['pf']} | {m['total']:+}R"

def _bt_group(title,trades,buckets):
    out=[title]
    for name,fn in buckets:
        sub=[x for x in trades if fn(x)]
        if sub: out.append("  "+_bt_line(name,sub))
    return out

def bt_breakdown(trades,tmin,tmax,nsym):
    tr,te=_bt_split(trades,tmin,tmax); n=len(trades)
    cost=sum(x.get("cost",0) for x in trades); net=sum(x["R"] for x in trades)
    avg = net/n if n else 0.0
    L=[f"🔬 تشخيص الباك تست · {BOT_VERSION}",f"{nsym} عملة | {n} صفقة","",
       _bt_line("TRAIN",tr),_bt_line("TEST ",te),_bt_line("الكل ",trades),
       f"💸 رسوم+انزلاق: {-cost:+.1f}R | قبلها: {net+cost:+.1f}R | متوسط/صفقة: {avg:+.3f}R",
       "🎯 وصول الأهداف: "+" | ".join(f"TP{k} {100*sum(1 for x in trades if x.get('tps',0)>=k)/n:.0f}%" for k in (1,2,3,4)),
       ""]
    L+=_bt_group("🚪 نوع الخروج",trades,[(k,(lambda x,k=k:x["exit"]==k)) for k in ("SL","BE","TRAIL","TP","TIME")])
    L+=[""]+_bt_group("↕️ الاتجاه",trades,[("شراء",lambda x:x["side"]==1),("بيع",lambda x:x["side"]==-1)])
    L+=[""]+_bt_group("🏅 الدرجة",trades,[(g,(lambda x,g=g:x["grade"]==g)) for g in ("A+","A","B")])
    p1=[]
    L+=[""]+_bt_group("📈 قوة الترند ADX",trades,[("ADX<22",lambda x:x.get("adx",0)<22),
        ("22–30",lambda x:22<=x.get("adx",0)<30),("ADX≥30",lambda x:x.get("adx",0)>=30)])
    p2=_bt_group("📏 بعد الوقف (ATR)",trades,[("<1.5",lambda x:x.get("sl_atr",0)<1.5),
        ("1.5–2.2",lambda x:1.5<=x.get("sl_atr",0)<2.2),("≥2.2",lambda x:x.get("sl_atr",0)>=2.2)])
    p2+=[""]+_bt_group("🎯 مسافة TP1 (R)",trades,[("<1.5R",lambda x:x.get("rr1",0)<1.5),
        ("1.5–2.5R",lambda x:1.5<=x.get("rr1",0)<2.5),("≥2.5R",lambda x:x.get("rr1",0)>=2.5)])
    yrs=sorted({dt.datetime.fromtimestamp(x["t"]/1000,dt.timezone.utc).year for x in trades})
    p2+=[""]+_bt_group("📅 السنة",trades,[(str(y),(lambda x,y=y:dt.datetime.fromtimestamp(x["t"]/1000,dt.timezone.utc).year==y)) for y in yrs])
    per={}
    for x in trades: per.setdefault(x["sym"],[]).append(x["R"])
    rk=sorted(per.items(),key=lambda kv:-sum(kv[1]))
    p2+=["","🏆 أفضل 5 عملات: "+" | ".join(f"{k} {sum(v):+.1f}R" for k,v in rk[:5]),
         "📉 أسوأ 5 عملات: "+" | ".join(f"{k} {sum(v):+.1f}R" for k,v in rk[-5:])]
    return ["\n".join(L),"\n".join(p2)]

def bt_grid_one(data,ms,adx,sl):
    cfg=dict(ENG_CFG,min_score=ms,min_adx=adx,max_sl_atr=sl)
    trs,a,b=bt_run(data,cfg)
    if not trs: return dict(ms=ms,adx=adx,sl=sl,tr=metrics([]),te=metrics([]))
    tr,te=_bt_split(trs,a,b); return dict(ms=ms,adx=adx,sl=sl,tr=metrics(tr),te=metrics(te))

BT_GRID=[(ms,adx,sl) for ms in (6.5,8.0) for adx in (18,25) for sl in (2.0,2.8)]

def bt_grid_text(rows,total):
    L=[f"🧪 جدول الإعدادات ({len(rows)}/{total}) · {BOT_VERSION}","score/ADX/SL  |  TRAIN n·PF·R  |  TEST n·PF·R"]
    for r in rows:
        L.append(f"{r['ms']}/{r['adx']}/{r['sl']}  |  {r['tr']['n']}·{r['tr']['pf']}·{r['tr']['total']:+}  |  {r['te']['n']}·{r['te']['pf']}·{r['te']['total']:+}")
    ok=[r for r in rows if r["tr"]["n"]>=60]
    if len(rows)==total and ok:
        b=max(ok,key=lambda r:r["tr"]["pf"])
        L+=["",f"المختار على TRAIN فقط: score {b['ms']} | ADX {b['adx']} | SL {b['sl']}",
            f"→ TEST: n={b['te']['n']} | PF {b['te']['pf']} | {b['te']['total']:+}R",
            "⚠️ الحكم النهائي على TEST. إذا PF<1 هناك فلا ميزة، والضبط وحده ما نفع."]
    return "\n".join(L)

@admin_only
async def cmd_btdiag(update,ctx):
    """/btdiag [سنوات] [grid]  — تفصيل الخسارة، و grid لجدول إعدادات (أبطأ)."""
    if _BT_BUSY[0]:
        await update.message.reply_text("⏳ تشخيص آخر قيد التشغيل، انتظر"); return
    grid=any(a.lower()=="grid" for a in ctx.args); years=3.0
    for a in ctx.args:
        try: years=float(a)
        except ValueError: pass
    _BT_BUSY[0]=True
    try:
        wait=await update.message.reply_text(f"⏳ تشخيص {BOT_VERSION}: جلب بيانات Top 50 ({years:g} سنة)... قد يأخذ عدة دقائق")
        syms=await asyncio.to_thread(top_symbols,50)
        syms=syms if isinstance(syms, (list, tuple)) else []
        data=await asyncio.to_thread(bt_prepare_all,syms,years)
        if not data: await wait.edit_text("❌ لا توجد بيانات"); return
        await wait.edit_text(f"⏳ تشغيل الباك تست على {len(data)} عملة...")
        trades,a,b=await asyncio.to_thread(bt_run,data,None)
        if not trades: await wait.edit_text("❌ لا صفقات"); return
        parts=bt_breakdown(trades,a,b,len(data))
        await wait.edit_text(parts[0][:4000],link_preview_options=NO_PREVIEW)
        await update.message.reply_text(parts[1][:4000],link_preview_options=NO_PREVIEW)
        if grid:
            msg=await update.message.reply_text(f"🧪 الجدول 0/{len(BT_GRID)}...")
            rows=[]
            for ms,adx,sl in BT_GRID:
                rows.append(await asyncio.to_thread(bt_grid_one,data,ms,adx,sl))
                try: await msg.edit_text(bt_grid_text(rows,len(BT_GRID))[:4000])
                except Exception as _ex: log.debug('ignored: %s',_ex)
    finally:
        _BT_BUSY[0]=False

# ======================= تصدير بيانات البحث (/exportdata) =======================
# يقرأ بيانات عامة من Binance فقط (لا توكن ولا مفاتيح) ويرسل ملفات ZIP للأدمن.
EXP_PACE=0.35               # ثوانٍ بين الطلبات: نحمي حد الطلبات المشترك مع باقي البوت
EXP_4H_BARS=11000           # ~5 سنوات
EXP_1D_BARS=1900
EXP_1H_BARS=13000           # ~1.5 سنة
EXP_FUND_YEARS=5.0
EXP_ZIP_MAX=44*1024*1024    # حد تيليغرام للملف 50MB
_EXP_BUSY=[False]

def _exp_get(url,params,tries=5):
    """طلب مع إعادة محاولة. يرجع None إذا الرمز غير موجود/فشل. 418 = حظر IP فنوقف."""
    for k in range(tries):
        try:
            r=requests.get(url,params=params,timeout=15)
            if r.status_code==200: return r.json()
            if r.status_code==418: raise RuntimeError("418: Binance حظر الـIP مؤقتاً، أعد المحاولة بعد دقائق")
            if r.status_code==400: return None
            if r.status_code==429: time.sleep(min(int(r.headers.get("Retry-After","5") or 5),60)); continue
            time.sleep(1+k)
        except RuntimeError: raise
        except Exception: time.sleep(1+k)
    return None

def _exp_klines(sym,interval,bars,tf_ms):
    """يرجع (rows, complete). rows: قوائم كلاينز Binance الخام، بدون الشمعة غير المغلقة."""
    out,end,complete=[],None,True
    while len(out)<bars:
        p={"symbol":f"{sym}USDT","interval":interval,"limit":1000}
        if end: p["endTime"]=end
        data=_exp_get(f"{BINANCE_FAPI_BASE}/fapi/v1/klines",p); time.sleep(EXP_PACE)
        if data is None: complete=False; break          # فشل الطلب: بيانات ناقصة
        if not isinstance(data,list) or not data: break # [] = بداية التاريخ
        out=data+out; end=int(data[0][0])-1
        if len(data)<1000: break
    now=int(time.time()*1000)
    by_t={int(k[0]):k for k in out}
    rows=[by_t[t] for t in sorted(by_t) if t+tf_ms<=now]
    return rows[-bars:],complete

def _exp_funding(sym,years):
    cur=int((time.time()-years*365*86400)*1000); out=[]
    while True:
        data=_exp_get(f"{BINANCE_FAPI_BASE}/fapi/v1/fundingRate",{"symbol":f"{sym}USDT","startTime":cur,"limit":1000})
        time.sleep(EXP_PACE)
        if not isinstance(data,list) or not data: break
        out+=data; last=int(data[-1]["fundingTime"])
        if len(data)<1000 or last<cur: break
        cur=last+1
    return out

def exp_collect(syms,tmp,lite,progress=None):
    import csv
    meta=dict(version=BOT_VERSION,created_utc=dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
              lite=bool(lite),symbols={},failed=[])
    sets=[("4h","4h",14_400_000,EXP_4H_BARS if not lite else 6900),("1d","1d",86_400_000,EXP_1D_BARS)]
    if not lite: sets.append(("1h","1h",3_600_000,EXP_1H_BARS))
    for d_ in ("4h","1d","1h","funding"): os.makedirs(os.path.join(tmp,d_),exist_ok=True)
    for i,sy in enumerate(syms,1):
        if progress and (i==1 or i%3==0): progress(i,len(syms),sy)
        info={}
        try:
            for name,interval,tfms,bars in sets:
                rows,ok=_exp_klines(sy,interval,bars,tfms)
                if len(rows)<50: raise ValueError(f"{name}: بيانات قليلة")
                with open(os.path.join(tmp,name,f"{sy}.csv"),"w",newline="") as f:
                    w=csv.writer(f); w.writerow(["t","open","high","low","close","volume","quote_volume","trades","taker_buy_base","taker_buy_quote"])
                    for k in rows: w.writerow([k[0],k[1],k[2],k[3],k[4],k[5],k[7],k[8],k[9],k[10]])
                info[name]=dict(bars=len(rows),first=int(rows[0][0]),last=int(rows[-1][0]),complete=bool(ok))
            fr=_exp_funding(sy,EXP_FUND_YEARS)
            with open(os.path.join(tmp,"funding",f"{sy}.csv"),"w",newline="") as f:
                w=csv.writer(f); w.writerow(["fundingTime","fundingRate","markPrice"])
                for r in fr: w.writerow([r.get("fundingTime"),r.get("fundingRate"),r.get("markPrice","")])
            info["funding"]=dict(rows=len(fr))
            meta["symbols"][sy]=info
        except RuntimeError: raise
        except Exception as e:
            meta["failed"].append(dict(symbol=sy,error=str(e)[:120]))
    return meta

EXP_README="""Ryma research data (public Binance USDT-M futures data only)
- t / fundingTime: milliseconds since epoch, UTC. Candle t = OPEN time.
- Only CLOSED candles are included (the forming candle is dropped).
- klines columns: t,open,high,low,close,volume,quote_volume,trades,taker_buy_base,taker_buy_quote
- funding columns: fundingTime,fundingRate,markPrice (every 8h)
- Symbols = top by current 24h volume (survivorship bias: coins that died are missing; new coins have short history).
- meta.json lists bars per symbol, and `complete=false` if a download was cut short.
"""

def exp_zip(tmp,meta):
    import zipfile
    meta_path=os.path.join(tmp,"meta.json")
    with open(meta_path,"w",encoding="utf-8") as f: json.dump(meta,f,ensure_ascii=False,indent=1)
    readme=os.path.join(tmp,"README.txt")
    with open(readme,"w",encoding="utf-8") as f: f.write(EXP_README)
    groups=[("ryma_4h",["4h"]),("ryma_1d_funding",["1d","funding"]),("ryma_1h",["1h"])]
    outs=[]
    for base,dirs in groups:
        files=[]
        for d_ in dirs:
            p=os.path.join(tmp,d_)
            if os.path.isdir(p): files+=[(os.path.join(p,x),f"{d_}/{x}") for x in sorted(os.listdir(p))]
        if not files: continue
        parts=[]
        def open_part(n):
            path=os.path.join(tmp,f"{base}_p{n}.zip")
            z=zipfile.ZipFile(path,"w",zipfile.ZIP_DEFLATED,compresslevel=6)
            z.write(meta_path,"meta.json"); z.write(readme,"README.txt"); parts.append(path); return z,path
        n=1; zf,path=open_part(n)
        for full,arc in files:
            zf.fp.flush()
            if os.path.getsize(path)>=EXP_ZIP_MAX:      # الجزء امتلأ: نبدأ جزءاً جديداً قبل الملف التالي
                zf.close(); n+=1; zf,path=open_part(n)
            zf.write(full,arc)
        zf.close()
        if len(parts)==1:
            final=os.path.join(tmp,f"{base}.zip"); os.replace(parts[0],final); outs.append(final)
        else: outs+=parts
    return outs

async def _safe_edit(msg,text):
    try: await msg.edit_text(text)
    except Exception as _ex: log.debug('ignored: %s',_ex)

@admin_only
async def cmd_exportdata(update,ctx):
    """/exportdata [lite] — يصدّر بيانات 50 عملة (4س/يومي/ساعة/تمويل) كملفات ZIP لغرض البحث."""
    if _EXP_BUSY[0]:
        await update.message.reply_text("⏳ تصدير آخر قيد التشغيل"); return
    import shutil, tempfile
    lite=any(a.lower()=="lite" for a in ctx.args)
    _EXP_BUSY[0]=True; tmp=None
    try:
        msg=await update.message.reply_text(
            "⏳ بدأ التصدير (%s). يأخذ حوالي %s، ويرسل الملفات في الآخر. لا تعيد الأمر."%(
                BOT_VERSION,"4–6 دقائق" if lite else "10–15 دقيقة"))
        syms=await asyncio.to_thread(top_symbols,50)
        syms=["BTC"]+[x for x in syms if x!="BTC"]; syms=syms[:50]
        tmp=tempfile.mkdtemp(prefix="ryma_exp_")
        loop=asyncio.get_running_loop()
        def progress(i,n,sy): asyncio.run_coroutine_threadsafe(_safe_edit(msg,f"⏳ {i}/{n} · {sy}"),loop)
        meta=await asyncio.to_thread(exp_collect,syms,tmp,lite,progress)
        if not meta["symbols"]:
            await msg.edit_text("❌ ما اتجمعت بيانات. الأخطاء: "+"; ".join(f"{x['symbol']}:{x['error']}" for x in meta["failed"][:5])); return
        files=await asyncio.to_thread(exp_zip,tmp,meta)
        await msg.edit_text(f"✅ تم الجمع: {len(meta['symbols'])} عملة، فشل {len(meta['failed'])}. جارٍ إرسال {len(files)} ملف...")
        for path in files:
            mb=os.path.getsize(path)/1048576
            with open(path,"rb") as fh:
                await ctx.bot.send_document(update.effective_chat.id,document=fh,filename=os.path.basename(path),
                    caption=f"{os.path.basename(path)} · {mb:.1f}MB · {BOT_VERSION}",protect_content=False,
                    read_timeout=600,write_timeout=600,connect_timeout=60)
        if meta["failed"]:
            await update.message.reply_text("⚠️ فشل: "+", ".join(f"{x['symbol']}" for x in meta["failed"])[:3500])
    finally:
        _EXP_BUSY[0]=False
        if tmp: shutil.rmtree(tmp,ignore_errors=True)

@admin_only
async def cmd_bt50(update,ctx):
    try: years=float(ctx.args[0]) if ctx.args else 3.0
    except ValueError: years=3.0
    wait=await update.message.reply_text("⏳ Top 50...")
    syms=await asyncio.to_thread(top_symbols,50)
    syms=syms if isinstance(syms, (list, tuple)) else []
    per,a,b,errors=await asyncio.to_thread(backtest_many,syms,years)
    if not per:
        text,_=bt_report(per,a,b,"TOP50",errors)
        await wait.edit_text(text[:4000],link_preview_options=NO_PREVIEW); return
    text,_=bt_report(per,a,b,"TOP50",errors)
    await wait.edit_text(text[:4000],link_preview_options=NO_PREVIEW)

@admin_only
async def cmd_scan(update,ctx):
    wait=await update.message.reply_text("⏳ فحص...")
    syms=await asyncio.to_thread(top_symbols,60,True)
    res=await asyncio.to_thread(scan_sync,syms[:30])
    res=[r for r in res if r["rank"]>=0 and not r.get("forced")]; res.sort(key=lambda r:-r["rank"])
    if not res: await wait.edit_text("لا شيء"); return
    lines=["🎯 أفضل الإعدادات:",""]
    for r in res[:10]:
        s="🟢" if r["side"]==1 else "🔴"; dec="🟢 شراء" if r["side"]==1 else "🔴 بيع"
        pats=r.get("patterns",[]); pat_txt=""
        if pats:
            p=pats[0]; tf_lbl=f"[{p.get('tf','').upper()}]" if p.get('tf') else ""
            pat_txt=f" 🕯️{p['name']}{tf_lbl}"
        lines+=[f"{r['grade']} | #{r['sym']} {s}{pat_txt}",f"   🎯 القرار المتوقع: {dec}"]
    await wait.edit_text("\n".join(lines),link_preview_options=NO_PREVIEW)

@admin_only
async def cmd_short(update,ctx):
    wait=await update.message.reply_text("⏳...")
    res=await asyncio.to_thread(scan_sync,COINS)
    res=[r for r in res if r["side"]==-1 and r["rank"]>=0 and not r.get("forced")]
    if not res: await wait.edit_text("لا شورت"); return
    lines=["🔻 فرص شورت:"]+[f"{r['grade']} | #{r['sym']} | 🎯 بيع" for r in res[:15]]
    await wait.edit_text("\n".join(lines),link_preview_options=NO_PREVIEW)

@admin_only
async def cmd_delist(update,ctx):
    wait=await update.message.reply_text("⏳...")
    try:
        r=await asyncio.to_thread(requests.get,"https://www.binance.com/bapi/composite/v1/public/cms/article/list/query",
                       params=dict(type=1,catalogId=161,pageNo=1,pageSize=20),timeout=10)
        arts=r.json()["data"]["catalogs"][0]["articles"]
        skip={"BINANCE","WILL","DELIST","ON","AND","THE","FOR","USD","USDT","FROM","SPOT","TRADING",
            "PAIRS","PAIR","TOKEN","TOKENS","ANNOUNCEMENT","API","BNB","LISTING","MARGIN","FUTURES",
            "MONITORING","DELISTING"}
        now_ms=int(time.time()*1000); rows=[]
        for a in arts[:40]:
            title=a.get("title",""); release_ts=a.get("releaseDate",0)
            if release_ts and release_ts>now_ms:
                dt_str=dt.datetime.utcfromtimestamp(release_ts/1000).strftime("%Y-%m-%d")
                for w in re.findall(r'\b[A-Z0-9]{2,10}\b',title):
                    if w in skip or w.isdigit() or len(w)<2: continue
                    rows.append((w,dt_str))
        if not rows: await wait.edit_text("✅ لا توجد"); return
        seen=set(); lines=["⚠️ للحذف قريباً:",""]
        for c,d in rows:
            if c in seen: continue
            seen.add(c); lines.append(f"🔻 {c} — 📅 {d}")
        await wait.edit_text("\n".join(lines[:40]),link_preview_options=NO_PREVIEW)
    except Exception as e: await wait.edit_text(f"⚠️ {str(e)[:150]}")

@admin_only
async def cmd_price(update,ctx):
    if not ctx.args: await update.message.reply_text("مثال: /price BTC"); return
    sym=clean_symbol(ctx.args[0]); prices=await asyncio.to_thread(all_prices,sym)
    if not prices: await update.message.reply_text("لا بيانات"); return
    lo,hi=min(prices.values()),max(prices.values())
    lines=[f"💲 {sym}/USDT"]+[f"• {n}: {fmt(p)}" for n,p in sorted(prices.items(),key=lambda z:z[1])]
    lines.append(f"\n📊 الفرق: {100*(hi/lo-1):.2f}%")
    await update.message.reply_text("\n".join(lines),link_preview_options=NO_PREVIEW)

@admin_only
async def cmd_why(update,ctx):
    if not ctx.args: await update.message.reply_text("مثال: /why BTC"); return
    sym=clean_symbol(ctx.args[0])
    try:
        res=await asyncio.to_thread(get_analysis,sym,True); df=res["df"]; r=df.iloc[-1]
        side="شراء" if res["side"]==1 else "بيع"
        lines=[f"🔎 {sym} · {res['tf']} · {res.get('source')}",f"القرار: {side} | {res['trend']}",
            f"الإغلاق {fmt(r['close'])} | EMA20 {fmt(r['ema20'])} | RSI {r['rsi']:.0f}"]
        pats=res.get("patterns",[])
        if pats: lines.append(f"🕯️ النماذج: {', '.join(p['name'] + '[' + p.get('tf','').upper() + ']' for p in pats[:3])}")
        await update.message.reply_text("\n".join(lines))
    except Exception as e: await update.message.reply_text(f"⚠️ {str(e)[:150]}")

@admin_only
async def cmd_users(update,ctx):
    with store.lock: users=list(store.data["users"].values())
    users.sort(key=lambda u:-u.get("last_seen",0))
    lines=[f"👥 ({len(users)})",""]
    for u in users[:30]:
        n=(u.get("name") or "").strip(); un=(u.get("username") or "").strip()
        label=f"{n} (@{un})" if n and un else (n or (f"@{un}" if un else f"ID:{u['id']}"))
        st,_=user_status(int(u["id"]),u)
        icon={"admin":"👑","vip":"💎","trial":"🆓","warning":"⚠️","blocked":"⛔","grace":"🎁"}.get(st,"•")
        lines+=[f"{icon} {label}",f"    📊 {u.get('requests',0)} | نقاط: {u.get('points',0)} | آخر: {time_ago(u.get('last_seen'))}"]
    await update.message.reply_text("\n".join(lines)[:4000],link_preview_options=NO_PREVIEW)

@admin_only
async def cmd_testchannels(update,ctx):
    out=[]
    for name,chat in (("FREE",CHANNEL_ID),("VIP",VIP_CHANNEL_ID)):
        if not chat: out.append(f"❌ {name}"); continue
        try:
            await ctx.bot.send_message(chat,f"✅ اختبار - {BRAND}"); out.append(f"✅ {name}")
        except Exception as e: out.append(f"❌ {name}: {str(e)[:80]}")
    await update.message.reply_text("\n".join(out))

@admin_only
async def cmd_stats(update,ctx):
    since=(time.time()-7*DAY)*1000
    with store.lock: h=[x for x in store.data["history"] if (x.get("closed") or 0)>=since]
    if not h: await update.message.reply_text("لا صفقات"); return
    R=np.array([x["R"] for x in h]); g,l=R[R>0].sum(),-R[R<=0].sum()
    pf=round(float(g/l),2) if l>0 else 999
    await update.message.reply_text(f"📈 آخر 7 أيام\n\nصفقات: {len(R)} | WR: {100*(R>0).mean():.1f}%\nPF: {pf} | {R.sum():+.1f}R")

@admin_only
async def cmd_history(update,ctx):
    with store.lock: h=store.data["history"][-15:][::-1]
    if not h: await update.message.reply_text("لا يوجد"); return
    await update.message.reply_text("🗂 آخر 15:\n"+"\n".join(
        f"{'✅' if x['R']>0 else '❌'} #{x['coin']} {x['R']:+.2f}R | {x['result']}" for x in h))

@admin_only
async def cmd_dashboard(update,ctx):
    act=active_signals()
    with store.lock:
        nu=len(store.data["users"]); nv=sum(1 for k in store.data["vip"] if is_vip(int(k)))
    maint="🔧 صيانة" if is_maintenance() else "✅ يعمل"
    msg=(f"🎛 لوحة التحكم — {maint}\n\n👥 {nu} | 💎 VIP: {nv}\n"
         f"📊 نشطة: {len(act)}/{MAX_OPEN}\n💾 {store.remote_msg}\n"
         f"📡 Binance Futures: ✅\n"
         f"📢 FREE {'✅' if CHANNEL_ID else '—'} | VIP {'✅' if VIP_CHANNEL_ID else '—'}\n\n"
         f"🎯 نقاط: {VIP_THRESHOLD}=VIP{VIP_REWARD_DAYS}d | +{POINTS_REFERRAL}/إحالة\n"
         f"🕯️ النماذج: MTF ✅")
    await update.message.reply_text(msg)

@admin_only
async def cmd_addvip(update,ctx):
    try: uid,days=int(ctx.args[0]),int(ctx.args[1])
    except Exception: await update.message.reply_text("/addvip ID DAYS"); return
    exp=add_vip(uid,days); e=dt.datetime.utcfromtimestamp(exp).strftime("%Y-%m-%d")
    await update.message.reply_text(f"✅ VIP {uid} حتى {e}")

@admin_only
async def cmd_removevip(update,ctx):
    try: uid=int(ctx.args[0])
    except Exception: await update.message.reply_text("/removevip ID"); return
    await update.message.reply_text("✅" if remove_vip(uid) else "ليس VIP")

@admin_only
async def cmd_viplist(update,ctx):
    with store.lock: rows=sorted(store.data["vip"].items(),key=lambda kv:kv[1].get("expires",0))
    lines=[f"👑 VIP ({len(rows)})"]
    for uid,v in rows[:40]:
        left=(int(v.get("expires",0))-now_s())//DAY
        lines.append(f"{uid} - {'متبقي '+str(left)+' يوم' if left>=0 else 'منتهي'}")
    await update.message.reply_text("\n".join(lines))

class _FakeUpdate:
    def __init__(self,msg,user):
        self.message=msg; self.effective_user=user; self.callback_query=None

@vip_only
async def cmd_radar(update, ctx):
    """مسح يدوي لرادار الفرص المبكرة + تنبيه VIP والأدمن."""
    wait = await update.message.reply_text("⚡ <b>رادار الفرص المبكرة</b>\n⏳ مسح السوق (15د)...", parse_mode=ParseMode.HTML)
    try:
        syms = await asyncio.to_thread(top_symbols, 80)
        hits = await asyncio.to_thread(radar_scan, syms)
        if not hits:
            await wait.edit_text("⚡ لا توجد فرص مبكرة حالياً (السوق هادئ)."); return
        lines = [f"⚡ <b>رادار الفرص المبكرة</b> · {len(hits)} فرصة", "━━━━━━━━━━━━━━━━", ""]
        for h in hits[:12]:
            side_e = "🟢 LONG" if h["side"] == 1 else "🔴 SHORT"
            cl = coin_link(h["sym"], "binance_fut")
            lines.append(f"{side_e} <a href='{cl}'>#{h['sym']}</a> · {fmt(h['price'])}")
            lines.append(f"   تحرك {h['move']:+.1f}% | حجم ×{h['vol_ratio']} | 24س {h['ch24']:+.1f}%")
            lv = h["lv"]
            lines.append(f"   ➡️ {fmt(lv['entry'])} | 🛑 {fmt(lv['sl'])} | 🎯 {fmt(lv['tps'][0]['px'])}")
            lines.append("")
            key = f"{h['sym']}:{h['bar_t']}"
            if key not in _radar_seen:
                _radar_seen[key] = time.time()
                if not any(p.get("coin") == h["sym"] and p.get("state") in ("pending", "open") for p in active_signals()):
                    try: await asyncio.to_thread(track_signal, h)
                    except Exception: pass
        lines += [RISK_WARNING_AR, f"👤 {BRAND}"]
        txt = "\n".join(lines)[:4000]
        await wait.edit_text(txt, parse_mode=ParseMode.HTML, link_preview_options=NO_PREVIEW)
        for chat in (VIP_CHANNEL_ID, ADMIN_ID or None):
            if not chat: continue
            if update.effective_chat and chat == update.effective_chat.id: continue
            try:
                await ctx.bot.send_message(chat, txt, parse_mode=ParseMode.HTML,
                                           protect_content=(PROTECT if chat != ADMIN_ID else False),
                                           link_preview_options=NO_PREVIEW)
            except Exception as _ex: log.debug("cmd_radar notify: %s", _ex)
    except Exception as e:
        log.exception("cmd_radar")
        try: await wait.edit_text(f"⚠️ {str(e)[:150]}")
        except Exception: pass

ADMIN_CMD_MAP={
    "/forcepost":cmd_forcepost,"/clearsignals":cmd_clearsignals,
    "/smartcleanup":cmd_smartcleanup,"/debug":cmd_debug,"/daily":cmd_daily,
    "/cross":cmd_cross,"/predict":cmd_predict,"/whales":cmd_whales,
    "/explosions":cmd_explosions,"/pump":cmd_pump,"/bottom":cmd_bottoms,
    "/shortsradar":cmd_shorts_radar,"/alpha":cmd_alpha,"/scan":cmd_scan,
    "/short":cmd_short,"/bt":cmd_bt,"/bt50":cmd_bt50,"/users":cmd_users,
    "/viplist":cmd_viplist,"/stats":cmd_stats,"/history":cmd_history,
    "/dashboard":cmd_dashboard,"/testchannels":cmd_testchannels,"/delist":cmd_delist,
    "/golden":cmd_golden,"/goldenconfig":cmd_goldenconfig,"/share":cmd_share,
    "/alpha_v2":cmd_alpha_radar_v2,"/patterns":cmd_patterns,"/neon":cmd_neon,
    "/radar":cmd_radar,"/spot":cmd_spot,"/plan":cmd_plan,"/active":cmd_active}

ADMIN_ARGS_HINTS={
    "liquidity":"أرسل: /liquidity BTC","futures":"أرسل: /futures BTC",
    "price":"أرسل: /price BTC","why":"أرسل: /why BTC","post":"أرسل: /post BTC",
    "addvip":"أرسل: /addvip ID DAYS","removevip":"أرسل: /removevip ID",
    "share":"أرسل: /share BTC","patterns":"أرسل: /patterns BTC (أو /patterns بدون رمز للفحص التلقائي)",
    "addpoints":"أرسل: /addpoints ID 5 ترويج"}

async def cb_handler(update,ctx):
    q=update.callback_query
    try: await q.answer()
    except Exception as _ex: log.debug('ignored: %s', _ex)
    if q.data.startswith("gc:"):
        if q.from_user.id!=ADMIN_ID: return
        key=q.data.split(":",1)[1]
        if key=="refresh":
            try: await q.message.edit_text(build_golden_config_text(),reply_markup=keyboard_golden_config(),parse_mode=ParseMode.HTML)
            except Exception as _ex: log.debug('ignored: %s', _ex)
            return
        if key=="min_score":
            cfg=get_golden_config(); current=cfg.get("min_score",1)
            cycle={0:1,1:2,2:3,3:4,4:5,5:6,6:7,7:8,8:9,9:0}
            set_golden_config("min_score",cycle.get(current,1))
            try: await q.message.edit_text(build_golden_config_text(),reply_markup=keyboard_golden_config(),parse_mode=ParseMode.HTML)
            except Exception as _ex: log.debug('ignored: %s', _ex)
            return
        if key in DEFAULT_GOLDEN_CONFIG:
            cfg=get_golden_config(); current=cfg.get(key,False); set_golden_config(key,not current)
            try: await q.message.edit_text(build_golden_config_text(),reply_markup=keyboard_golden_config(),parse_mode=ParseMode.HTML)
            except Exception as _ex: log.debug('ignored: %s', _ex)
        return
    if q.data == "hype_radar":
        if q.from_user.id != ADMIN_ID:
            await q.message.reply_text("⛔ للأدمن فقط"); return
        wait = await q.message.reply_text("🔥 رادار الضجة... يجلب الترندات")
        try:
            results = await asyncio.to_thread(hype_radar, 10)
            if not results:
                await wait.edit_text("🔥 لا توجد ترندات حالياً"); return
            txt = build_hype_radar_text(results)
            txt += "\n\n" + RISK_WARNING_AR
            await wait.edit_text(txt[:4000], parse_mode=ParseMode.HTML, link_preview_options=NO_PREVIEW)
        except Exception as e:
            await wait.edit_text(f"⚠️ {str(e)[:150]}")
        return
    if q.data.startswith("neon:"):
        if q.from_user.id != ADMIN_ID: return
        sym = q.data.split(":")[1]
        wait = await q.message.reply_text(f"💳 جارٍ توليد بطاقة النيون لـ {sym}...")
        try:
            res = await asyncio.to_thread(get_analysis, sym, True)
            if res.get("wait"):
                await wait.edit_text("⏳ لا توجد صفقة مؤهلة (انتظار)."); return
            card = await asyncio.to_thread(render_trade_card, res, "admin")
            await q.message.reply_photo(photo=card)
            await wait.delete()
        except Exception as e:
            await wait.edit_text(f"⚠️ {str(e)[:150]}")
        return
    if q.data == "neon_card_manual":
        if q.from_user.id != ADMIN_ID: return
        await q.message.reply_text("💳 <b>بطاقة النيون</b>\n\nأرسل: <code>/neon BTC</code>", parse_mode=ParseMode.HTML)
        return
    if q.data == "patterns_chart_manual":
        if q.from_user.id != ADMIN_ID: return
        await q.message.reply_text("🖼️ <b>شارت النماذج</b>\n\nأرسل: <code>/patterns BTC</code>", parse_mode=ParseMode.HTML)
        return
    if q.data=="toggle_maintenance":
        if q.from_user.id!=ADMIN_ID:
            await q.message.reply_text("⛔ للأدمن فقط"); return
        new_state=set_maintenance(not is_maintenance())
        msg="🔧 <b>وضع الصيانة مفعّل</b>" if new_state else "✅ <b>وضع الصيانة معطّل</b>"
        await q.message.reply_text(msg,parse_mode=ParseMode.HTML); return
    if q.data=="toggle_multi_exchange":
        if q.from_user.id!=ADMIN_ID:
            await q.message.reply_text("⛔ للأدمن فقط"); return
        new_state=set_multi_exchange(not is_multi_exchange())
        if new_state: msg=("🌐 <b>تعدد المنصات: مفعّل</b>\n\n✅ سيبحث البوت في جميع المنصات\n✅ سيظهر اسم المنصة المصدر في كل توصية.")
        else: msg=("🛑 <b>تعدد المنصات: معطّل</b>\n\n✅ سيعتمد البوت على Binance فقط.")
        await q.message.reply_text(msg,parse_mode=ParseMode.HTML)
        try: await q.message.edit_reply_markup(reply_markup=keyboard_admin_menu())
        except Exception as _ex: log.debug('ignored: %s', _ex)
        return
    if q.data=="admin_menu":
        if q.from_user.id!=ADMIN_ID: return
        try: await q.message.reply_text(build_admin_menu_text(),reply_markup=keyboard_admin_menu(),parse_mode=ParseMode.HTML)
        except Exception as _ex: log.debug('ignored: %s', _ex)
        return
    if q.data=="my_portfolio":
        await cmd_portfolio(_FakeUpdate(q.message,q.from_user),ctx); return
    if q.data=="my_points":
        rec=store.data["users"].get(str(q.from_user.id),{}); pts=rec.get("points",0)
        bot_username=ctx.application.bot_data.get("username","")
        ref_link=f"https://t.me/{bot_username}?start=ref_{q.from_user.id}" if bot_username else "—"
        await q.message.reply_text(
            f"🎯 <b>نقاطك:</b> {pts}/{VIP_THRESHOLD}\n\n{points_visual(pts)}\n\n"
            f"📢 <b>نشر رابط القناة أو البوت = {POINTS_PROMO} نقاط</b>\n"
            f"👥 <b>دعوة مشترك جديد = {POINTS_REFERRAL} نقطة</b>\n\n"
            f"🔗 <b>رابط البوت (إحالتك):</b>\n<code>{ref_link}</code>\n\n"
            f"📢 <b>قناة التوصيات:</b>\n<code>{CHANNEL_LINK}</code>",
            parse_mode=ParseMode.HTML,link_preview_options=NO_PREVIEW)
        return
    if q.data=="my_ref":
        bot_username=ctx.application.bot_data.get("username","")
        if not bot_username:
            await q.message.reply_text("⚠️ جرب بعد دقيقة"); return
        ref_link=f"https://t.me/{bot_username}?start=ref_{q.from_user.id}"
        rec=store.data["users"].get(str(q.from_user.id),{}); pts=rec.get("points",0)
        await q.message.reply_text(
            f"🔗 <b>رابط الإحالة:</b>\n\n<code>{ref_link}</code>\n\n"
            f"💰 كل مشترك جديد = <b>{POINTS_REFERRAL} نقطة</b>\n\n"
            f"🎯 نقاطك: <b>{pts}/{VIP_THRESHOLD}</b>\n{points_visual(pts)}",
            parse_mode=ParseMode.HTML,link_preview_options=NO_PREVIEW)
        return
    if q.data=="close_menu":
        try: await q.message.delete()
        except Exception as _ex: log.debug('ignored: %s', _ex)
        return
    if q.data.startswith("calc_risk:"):
        sym=q.data.split(":")[1]; ctx.user_data['awaiting_risk']=sym
        await q.message.reply_text(
            f"🧮 <b>حاسبة إدارة المخاطر ({sym})</b>\n\nأرسل رأس مالك ونسبة المخاطرة (مفصولة بمسافة).\n"
            f"مثال: <code>1000 2</code>",parse_mode=ParseMode.HTML); return
    if q.data.startswith("add_port:"):
        sym=q.data.split(":")[1]; ctx.user_data['awaiting_port']=sym
        await q.message.reply_text(
            f"📊 <b>إضافة {sym} للمحفظة</b>\n\nكم المبلغ الذي استثمرته بالدولار؟ (مثال: <code>100</code>)",
            parse_mode=ParseMode.HTML); return
    if q.data.startswith("news:"):
        sym=q.data.split(":")[1]
        news=await asyncio.to_thread(get_crypto_news,sym)
        await q.message.reply_text(f"📰 <b>آخر أخبار {sym}:</b>\n\n{news}",parse_mode=ParseMode.HTML); return
    if q.data.startswith("share:"):
        sym=q.data.split(":")[1]
        try:
            res=await asyncio.to_thread(get_analysis,sym,True)
            if res.get("wait"):
                await q.message.reply_text("⏳ لا توجد صفقة مؤهلة حالياً."); return
            await q.message.reply_text("👇 اختر المنصة لنسخ المنشور:",reply_markup=keyboard_share_post(res))
        except Exception: pass
        return
    if q.data.startswith("pat:"):
        if q.from_user.id!=ADMIN_ID: return
        try:
            sym=q.data.split(":")[1]; res=await asyncio.to_thread(get_analysis,sym,True); pats=res.get("patterns",[])
            if not pats:
                await q.message.reply_text(f"🕯️ #{sym} — لا نماذج مؤكدة."); return
            lines=[f"🕯️ <b>نماذج #{sym} (MTF)</b>",""]
            for p in pats:
                emoji="🟢" if p["bias"]=="bullish" else ("🔴" if p["bias"]=="bearish" else "⚪")
                tf_lbl=f" [{p.get('tf','').upper()}]" if p.get('tf') else ""
                lines.append(f"{emoji} {p['name']}{tf_lbl} — {p['bias']}")
                if p.get("neckline"): lines.append(f"   📏 Neckline: {fmt(p['neckline'])}")
            await q.message.reply_text("\n".join(lines),parse_mode=ParseMode.HTML,link_preview_options=NO_PREVIEW)
        except Exception as _ex: log.debug('ignored: %s', _ex)
        return
    if q.data.startswith("refs:"):
        try:
            msg=("🌐 <b>منصات التداول المعتمدة</b>\n\n💡 <b>لماذا التسجيل عبر البوت؟</b>\n"
                "✅ رسوم تداول أقل\n✅ دعم فني من فريقنا\n✅ دعم استمرارية القناة المجانية\n\n👇 اختر منصتك للتسجيل:")
            await q.message.reply_text(msg,reply_markup=keyboard_refs("binance_fut"),parse_mode=ParseMode.HTML,link_preview_options=NO_PREVIEW)
        except Exception as _ex: log.debug('ignored: %s', _ex)
        return
    if q.data.startswith("adm_args:"):
        if q.from_user.id!=ADMIN_ID: return
        key=q.data.split(":",1)[1]
        await q.message.reply_text(f"✏️ {ADMIN_ARGS_HINTS.get(key,'الأمر مع الوسيط')}"); return
    if q.data.startswith("adm:"):
        if q.from_user.id!=ADMIN_ID: return
        cmd_name=q.data.split(":",1)[1]; fn=ADMIN_CMD_MAP.get(cmd_name)
        if not fn:
            await q.message.reply_text("⚠️ غير معروف"); return
        fake=_FakeUpdate(q.message,q.from_user)
        try: await fn(fake,ctx)
        except Exception as e:
            try: await q.message.reply_text(f"⚠️ {str(e)[:150]}")
            except Exception as _ex: log.debug('ignored: %s', _ex)
        return
    if q.data in ("vip","vip_page"):
        kb=InlineKeyboardMarkup([
            [_btn("💎 الانضمام لقناة VIP ✨",url=VIP_INVITE_LINK)],
            [_btn("🆓 قناة التوصيات المجانية",url=CHANNEL_LINK)],
            [_btn("🎁 تجربة يوم",callback_data="vip_trial")]])
        try: await q.message.reply_text(build_vip_page(),reply_markup=kb)
        except Exception as _ex: log.debug('ignored: %s', _ex)
        return
    if q.data=="vip_trial":
        u=q.from_user; uid=str(u.id)
        with store.lock:
            u_rec=store.data["users"].get(uid,{})
            if u_rec.get("trial_used"):
                await q.message.reply_text("⛔ استخدمت التجربة",
                    reply_markup=InlineKeyboardMarkup([[_btn("💎 اشترك",url=VIP_INVITE_LINK)]])); return
            u_rec["trial_used"]=True; store.data["users"][uid]=u_rec; store.save("users")
        add_vip(u.id,1)
        await q.message.reply_text("🎉 تجربة VIP 24 ساعة!",
            reply_markup=InlineKeyboardMarkup([[_btn("📋 البدء",callback_data="vip_menu")]])); return
    if q.data=="vip_menu":
        u=q.from_user; rec=store.data["users"].get(str(u.id),{}); st,_=user_status(u.id,rec)
        if st not in ("admin","vip","trial","warning"):
            await q.message.reply_text("🔒 VIP فقط"); return
        await q.message.reply_text(build_vip_menu_text(),reply_markup=keyboard_vip_menu()); return
    if q.data.startswith("vip_cmd_args:"):
        key=q.data.split(":",1)[1]
        u=q.from_user; rec=store.data["users"].get(str(u.id),{}); st,_=user_status(u.id,rec)
        if st not in ("admin","vip","trial","warning"):
            await q.message.reply_text("🔒 VIP فقط"); return
        hints={"liquidity":"💧 /liquidity BTC","futures":"📊 /futures BTC"}
        await q.message.reply_text(f"✏️ {hints.get(key,'الأمر مع الرمز')}"); return
    if q.data.startswith("vip_cmd:"):
        cmd=q.data.split(":",1)[1]
        u=q.from_user; rec=store.data["users"].get(str(u.id),{}); st,_=user_status(u.id,rec)
        if st not in ("admin","vip","trial","warning"):
            await q.message.reply_text("🔒 VIP فقط"); return
        try:
            fake=_FakeUpdate(q.message,q.from_user)
            handlers={"/predict":cmd_predict,"/whales":cmd_whales,"/explosions":cmd_explosions,
                "/pump":cmd_pump,"/bottom":cmd_bottoms,"/shortsradar":cmd_shorts_radar,
                "/alpha":cmd_alpha,"/active":cmd_active,"/golden":cmd_golden,"/daily":cmd_daily,
                "/alpha_v2":cmd_alpha_radar_v2,"/radar":cmd_radar,"/spot":cmd_spot}
            fn=handlers.get(cmd)
            if fn: await fn(fake,ctx)
        except Exception as e:
            try: await q.message.reply_text(f"⚠️ {str(e)[:150]}")
            except Exception as _ex: log.debug('ignored: %s', _ex)
        return
    if q.data.startswith("ind:"):
        if q.from_user.id!=ADMIN_ID: return
        try:
            sym=q.data.split(":")[1]; res=await asyncio.to_thread(get_analysis,sym)
            await q.message.reply_text(build_admin_extras(res))
        except Exception as e: await q.message.reply_text(f"⚠️ {str(e)[:200]}")
        return
    if q.data.startswith("liq:"):
        if q.from_user.id!=ADMIN_ID: return
        try:
            sym=q.data.split(":")[1]; liq=await asyncio.to_thread(liquidity_radar,sym)
            if not liq: await q.message.reply_text("لا بيانات"); return
            bias="🟢" if liq["ratio"]>0.55 else ("🔴" if liq["ratio"]<0.45 else "⚖️")
            await q.message.reply_text(f"💧 {sym}\n🟢 ${liq['bids']:,.0f}\n🔴 ${liq['asks']:,.0f}\n{liq['ratio']*100:.1f}% | {bias}")
        except Exception as _ex: log.debug('ignored: %s', _ex)
        return
    if q.data.startswith("whale:"):
        if q.from_user.id!=ADMIN_ID: return
        try:
            sym=q.data.split(":")[1]; w,largest,bv,sv=await asyncio.to_thread(whale_radar,sym)
            if w is None: await q.message.reply_text("لا بيانات"); return
            total=bv+sv; buy_pct=(bv/total*100) if total>0 else 0
            direction="🟢" if buy_pct>55 else ("🔴" if buy_pct<45 else "⚖️")
            await q.message.reply_text(f"🐋 {sym}\n📊 {w} صفقة\n💰 ${largest:,.0f}\n{direction} {buy_pct:.0f}%")
        except Exception as _ex: log.debug('ignored: %s', _ex)
        return
    if q.data.startswith("btcb:"):
        if q.from_user.id!=ADMIN_ID: return
        sym=q.data.split(":")[1]
        try:
            per,a,b,errors=await asyncio.to_thread(backtest_many,[sym],3.0)
            if not per:
                text,_=bt_report(per,a,b,sym,errors)
                await q.message.reply_text(text[:4000]); return
            text,_=bt_report(per,a,b,sym,errors)
            await q.message.reply_text(text[:4000])
        except Exception as _ex: log.debug('ignored: %s', _ex)
        return
    if q.data.startswith(("tv:","chart:")):
        parts=q.data.split(":"); sym=parts[1]; tf=parts[2] if len(parts)>2 and parts[2] in TV_IV else None
        if cb_throttled(q.from_user.id,4):
            await q.message.reply_text("⏳ انتظر قليلاً ثم أعد المحاولة"); return
        user=touch_user(q.from_user,"tvchart"); st,_=user_status(q.from_user.id,user)
        if st=="blocked":
            await q.message.reply_text(t("blocked",get_user_lang(q.from_user.id),contact=(CONTACT_LINK or CHANNEL_LINK))); return
        lang=get_user_lang(q.from_user.id)
        wait_m=await q.message.reply_text("📈 جارٍ رسم الشارت والنماذج..." if lang=="ar" else "📈 Drawing chart & patterns...")
        try:
            res=await asyncio.to_thread(get_analysis,sym)
            tf=tf or res["tf"]
            img,pats=await asyncio.to_thread(build_tv_for,sym,tf,res)
            await ctx.bot.send_photo(q.message.chat.id,photo=img,caption=tv_caption(sym,tf,pats,lang)[:1000],
                                     parse_mode=ParseMode.HTML,reply_markup=tv_keyboard(sym,tf,lang),
                                     protect_content=(st!="admin" and PROTECT))
        except PairNotFound: await q.message.reply_text(t("not_found",lang,sym=sym))
        except NoData: await q.message.reply_text(t("no_data",lang))
        except Exception:
            log.exception("tvchart"); await q.message.reply_text("⚠️ تعذر إنشاء الشارت الآن" if lang=="ar" else "⚠️ Couldn't build the chart now")
        finally:
            try: await wait_m.delete()
            except Exception as _ex: log.debug('ignored: %s', _ex)
        return
    if q.data.startswith("pchart:"):
        parts=q.data.split(":"); sym=parts[1]
        if q.from_user.id != ADMIN_ID:
            await q.message.reply_text("⛔ للأدمن فقط"); return
        if cb_throttled(q.from_user.id): await q.message.reply_text("⏳ انتظر قليلاً ثم أعد المحاولة"); return
        try:
            res=await asyncio.to_thread(get_analysis,sym)
            img=await asyncio.to_thread(render_pattern_chart,res,"admin")
            await ctx.bot.send_photo(q.message.chat.id,photo=img)
        except Exception:
            log.exception("pchart"); await q.message.reply_text("⚠️ تعذر إنشاء الشارت الآن")
        return
    if q.data.startswith("tf:"):
        parts=q.data.split(":"); sym,tf=parts[1],parts[2]
        if cb_throttled(q.from_user.id): await q.message.reply_text("⏳ انتظر قليلاً ثم أعد المحاولة"); return
        user=touch_user(q.from_user,"refresh"); st,_=user_status(q.from_user.id,user)
        tier="admin" if st=="admin" else ("vip" if st=="vip" else "free")
        lang=get_user_lang(q.from_user.id)
        _candle_cache.pop((sym,tf,450,215,False),None); _analysis_cache.pop(sym,None)
        try:
            res=await asyncio.to_thread(get_analysis,sym); source=res.get("source","binance_fut")
            kb=keyboard(sym,res["tf"],tier,source,lang)
            img=await asyncio.to_thread(render_chart,res,tier)
            await ctx.bot.send_photo(q.message.chat.id,photo=img,protect_content=(st!="admin" and PROTECT),reply_markup=kb)
            if tier=="admin" and not res.get("wait"): await send_admin_copy(q.message,res)
            cur_pts=store.data["users"].get(str(q.from_user.id),{}).get("points",0)
            bot_un=ctx.application.bot_data.get("username","")
            await send_points_flash(q.message,cur_pts,q.from_user.id,bot_un)
        except Exception as _ex: log.debug('ignored: %s', _ex)

def free_event_text(coin,tf,side,e,R,trade):
    k=e["kind"]
    if k=="TP" and e.get("j",99)>2: return None
    if k in ("ENTRY","BE_MOVED"): return None
    t2=dict(trade); t2["tps"]=[dict(x) for x in trade["tps"][:2]]
    return event_text(coin,tf,side,e,R,"free",trade=t2)

def grade_winrate(grade):
    with store.lock: h=[x for x in store.data["history"] if x.get("grade")==grade]
    if len(h)<20: return None,len(h)
    wins=sum(1 for x in h if (x.get("R") or 0)>0)
    return round(100*wins/len(h),1),len(h)

def admin_stats_line(res):
    wr,n=grade_winrate(res.get("grade"))
    if wr is None:
        return f"📊 [أدمن] درجة {res.get('grade')}: بيانات غير كافية ({n}/20 صفقة مغلقة)"
    return f"📊 [أدمن] درجة {res.get('grade')}: نجاح تاريخي {wr}% من {n} صفقة"

def eth_exchange_flows(hours=1,min_eth=ETH_MIN_TX):
    if not ETHERSCAN_KEY or not ETH_WATCH: return []
    out=[]; start_ts=time.time()-hours*3600
    for addr in ETH_WATCH[:10]:
        try:
            r=requests.get("https://api.etherscan.io/v2/api",
                params={"chainid":1,"module":"account","action":"txlist","address":addr,
                    "startblock":0,"endblock":99999999,"page":1,"offset":50,"sort":"desc","apikey":ETHERSCAN_KEY},timeout=10)
            data=r.json()
            if str(data.get("status"))!="1": continue
            for tx in data.get("result",[]):
                if int(tx.get("timeStamp",0))<start_ts: break
                eth=int(tx.get("value",0))/1e18
                if eth<min_eth: continue
                frm=(tx.get("from") or "").lower(); to=(tx.get("to") or "").lower()
                if to==addr: flow="إيداع في البورصة (ضغط بيع محتمل)"
                elif frm==addr: flow="سحب من البورصة (تجميع محتمل)"
                else: continue
                out.append(f"• ETH {eth:,.0f} — {flow}")
        except Exception: continue
        time.sleep(0.25)
    return out[:10]

def whale_alert_recent(min_usd=1_000_000,minutes=60):
    if not WHALE_ALERT_KEY: return []
    try:
        start=int(time.time())-minutes*60
        r=requests.get("https://api.whale-alert.io/v1/transactions",
            params={"api_key":WHALE_ALERT_KEY,"min_value":int(min_usd),"start":start},timeout=10)
        if r.status_code!=200: return []
        out=[]
        for tx in r.json().get("transactions",[]):
            sym=(tx.get("symbol") or "").upper(); usd=float(tx.get("amount_usd") or 0)
            frm=(tx.get("from") or {}).get("owner_type","unknown"); to=(tx.get("to") or {}).get("owner_type","unknown")
            if to=="exchange" and frm!="exchange": flow="إيداع في بورصة (ضغط بيع محتمل)"
            elif frm=="exchange" and to!="exchange": flow="سحب من بورصة (تجميع محتمل)"
            else: flow="تحويل بين محافظ"
            out.append(f"• {sym} ${usd:,.0f} — {flow}")
        return out
    except Exception: return []

async def on_error(update,ctx):
    err=ctx.error
    if err.__class__.__name__=="Conflict":
        log.error("⚠️ 409 Conflict: نسخة أخرى من البوت تعمل بنفس التوكن (Railway: أوقف النشر القديم/اجعل replicas=1، وأوقف أي نسخة محلية)")
        return
    log.error("err: %s",err,exc_info=err)

async def log_updates(update,ctx):
    q=update.callback_query
    if q: log.info("🔘 callback %s من %s",q.data,q.from_user.id)

async def cmd_botinfo(update,ctx):
    if update.effective_user.id!=ADMIN_ID: return
    w=await ctx.bot.get_webhook_info()
    await update.message.reply_text(
        f"version: {BOT_VERSION}\nwebhook: {w.url or 'لا يوجد ✅'}\nallowed_updates: {w.allowed_updates or 'الافتراضي ✅'}\n"
        f"pending: {w.pending_update_count}\nlast_error: {w.last_error_message or '-'}")

async def on_text(update,ctx):
    if not update.message or not update.message.text: return
    text=update.message.text.strip(); uid=str(update.effective_user.id)
    if ctx.user_data is None: ctx.user_data={}
    if re.search(r'[؀-ۿ]',text): lang="ar"
    elif len(text.split())>=3: lang="en"
    else: lang=get_user_lang(update.effective_user.id)
    set_user_lang(update.effective_user.id,lang)
    st,ok=await gate(update)
    if not ok: return
    if 'awaiting_risk' in ctx.user_data:
        sym=ctx.user_data.pop('awaiting_risk')
        try:
            cap,risk_pct=map(float,text.split())
            res=await asyncio.to_thread(get_analysis,sym,True)
            if res.get("wait"):
                await update.message.reply_text("⏳ لا توجد صفقة حالياً لهذه العملة، فلا حساب مخاطر." if lang!="en" else "⏳ No active setup for this coin, so no risk calc."); return
            entry=res['lv']['entry']; sl=res['lv']['sl']
            risk_amount=cap*(risk_pct/100); price_diff=abs(entry-sl)
            position_size=risk_amount/price_diff; position_value=position_size*entry
            leverage=position_value/cap if cap>0 else 0
            if lang=="en":
                msg=(f"🧮 <b>Risk Management Result for {sym}:</b>\n\n💰 Capital: ${cap}\n"
                    f"🛑 Risk ({risk_pct}%): ${risk_amount:.2f}\n➡️ Position Size: {position_size:.4f}\n"
                    f"💵 Position Value: ${position_value:.2f}\n\n⚙️ <b>Suggested Leverage:</b> {leverage:.1f}x\n"
                    f"<i>(Always use Isolated margin)</i>")
            else:
                msg=(f"🧮 <b>نتيجة إدارة المخاطر لـ {sym}:</b>\n\n💰 رأس المال: ${cap}\n"
                    f"🛑 المخاطرة ({risk_pct}%): ${risk_amount:.2f}\n➡️ حجم العقد: {position_size:.4f}\n"
                    f"💵 قيمة الصفقة: ${position_value:.2f}\n\n⚙️ <b>الرافعة المقترحة:</b> {leverage:.1f}x\n"
                    f"<i>(استخدم دائماً العزل Isolated)</i>")
            await update.message.reply_text(msg,parse_mode=ParseMode.HTML)
        except Exception:
            await update.message.reply_text("⚠️ Invalid format (1000 2)" if lang=="en" else "⚠️ صيغة خاطئة (مثال: 1000 2)")
        return
    if 'awaiting_port' in ctx.user_data:
        sym=ctx.user_data.pop('awaiting_port')
        try:
            amount=float(text)
            res=await asyncio.to_thread(get_analysis,sym,True); entry=res['price']
            with store.lock:
                if uid not in store.data["portfolios"]: store.data["portfolios"][uid]=[]
                store.data["portfolios"][uid].append({"sym":sym,"entry":entry,"amount":amount})
                store.save("portfolios")
            await update.message.reply_text(
                f"✅ {sym} added to your portfolio (${amount}).\n/portfolio" if lang=="en"
                else f"✅ تمت إضافة {sym} إلى محفظتك (${amount}).\n/portfolio")
        except Exception:
            await update.message.reply_text("⚠️ Send a valid number (100)" if lang=="en" else "⚠️ أرسل رقماً صحيحاً (مثال: 100)")
        return
    sym=clean_symbol(text.split()[0])
    if not sym:
        await update.message.reply_text(t("invalid_symbol",lang)); return
    if sym in EXCLUDE_TICKERS:
        await update.message.reply_text(t("not_supported",lang)); return
    wait=await update.message.reply_text(t("analyzing",lang,sym=sym))
    try:
        res=await asyncio.to_thread(get_analysis,sym)
        tier="admin" if st=="admin" else ("vip" if st=="vip" else "free")
        
        kb=keyboard(sym,res["tf"],tier,"binance_fut",lang)
        text_caption = caption_core(res, tier, lang)
        img=await asyncio.to_thread(render_chart,res,tier)
        await update.message.reply_photo(photo=img, reply_markup=kb)
        await update.message.reply_text(text_caption, parse_mode=ParseMode.HTML, link_preview_options=NO_PREVIEW)
        
        if tier == "admin" and not res.get("wait"):
            await send_admin_copy(update.message, res)
            
        cur_pts=store.data["users"].get(uid,{}).get("points",0)
        bot_un=ctx.application.bot_data.get("username","")
        await send_points_flash(update.message,cur_pts,update.effective_user.id,bot_un)
    except PairNotFound: await update.message.reply_text(t("not_found",lang,sym=sym))
    except NoData: await update.message.reply_text(t("no_data",lang))
    except Exception:
        log.exception("handle"); await update.message.reply_text(t("error",lang))
    finally:
        try: await wait.delete()
        except Exception as _ex: log.debug('ignored: %s', _ex)

# ======================= أوامر عامة إضافية (من النسخة الخفيفة) =======================
def _http_json(url,timeout=15):
    r=requests.get(url,headers={"User-Agent":"Mozilla/5.0"},timeout=timeout); r.raise_for_status(); return r.json()

def _big(n):
    for u,d in (("T",1e12),("B",1e9),("M",1e6),("K",1e3)):
        if abs(n)>=d: return f"{n/d:.2f}{u}"
    return f"{n:.0f}"

async def cmd_fear(update,ctx):
    touch_user(update.effective_user,"/fear")
    try:
        d=(await asyncio.to_thread(_http_json,"https://api.alternative.me/fng/?limit=2"))["data"]
        await update.message.reply_text(f"😱 <b>مؤشر الخوف والطمع</b>\nالآن: <b>{d[0]['value']}</b> — {d[0]['value_classification']}\n"
                                        f"أمس: {d[1]['value']} — {d[1]['value_classification']}",parse_mode=ParseMode.HTML)
    except Exception: await update.message.reply_text("⚠️ تعذر جلب المؤشر حالياً")

async def cmd_market(update,ctx):
    touch_user(update.effective_user,"/market")
    try:
        g=(await asyncio.to_thread(_http_json,"https://api.coingecko.com/api/v3/global"))["data"]
        p=g["market_cap_percentage"]
        await update.message.reply_text(
            f"🌍 <b>السوق</b>\nالقيمة السوقية: ${_big(g['total_market_cap']['usd'])} "
            f"({g['market_cap_change_percentage_24h_usd']:+.2f}% / 24س)\n"
            f"هيمنة BTC: {p.get('btc',0):.1f}% | ETH: {p.get('eth',0):.1f}%\n"
            f"حجم 24س: ${_big(g['total_volume']['usd'])}",parse_mode=ParseMode.HTML)
    except Exception: await update.message.reply_text("⚠️ تعذر جلب بيانات السوق حالياً")

async def cmd_top(update,ctx):
    touch_user(update.effective_user,"/top")
    try:
        rows=await asyncio.to_thread(binance_futures_all_tickers)
        rows=[r for r in (rows or []) if r["volume"]>20e6]
        rows.sort(key=lambda r:-r["change"])
        f=lambda r:f"• {r['symbol']}: {r['change']:+.2f}% (${_big(r['volume'])})"
        await update.message.reply_text("🚀 <b>الأكثر ارتفاعاً</b>\n"+"\n".join(map(f,rows[:7]))+
                                        "\n\n📉 <b>الأكثر انخفاضاً</b>\n"+"\n".join(map(f,rows[-7:][::-1])),parse_mode=ParseMode.HTML)
    except Exception: await update.message.reply_text("⚠️ تعذر جلب البيانات حالياً")

async def cmd_tools(update,ctx):
    touch_user(update.effective_user,"/tools")
    await update.message.reply_text(
        "🧰 <b>أدوات مفيدة</b>\n• coinmarketcap.com — الأسعار\n• coingecko.com — بيانات اجتماعية\n"
        "• tradingview.com — الرسوم والتحليل\n• defillama.com — القيمة المقفلة DeFi\n"
        "• cryptoquant.com — تحليلات on-chain\n• token.unlocks.app — فتح الرموز\n"
        "• arkhamintelligence.com — تتبع المحافظ\n• dappradar.com — التطبيقات اللامركزية",
        parse_mode=ParseMode.HTML,link_preview_options=NO_PREVIEW)

async def cmd_riskcalc(update,ctx):
    touch_user(update.effective_user,"/risk")
    try:
        cap,rk,entry,sl=[float(x) for x in ctx.args[:4]]
        dist=abs(entry-sl); amt=cap*rk/100; size=amt/dist
        await update.message.reply_text(f"🧮 <b>حجم الصفقة</b>\nالمخاطرة: ${amt:.2f}\nالكمية: <code>{size:.6g}</code>\n"
                                        f"قيمة الصفقة: ${size*entry:,.2f}",parse_mode=ParseMode.HTML)
    except Exception:
        await update.message.reply_text("الاستخدام: <code>/risk رأس_المال نسبة_المخاطرة دخول وقف</code>\nمثال: /risk 1000 1 65000 63500",
                                        parse_mode=ParseMode.HTML)

PUBLIC_COMMANDS=[("start","ابدأ / Start"),("help","المساعدة / Help"),("myid","رقمي / My ID"),
                 ("vip","VIP"),("menu","قائمتي / Menu 📋"),("portfolio","محفظتي / Portfolio 📊"),
                 ("points","نقاطي / Points 🎯"),("myref","رابط الإحالة / Referral 🔗"),
                 ("fear","الخوف والطمع"),("market","حالة السوق"),("top","الأكثر ارتفاعاً"),("risk","حاسبة المخاطر"),("spot","فرص السبوت"),("plan","خطة DCA/Grid")]


def info_dca_plan(entry, capital=1000.0, slices=3, step_pct=4.0):
    """خطة DCA معلوماتية فقط — لا تنفّذ أوامر."""
    levels = []
    per = capital / max(1, slices)
    for i in range(slices):
        px = entry * (1 - (i * step_pct / 100.0))
        levels.append(dict(n=i+1, price=round(px, 8), amount=round(per, 2), qty=round(per/px, 6) if px>0 else 0))
    avg = sum(x["price"]*x["amount"] for x in levels) / capital if capital else entry
    return dict(levels=levels, avg_entry=round(avg, 8), capital=capital, note="معلومات فقط — لا تنفيذ تلقائي")


def info_grid_plan(mid, capital=1000.0, grids=6, spacing_pct=1.5):
    """خطة Grid معلوماتية فقط — مستويات للمراقبة اليدوية."""
    half = grids // 2
    levels = []
    per = capital / max(1, grids)
    for i in range(-half, half+1):
        if i == 0 and grids % 2 == 0:
            continue
        px = mid * (1 + i * spacing_pct / 100.0)
        side = "شراء" if i < 0 else ("بيع" if i > 0 else "محور")
        levels.append(dict(side=side, price=round(px, 8), amount=round(per, 2)))
    return dict(levels=levels[:grids], mid=mid, capital=capital, note="معلومات فقط — لا تنفيذ تلقائي")


def get_crypto_news(symbol):
    try:
        url=f"https://min-api.cryptocompare.com/data/v2/news/?lang=EN&categories={symbol}"
        r=requests.get(url,timeout=5); data=r.json()
        if data.get('Data'): return data['Data'][0]['title']
    except Exception as _ex: log.debug('ignored: %s', _ex)
    return "لا توجد أخبار هامة حالياً."

# ============ رادار الفرص المبكرة (وضع الظل) ============
_radar_seen = {}


def spot_opportunities(limit=12):
    """فرص سبوط طويلة الأمد (شهر–شهرين) — معلومات وتنبيه فقط، بدون تنفيذ.
    عملات رخيصة نسبياً تحت ضغط بيع مع بوادر استقرار + خطة تجميع hypothetical."""
    out = []
    try:
        rows = binance_futures_all_tickers() or []
    except Exception:
        rows = []
    cands = []
    for x in rows:
        try:
            sym = x["symbol"]; price = float(x["price"]); vol = float(x["volume"])
            if sym in STABLES or sym in EXCLUDE_TICKERS or sym in DAILY_EXCLUDE: continue
            if price <= 0 or price > 5.0: continue
            if vol < 1_000_000: continue
            cands.append((vol, sym, price, float(x.get("change", 0))))
        except Exception:
            continue
    cands.sort(reverse=True)
    for _, sym, price, ch24 in cands[:100]:
        if len(out) >= limit: break
        try:
            df = get_candles(sym, "1d", 120, ttl=600, min_bars=45)
            if df is None or len(df) < 45: continue
            df = add_indicators(df, 100)
            last = df.iloc[-1]
            if pd.isna(last.get("rsi")) or pd.isna(last.get("ema50")) or pd.isna(last.get("ema200")): continue
            rsi = float(last["rsi"]); c = float(last["close"])
            e50 = float(last["ema50"]); e200 = float(last["ema200"])
            atr = float(last["atr"]) if not pd.isna(last.get("atr")) else c * 0.04
            ch30 = (c / float(df["close"].iloc[-30]) - 1) * 100 if len(df) >= 30 else 0
            ch7 = (c / float(df["close"].iloc[-7]) - 1) * 100 if len(df) >= 7 else 0
            low60 = float(df["low"].tail(60).min())
            dist_low = (c / low60 - 1) * 100 if low60 > 0 else 99
            if ch30 > -8: continue
            if rsi > 48: continue
            if c > e50 * 1.12: continue
            if dist_low > 25: continue  # بعيدة جداً عن قاع 60 يوم
            eng = engine_direction(sym, c)
            if eng == -1: continue
            score = 0
            reasons = []
            if ch30 < -25: score += 2; reasons.append("هبوط 30ي حاد")
            elif ch30 < -15: score += 1; reasons.append("هبوط 30ي متوسط")
            if rsi < 32: score += 2; reasons.append("RSI منخفض جداً")
            elif rsi < 40: score += 1; reasons.append("RSI منخفض")
            if ch7 > -2: score += 1; reasons.append("استقرار أسبوعي")
            if eng == 1: score += 1; reasons.append("محرك يدعم")
            if c < e200: score += 1; reasons.append("تحت EMA200")
            if dist_low < 8: score += 1; reasons.append("قرب قاع 60ي")
            if score < 3: continue
            # خطة تجميع معلوماتية فقط (DCA hypothetical) — لا تنفيذ
            buy1 = round(c, 8)
            buy2 = round(max(c - 1.2 * atr, low60), 8)
            buy3 = round(max(c - 2.2 * atr, low60 * 0.98), 8)
            invalid = round(low60 * 0.96, 8)
            target1 = round(c + 2.0 * atr, 8)
            target2 = round(max(e50, c + 3.5 * atr), 8)
            out.append(dict(
                sym=sym, price=c, ch24=round(ch24,1), ch7=round(ch7,1), ch30=round(ch30,1),
                rsi=round(rsi,1), score=score, side=1, horizon="1–2 شهر",
                note="معلومات/تنبيه فقط — لا تنفيذ تلقائي", engine_aligned=(eng==1),
                reasons=reasons, low60=low60, dist_low=round(dist_low,1),
                plan=dict(buy1=buy1, buy2=buy2, buy3=buy3, invalid=invalid,
                          tp1=target1, tp2=target2, atr=round(atr, 8))
            ))
        except Exception:
            continue
    out.sort(key=lambda x: -x["score"])
    return out[:limit]


def build_spot_text(rows, lang="ar"):
    if not rows:
        return "🌱 لا توجد فرص سبوت مؤهلة حالياً." if lang != "en" else "🌱 No qualifying spot setups now."
    lines = [
        "🌱 <b>فرص السبوت (أفق 1–2 شهر)</b>",
        "📋 تنبيه ومعلومات فقط — البوت لا ينفّذ أي صفقة.",
        "تجميع حذر محتمل بعد هبوط، مع خطة مستويات للمراقبة.",
        "━━━━━━━━━━━━━━━━",
        ""
    ]
    for r in rows[:10]:
        plan = r.get("plan") or {}
        lines.append(f"• <b>#{r['sym']}</b> · ${r['price']:.6g} · نقاط {r['score']}")
        lines.append(f"  30ي {r['ch30']:+.1f}% | 7ي {r['ch7']:+.1f}% | RSI {r['rsi']} | قرب القاع {r.get('dist_low', '—')}%")
        if r.get("reasons"):
            lines.append(f"  ✓ {' · '.join(r['reasons'][:4])}")
        if plan:
            lines.append(f"  📥 تجميع مقترح: {plan.get('buy1')} / {plan.get('buy2')} / {plan.get('buy3')}")
            lines.append(f"  🎯 أهداف مراقبة: {plan.get('tp1')} → {plan.get('tp2')}")
            lines.append(f"  🛑 إبطال الفكرة: {plan.get('invalid')}")
        lines.append(f"  <i>{r.get('note','')}</i>")
        lines.append("")
    lines += [
        "⚠️ ليست نصيحة مالية ولا أمر شراء.",
        "البوت للتنبيه والتحليل فقط — التنفيذ قرارك اليدوي.",
        RISK_WARNING_AR
    ]
    return chr(10).join(lines)


def radar_scan(symbols):
    """رادار الفرص المبكرة (فريم 15د — فيوتشر).
    يلتقط تحركاً قوياً + حجم استثنائي.
    توافق إلزامي مع اتجاه 4س والمحرك: لا يُنشر SHORT إذا المحرك LONG والعكس.
    مدة الصفقة قصيرة (يوم تداول)."""
    out = []
    for s in symbols:
        try:
            df = get_candles(s, "15m", 80, ttl=60, min_bars=40)
            d = add_indicators(df, 100)
            if len(d) < 25: continue
            last = len(d) - 1
            c = d["close"]; v = d["volume"]; h = d["high"]; l = d["low"]
            move = float(c.iloc[last] / c.iloc[last - 2] - 1)
            base_v = float(v.iloc[last - 20:last].mean())
            if base_v <= 0: continue
            vol_ratio = float(v.iloc[last]) / base_v
            if vol_ratio < 2.5: continue
            _p, ch24, _c7 = ticker_change(s)
            if ch24 is None: continue
            atr = float(d["atr"].iloc[last])
            price = float(c.iloc[last])
            if not (atr > 0 and price > 0) or price < 0.000001: continue
            try:
                df_4h = get_candles(s, "4h", 100, ttl=300, min_bars=50)
                df_4h = add_indicators(df_4h, 100)
                trend_4h = detect_trend(df_4h, len(df_4h) - 1)
            except Exception:
                trend_4h = "range"
            side = 0
            # زخم 15د + توافق صارم مع 4س (لا range معاكس)
            if move >= 0.04 and float(c.iloc[last]) >= float(h.iloc[last - 20:last].max()) and ch24 < 12:
                if trend_4h in ("strong_up", "up"):
                    side = 1
            elif move <= -0.04 and float(c.iloc[last]) <= float(l.iloc[last - 20:last].min()) and ch24 > -12:
                if trend_4h in ("strong_down", "down"):
                    side = -1
            if side == 0: continue
            # توافق مع المحرك الرئيسي: إن تعارض → لا تنشر كفرصة تداول
            eng = engine_direction(s, price)
            if eng != 0 and eng != side:
                continue  # تناقض مع التحليل — تجاهل
            # وقف ≤ 3%
            risk = max(1.5 * atr, price * 0.008)
            risk = min(risk, 2.8 * atr, price * 0.03)
            sl = price - side * risk
            if abs(price - sl) / price > 0.03:
                sl = price * (1 - side * 0.03)
                risk = abs(price - sl)
            tps = []
            for mult, frac in ((1.2, 0.50), (2.0, 0.30), (3.0, 0.20)):
                px = price + side * risk * mult
                tps.append(dict(px=float(px), pct=round(100 * side * (px - price) / price, 2),
                                frac=frac, hit=False, src="radar"))
            lv = dict(entry=price, sl=float(sl), risk=float(risk), atr=atr, tps=tps)
            grade = "B" if eng == side else "R"
            out.append(dict(sym=s, tf="15m", side=side, lv=lv, bar_t=int(d["t"].iloc[last]),
                            grade=grade, setup="RADAR", source="binance_fut", rank=1 if eng == side else 0, ok=1,
                            move=round(move * 100, 2), vol_ratio=round(vol_ratio, 1),
                            ch24=round(ch24, 2), price=price, forced=False, wait=False,
                            engine_aligned=(eng == side)))
        except Exception:
            continue
    return out

async def shadow_loop(app):
    await asyncio.sleep(120)
    while True:
        try:
            cutoff = time.time() - 48 * 3600
            for k in [k for k, t in _radar_seen.items() if t < cutoff]: _radar_seen.pop(k, None)
            syms = await asyncio.to_thread(top_symbols, 80)
            hits = await asyncio.to_thread(radar_scan, syms)
            new = []
            for hit in hits:
                key = f"{hit['sym']}:{hit['bar_t']}"
                if key in _radar_seen: continue
                _radar_seen[key] = time.time()
                if any(p.get("coin") == hit["sym"] and p.get("state") in ("pending", "open") for p in active_signals()):
                    continue
                await asyncio.to_thread(track_signal, hit)
                new.append(hit)
            if new:
                lines = [f"⚡ <b>رادار الفرص المبكرة</b> · {len(new)} رصد", "━━━━━━━━━━━━━━━━"]
                for h in new:
                    side_e = "🟢 LONG" if h["side"] == 1 else "🔴 SHORT"
                    lines.append(
                        f"{side_e} <b>#{h['sym']}</b> | {fmt(h['price'])}\n"
                        f"   تحرك {h['move']:+.1f}% | حجم ×{h['vol_ratio']} | 24س {h['ch24']:+.1f}%"
                    )
                lines += ["", RISK_WARNING_AR, f"👤 {BRAND}"]
                txt = "\n".join(lines)
                for chat in (VIP_CHANNEL_ID, ADMIN_ID or None):
                    if not chat: continue
                    try:
                        await app.bot.send_message(chat, txt, parse_mode=ParseMode.HTML,
                                                   protect_content=(PROTECT if chat != ADMIN_ID else False),
                                                   link_preview_options=NO_PREVIEW)
                    except Exception as _ex:
                        log.debug("radar notify %s: %s", chat, _ex)
        except asyncio.CancelledError: raise
        except Exception: log.exception("shadow_loop")
        await asyncio.sleep(CYCLE_RADAR)



async def cmd_shadow(update, ctx):
    if not update.effective_user or update.effective_user.id != ADMIN_ID: return
    with store.lock:
        hist = [x for x in store.data["history"] if x.get("setup") == "RADAR"]
        act = [p for p in store.data["signals"] if p.get("setup") == "RADAR" and p.get("state") in ("pending", "open")]
    rs = [x["R"] for x in hist if x.get("R") is not None]
    wins = sum(1 for r in rs if r > 0)
    gross = sum(r for r in rs if r > 0); loss = -sum(r for r in rs if r <= 0)
    pf = round(gross / loss, 2) if loss > 0 else "—"
    await update.message.reply_text(
        f"🧪 وضع الظل — الرادار\nمغلقة: {len(rs)} | رابحة: {wins}\nمجموع R بعد الرسوم: {sum(rs):+.2f} | PF: {pf}\nمفتوحة الآن: {len(act)}\n"
        f"ملاحظة: هذه نتائج داخلية فقط، لم تُنشر لأحد.")

async def post_init(app):
    try: await app.bot.delete_webhook(drop_pending_updates=False)
    except Exception as _ex: log.debug('ignored: %s', _ex)
    try: await app.bot.set_my_commands([BotCommand(c,d) for c,d in PUBLIC_COMMANDS])
    except Exception as _ex: log.debug('ignored: %s', _ex)
    try:
        me=await app.bot.get_me(); app.bot_data["username"]=me.username
        log.info(f"bot username: @{me.username}")
    except Exception as e: log.warning(f"get_me: {e}")
    try:
        n=await asyncio.to_thread(smart_cleanup_trades)
        log.info(f"startup: smart_cleanup نظّفت {n} صفقة")
    except Exception as e: log.warning(f"startup cleanup: {e}")
    app.bot_data["tasks"]=[
        asyncio.create_task(autopost_loop(app)),asyncio.create_task(tracker_loop(app)),
        asyncio.create_task(predict_loop(app)),asyncio.create_task(market_summary_loop(app)),
        asyncio.create_task(short_free_loop(app)),asyncio.create_task(alpha_summary_loop(app)),
        asyncio.create_task(bottoms_auto_loop(app)),asyncio.create_task(cross_loop(app)),
        asyncio.create_task(golden_loop(app)),asyncio.create_task(promo_loop(app)),
        asyncio.create_task(whale_hunter_loop(app)),
        asyncio.create_task(weekly_report_loop(app)),asyncio.create_task(trial_notice_loop(app)),
        asyncio.create_task(morning_brief_loop(app)),
        asyncio.create_task(shadow_loop(app))]
    log.info("version %s",BOT_VERSION)
    log.info("ryma FINAL (engine+always-trade) | max_open=%d | golden=%ds | min_score=%d | multi=%s | points=%d→VIP%d | patterns=MTF | bilingual=auto | whale_min=$500000 | SL≤3%% hard | RR≥1.5 | min_score=8.5 | MTF strict | hype=admin | neon=admin | patterns_chart=admin",
             MAX_OPEN,CYCLE_GOLDEN,get_golden_config().get("min_score",1),is_multi_exchange(),
             VIP_THRESHOLD,VIP_REWARD_DAYS)

async def post_shutdown(app):
    for t in app.bot_data.get("tasks",[]): t.cancel()
    try: store.flush()
    except Exception as _ex: log.debug('ignored: %s', _ex)

def main():
    if not BOT_TOKEN: raise SystemExit("BOT_TOKEN not set")
    store.start_flusher()
    app=(ApplicationBuilder().token(BOT_TOKEN).post_init(post_init).post_shutdown(post_shutdown).build())
    handlers=[
        ("start",cmd_start),("help",cmd_help),("myid",cmd_myid),("vip",cmd_vip),
        ("menu",cmd_menu),("daily",cmd_daily),("cross",cmd_cross),("predict",cmd_predict),
        ("forcepost",cmd_forcepost),("clearsignals",cmd_clearsignals),
        ("smartcleanup",cmd_smartcleanup),("debug",cmd_debug),("active",cmd_active),
        ("post",cmd_post),("bt",cmd_bt),("backtest",cmd_bt),("bt50",cmd_bt50),
        ("scan",cmd_scan),("short",cmd_short),("delist",cmd_delist),("price",cmd_price),
        ("users",cmd_users),("testchannels",cmd_testchannels),("stats",cmd_stats),
        ("history",cmd_history),("dashboard",cmd_dashboard),
        ("addvip",cmd_addvip),("removevip",cmd_removevip),("viplist",cmd_viplist),
        ("whales",cmd_whales),("liquidity",cmd_liquidity),("futures",cmd_futures),
        ("explosions",cmd_explosions),("pump",cmd_pump),
        ("bottom",cmd_bottoms),("bottoms",cmd_bottoms),
        ("shortsradar",cmd_shorts_radar),("alpha",cmd_alpha),("why",cmd_why),
        ("golden",cmd_golden),("goldenconfig",cmd_goldenconfig),("share",cmd_share),
        ("portfolio",cmd_portfolio),("alpha_v2",cmd_alpha_radar_v2),
        ("patterns",cmd_patterns),("neon",cmd_neon),
        ("points",cmd_points),("myref",cmd_myref),("addpoints",cmd_addpoints),("radar",cmd_radar),("spot",cmd_spot),("plan",cmd_plan),
        ("btdiag",cmd_btdiag),("exportdata",cmd_exportdata),("fear",cmd_fear),("market",cmd_market),("top",cmd_top),("tools",cmd_tools),("risk",cmd_riskcalc)]
    for name,fn in handlers: app.add_handler(CommandHandler(name,fn))
    app.add_handler(TypeHandler(Update,log_updates),group=-1)
    app.add_handler(CommandHandler("shadow", cmd_shadow))
    app.add_handler(CommandHandler("botinfo",cmd_botinfo))
    app.add_handler(CallbackQueryHandler(cb_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE,on_text))
    app.add_error_handler(on_error)
    # allowed_updates صريحة: تيليغرام يتذكر آخر قيمة، وأي نسخة قديمة طلبت message فقط تُسقط callback_query (الأزرار)
    app.run_polling(allowed_updates=Update.ALL_TYPES,drop_pending_updates=True)

if __name__ == "__main__":
    main()
