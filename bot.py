 #!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
بوت تحليل التداول التليجرام - يعمل بمكتبات بايثون الأساسية فقط
بيانات من Binance + CoinGecko + alternative.me (كلها مجانية وبدون مفاتيح)
"""

import os
import json
import time
import html
import threading
import urllib.request
import urllib.parse
import urllib.error

TOKEN = os.environ.get("BOT_TOKEN", "").strip()
ALLOWED = {x.strip() for x in os.environ.get("ALLOWED_CHAT_IDS", "").split(",") if x.strip()}

SPOT_BASES = ["https://api.binance.com", "https://data-api.binance.vision"]
FUT_BASE = "https://fapi.binance.com"
UA = {"User-Agent": "Mozilla/5.0 (TradingBot)"}
TP_MULTS = [1.2, 2.0, 3.0, 4.5]

# --- دوال مساعدة ---
def http_json(url, timeout=15):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))

def get_klines(pair, interval, limit=250):
    last_err = None
    for base in SPOT_BASES:
        try:
            url = f"{base}/api/v3/klines?symbol={pair}&interval={interval}&limit={limit}"
            raw = http_json(url)
            return [{"o": float(k[1]), "h": float(k[2]), "l": float(k[3]),
                     "c": float(k[4]), "v": float(k[5]), "qv": float(k[7])} for k in raw]
        except Exception as e:
            last_err = e
    raise RuntimeError(f"فشل جلب البيانات: {last_err}")

def get_ticker24(pair):
    for base in SPOT_BASES:
        try:
            return http_json(f"{base}/api/v3/ticker/24hr?symbol={pair}")
        except Exception:
            pass
    return None

def get_funding(pair):
    try:
        d = http_json(f"{FUT_BASE}/fapi/v1/premiumIndex?symbol={pair}")
        oi = http_json(f"{FUT_BASE}/fapi/v1/openInterest?symbol={pair}")
        return float(d["lastFundingRate"]) * 100, float(oi["openInterest"])
    except Exception:
        return None, None

def get_cg_info(sym):
    try:
        url = ("https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd"
               f"&symbols={urllib.parse.quote(sym.lower())}&order=market_cap_desc&per_page=1")
        arr = http_json(url)
        return arr[0] if arr else None
    except Exception:
        return None

# --- المؤشرات الفنية ---
def ema(val, n):
    k = 2 / (n + 1)
    out = []
    e = None
    for v in val:
        if e is None:
            e = v
        else:
            e = v * k + e * (1 - k)
        out.append(e)
    return out

def rsi(close, n=14):
    if len(close) < n + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(close)):
        d = close[i] - close[i - 1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    ag = sum(gains[:n]) / n
    al = sum(losses[:n]) / n
    for i in range(n, len(gains)):
        ag = (ag * (n - 1) + gains[i]) / n
        al = (al * (n - 1) + losses[i]) / n
    if al == 0:
        return 100.0
    rs = ag / al
    return 100 - 100 / (1 + rs)

def macd(closes, fast=12, slow=26, sig=9):
    ef, es = ema(closes, fast), ema(closes, slow)
    line = [a - b for a, b in zip(ef, es)]
    signal = ema(line, sig)
    hist = [a - b for a, b in zip(line, signal)]
    return line, signal, hist

def atr(cs, n=14):
    trs = []
    for i in range(1, len(cs)):
        h, l, pc = cs[i]["h"], cs[i]["l"], cs[i - 1]["c"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    if len(trs) < n:
        return sum(trs) / max(len(trs), 1)
    a = sum(trs[:n]) / n
    for t in trs[n:]:
        a = (a * (n - 1) + t) / n
    return a

def bollinger(closes, n=20, k=2):
    w = closes[-n:]
    m = sum(w) / len(w)
    sd = (sum((x - m) ** 2 for x in w) / len(w)) ** 0.5
    return m - k * sd, m, m + k * sd

def pivots(cs, w=3, lookback=120):
    seg = cs[-lookback:]
    highs, lows = [], []
    for i in range(w, len(seg) - w):
        if seg[i]["h"] == max(x["h"] for x in seg[i - w:i + w + 1]):
            highs.append(seg[i]["h"])
        if seg[i]["l"] == min(x["l"] for x in seg[i - w:i + w + 1]):
            lows.append(seg[i]["l"])
    return highs, lows

def nearest_levels(cs, price):
    highs, lows = pivots(cs)
    res = sorted([h for h in highs if h > price])
    sup = sorted([l for l in lows if l < price], reverse=True)
    return (sup[:2], res[:2])

def tf_view(cs):
    closes = [c["c"] for c in cs]
    e20, e50, e200 = ema(closes, 20)[-1], ema(closes, 50)[-1], ema(closes, 200)[-1]
    price = closes[-1]
    r = rsi(closes)
    _, _, hist = macd(closes)
    if price > e50 and e20 > e50:
        trend = 1
    elif price < e50 and e20 < e50:
        trend = -1
    else:
        trend = 0
    vols = [c["qv"] for c in cs[-21:-1]]
    vr = cs[-1]["qv"] / (sum(vols) / len(vols)) if vols and sum(vols) else 1
    return {"price": price, "e20": e20, "e50": e50, "e200": e200, "rsi": r,
            "hist": hist[-1], "hist_prev": hist[-2], "trend": trend,
            "above200": price > e200, "atr": atr(cs), "vr": vr}

def fmt(p):
    if p >= 1000: return f"{p:.2f}"
    if p >= 1: return f"{p:.4f}"
    if p >= 0.01: return f"{p:.5f}"
    return f"{p:.8f}"

def fmt_big(n):
    for unit, d in (("T", 1e12), ("B", 1e9), ("M", 1e6), ("K", 1e3)):
        if abs(n) >= d: return f"{n / d:.2f}{unit}"
    return f"{n:.0f}"

def arrow(t):
    return "📈 صاعد" if t > 0 else ("📉 هابط" if t < 0 else "➖ عرضي")

def norm_symbol(text):
    s = text.upper().strip().replace("/", "").replace("-", "").replace("$", "")
    for suf in ("USDT", "USD"):
        if s.endswith(suf) and len(s) > len(suf):
            s = s[:-len(suf)]
    return s, s + "USDT"

# --- بناء الإشارة ---
def build_signal(views, funding):
    w = {"1d": 2.0, "4h": 2.0, "1h": 1.0}
    score = 0.0
    notes = []
    for tf, wt in w.items():
        score += wt * views[tf]["trend"]
    v4 = views["4h"]
    if v4["rsi"] > 72:
        score -= 0.5
        notes.append("RSI 4h في تشبع شراء (تحصيل)")
    elif v4["rsi"] < 28:
        score += 0.5
        notes.append("RSI 4h في تشبع بيع (فرصة)")
    return score, notes

def build_levels(direction, price, atr4, sup, res):
    mult = 1.5
    if direction == "LONG":
        sl = price - atr4 * mult
        tps = [price + atr4 * m for m in TP_MULTS]
    else:
        sl = price + atr4 * mult
        tps = [price - atr4 * m for m in TP_MULTS]
    risk = abs(price - sl)
    return sl, tps, risk

# --- ملف الإشارات ---
DB_FILE = os.environ.get("SIGNALS_FILE", os.path.join(os.path.dirname(os.path.abspath(__file__)), "signals.json"))
DB_LOCK = threading.RLock()
EXPIRE_SECS = 7 * 86400
CHECK_EVERY = 300
CANDLE_MS = 300000

def db_load():
    try:
        with open(DB_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def db_save(rows):
    with open(DB_FILE, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

def record_signal(sym, pair, direction, price, sl, tps, chat_id):
    with DB_LOCK:
        rows = db_load()
        sid = max([r.get("id", 0) for r in rows] + [0]) + 1
        rows.append({
            "id": sid, "sym": sym, "pair": pair, "dir": direction,
            "entry": price, "sl": sl, "sl_cur": sl, "tps": tps,
            "ts_ms": int(time.time() * 1000), "last_ms": int(time.time() * 1000),
            "last_price": price, "hits": 0, "status": "open",
            "outcome": None, "r": 0.0, "closed_ms": None, "chat_id": chat_id
        })
        db_save(rows)
        return sid

# --- التحليل الرئيسي ---
def analyze(sym_input, chat_id):
    sym, pair = norm_symbol(sym_input)
    data = {}
    for tf in ("1d", "4h", "1h"):
        data[tf] = get_klines(pair, tf, 250)
    tk = get_ticker24(pair)
    funding, oi = get_funding(pair)
    cg = get_cg_info(sym)
    views = {tf: tf_view(cs) for tf, cs in data.items()}
    price = views["1h"]["price"]
    atr4 = views["4h"]["atr"]
    sup, res = nearest_levels(data["4h"], price)
    bb_lo, bb_mid, bb_hi = bollinger([c["c"] for c in data["4h"]])
    score, notes = build_signal(views, funding)
    if score >= 3: direction = "LONG"
    elif score <= -3: direction = "SHORT"
    else: direction = "WAIT"
    strength = min(95, int(abs(score) / 7 * 100))
    
    out = []
    out.append(f"<b>{html.escape(sym)}/USD</b>")
    line = f"<code>{fmt(price)}</code>"
    if tk:
        ch = float(tk["priceChangePercent"])
        line += f" ({'🟢' if ch >= 0 else '🔴'} {ch:+.2f}% / 24س)"
    out.append(line)
    if tk:
        out.append(f"24س: {fmt(float(tk['lowPrice']))} - {fmt(float(tk['highPrice']))} | حجم: ${fmt_big(float(tk['quoteVolume']))}")
    else:
        out.append(line)
    if cg:
        out.append(f"💎 الترتيب: #{cg.get('market_cap_rank')} | القيمة السوقية: ${fmt_big(cg.get('market_cap') or 0)}")
    out.append("")
    out.append("📊 <b>الاتجاه حسب الإطار</b>")
    for tf, name in (("1d", "يومي"), ("4h", "4 ساعات"), ("1h", "ساعة")):
        v = views[tf]
        out.append(f"• {name}: {arrow(v['trend'])} | RSI {v['rsi']:.0f}")
    out.append("")
    if direction == "WAIT":
        out.append("⏳ <b>الإشارة: انتظار</b> - لا توجد صفقة واضحة الآن")
        out.append(f"قوة الإشارة: {strength}% (الاتجاهات غير متوافقة)")
    else:
        sl, tps, risk = build_levels(direction, price, atr4, sup, res)
        emoji = "🟢 شراء (LONG)" if direction == "LONG" else "🔴 بيع (SHORT)"
        out.append(f"🎯 <b>الصفقة: {emoji}</b>")
        out.append(f"قوة الإشارة: {strength}%")
        out.append(f"💰 الدخول: <code>{fmt(price)}</code>")
        out.append(f"🛑 وقف الخسارة: <code>{fmt(sl)}</code> ({risk / price * 100:.2f}%)")
        for i, (t, m) in enumerate(zip(tps, TP_MULTS), 1):
            out.append(f"✅ الهدف {i}: <code>{fmt(t)}</code> (+{abs(t - price) / price * 100:.2f}% | {m}R)")
        out.append("💡 الخطة: أغلق 25% عند كل هدف، وانقل الوقف لنقطة الدخول بعد الهدف 1")
        sid = record_signal(sym, pair, direction, price, sl, tps, chat_id)
        out.append(f"📝 تم تسجيل الصفقة #{sid} وسيتم تتبعها.")
        out.append("")
    out.append("📐 <b>مستويات مهمة (4س)</b>")
    if res: out.append("مقاومة: " + " / ".join(fmt(x) for x in res))
    if sup: out.append("دعم: " + " / ".join(fmt(x) for x in sup))
    out.append(f"EMA50 (4h): {fmt(views['4h']['e50'])} | EMA200 (يومي): {fmt(views['1d']['e200'])}")
    out.append(f"بولنجر 4س: {fmt(bb_lo)} - {fmt(bb_hi)}")
    if funding is not None:
        out.append(f"💸 التمويل: {funding:+.4f}% | الفائدة المفتوحة: {fmt_big(oi)}")
    if notes:
        out.append("")
        out.append("📝 <b>ملاحظات</b>")
        out.extend("• " + n for n in notes)
    out.append("")
    out.append("⚠️ تحليل فني وليس نصيحة مالية. لا توجد إشارة مضمونة! التزم بإدارة المخاطر (1-2% من رأس المال).")
    return "\n".join(out)

# --- تتبع الإشارات ---
def get_klines_since(pair, start_ms):
    for base in SPOT_BASES:
        try:
            url = (f"{base}/api/v3/klines?symbol={pair}&interval=5m"
                   f"&startTime={int(start_ms)}&limit=1000")
            raw = http_json(url)
            return [(k[0], float(k[2]), float(k[3]), float(k[4])) for k in raw]
        except Exception:
            pass
    return None

def realized_r(sig):
    return sum(0.25 * m for m in TP_MULTS[:sig["hits"]])

def advance(sig, now_ms):
    msgs = []
    long = sig["dir"] == "LONG"
    tag = f"#{sig['id']} {sig['sym']} {'LONG' if long else 'SHORT'}"
    candles = get_klines_since(sig["pair"], sig["last_ms"])
    if candles is None:
        return msgs
    for t, h, l, c in candles:
        if t + CANDLE_MS > now_ms:
            break
        sig["last_ms"] = t + CANDLE_MS
        sig["last_price"] = c
        sl = sig["sl_cur"]
        if (l <= sl) if long else (h >= sl):
            sig["status"] = "closed"
            sig["closed_ms"] = now_ms
            if sig["hits"] == 0:
                sig["outcome"], sig["r"] = "sl", -1.0
                msgs.append(f"🔴 <b>{tag}</b>\nضرب وقف الخسارة <code>{fmt(sl)}</code> (-1R)")
            else:
                sig["outcome"], sig["r"] = "be", realized_r(sig)
                msgs.append(f"⚪ <b>{tag}</b>\nرجع السعر لنقطة الدخول بعد تحقق {sig['hits']} هدف - أغلقت الصفقة (+{sig['r']:.2f}R)")
            return msgs
        while sig["hits"] < 4:
            tp = sig["tps"][sig["hits"]]
            if (h >= tp) if long else (l <= tp):
                sig["hits"] += 1
                if sig["hits"] == 1:
                    sig["sl_cur"] = sig["entry"]
                if sig["hits"] < 4:
                    extra = "\n🛡️ نقل الوقف لنقطة الدخول" if sig["hits"] == 1 else ""
                    msgs.append(f"✅ <b>{tag}</b>\nتحقق الهدف {sig['hits']} عند <code>{fmt(tp)}</code>{extra}")
                else:
                    break
            else:
                break
        if sig["hits"] == 4:
            sig["status"], sig["outcome"] = "closed", "tp4"
            sig["r"], sig["closed_ms"] = realized_r(sig), now_ms
            msgs.append(f"🎉 <b>{tag}</b>\nتحققت الأهداف الأربعة (+{sig['r']:.2f}R)")
            return msgs
    if sig["status"] == "open" and now_ms - sig["ts_ms"] > EXPIRE_SECS * 1000:
        risk = abs(sig["entry"] - sig["sl"])
        move = (sig["last_price"] - sig["entry"]) if long else (sig["entry"] - sig["last_price"])
        rem = 1 - 0.25 * sig["hits"]
        sig["status"], sig["outcome"] = "closed", "expired"
        sig["r"] = realized_r(sig) + rem * move / risk
        sig["closed_ms"] = now_ms
        msgs.append(f"⌛ <b>{tag}</b>\nانتهت مدة التتبع (7 أيام) - النتيجة {sig['r']:+.2f}R")
        return msgs
    return msgs

def check_signals():
    notes = []
    with DB_LOCK:
        rows = db_load()
        now_ms = int(time.time() * 1000)
        changed = False
        for sig in rows:
            if sig["status"] != "open":
                continue
            before = (sig["last_ms"], sig["hits"], sig["status"])
            msgs = advance(sig, now_ms)
            if (sig["last_ms"], sig["hits"], sig["status"]) != before:
                changed = True
            notes += [(sig.get("chat_id"), m) for m in msgs]
        if changed:
            db_save(rows)
    for chat_id, m in notes:
        if chat_id is not None:
            send(chat_id, m)

def tracker_loop():
    while True:
        try:
            check_signals()
        except Exception as e:
            print("tracker error:", e)
        time.sleep(CHECK_EVERY)

# --- الأوامر ---
OUTCOME_AR = {"tp4": "كل الأهداف", "be": "جزئي", "sl": "وقف خسارة", "expired": "انتهت المدة"}

def cmd_stats():
    rows = db_load()
    closed = [r for r in rows if r["status"] == "closed"]
    n_open = sum(1 for r in rows if r["status"] == "open")
    if not closed:
        return f"📊 لا توجد صفقات مغلقة بعد.\nالصفقات قيد التتبع: {n_open}"
    n = len(closed)
    wins = [r for r in closed if r["r"] > 0]
    losses = [r for r in closed if r["r"] < 0]
    total = sum(r["r"] for r in closed)
    gp = sum(r["r"] for r in wins)
    gl = abs(sum(r["r"] for r in losses))
    out = ["📈 <b>إحصائيات الإشارات</b>",
           f"الصفقات المغلقة: {n} | قيد التتبع: {n_open}",
           f"✅ رابحة: {len(wins)} | 🔴 خاسرة: {len(losses)} | نسبة الربح: {len(wins) / n * 100:.0f}%",
           f"💰 إجمالي النتيجة: <b>{total:+.2f}R</b> | المتوسط: {total / n:+.2f}R",
           "عامل الربح: " + (f"{gp / gl:.2f}" if gl else "∞"),
           "", "🎯 <b>نسبة تحقق الأهداف</b>"]
    for i in range(1, 5):
        c = sum(1 for r in closed if r["hits"] >= i)
        out.append(f"الهدف {i}: {c}/{n} ({c / n * 100:.0f}%)")
    for d, name in (("LONG", "شراء"), ("SHORT", "بيع")):
        sub = [r for r in closed if r["dir"] == d]
        if sub:
            w = sum(1 for r in sub if r["r"] > 0)
            out.append(f"{name}: {w}/{len(sub)} رابحة ({sum(r['r'] for r in sub):+.2f}R)")
    out.append("\n1R = المبلغ المخاطر به في الصفقة. الحساب على خطة 25% لكل هدف.")
    return "\n".join(out)

def cmd_open():
    rows = [r for r in db_load() if r["status"] == "open"]
    if not rows:
        return "لا توجد صفقات قيد التتبع حاليًا."
    out = ["🎯 <b>صفقات قيد التتبع</b>"]
    now = time.time() * 1000
    for r in rows:
        age = (now - r["ts_ms"]) / 3600000
        side = "🟢" if r["dir"] == "LONG" else "🔴"
        out.append(f"{side} #{r['id']} {r['sym']} | دخول {fmt(r['entry'])} | وقف {fmt(r['sl_cur'])} | "
                   f"أهداف: {r['hits']}/4 | منذ {age:.0f}س")
    return "\n".join(out)

def cmd_history():
    closed = [r for r in db_load() if r["status"] == "closed"]
    if not closed:
        return "لا يوجد سجل بعد"
    closed.sort(key=lambda r: r.get("closed_ms", 0), reverse=True)
    out = ["<b>آخر الصفقات المغلقة</b>"]
    for r in closed[:10]:
        icon = "🟢" if r["r"] > 0 else "🔴"
        out.append(f"{icon} #{r['id']} {r['sym']} {r['dir']} | {r['r']:+.2f}R | {OUTCOME_AR.get(r['outcome'], '')}")
    return "\n".join(out)

def cmd_fear():
    d = http_json("https://api.alternative.me/fng/?limit=2")["data"]
    now, prev = d[0], d[1]
    return (f"<b>مؤشر الخوف والطمع</b>\n"
            f"الآن: <b>{now['value']}</b> - {now['value_classification']}\n"
            f"أمس: {prev['value']} - {prev['value_classification']}")

def cmd_market():
    g = http_json("https://api.coingecko.com/api/v3/global")["data"]
    mc = g["total_market_cap"]["usd"]
    ch = g["market_cap_change_percentage_24h_usd"]
    p = g["market_cap_percentage"]
    return (f"<b>حالة السوق</b>\nالقيمة السوقية: ${fmt_big(mc)} ({ch:+.2f}% / 24س)\n"
            f"هيمنة BTC: {p.get('btc', 0):.1f}% | ETH: {p.get('eth', 0):.1f}%\n"
            f"حجم 24س: ${fmt_big(g['total_volume']['usd'])}")

def cmd_top():
    rows = http_json(SPOT_BASES[0] + "/api/v3/ticker/24hr")
    bad = ("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")
    rows = [r for r in rows if r["symbol"].endswith("USDT") and not r["symbol"].endswith(bad)
            and float(r["quoteVolume"]) > 20e6]
    rows.sort(key=lambda r: float(r["priceChangePercent"]), reverse=True)
    top, low = rows[:7], rows[-7:][:-1]
    f = lambda r: f"`{r['symbol'][:-4]}`: {float(r['priceChangePercent']):+.2f}% (${fmt_big(float(r['quoteVolume']))})"
    return "<b>الأكثر ارتفاعًا</b>\n" + "\n".join(map(f, top)) + \
           "\n\n<b>الأكثر انخفاضًا</b>\n" + "\n".join(map(f, low))

def cmd_funding(arg):
    sym, pair = norm_symbol(arg or "BTC")
    fr, oi = get_funding(pair)
    if fr is None:
        return "لا توجد بيانات عقود آجلة لهذه العملة."
    mood = "ازدحام شراء" if fr > 0.05 else ("ازدحام بيع" if fr < -0.03 else "متوازن")
    return f"💎 <b>{sym}</b>\nالتمويل: {fr:+.4f}% ({mood})\nالفائدة المفتوحة: {fmt_big(oi)}"

def cmd_risk(args):
    try:
        cap, rk, entry, sl = [float(x) for x in args[:4]]
        dist = abs(entry - sl)
        risk_amt = cap * rk / 100
        size = risk_amt / dist
        return (f"🧮 <b>حجم الصفقة</b>\nالمخاطرة: ${risk_amt:.2f}\n"
                f"الكمية: <code>{size:.6g}</code> وحدة\nقيمة الصفقة: ${size * entry:.2f}")
    except Exception:
        return "الاستخدام: <code>/risk رأس_المال نسبة_المخاطرة دخول وقف</code>\nمثال: /risk 1000 1 65000 63500"

TOOLS = (
    "🛠 <b>أدوات مفيدة</b>\n"
    "• coinmarketcap.com - الأسعار والتصنيفات\n"
    "• coingecko.com - بيانات اجتماعية وتطوير\n"
    "• tradingview.com - الرسوم والتحليل الفني\n"
    "• defillama.com - القيمة المقفلة DeFi\n"
    "• airdrops.io - فرص الإيردروب\n"
    "• cryptoquant.com - تحليلات على السلسلة\n"
    "• token.unlocks.app - جداول فتح الرموز\n"
    "• messari.io - تقارير بحثية\n"
    "• arkhamintelligence.com - تتبع المحافظ\n"
    "• debank.com / zapper.fi - إدارة المحافظ\n"
    "• dappradar.com - التطبيقات اللامركزية\n"
    "• cryptorank.io - IDO والتقييمات"
)

HELP = (
    "🤖 <b>بوت التحليل</b>\nاكتب اسم العملة فقط\n"
    "(مثل <code>BTC</code> أو <code>SOL</code>) وأعطيك الصفقة كاملة.\n\n"
    "<b>الأوامر:</b>\n/fear - الخوف والطمع\n/market - حالة السوق\n/top - الأكثر ارتفاعًا/انخفاضًا\n"
    "/funding BTC - التمويل والفائدة المفتوحة\n/risk 1000 1 65000 63500 - حجم الصفقة\n"
    "/tools - أدوات مفيدة\n\n"
    "<b>التتبع:</b>\n/stats - إحصائيات الإشارات\n/open - الصفقات المفتوحة\n/history - آخر الصفقات"
)

# --- اتصال تيليجرام ---
API = f"https://api.telegram.org/bot{TOKEN}"

def send(chat_id, text):
    data = urllib.parse.urlencode({"chat_id": chat_id, "text": text, "parse_mode": "HTML",
                                   "disable_web_page_preview": "true"}).encode()
    try:
        urllib.request.urlopen(urllib.request.Request(f"{API}/sendMessage", data=data, headers=UA), timeout=20)
    except Exception as e:
        print("send error:", e)

def tg(method, **params):
    data = urllib.parse.urlencode(params).encode()
    req = urllib.request.Request(f"{API}/{method}", data=data, headers=UA)
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode())

def handle(m):
    chat = m.get("chat", {})
    chat_id = chat.get("id")
    text = (m.get("text") or "").strip()
    if not text:
        return
    if ALLOWED and str(chat_id) not in ALLOWED:
        return
    if text.startswith("/"):
        cmd = text.split()[0][1:].split("@")[0].lower()
        args = text.split()[1:]
        if cmd in ("start", "help"):
            send(chat_id, HELP)
        elif cmd == "stats":
            send(chat_id, cmd_stats())
        elif cmd == "open":
            send(chat_id, cmd_open())
        elif cmd == "history":
            send(chat_id, cmd_history())
        elif cmd == "fear":
            send(chat_id, cmd_fear())
        elif cmd == "market":
            send(chat_id, cmd_market())
        elif cmd == "top":
            send(chat_id, cmd_top())
        elif cmd == "funding":
            send(chat_id, cmd_funding(args[0] if args else "BTC"))
        elif cmd == "risk":
            send(chat_id, cmd_risk(args))
        elif cmd == "tools":
            send(chat_id, TOOLS)
    else:
        send(chat_id, "⏳ جاري التحليل...")
        try:
            send(chat_id, analyze(text, chat_id))
        except Exception as e:
            send(chat_id, f"❌ تعذر التحليل:\n{html.escape(str(e))}\nتأكد من اسم العملة (مثل BTC).")

def main():
    if not TOKEN:
        raise SystemExit("ضع التوكن أولاً:\nexport BOT_TOKEN='123:ABC...'")
    print("البوت يعمل... (Ctrl+C للإيقاف)")
    threading.Thread(target=tracker_loop, daemon=True).start()
    offset = 0
    while True:
        try:
            res = tg("getUpdates", offset=offset, timeout=50,
                     allowed_updates=json.dumps(["message", "channel_post"]))
            for u in res.get("result", []):
                offset = u["update_id"] + 1
                m = u.get("message") or u.get("channel_post")
                if m:
                    threading.Thread(target=handle, args=(m,), daemon=True).start()
        except Exception as e:
            print("poll error:", e)
            time.sleep(5)

if __name__ == "__main__":
    main()
