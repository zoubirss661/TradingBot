import os, io, re, json, time, asyncio, logging, threading, functools
import datetime as dt
from concurrent.futures import ThreadPoolExecutor
import numpy as np, pandas as pd, requests, ccxt, matplotlib
import urllib.parse
import concurrent.futures

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from telegram import (Update, InlineKeyboardButton, InlineKeyboardMarkup,
                      BotCommand, LinkPreviewOptions, CopyTextButton)
from telegram.constants import ParseMode
from telegram.ext import (ApplicationBuilder, CommandHandler, MessageHandler,
                          ContextTypes, filters, CallbackQueryHandler)

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
ADMIN_ID = _int("ADMIN_ID", 7002618091)
GIST_ID = _e("GIST_ID")
GITHUB_TOKEN = _e("GITHUB_TOKEN")
CONTACT_LINK = _e("CONTACT_LINK", "@rym_rima1")
CHANNEL_LINK = _e("CHANNEL_LINK", "https://t.me/rym_rima16")
VIP_INVITE_LINK = _e("VIP_INVITE_LINK", "https://t.me/+gTMJiBiiC_IyZjZk")
PAYMENT_INFO = _e("PAYMENT_INFO", "Binance Pay: 905142395")
PROTECT = _e("PROTECT_CONTENT", "1") == "1"
MAINTENANCE = _e("MAINTENANCE", "0") == "1"
BRAND = "💎 Ryma Crypto 💎"
RISK_WARNING = "⚠️ <i>التداول ينطوي على مخاطر عالية. يرجى إدارة رأس المال بذكاء. ليست نصيحة مالية.</i>"
NO_PREVIEW = LinkPreviewOptions(is_disabled=True)

EXCLUDE_TICKERS = {"MU", "SPCX", "SNXX", "SOXS", "CL", "TSLA", "AAPL", "NVDA", "META", "GOOG", "AMZN", "MSFT", "NFLX", "AMD", "INTC", "BABA", "COIN", "MSTR", "GME", "AMC"}

REFERRAL_LINKS = {
    "binance": ("Binance", "https://www.binance.com/register?ref=WELCOMBAKC"),
    "okx":     ("OKX",     "https://okx.com/join/QW22UB2Y"),
    "bybit":   ("Bybit",   "https://www.bybit.com/invite?ref=A3YBDWZ"),
    "gateio":  ("Gate",    "https://www.gate.com/ar/referral/registry?ref=VANEUF1XBW"),
    "mexc":    ("MEXC",    "https://s.mexc.com/referral/o0HbNZxKxg"),
    "kucoin":  ("KuCoin",  "https://www.kucoin.com/r/rf/CXEJQGFK"),
    "bitget":  ("Bitget",  "https://www.bitget.com/ar/referral/register?clacCode=UX7TVDCT"),
    "bingx":   ("BingX",   "https://bingxdao.com/invite/6Y2TXMV4/"),
}

PLATFORM_META = {
    "binance":  {"emoji": "🟡", "name": "Binance Square", "tags": "#Binance", "ref": "binance"},
    "bybit":    {"emoji": "🔵", "name": "Bybit", "tags": "#Bybit", "ref": "bybit"},
    "okx":      {"emoji": "⚫", "name": "OKX", "tags": "#OKX", "ref": "okx"},
    "bitget":   {"emoji": "🟢", "name": "Bitget", "tags": "#Bitget", "ref": "bitget"},
    "kucoin":   {"emoji": "🟢", "name": "KuCoin", "tags": "#KuCoin", "ref": "kucoin"},
    "gateio":   {"emoji": "⚪", "name": "Gate.io", "tags": "#Gate", "ref": "gateio"},
    "twitter":  {"emoji": "🐦", "name": "Twitter (X)", "tags": "#Crypto", "ref": "binance"},
    "telegram": {"emoji": "📱", "name": "Telegram", "tags": "#Crypto", "ref": "binance"},
    "facebook": {"emoji": "📘", "name": "Facebook", "tags": "#Crypto", "ref": "binance"},
    "whatsapp": {"emoji": "💬", "name": "WhatsApp", "tags": "#Crypto", "ref": "binance"},
    "reddit":   {"emoji": "🔴", "name": "Reddit", "tags": "#Crypto", "ref": "binance"},
    "discord":  {"emoji": "🎮", "name": "Discord", "tags": "#Crypto", "ref": "binance"},
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
    s = (source or "binance").lower()
    sym_u = sym.upper()
    if s.startswith("binance"): return f"https://www.binance.com/futures/{sym_u}USDT?ref=WELCOMBAKC"
    if s == "okx":     return f"https://www.okx.com/trade-swap/{sym_u.lower()}-usdt-swap"
    if s == "bybit":   return f"https://www.bybit.com/trade/usdt/{sym_u}USDT"
    if s == "gateio":  return f"https://www.gate.com/futures/USDT/{sym_u}_USDT"
    if s == "mexc":    return f"https://futures.mexc.com/exchange/{sym_u}_USDT"
    if s == "kucoin":  return f"https://www.kucoin.com/futures/trade/{sym_u}USDTM"
    if s == "bitget":  return f"https://www.bitget.com/futures/usdt/{sym_u}USDT"
    if s == "bingx":   return f"https://bingx.com/en-us/perpetual/{sym_u}-USDT/"
    return REFERRAL_LINKS["binance"][1]

FAST_EXCHANGES = ["okx", "bybit", "gateio", "mexc", "kucoin", "bitget", "htx", "binance"]
EXCHANGES = ["okx", "bybit", "gateio", "mexc", "kucoin", "bitget", "htx", "binance", "bingx", "lbank", "bitmart", "poloniex"]

BINANCE_FAPI_BASE = "https://fapi.binance.com"
BINANCE_SPOT_MIRRORS = ["https://data-api.binance.vision", "https://api1.binance.com", "https://api2.binance.com", "https://api3.binance.com"]
BINANCE_INTERVAL_MAP = {"15m": "15m", "1h": "1h", "4h": "4h", "1d": "1d"}

MAJORS = {"BTC","ETH","BNB","SOL","XRP","ADA","DOGE","AVAX","LINK","DOT","LTC","TRX"}
CORE_COINS = ["BTC","ETH","BNB","XRP","ADA","DOGE","AVAX","LTC","NEAR","SUI","ATOM","TRX","BONK","OP"]
MEME_COINS = ["TIA","FLOKI","SOL","DOT","INJ"]
ALPHA_COINS = ["ETH","BNB","SOL","SUI","NEAR","INJ","TIA","FLOKI","APT","ARB"]
COINS = CORE_COINS + MEME_COINS
WEAK_COINS = set()
DAILY_EXCLUDE = {"BTC","ETH","BNB","SOL","XRP","DOGE","ADA","TRX","LTC","DOT","LINK","AVAX","TON","SHIB","PEPE","USDC","FDUSD","TUSD","USDT","DAI","BUSD","USDP","USDD","USDE","PYUSD","EUR","AEUR"} | EXCLUDE_TICKERS
STABLES = DAILY_EXCLUDE

TRIAL_DAYS = 7
VIP_FORCE_DAY = 13
FEE, SLIP = 0.0008, 0.0005
TF_MS = {"15m": 900_000, "1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000}
CANDLE_TTL, PRICE_TTL = 240, 60
MIN_BARS = 215
CHART_BARS = 90
SCAN_WORKERS = 8
MAX_OPEN = 100

VIP_POSTS_PER_CYCLE = 5
FREE_POSTS_PER_CYCLE = 10
FREE_MAX_PER_DAY = 40

CYCLE_AUTOPOST       = 12600
CYCLE_SHORT_FREE     = 14400
CYCLE_MARKET_SUMMARY = 16200
CYCLE_PREDICT        = 14400
CYCLE_ALPHA_SUMMARY  = 14400
CYCLE_BOTTOMS_AUTO   = 21600
CYCLE_GOLDEN         = 18000
CYCLE_PROMO          = 604800
CROSS_CHECK_INTERVAL = 1800

SMART_PROFIT_DAYS = 7
SMART_MAX_AGE_DAYS = 14
SMART_FORCE_DAYS = 3
SMART_FILL_TRIGGER = 0.8
CLEANUP_EVERY_CYCLES = 12

PIVOT_WINDOW = 100
PIVOT_LOOKBACK = 3
MIN_PIVOT_DIST = 0.5
CORE_SL_MAX = 4.0
CORE_SL_MIN = 1.5
CORE_TP_FRACS = [0.50, 0.25, 0.15, 0.10]
BE_TRIGGER_R = 0.1
TRAIL_ATR = 3.0
MAX_HOLD = 60
CORE_VOL_MULT = 0.7
CORE_RSI_LONG = (35, 70)
CORE_RSI_SHORT = (50, 80)
EXT_MAX_ATR = 3.5

MIN_DAILY_VOLUME_USD = 5_000_000
VIP_MIN_LIQUIDITY = 3_000_000
REQUIRE_MTF_FOR_SMALL = True

MEME_TP_PCTS = [3.0, 6.0, 10.0, 15.0]
MEME_TP_FRACS = [0.50, 0.25, 0.15, 0.10]
MEME_SL_PCT = 6.5
MEME_VOL_MULT = 1.5
MEME_RSI_LOW = 25
MEME_RSI_HIGH = 75
MEME_BREAKOUT_BARS = 5
MEME_TRAIL_ATR = 2.0
MEME_TIME_STOP = 15
MEME_MAX_HOLD = 50

RISK_SIZE = {"A+": 0.10, "A": 0.08, "B": 0.05, "C": 0.02}

PREDICT_BB_SQZ = 0.75
PREDICT_VOL_BUILDUP = 1.4
PREDICT_RSI_NEUTRAL = (42, 58)
PREDICT_OI_CHANGE_MIN = 3.0
PREDICT_FUNDING_EXTREME = 0.05
PREDICT_TOP_N = 10
PREDICT_TF = "4h"

BOTTOM_MIN_DIFF = -60
BOTTOM_MAX_DIFF = -10
BOTTOM_RSI_RANGE = (25, 55)
BOTTOM_MIN_VOL_RATIO = 0.8
BOTTOM_MIN_REBOUND = 1.5
BOTTOM_AVOID_BREAK = 0.98

SR_LOOKBACK = 50
SR_MERGE_PCT = 0.5
FIB_LEVELS = [0.236, 0.382, 0.5, 0.618, 0.786, 1.272, 1.618]
MACD_FAST, MACD_SLOW, MACD_SIGNAL = 12, 26, 9

DAILY_LONGS = 2
DAILY_SHORTS = 2
DAILY_TP_COUNT = 3
DAILY_TOP_N = 20
DAILY_MIN_CHANGE = 12.0
DAILY_MIN_VOLUME = 5_000_000
CROSS_EXCLUDE = {"BTC","ETH","BNB"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("ryma")
START_TS = time.time()
DAY = 86400
def now_s(): return int(time.time())

DEFAULT_GOLDEN_CONFIG = {
    "require_1d_trend": False, "require_mtf": False, "require_rsi": True,
    "require_volume": False, "require_breakout": False, "require_liquidity": False,
    "require_alpha": False, "min_score": 1,
}

KEYS = ["users","vip","signals","history","posted","settings"]
DEFAULTS = {"users":{},"vip":{},"signals":[],"history":[],"posted":{},
            "settings":{"maintenance": _e("MAINTENANCE","0")=="1",
                        "multi_exchange": True,
                        "golden_config": dict(DEFAULT_GOLDEN_CONFIG)}}

class Store:
    def __init__(self, local_path="data_local.json"):
        self.lock = threading.RLock()
        self.local_path = local_path
        self.data = json.loads(json.dumps(DEFAULTS))
        self.dirty = set()
        self.fname = {k: k+".json" for k in KEYS}
        self.remote_ok, self.remote_msg = False, "in-memory"
        self._load()
    def _hdr(self):
        return {"Authorization": f"Bearer {GITHUB_TOKEN}", "Accept": "application/vnd.github+json"}
    def _load(self):
        if GIST_ID and GITHUB_TOKEN:
            try:
                r = requests.get(f"https://api.github.com/gists/{GIST_ID}", headers=self._hdr(), timeout=20)
                r.raise_for_status()
                files = r.json().get("files", {})
                for k in KEYS:
                    f = files.get(k+".json")
                    if not f: continue
                    txt = f.get("content")
                    if f.get("truncated"):
                        txt = requests.get(f["raw_url"], headers=self._hdr(), timeout=20).text
                    val = json.loads(txt) if txt and txt.strip() else DEFAULTS[k]
                    if type(val) is type(DEFAULTS[k]): self.data[k] = val
                self.remote_ok, self.remote_msg = True, "Gist OK"
                return
            except Exception as e:
                self.remote_msg = f"Gist fail: {str(e)[:60]}"
        if os.path.exists(self.local_path):
            try:
                with open(self.local_path, encoding="utf-8") as fh:
                    self.data.update(json.load(fh))
            except Exception: pass
    def save(self, *keys):
        with self.lock: self.dirty.update(keys)
    def flush(self):
        with self.lock:
            keys = list(self.dirty)
            if not keys: return
            snap = {k: json.dumps(self.data[k], ensure_ascii=False) for k in keys}
            if not self.remote_ok:
                self.dirty.clear(); return
        try:
            r = requests.patch(f"https://api.github.com/gists/{GIST_ID}",
                               headers=self._hdr(), timeout=25,
                               json={"files":{self.fname[k]:{"content":snap[k]} for k in keys}})
            r.raise_for_status()
            with self.lock: self.dirty.difference_update(keys)
        except Exception as e:
            self.remote_msg = f"flush fail: {str(e)[:60]}"
    def start_flusher(self, every=20):
        def loop():
            while True:
                time.sleep(every)
                try: self.flush()
                except Exception: pass
        threading.Thread(target=loop, daemon=True).start()
store = Store()

def is_maintenance():
    with store.lock: return bool(store.data.get("settings", {}).get("maintenance", False))
def set_maintenance(flag):
    with store.lock:
        store.data.setdefault("settings", {})["maintenance"] = bool(flag)
        store.save("settings")
    return bool(flag)
def is_multi_exchange():
    with store.lock: return bool(store.data.get("settings", {}).get("multi_exchange", True))
def set_multi_exchange(flag):
    with store.lock:
        store.data.setdefault("settings", {})["multi_exchange"] = bool(flag)
        store.save("settings")
    return bool(flag)
def get_golden_config():
    with store.lock:
        cfg = store.data.setdefault("settings", {}).get("golden_config")
        if not cfg or not isinstance(cfg, dict):
            cfg = dict(DEFAULT_GOLDEN_CONFIG)
            store.data["settings"]["golden_config"] = cfg
            store.save("settings")
        merged = dict(DEFAULT_GOLDEN_CONFIG); merged.update(cfg)
        return merged
def set_golden_config(key, value):
    with store.lock:
        cfg = store.data.setdefault("settings", {}).setdefault("golden_config", dict(DEFAULT_GOLDEN_CONFIG))
        cfg[key] = value
        store.save("settings")

def touch_user(user, cmd="text"):
    uid = str(user.id)
    with store.lock:
        u = store.data["users"].get(uid) or dict(id=user.id, first_seen=now_s(), requests=0, cmds={}, last_cmd=None)
        u.update(name=getattr(user,"first_name","") or "", username=getattr(user,"username","") or "", last_seen=now_s())
        u["requests"] = u.get("requests",0)+1
        u["last_cmd"] = cmd
        u["cmds"] = u.get("cmds") or {}
        u["cmds"][cmd] = u["cmds"].get(cmd, 0) + 1
        store.data["users"][uid] = u
        store.save("users")
    return u

def vip_until(uid): return int(store.data["vip"].get(str(uid),{}).get("expires",0))
def is_vip(uid): return vip_until(uid) > now_s()

def user_status(uid, rec):
    if ADMIN_ID and uid == ADMIN_ID: return "admin", 0
    if is_vip(uid): return "vip", 0
    actual_days = (now_s() - int(rec.get("first_seen", now_s()))) // DAY + 1
    calculated_day = actual_days - 3 
    if calculated_day <= TRIAL_DAYS: return "trial", actual_days
    if calculated_day < VIP_FORCE_DAY: return "warning", actual_days
    return "blocked", actual_days

def add_vip(uid, days):
    with store.lock:
        exp = max(now_s(), vip_until(uid)) + int(days)*DAY
        store.data["vip"][str(uid)] = dict(expires=exp, added=now_s(), days=int(days))
        store.save("vip")
    return exp
def remove_vip(uid):
    with store.lock:
        ok = store.data["vip"].pop(str(uid), None) is not None
        store.save("vip")
    return ok
def time_ago(ts):
    if not ts: return "—"
    d = now_s() - int(ts)
    if d < 60: return "الآن"
    if d < 3600: return f"{d//60} د"
    if d < 86400: return f"{d//3600} س"
    if d < 604800: return f"{d//86400} يوم"
    return f"{d//604800} أسبوع"

def binance_futures_klines(sym, tf, bars=500):
    interval = BINANCE_INTERVAL_MAP.get(tf)
    if not interval: return None
    try:
        r = requests.get(f"{BINANCE_FAPI_BASE}/fapi/v1/klines",
                         params={"symbol": f"{sym}USDT", "interval": interval, "limit": min(bars,1500)},
                         timeout=10)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list) and data:
                return [[int(k[0]), float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5])] for k in data]
    except Exception: pass
    return None

def binance_futures_ticker(sym):
    try:
        r = requests.get(f"{BINANCE_FAPI_BASE}/fapi/v1/ticker/24hr", params={"symbol": f"{sym}USDT"}, timeout=10)
        if r.status_code == 200:
            d = r.json()
            return {"last": float(d.get("lastPrice", 0)), "percentage": float(d.get("priceChangePercent", 0)),
                    "quoteVolume": float(d.get("quoteVolume", 0)), "high": float(d.get("highPrice", 0)),
                    "low": float(d.get("lowPrice", 0))}
    except Exception: pass
    return None

def binance_futures_all_tickers():
    try:
        r = requests.get(f"{BINANCE_FAPI_BASE}/fapi/v1/ticker/24hr", timeout=15)
        if r.status_code != 200: return None
        data = r.json()
        if not isinstance(data, list): return None
        rows = []
        for t in data:
            try:
                sym_full = t.get("symbol", "")
                if not sym_full.endswith("USDT"): continue
                base_sym = sym_full[:-4]
                if base_sym in DAILY_EXCLUDE or base_sym in EXCLUDE_TICKERS: continue
                if re.search(r"(UP|DOWN|BULL|BEAR|3L|3S|USDC)$", base_sym): continue
                ch = float(t.get("priceChangePercent", 0)); price = float(t.get("lastPrice", 0))
                qv = float(t.get("quoteVolume", 0))
                if price <= 0: continue
                rows.append({"symbol": base_sym, "change": ch, "price": price, "volume": qv})
            except Exception: continue
        return rows
    except Exception: return None

def _binance_mirror_request(path, params):
    for base in BINANCE_SPOT_MIRRORS:
        try:
            r = requests.get(f"{base}{path}", params=params, timeout=10)
            if r.status_code == 200: return r.json(), base
        except Exception: continue
    return None, None

def binance_mirror_klines(sym, tf, bars=500):
    interval = BINANCE_INTERVAL_MAP.get(tf)
    if not interval: return None
    data, base = _binance_mirror_request("/api/v3/klines", {"symbol": f"{sym}USDT", "interval": interval, "limit": min(bars,1000)})
    if not data or not isinstance(data, list): return None
    rows = []
    for k in data:
        try: rows.append([int(k[0]), float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5])])
        except Exception: continue
    return rows

class PairNotFound(Exception): pass
class NoData(Exception): pass
_ex, _ex_lock, _down = {}, {}, {}
_ex_init = threading.Lock()

def get_ex(name):
    with _ex_init:
        if name not in _ex:
            _ex[name] = getattr(ccxt, name)({"enableRateLimit":True,"timeout":12000})
            _ex_lock[name] = threading.Lock()
    return _ex[name]

def default_tf(sym): return "1d" if sym in MAJORS else "4h"

def _rows_to_df(rows, tf, bars):
    cols = ["t","open","high","low","close","volume"]
    if not rows: return pd.DataFrame(columns=cols)
    df = pd.DataFrame(rows, columns=cols).dropna()
    df = df.drop_duplicates("t").sort_values("t").reset_index(drop=True)
    df["t"] = df["t"].astype("int64")
    df = df[df["t"] + TF_MS[tf] <= int(time.time()*1000)]
    return df.tail(bars).reset_index(drop=True)

def _fetch_exchange(name, sym, tf, bars):
    ex = get_ex(name); tfms = TF_MS[tf]
    now = int(time.time()*1000); since = now - bars * tfms
    rows, last_ts = [], None
    with _ex_lock[name]:
        for _ in range(80):
            chunk = ex.fetch_ohlcv(f"{sym}/USDT", tf, since=since, limit=1000)
            if not chunk: break
            rows += chunk
            if last_ts is not None and chunk[-1][0] <= last_ts: break
            last_ts = chunk[-1][0]
            if last_ts + tfms >= now: break
            since = last_ts + tfms
    return rows

_candle_cache = {}
def get_candles(sym, tf, bars=450, ttl=CANDLE_TTL, min_bars=MIN_BARS, binance_only=False):
    key = (sym, tf, bars, min_bars, binance_only)
    hit = _candle_cache.get(key)
    if hit and time.time()-hit[0] < ttl: return hit[1]

    try:
        rows = binance_futures_klines(sym, tf, bars)
        if rows and len(rows) >= min_bars:
            df = _rows_to_df(rows, tf, bars)
            if len(df) >= min_bars:
                df.attrs.update(source="binance_fut", degraded=False)
                _candle_cache[key] = (time.time(), df); return df
    except Exception: pass

    try:
        rows = binance_mirror_klines(sym, tf, bars)
        if rows and len(rows) >= min_bars:
            df = _rows_to_df(rows, tf, bars)
            if len(df) >= min_bars:
                df.attrs.update(source="binance", degraded=False)
                _candle_cache[key] = (time.time(), df); return df
    except Exception: pass

    if binance_only: raise NoData(sym)
    multi = is_multi_exchange()
    if not multi: raise NoData(sym)

    bad, tried, best = 0, 0, None
    for name in EXCHANGES:
        if name == "binance": continue
        if _down.get(name, 0) > time.time(): continue
        tried += 1
        try: rows = _fetch_exchange(name, sym, tf, bars)
        except ccxt.BadSymbol: bad += 1; continue
        except ccxt.NetworkError: _down[name] = time.time() + 120; continue
        except Exception: continue
        if not rows: continue
        df = _rows_to_df(rows, tf, bars)
        if len(df) >= min_bars:
            df.attrs.update(source=name, degraded=False)
            _candle_cache[key] = (time.time(), df); return df
        if len(df) >= 30 and (best is None or len(df) > len(best)):
            df.attrs.update(source=name, degraded=True); best = df
    if best is not None:
        _candle_cache[key] = (time.time(), best); return best
    if tried > 0 and bad == tried: raise PairNotFound(sym)
    raise NoData(sym)

_price_cache = {}
def live_price(sym):
    hit = _price_cache.get(sym)
    if hit and time.time()-hit[0] < PRICE_TTL: return hit[1], hit[2]
    try:
        t = binance_futures_ticker(sym)
        if t and t["last"] > 0:
            _price_cache[sym] = (time.time(), t["last"], "binance_fut")
            return t["last"], "binance_fut"
    except Exception: pass
    for name in FAST_EXCHANGES:
        if _down.get(name, 0) > time.time(): continue
        try:
            ex = get_ex(name)
            with _ex_lock[name]: t = ex.fetch_ticker(f"{sym}/USDT")
            p = float(t.get("last") or 0)
            if p > 0:
                _price_cache[sym] = (time.time(), p, name); return p, name
        except ccxt.NetworkError: _down[name] = time.time()+120
        except Exception: continue
    return None, None

def all_prices(sym):
    def one(name):
        try:
            ex = get_ex(name)
            with _ex_lock[name]: t = ex.fetch_ticker(f"{sym}/USDT")
            p = float(t.get("last") or 0)
            return name, p if p > 0 else None
        except Exception: return name, None
    with ThreadPoolExecutor(len(EXCHANGES)) as pool:
        return {n:p for n,p in pool.map(one, EXCHANGES) if p}

def ticker_change(sym):
    price, change_24h = 0.0, 0.0
    try:
        t = binance_futures_ticker(sym)
        if t and t["last"] > 0: price = t["last"]; change_24h = t["percentage"]
    except Exception: pass
    if price <= 0:
        for ex_name in FAST_EXCHANGES:
            if _down.get(ex_name, 0) > time.time(): continue
            try:
                ex = get_ex(ex_name)
                with _ex_lock[ex_name]: t = ex.fetch_ticker(f"{sym}/USDT")
                p = float(t.get("last") or 0)
                if p > 0: price = p; change_24h = float(t.get("percentage") or 0); break
            except ccxt.NetworkError: _down[ex_name] = time.time() + 120; continue
            except Exception: continue
    change_7d = 0.0
    try:
        df = get_candles(sym, "1d", 10, ttl=600, min_bars=8)
        if len(df) >= 8 and float(df["close"].iloc[-8]) > 0:
            change_7d = (float(df["close"].iloc[-1]) / float(df["close"].iloc[-8]) - 1) * 100
    except Exception: pass
    return price, change_24h, change_7d

_top_movers_cache = [0, [], []]
def _movers_from_ccxt(ex_name, min_change=DAILY_MIN_CHANGE, min_vol=DAILY_MIN_VOLUME):
    try:
        if _down.get(ex_name, 0) > time.time(): return []
        ex = get_ex(ex_name)
        with _ex_lock[ex_name]: tickers = ex.fetch_tickers()
        rows = []
        for s, t in tickers.items():
            if not s.endswith("/USDT") or ":" in s: continue
            base = s.split("/")[0]
            if not base or base in DAILY_EXCLUDE or base in EXCLUDE_TICKERS: continue
            if re.search(r"(UP|DOWN|BULL|BEAR|3L|3S|USDT|USDC)$", base): continue
            try:
                ch = float(t.get("percentage") or 0); price = float(t.get("last") or 0)
                qv = float(t.get("quoteVolume") or 0)
            except: continue
            if price <= 0 or qv < min_vol or abs(ch) < min_change: continue
            rows.append(dict(symbol=base, change=ch, price=price, volume=qv, src=ex_name))
        return rows
    except Exception: return []

def _movers_from_binance_mirror(min_change=DAILY_MIN_CHANGE, min_vol=DAILY_MIN_VOLUME):
    try:
        rows = binance_futures_all_tickers()
        if not rows: return []
        return [dict(symbol=x["symbol"], change=x["change"], price=x["price"], volume=x["volume"], src="binance_fut")
                for x in rows if abs(x["change"]) >= min_change and x["volume"] >= min_vol]
    except Exception: return []

def get_top_movers(force=False):
    if not force and time.time() - _top_movers_cache[0] < 180:
        return _top_movers_cache[1], _top_movers_cache[2]
    all_rows = _movers_from_binance_mirror()
    if is_multi_exchange():
        for ex_name in ["okx", "kucoin", "bitget", "gateio", "mexc", "bybit"]:
            all_rows += _movers_from_ccxt(ex_name)
    best_by_sym = {}
    for x in all_rows:
        sym = x["symbol"]
        if sym not in best_by_sym: best_by_sym[sym] = x
        else:
            if best_by_sym[sym].get("src","").startswith("binance"): continue
            if x.get("src","").startswith("binance"): best_by_sym[sym] = x
            elif abs(x["change"]) > abs(best_by_sym[sym]["change"]): best_by_sym[sym] = x
    rows = list(best_by_sym.values())
    gainers = sorted([x for x in rows if x["change"] > 0], key=lambda x: -x["change"])[:DAILY_TOP_N]
    losers = sorted([x for x in rows if x["change"] < 0], key=lambda x: x["change"])[:DAILY_TOP_N]
    _top_movers_cache[0] = time.time(); _top_movers_cache[1] = gainers; _top_movers_cache[2] = losers
    return gainers, losers

def _bybit_get(path, params):
    try:
        r = requests.get(f"https://api.bybit.com{path}", params=params, timeout=8)
        if r.status_code == 200: return r.json()
    except Exception: pass
    return None

def futures_oi_history(sym, period="4h", limit=20):
    try:
        bp = {"15m":"15min","1h":"1h","4h":"4h","1d":"1d"}.get(period, "4h")
        d = _bybit_get("/v5/market/open-interest", {"category":"linear","symbol":f"{sym}USDT","intervalTime":bp,"limit":limit})
        if d and d.get("retCode") == 0:
            lst = d.get("result", {}).get("list", [])
            if lst and len(lst) >= 3:
                curr = float(lst[0].get("openInterest", 0)); old = float(lst[-1].get("openInterest", 0))
                if old > 0 and curr > 0:
                    return dict(current=curr, change_pct=round((curr/old-1)*100, 2), src="bybit")
    except Exception: pass
    return None

def futures_funding_rate(sym):
    try:
        d = _bybit_get("/v5/market/tickers", {"category":"linear","symbol":f"{sym}USDT"})
        if d and d.get("retCode") == 0:
            lst = d.get("result", {}).get("list", [])
            if lst: return float(lst[0].get("fundingRate", 0)) * 100
    except Exception: pass
    return None

def futures_long_short_ratio(sym, period="4h", limit=10):
    try:
        bp = {"15m":"15min","1h":"1h","4h":"4h","1d":"1d"}.get(period, "4h")
        d = _bybit_get("/v5/market/account-ratio", {"category":"linear","symbol":f"{sym}USDT","period":bp,"limit":1})
        if d and d.get("retCode") == 0:
            lst = d.get("result", {}).get("list", [])
            if lst:
                buy = float(lst[0].get("buyRatio", 0)); sell = float(lst[0].get("sellRatio", 0))
                if sell > 0: return round(buy/sell, 3)
    except Exception: pass
    return None

def futures_signal(sym):
    try:
        oi = futures_oi_history(sym, "4h", 20); fr = futures_funding_rate(sym); ls = futures_long_short_ratio(sym)
        price, _ = live_price(sym)
        if not price or price <= 0: return None
        score = 0; reasons = []
        if oi and oi.get("change_pct", 0) != 0:
            oc = oi["change_pct"]
            if oc > 5: score += 2; reasons.append(f"OI +{oc:.1f}%")
            elif oc > 2: score += 1
            elif oc < -3: score -= 1
        if fr is not None:
            if fr > 0.05: score -= 2; reasons.append(f"Funding {fr:+.3f}%")
            elif fr < -0.05: score += 2; reasons.append(f"Funding {fr:+.3f}%")
            elif abs(fr) < 0.01: score += 1
        if ls is not None:
            if ls > 2.0: score -= 2
            elif ls < 0.67: score += 2
            elif ls < 0.85: score += 1
        if score >= 3: direction, conf = "LONG", "قوية"
        elif score >= 1: direction, conf = "LONG", "متوسطة"
        elif score <= -3: direction, conf = "SHORT", "قوية"
        elif score <= -1: direction, conf = "SHORT", "متوسطة"
        else: direction, conf = None, "—"
        return dict(direction=direction, confidence=conf, score=score, reasons=reasons,
                    price=price, oi=oi, fr=fr, ls=ls)
    except Exception: return None

_futures_score_cache = {}
def futures_score_for(sym, side):
    key = (sym, side)
    hit = _futures_score_cache.get(key)
    if hit and time.time() - hit[0] < 300: return hit[1]
    try:
        oi = futures_oi_history(sym, "4h", 20); fr = futures_funding_rate(sym); ls = futures_long_short_ratio(sym)
        score = 0; details = []
        if oi and oi.get("change_pct", 0) != 0:
            oc = oi["change_pct"]
            if oc > 5: score += 2; details.append(f"OI+{oc:.1f}%")
            elif oc > 2: score += 1
            elif oc < -3: score -= 1
        if fr is not None:
            if side == 1:
                if fr < -0.05: score += 2
                elif abs(fr) < 0.01: score += 1
                elif fr > 0.05: score -= 2
            else:
                if fr > 0.05: score += 2
                elif abs(fr) < 0.01: score += 1
                elif fr < -0.05: score -= 2
        if ls is not None:
            if side == 1:
                if ls < 0.67: score += 2
                elif ls < 0.85: score += 1
                elif ls > 2.0: score -= 2
            else:
                if ls > 2.0: score += 2
                elif ls > 1.5: score += 1
                elif ls < 0.67: score -= 2
        result = dict(score=score, details=details, oi=oi, fr=fr, ls=ls)
        _futures_score_cache[key] = (time.time(), result)
        return result
    except Exception: return dict(score=0, details=[], oi=None, fr=None, ls=None)

def detect_cross(df):
    try:
        if len(df) < 210: return None
        last = df.iloc[-1]; prev = df.iloc[-2]
        if pd.isna(last["ema50"]) or pd.isna(last["ema200"]): return None
        if pd.isna(prev["ema50"]) or pd.isna(prev["ema200"]): return None
        le50 = float(last["ema50"]); le200 = float(last["ema200"])
        pe50 = float(prev["ema50"]); pe200 = float(prev["ema200"])
        if pe50 <= pe200 and le50 > le200:
            return dict(type="golden", price=float(last["close"]), ema50=le50, ema200=le200, bar_t=int(last["t"]))
        if pe50 >= pe200 and le50 < le200:
            return dict(type="death", price=float(last["close"]), ema50=le50, ema200=le200, bar_t=int(last["t"]))
        return None
    except Exception: return None

def check_liquidity(sym, min_usd=MIN_DAILY_VOLUME_USD):
    try:
        df = get_candles(sym, "1h", 24, ttl=600, min_bars=20)
        if df.empty or len(df) < 20: return False
        return float((df["volume"] * df["close"]).sum()) >= min_usd
    except Exception: return False

def whale_radar(sym):
    try:
        for ex_name in ["binance", "okx", "bybit"]:
            try:
                ex = get_ex(ex_name)
                with _ex_lock[ex_name]: trades = ex.fetch_trades(f"{sym}/USDT", limit=200)
                if not trades: continue
                large, largest, buy_vol, sell_vol = 0, 0.0, 0.0, 0.0
                for t in trades:
                    notional = float(t.get("price",0)) * float(t.get("amount",0))
                    largest = max(largest, notional)
                    if notional >= 5000: 
                        large += 1
                        if t.get("side") == "buy": buy_vol += notional
                        elif t.get("side") == "sell": sell_vol += notional
                if largest > 0: return large, largest, buy_vol, sell_vol
            except Exception: continue
        return None, None, 0, 0
    except Exception: return None, None, 0, 0

def liquidity_radar(sym):
    try:
        for ex_name in ["okx", "bybit", "gateio", "mexc", "kucoin"]:
            try:
                ex = get_ex(ex_name)
                with _ex_lock[ex_name]: ob = ex.fetch_order_book(f"{sym}/USDT", limit=50)
                bids = sum(b[0]*b[1] for b in ob.get("bids",[]))
                asks = sum(a[0]*a[1] for a in ob.get("asks",[]))
                total = bids + asks
                if total > 0: return dict(bids=bids, asks=asks, ratio=bids/total, source=ex_name)
            except Exception: continue
        return None
    except Exception: return None

def explosion_radar(sym):
    try:
        df = get_candles(sym, "1h", 60, ttl=120, min_bars=30)
        if df.empty or len(df) < 20: return None
        last_vol = float(df["volume"].iloc[-1]); avg_vol = float(df["volume"].iloc[-20:-1].mean())
        if avg_vol <= 0: return None
        vol_ratio = round(last_vol/avg_vol, 2)
        if vol_ratio >= 1.2: 
            return dict(vol_ratio=vol_ratio, price_change=round((float(df["close"].iloc[-1])/float(df["close"].iloc[-5])-1)*100, 2))
        return None
    except Exception: return None

def pump_radar(sym):
    try:
        df = get_candles(sym, "1h", 100, ttl=180, min_bars=30)
        if df.empty or len(df) < 30: return None
        change_1h = (float(df["close"].iloc[-1]) / float(df["close"].iloc[-2]) - 1) * 100
        change_4h = (float(df["close"].iloc[-1]) / float(df["close"].iloc[-5]) - 1) * 100
        last_vol = float(df["volume"].iloc[-1]); avg_vol = float(df["volume"].iloc[-20:-1].mean())
        if avg_vol <= 0: return None
        vol_ratio = last_vol / avg_vol
        if change_1h > 0.5 and vol_ratio > 1.0: 
            return dict(change_1h=round(change_1h,2), change_4h=round(change_4h,2), vol_ratio=round(vol_ratio,2))
        return None
    except Exception: return None

def bottom_radar(sym):
    try:
        df = get_candles(sym, "4h", 400, ttl=300, min_bars=200)
        if df.empty or len(df) < 200: return None
        df = add_indicators(df, 100); last = df.iloc[-1]
        if pd.isna(last["rsi"]) or pd.isna(last["ema200"]): return None
        price = float(last["close"]); ema200 = float(last["ema200"])
        diff_pct = (price / ema200 - 1) * 100
        if not (BOTTOM_MIN_DIFF <= diff_pct <= BOTTOM_MAX_DIFF): return None
        rsi_v = float(last["rsi"])
        if not (BOTTOM_RSI_RANGE[0] <= rsi_v <= BOTTOM_RSI_RANGE[1]): return None
        recent_vol = float(df["volume"].tail(5).mean()); avg_vol = float(df["volume"].tail(30).mean())
        if avg_vol <= 0: return None
        vol_ratio = recent_vol / avg_vol
        if vol_ratio < BOTTOM_MIN_VOL_RATIO: return None
        low20 = float(df["low"].tail(20).min())
        if price < low20 * BOTTOM_AVOID_BREAK: return None
        low10 = float(df["low"].tail(10).min())
        rebound_pct = (price / low10 - 1) * 100
        if rebound_pct < BOTTOM_MIN_REBOUND: return None
        if diff_pct < -40 and rsi_v < 35 and vol_ratio > 1.2: severity = "فرصة قوية"
        elif diff_pct < -25 and rebound_pct > 3: severity = "فرصة جيدة"
        else: severity = "مراقبة"
        return dict(diff=round(diff_pct, 1), rsi=round(rsi_v, 1), rebound=round(rebound_pct, 2),
                    vol_ratio=round(vol_ratio, 2), severity=severity, price=round(price, 6))
    except Exception: return None

def short_radar(sym):
    try:
        df = get_candles(sym, "4h", 200, ttl=300, min_bars=100)
        if df.empty or len(df) < 100: return None
        df = add_indicators(df, 100); last = df.iloc[-1]
        if pd.isna(last["rsi"]): return None
        if last["rsi"] > 55: 
            return dict(rsi=round(float(last["rsi"]),1), price=round(float(last["close"]),6))
        return None
    except Exception: return None

def alpha_radar(sym):
    try:
        df = get_candles(sym, "1d", 100, ttl=600, min_bars=50)
        if df.empty or len(df) < 50: return None
        df = add_indicators(df, 100); last = df.iloc[-1]
        if pd.isna(last["rsi"]) or pd.isna(last["ema20"]) or pd.isna(last["ema50"]): return None
        change_7d = (float(df["close"].iloc[-1]) / float(df["close"].iloc[-7]) - 1) * 100
        change_30d = (float(df["close"].iloc[-1]) / float(df["close"].iloc[-30]) - 1) * 100
        if last["ema20"] > last["ema50"] and 30 < last["rsi"] < 75 and change_7d > 3:
            return dict(change_7d=round(change_7d,2), change_30d=round(change_30d,2), rsi=round(float(last["rsi"]),1))
        return None
    except Exception: return None

_top_cache = [0, []]
def top_symbols(n=60, binance_only=False):
    if not binance_only and time.time()-_top_cache[0] < 1800 and _top_cache[1]: return _top_cache[1][:n]
    try:
        rows = binance_futures_all_tickers()
        if rows:
            rows = [(x["volume"], x["symbol"]) for x in rows if x["volume"] > 2e6 and x["symbol"] not in EXCLUDE_TICKERS]
            rows.sort(reverse=True)
            if rows:
                _top_cache[0], _top_cache[1] = time.time(), [b for _,b in rows]
                return _top_cache[1][:n]
    except Exception: pass
    if binance_only: return COINS[:n]
    if is_multi_exchange():
        for name in FAST_EXCHANGES:
            if name == "binance": continue
            if _down.get(name, 0) > time.time(): continue
            try:
                ex = get_ex(name)
                with _ex_lock[name]: tk = ex.fetch_tickers()
                rows = []
                for s,t in tk.items():
                    if not s.endswith("/USDT") or ":" in s: continue
                    base = s.split("/")[0]
                    if base in DAILY_EXCLUDE or base in EXCLUDE_TICKERS: continue
                    if re.search(r"(UP|DOWN|BULL|BEAR|3L|3S)$", base): continue
                    qv = t.get("quoteVolume") or 0
                    if qv and qv > 2e6: rows.append((qv, base))
                rows.sort(reverse=True)
                if rows:
                    _top_cache[0], _top_cache[1] = time.time(), [b for _,b in rows]
                    return _top_cache[1][:n]
            except Exception: continue
    return COINS[:n]

_HOT = [0, ["BTC", "ETH"]]
def hot_coins_for(sym):
    if time.time() - _HOT[0] > 3600:
        try:
            rows = binance_futures_all_tickers()
            if rows:
                pop = sorted([r for r in rows if r["symbol"] in ("BTC","ETH","SOL","BNB","XRP","DOGE","ADA")],
                             key=lambda x: -x["volume"])
                _HOT[0] = time.time()
                _HOT[1] = [r["symbol"] for r in pop[:2]] or ["BTC", "ETH"]
        except Exception: pass
    others = [c for c in _HOT[1] if c != sym]
    coins = ([sym] + others)[:3]
    while len(coins) < 3:
        for c in ("BTC", "ETH", "SOL"):
            if c not in coins:
                coins.append(c); break
    return coins

def wilder(s,n): return s.ewm(alpha=1/n, adjust=False, min_periods=n).mean()
def ema(s,n): return s.ewm(span=n, adjust=False, min_periods=n).mean()
def true_range(df):
    pc = df["close"].shift()
    return pd.concat([df["high"]-df["low"], (df["high"]-pc).abs(), (df["low"]-pc).abs()], axis=1).max(axis=1)
def rsi_calc(close, n=14):
    d = close.diff()
    up, dn = wilder(d.clip(lower=0),n), wilder((-d).clip(lower=0),n)
    out = 100 - 100/(1 + up/dn.replace(0,np.nan))
    out[(dn==0) & up.notna()] = 100.0
    return out
def macd_calc(close, fast=MACD_FAST, slow=MACD_SLOW, sig=MACD_SIGNAL):
    efast = ema(close, fast); eslow = ema(close, slow)
    macd = efast - eslow; signal_line = ema(macd, sig); hist = macd - signal_line
    return macd, signal_line, hist

def add_indicators(df, atrp_win=100):
    df = df.copy(); c = df["close"]
    df["ema20"], df["ema50"], df["ema200"] = ema(c,20), ema(c,50), ema(c,200)
    df["rsi"] = rsi_calc(c); df["atr"] = wilder(true_range(df), 14)
    df["rvol"] = df["volume"] / df["volume"].rolling(20).mean().replace(0, np.nan)
    df["macd"], df["macd_sig"], df["macd_hist"] = macd_calc(c)
    mid, sd = c.rolling(20).mean(), c.rolling(20).std()
    df["bb_mid"], df["bb_up"], df["bb_lo"] = mid, mid+2*sd, mid-2*sd
    atrp = df["atr"] / c
    df["atrp"] = atrp.rolling(atrp_win, min_periods=min(atrp_win,30)).apply(lambda x: (x[-1] >= x).mean()*100, raw=True)
    rng = (df["high"] - df["low"]).replace(0, np.nan)
    df["wick_up"] = (df["high"] - df[["close","open"]].max(axis=1)) / rng
    df["wick_dn"] = (df[["close","open"]].min(axis=1) - df["low"]) / rng
    df["ext_atr"] = (c - df["ema20"]) / df["atr"]
    return df

def detect_trend(df, i):
    if i < 50: return "unknown"
    r = df.iloc[i]
    if pd.isna(r["ema20"]) or pd.isna(r["ema50"]) or pd.isna(r["ema200"]): return "unknown"
    if r["ema20"] > r["ema50"] > r["ema200"] and r["close"] > r["ema20"]: return "strong_up"
    if r["ema20"] > r["ema50"] and r["close"] > r["ema200"]: return "up"
    if r["ema20"] < r["ema50"] < r["ema200"] and r["close"] < r["ema20"]: return "strong_down"
    if r["ema20"] < r["ema50"] and r["close"] < r["ema200"]: return "down"
    return "range"

def daily_bias(df_d):
    try:
        if df_d is None or len(df_d) < 200: return 0
        last = df_d.iloc[-1]; c = float(last["close"])
        e20 = float(last["ema20"]) if not pd.isna(last["ema20"]) else c
        e50 = float(last["ema50"]) if not pd.isna(last["ema50"]) else c
        e200 = float(last["ema200"]) if not pd.isna(last["ema200"]) else c
        rsi_v = float(last["rsi"]) if not pd.isna(last["rsi"]) else 50
        score = 0
        if c < e50: score -= 1
        if c < e200: score -= 1
        if c > e50: score += 1
        if c > e200: score += 1
        if e20 < e50: score -= 1
        if e50 < e200: score -= 1
        if e20 > e50: score += 1
        if e50 > e200: score += 1
        if rsi_v < 45: score -= 1
        if rsi_v > 55: score += 1
        try:
            change_7d = (c / float(df_d["close"].iloc[-8]) - 1) * 100
            if change_7d < -5: score -= 1
            if change_7d > 5: score += 1
        except Exception: pass
        if score <= -2: return -1
        if score >= 2: return 1
        return 0
    except Exception: return 0

def mtf_score(sym, side):
    try:
        if sym in MAJORS: tfs = ["1d", "4h"]
        elif sym in MEME_COINS: tfs = ["4h", "1d"]
        else: tfs = ["4h", "1h", "1d"]
        confirms = 0; total = 0
        for tf in tfs:
            try:
                df = get_candles(sym, tf, 250, ttl=300, min_bars=100)
                df = add_indicators(df, 100); last = df.iloc[-1]
                if pd.isna(last["ema50"]) or pd.isna(last["ema200"]): continue
                total += 1
                if side == 1:
                    if last["close"] > last["ema50"] and last["close"] > last["ema200"]: confirms += 1
                else:
                    if last["close"] < last["ema50"] and last["close"] < last["ema200"]: confirms += 1
            except Exception: continue
        if total == 0: return 0, 0
        return confirms, total
    except Exception: return 0, 0

def find_pivots(df, window=PIVOT_WINDOW, lookback=PIVOT_LOOKBACK):
    try:
        n = len(df)
        if n < window + 20: window = max(20, n-20)
        if window < 20: return [], []
        sub = df.tail(window).reset_index(drop=True)
        highs, lows = [], []
        for i in range(lookback, len(sub)-lookback):
            try:
                h = float(sub["high"].iloc[i]); l = float(sub["low"].iloc[i])
                if pd.isna(h) or pd.isna(l): continue
                if all(h >= float(sub["high"].iloc[i-j]) for j in range(1,lookback+1)) and all(h >= float(sub["high"].iloc[i+j]) for j in range(1,lookback+1)): highs.append(h)
                if all(l <= float(sub["low"].iloc[i-j]) for j in range(1,lookback+1)) and all(l <= float(sub["low"].iloc[i+j]) for j in range(1,lookback+1)): lows.append(l)
            except: continue
        highs.sort(reverse=True); lows.sort(reverse=True)
        return highs, lows
    except Exception: return [], []

def select_targets(pivots, price, side, min_dist_pct=MIN_PIVOT_DIST):
    selected = []
    for p in pivots:
        if side == 1:
            if p <= price * (1 + min_dist_pct/100): continue
            if selected and p <= selected[-1] * (1 + min_dist_pct/100): continue
            selected.append(p)
        else:
            if p >= price * (1 - min_dist_pct/100): continue
            if selected and p >= selected[-1] * (1 - min_dist_pct/100): continue
            selected.append(p)
        if len(selected) >= 4: break
    return selected

def compute_core_levels(df, i, side, price=None):
    try:
        r = df.iloc[i]; atr = float(r["atr"]) if not pd.isna(r["atr"]) else 0
        if atr <= 0: return None
        entry = float(price) if price else float(r["close"])
        highs, lows = find_pivots(df, PIVOT_WINDOW)
        targets = select_targets(highs if side==1 else lows, entry, side)
        if len(targets) < 2:
            targets = ([entry*(1+x/100) for x in [2.5,5.0,7.0,8.0]] if side==1 else [entry*(1-x/100) for x in [2.5,5.0,7.0,8.0]])
        targets = targets[:4]
        lookback_df = df.tail(20)
        swing = float(lookback_df["low"].min()) if side==1 else float(lookback_df["high"].max())
        sl_raw = swing - 0.1*atr if side==1 else swing + 0.1*atr
        dist_raw = abs(entry - sl_raw)
        dist = min(max(dist_raw, entry*CORE_SL_MIN/100), entry*CORE_SL_MAX/100)
        sl = entry - side * dist; risk = abs(entry - sl)
        if risk <= 0: return None
        tps = []
        for idx, px in enumerate(targets):
            pct = 100 * side * (px - entry) / entry
            frac = CORE_TP_FRACS[idx] if idx < len(CORE_TP_FRACS) else 0
            tps.append(dict(px=float(px), pct=round(pct,2), frac=frac, hit=False, src=""))
        while len(tps) < 4:
            last_px = tps[-1]["px"]; next_px = last_px + side * atr * 1.5
            pct = 100 * side * (next_px - entry) / entry
            frac = CORE_TP_FRACS[len(tps)] if len(tps) < len(CORE_TP_FRACS) else 0
            tps.append(dict(px=float(next_px), pct=round(pct,2), frac=frac, hit=False, src=""))
        tps.sort(key=lambda x: x["px"], reverse=(side == -1))
        return dict(entry=entry, sl=sl, risk=risk, tps=tps, atr=atr)
    except Exception: return None

def compute_meme_levels(df, i, side, price=None):
    try:
        r = df.iloc[i]; entry = float(price) if price else float(r["close"])
        sl = entry * (1 - side * MEME_SL_PCT/100); risk = abs(entry - sl)
        if risk <= 0: return None
        tps = []
        for pct, f in zip(MEME_TP_PCTS, MEME_TP_FRACS):
            px = entry * (1 + side*pct/100)
            tps.append(dict(px=float(px), pct=pct, frac=f, hit=False, src=""))
        atr = float(r["atr"]) if not pd.isna(r["atr"]) else 0
        return dict(entry=entry, sl=sl, risk=risk, tps=tps, atr=atr)
    except Exception: return None

def detect_core_conditions(df, i, side):
    if i < 30: return False
    r = df.iloc[i]
    if pd.isna(r.rsi) or pd.isna(r.ema20) or pd.isna(r.ema50) or pd.isna(r.rvol): return False
    if side == 1:
        if not (CORE_RSI_LONG[0] <= r.rsi <= CORE_RSI_LONG[1]): return False
        if not (r.ema20 > r.ema50*0.99 and r.close > r.ema50*0.99): return False
    else:
        if not (CORE_RSI_SHORT[0] <= r.rsi <= CORE_RSI_SHORT[1]): return False
        if not (r.ema20 < r.ema50*1.01 and r.close < r.ema50*1.01): return False
    if r.rvol < CORE_VOL_MULT: return False
    if abs(r.ext_atr) > EXT_MAX_ATR: return False
    return True

def detect_meme_conditions(df, i, side):
    if i < MEME_BREAKOUT_BARS + 5: return False
    r = df.iloc[i]
    if pd.isna(r.rsi) or pd.isna(r.rvol): return False
    if not (MEME_RSI_LOW <= r.rsi <= MEME_RSI_HIGH): return False
    if r.rvol < MEME_VOL_MULT: return False
    rh = float(df["high"].iloc[i-MEME_BREAKOUT_BARS:i].max())
    rl = float(df["low"].iloc[i-MEME_BREAKOUT_BARS:i].min())
    if side == 1:
        if float(r.close) <= rh: return False
    else:
        if float(r.close) >= rl: return False
    return True

def grade_setup(df, i, side, futures_score=0):
    r = df.iloc[i]; score = 0
    if side==1 and 40 <= r.rsi <= 65: score += 1
    elif side==-1 and 55 <= r.rsi <= 75: score += 1
    ema_dist = abs(r.close-r.ema20)/r.atr if r.atr > 0 else 0
    if 0.3 <= ema_dist <= 2.5: score += 1
    if r.rvol >= 1.2: score += 1
    body = abs(r.close-r.open)/r.atr if r.atr > 0 else 0
    if body >= 0.4: score += 1
    if futures_score >= 3: score += 2
    elif futures_score >= 1: score += 1
    elif futures_score <= -3: score -= 2
    elif futures_score <= -1: score -= 1
    if score >= 4: return "A+"
    if score >= 3: return "A"
    if score >= 2: return "B"
    return "C"

def decide_side(df, i, trend, bias_d, long_ok, short_ok):
    r = df.iloc[i]
    try:
        c = float(r["close"]); sgn = lambda x: 1 if x > 0 else (-1 if x < 0 else 0)
        score = (sgn(c - float(r["ema20"])) + sgn(c - float(r["ema50"])) + sgn(c - float(r["ema200"]))
                 + sgn(float(r["ema20"]) - float(r["ema50"])) + sgn(float(r["macd_hist"])))
        if score <= -3: return -1
        if score >= 3: return 1
        if score in (-2, 2):
            d = 1 if score > 0 else -1
            if bias_d != -d: return d
            return d if abs(c - float(r["ema200"])) > 0 and sgn(c - float(r["ema200"])) == d else bias_d
    except Exception: pass
    tf_dir = 1 if trend in ("strong_up", "up") else (-1 if trend in ("strong_down", "down") else 0)
    if tf_dir: return tf_dir
    if bias_d: return bias_d
    if long_ok != short_ok: return 1 if long_ok else -1
    try:
        c, e50 = float(r["close"]), float(r["ema50"])
        return -1 if c < e50 else 1
    except Exception: return 1

def find_sr_levels(df, lookback=SR_LOOKBACK):
    try:
        sub = df.tail(lookback).reset_index(drop=True)
        if len(sub) < 20: return [], []
        highs, lows = [], []
        for i in range(3, len(sub)-3):
            h = float(sub["high"].iloc[i]); l = float(sub["low"].iloc[i])
            if pd.isna(h) or pd.isna(l): continue
            if all(h >= float(sub["high"].iloc[i-j]) for j in range(1,4)) and all(h >= float(sub["high"].iloc[i+j]) for j in range(1,4)): highs.append(h)
            if all(l <= float(sub["low"].iloc[i-j]) for j in range(1,4)) and all(l <= float(sub["low"].iloc[i+j]) for j in range(1,4)): lows.append(l)
        def merge(levels):
            if not levels: return []
            levels = sorted(levels); merged = [levels[0]]
            for lv in levels[1:]:
                if abs(lv - merged[-1]) / merged[-1] * 100 < SR_MERGE_PCT: merged[-1] = (merged[-1] + lv) / 2
                else: merged.append(lv)
            return merged
        return merge(highs), merge(lows)
    except Exception: return [], []

def detect_candle_pattern(df, i):
    if i < 5: return None
    r = df.iloc[i]; o, h, l, c = float(r["open"]), float(r["high"]), float(r["low"]), float(r["close"])
    body = abs(c - o); rng = h - l
    if rng == 0: return None
    upper = h - max(o, c); lower = min(o, c) - l
    if lower > body * 2 and upper < body * 0.5 and body > 0: return "مطرقة"
    if upper > body * 2 and lower < body * 0.5 and body > 0: return "شهاب"
    if body < rng * 0.1: return "دوجي"
    if i >= 1:
        prev = df.iloc[i-1]
        if c > o and prev["close"] < prev["open"] and c > float(prev["open"]) and o < float(prev["close"]): return "ابتلاع صاعد"
        if c < o and prev["close"] > prev["open"] and c < float(prev["open"]) and o > float(prev["close"]): return "ابتلاع هابط"
    return None

def fib_levels(df, lookback=SR_LOOKBACK):
    try:
        sub = df.tail(lookback); hi = float(sub["high"].max()); lo = float(sub["low"].min())
        diff = hi - lo
        if diff <= 0: return []
        return [round(hi - diff * f, 6) for f in FIB_LEVELS]
    except Exception: return []

_analysis_cache = {}

def analyze(sym):
    tf = default_tf(sym); df = None; last_err = None
    fallbacks = [tf] + [x for x in ("1h", "4h", "1d") if x != tf]
    for try_tf in fallbacks:
        try:
            df = get_candles(sym, try_tf, 450); tf = try_tf; break
        except PairNotFound: raise
        except NoData as e: last_err = e; continue
    if df is None: raise last_err or NoData(sym)
    degraded = bool(df.attrs.get("degraded"))
    df = add_indicators(df, 100); i = len(df) - 1
    is_meme = sym in MEME_COINS
    if is_meme:
        long_ok = detect_meme_conditions(df, i, 1); short_ok = detect_meme_conditions(df, i, -1)
        strategy = "Meme"
    else:
        long_ok = detect_core_conditions(df, i, 1); short_ok = detect_core_conditions(df, i, -1)
        strategy = "Core"
    price, _ = live_price(sym); price = float(price or df["close"].iloc[-1])
    trend = detect_trend(df, i); df_d = None
    try:
        df_d = get_candles(sym, "1d", 250, ttl=300, min_bars=100); df_d = add_indicators(df_d, 100)
    except Exception: pass
    bias_d = daily_bias(df_d) if df_d is not None else 0
    side = decide_side(df, i, trend, bias_d, long_ok, short_ok)
    try: fut = futures_score_for(sym, side)
    except Exception: fut = dict(score=0, details=[])
    fut_score = fut.get("score", 0)
    grade = grade_setup(df, i, side, futures_score=fut_score)
    lv = (compute_meme_levels(df, i, side, price) if is_meme else compute_core_levels(df, i, side, price))
    if lv is None: raise NoData(sym)
    candle = detect_candle_pattern(df, i); sr_high, sr_low = find_sr_levels(df); fibs = fib_levels(df)
    last = df.iloc[-1]
    return dict(sym=sym, tf=tf, df=df, side=side, strategy=strategy,
                rank={"A+":3,"A":2,"B":1,"C":0}.get(grade, 0), grade=grade, setup=strategy,
                ok=1 if grade in ("A+","A","B") else 0, total=4,
                lv=lv, price=price, degraded=degraded, source=df.attrs.get("source", "binance_fut"),
                bar_t=int(last["t"]), rsi=float(last["rsi"]) if not pd.isna(last["rsi"]) else 50.0,
                macd=float(last["macd"]) if not pd.isna(last["macd"]) else 0.0,
                candle=candle, trend=trend,
                trend_d=("down" if bias_d==-1 else ("up" if bias_d==1 else "range")),
                sr_high=sr_high[:3], sr_low=sr_low[:3], fibs=fibs,
                fut_score=fut_score, fut_details=fut.get("details", []))

def get_analysis(sym, force=False):
    hit = _analysis_cache.get(sym)
    if hit and not force and time.time()-hit[0] < 60: return hit[1]
    res = analyze(sym); _analysis_cache[sym] = (time.time(), res); return res

def scan_sync(symbols, workers=SCAN_WORKERS):
    def one(s):
        try: return get_analysis(s)
        except Exception: return None
    with ThreadPoolExecutor(workers) as ex:
        return [r for r in ex.map(one, symbols) if r]

def new_trade(sym, tf, side, lv, bar_t, grade, setup, size_pct=0.05):
    return dict(id=f"{sym}-{tf}-{bar_t}", coin=sym, tf=tf, side=side, grade=grade, setup=setup,
                entry=lv["entry"], sl=lv["sl"], original_sl=lv["sl"], risk=lv["risk"],
                size_pct=size_pct, tps=[dict(t) for t in lv["tps"]], state="open",
                wait=0, fill=lv["entry"], ext=lv["entry"], remaining=1.0, realized=0.0,
                bars=0, be=False, trail_on=False, mfe=0.0, mae=0.0,
                created=int(time.time()*1000), opened=int(time.time()*1000),
                closed=None, next_t=int(bar_t)+TF_MS[tf], R=None, result=None)

def _tighten(side, cur, new): return max(cur,new) if side==1 else min(cur,new)

def _finish(p, ev, kind):
    p["remaining"], p["state"], p["result"] = 0.0, "closed", kind
    p["closed"] = int(time.time()*1000)
    cost = 2*(FEE+SLIP)*p["fill"]/p["risk"]
    p["R"] = round(p["realized"]-cost, 3); ev.append(dict(kind="DONE")); return ev

def close_at(p, price, kind, ev=None):
    ev = [] if ev is None else ev
    p["realized"] += p["remaining"]*p["side"]*(price-p["fill"])/p["risk"]
    ev.append(dict(kind=kind, px=price)); return _finish(p, ev, kind)

def trade_step(p, o, h, l, c, atr):
    ev, s = [], p["side"]
    if p["state"] not in ("pending","open"): return ev
    p["bars"] += 1; fill, risk = p["fill"], p["risk"]
    fav, adv = (h,l) if s==1 else (l,h)
    p["mfe"] = max(p["mfe"], s*(fav-fill)/risk); p["mae"] = max(p["mae"], s*(fill-adv)/risk)
    if (s==1 and l<=p["sl"]) or (s==-1 and h>=p["sl"]):
        kind = "TRAIL" if p["trail_on"] else ("BE" if p["be"] else "SL")
        return close_at(p, p["sl"], kind, ev)
    for j,tp in enumerate(p["tps"]):
        if tp["hit"]: continue
        if not ((s==1 and h>=tp["px"]) or (s==-1 and l<=tp["px"])): break
        tp["hit"] = True
        p["realized"] += tp["frac"]*s*(tp["px"]-fill)/risk
        p["remaining"] -= tp["frac"]
        ev.append(dict(kind="TP", j=j+1, px=tp["px"], pct=tp["pct"]))
    if p["remaining"] <= 1e-9: return _finish(p, ev, "TP")
    if p["tps"][0]["hit"] and not p["be"]:
        p["sl"] = _tighten(s, p["sl"], fill - s*BE_TRIGGER_R*risk); p["be"] = True
        ev.append(dict(kind="BE_MOVED", px=p["sl"]))
    p["ext"] = max(p["ext"], h) if s==1 else min(p["ext"], l)
    if sum(t["hit"] for t in p["tps"]) >= 2:
        p["trail_on"] = True
        if atr and atr > 0: p["sl"] = _tighten(s, p["sl"], p["ext"] - s*TRAIL_ATR*atr)
    if p["bars"] >= MAX_HOLD: return close_at(p, c, "TIME", ev)
    return ev

def fmt(x):
    x = float(x)
    if x >= 1000: return f"{x:,.2f}"
    if x >= 1: return f"{x:.4f}".rstrip("0").rstrip(".")
    if x >= 0.01: return f"{x:.5f}"
    return f"{x:.8f}"

def smart_cleanup_trades():
    now_ms = int(time.time()*1000); act = active_signals()
    fill_ratio = len(act) / MAX_OPEN if MAX_OPEN > 0 else 0
    closed_profit = closed_age = closed_forced = 0
    with store.lock:
        for p in list(store.data["signals"]):
            if p.get("state") not in ("pending","open"): continue
            age_days = (now_ms - p.get("created", now_ms)) / (DAY*1000)
            cur_r = p.get("realized", 0.0)
            if age_days > SMART_MAX_AGE_DAYS:
                p["state"] = "closed"; p["result"] = "AGE_CLOSE"
                p["R"] = p.get("R") or round(cur_r, 3); p["closed"] = now_ms; closed_age += 1; continue
            if age_days > SMART_PROFIT_DAYS and cur_r > 0.5:
                p["state"] = "closed"; p["result"] = "PROFIT_TAKE"
                p["R"] = round(cur_r, 3); p["closed"] = now_ms; closed_profit += 1; continue
            if fill_ratio > SMART_FILL_TRIGGER and age_days > SMART_FORCE_DAYS and cur_r < 0:
                p["state"] = "closed"; p["result"] = "SMART_EXIT"
                p["R"] = round(cur_r, 3); p["closed"] = now_ms; closed_forced += 1; continue
        if closed_profit + closed_age + closed_forced: store.save("signals")
    return closed_profit + closed_age + closed_forced

def check_signals_sync():
    notes = []; by = {}
    for p in active_signals(): by.setdefault((p["coin"], p["tf"]), []).append(p)
    for (coin, tf), plist in by.items():
        try:
            raw = get_candles(coin, tf, 450, min_bars=60); d = add_indicators(raw, 100)
        except Exception: continue
        for p in list(plist):
            try:
                with store.lock:
                    rows = d[d["t"] >= p["next_t"]]
                    for row in rows.itertuples():
                        ev = trade_step(p, row.open, row.high, row.low, row.close, row.atr)
                        p["next_t"] = int(row.t) + TF_MS[tf]
                        notes += [(coin, tf, p["side"], e, p.get("R")) for e in ev if e["kind"] != "DONE"]
                        if p["state"] in ("closed","expired"): break
                    store.save("signals")
                if p["state"] == "closed": _archive(p)
            except Exception: continue
    return notes

def _archive(p):
    with store.lock:
        store.data["history"].append(dict(
            coin=p["coin"], tf=p["tf"], side=p["side"], grade=p["grade"], setup=p["setup"],
            R=p["R"], result=p["result"], opened=p["opened"], closed=p["closed"],
            bars=p["bars"], tps=sum(1 for t in p["tps"] if t["hit"]),
            mfe=round(p["mfe"],2), mae=round(p["mae"],2), fill=p.get("fill"), sl=p.get("sl"),
            tps_list=[dict(t) for t in p["tps"]], realized=p.get("realized", 0.0)))
        del store.data["history"][:-1500]
        cutoff = time.time()*1000 - 3*DAY*1000
        store.data["signals"] = [q for q in store.data["signals"]
                                 if q.get("state","open") in ("pending","open") or
                                    (q.get("closed") or q.get("created",0)) > cutoff]
        store.save("signals","history")

def format_trade_card(p, event_kind=None, event_data=None):
    source = display_source(p.get("source") or "binance_fut")
    cl = coin_link(p['coin'], p.get("source") or "binance_fut")
    lines = [f"📌 <a href='{cl}'>#{p['coin']}/USDT</a> 👆 اضغط هنا", "",
             f"📡 المصدر: <b>{source}</b>", "",
             f"➡️ Entry: {fmt(p['fill'])}", ""]
    for i, tp in enumerate(p["tps"], 1):
        check = "✅" if tp["hit"] else ""
        lines.append(f"🎯 Target {i}: {fmt(tp['px'])} {check}".rstrip())
    lines.append("")
    if p.get("be") and p.get("trail_on"): label = "🛑 Trail SL"
    elif p.get("be"): label = "🛑 Break-even"
    else: label = "🛑 Stop loss"
    lines.append(f"{label}: {fmt(p['sl'])}")
    tp_hits = sum(1 for t in p["tps"] if t["hit"])
    lines += ["", f"📊 R: {p.get('realized', 0.0):+.2f} | TP: {tp_hits}/4", "",
              f"⚠️ <i>تحقق من الأسعار على {source}</i>"]
    if p.get("state") == "closed":
        labels = {"TP":"✅ صفقة ناجحة بالكامل","SL":"🛑 ضرب وقف الخسارة","BE":"🔒 خروج على التعادل",
                  "TRAIL":"🔁 خروج بالوقف المتحرك","TIME":"⏱ انتهت المدة","PROFIT_TAKE":"💰 أخذ الربح الذكي",
                  "SMART_EXIT":"🧹 خروج ذكي","AGE_CLOSE":"📅 انتهاء العمر"}
        lines += ["", f"{labels.get(p.get('result',''), '🏁 انتهت')} | {p.get('R', 0):+.2f}R"]
    return "\n".join(lines)

def event_text(coin, tf, side, e, R, tier="free", trade=None):
    k = e["kind"]
    if trade is not None:
        card = format_trade_card(trade, k, e)
        headers = {"TP":f"🎯 <b>تحقق الهدف {e['j']}</b>\n\n","SL":"🛑 <b>ضرب وقف الخسارة</b>\n\n",
                   "BE":"🔒 <b>خروج على التعادل</b>\n\n","TRAIL":"🔁 <b>خروج بالوقف المتحرك</b>\n\n",
                   "TIME":"⏱ <b>انتهت المدة</b>\n\n","BE_MOVED":"🔒 <b>نُقل الوقف للتعادل</b>\n\n",
                   "ENTRY":"✅ <b>تفعّل الدخول</b>\n\n"}
        return headers.get(k, "") + card
    return None

# ==========================================
# 🛠️ الإصلاح الجذري لصفقة اليوم (Daily Fix)
# ==========================================
def _build_daily_lists(force_side=None):
    # 1. جلب بيانات المتحركين للسياق فقط
    gainers, losers = get_top_movers(force=True)
    all_movers = gainers + losers
    top_syms = [x["symbol"] for x in all_movers[:40]]
    
    if len(top_syms) < 20:
        top_syms.extend(top_symbols(20))
        
    # 2. استدعاء العقل المركزي لتحليل هذه العملات
    results = scan_sync(list(set(top_syms)))
    
    longs, shorts = [], []
    
    # 3. الفرز بصرامة بناءً على اتجاه العقل المركزي فقط
    for r in results:
        if r["grade"] not in ("A+", "A", "B"): continue
        if r["sym"] in EXCLUDE_TICKERS: continue
        
        # البحث عن البيانات السياقية
        mv = next((x for x in all_movers if x["symbol"] == r["sym"]), {"change": 0, "volume": 0})
        item = dict(res=r, change=mv["change"], volume=mv["volume"])
        
        if r["side"] == 1 and len(longs) < 2:
            longs.append(item)
        elif r["side"] == -1 and len(shorts) < 2:
            shorts.append(item)
            
    # 4. إذا لم نجد 2 شورت حقيقيات، نبحث في بقية السوق (بدون إجبار)
    if len(longs) < 2 or len(shorts) < 2:
        more_syms = top_symbols(60)
        more_res = scan_sync(more_syms)
        for r in more_res:
            if r["grade"] not in ("A+", "A", "B"): continue
            if r["sym"] in EXCLUDE_TICKERS: continue
            if any(x["res"]["sym"] == r["sym"] for x in longs + shorts): continue
            
            item = dict(res=r, change=0, volume=0)
            if r["side"] == 1 and len(longs) < 2:
                longs.append(item)
            elif r["side"] == -1 and len(shorts) < 2:
                shorts.append(item)
                
    return longs[:2], shorts[:2]

def _format_daily_card(res, side_label, change, volume):
    lv = res["lv"]; decision = "🟢 شراء" if res["side"] == 1 else "🔴 بيع"
    cl = coin_link(res['sym'], res.get("source", "binance_fut"))
    lines = [side_label, "",
             f"📌 <a href='{cl}'>#{res['sym']}/USDT</a> 👆 اضغط هنا",
             f"📡 {display_source(res.get('source','binance_fut'))}",
             f"➡️ Entry: {fmt(lv['entry'])}"]
    for i, tp in enumerate(lv["tps"][:DAILY_TP_COUNT], 1):
        lines.append(f"🎯 Target {i}: {fmt(tp['px'])}")
    lines += [f"🛑 Stop loss: {fmt(lv['sl'])}", "",
              f"🎯 <b>القرار المتوقع: {decision}</b>",
              f"📊 {'🔺' if change >= 0 else '🔻'} {change:+.1f}% | 💰 ${volume/1_000_000:.1f}M"]
    return "\n".join(lines)

# ==========================================
# 🛠️ الإصلاح الجذري للفرصة الذهبية (Golden Fix)
# ==========================================
def golden_score(sym, debug=False, relaxed=False):
    try:
        # الاعتماد على العقل المركزي أولاً
        res = get_analysis(sym)
        if not res: return None
        
        # الدستور: يجب أن تكون الدرجة A+ أو A
        if res["grade"] not in ("A+", "A"):
            if debug: return dict(sym=sym, score=0, reasons=[f"الدرجة {res['grade']} غير كافية"])
            return None
            
        side = res["side"]
        price = res["price"]
        atr = res["lv"]["atr"]
        
        # الأهداف الدستورية: 5% / 10% / 15% / 25%
        if side == 1:
            sl = price * 0.95
            tps = [price * 1.05, price * 1.10, price * 1.15, price * 1.25]
        else:
            sl = price * 1.05
            tps = [price * 0.95, price * 0.90, price * 0.85, price * 0.75]
            
        return dict(
            sym=sym, side=side, score=100 if res["grade"] == "A+" else 80,
            reasons=[f"العقل المركزي: {res['grade']}", f"اتجاه: {'صاعد' if res['trend'] in ('up','strong_up') else 'هابط'}"],
            price=price, atr=atr, sl=sl, tps=tps,
            risk=abs(price - sl), trend_1d=res["trend_d"], trend_4h=res["trend"],
            change_7d=0, rsi=res["rsi"], rvol=1.0, source=res.get("source", "binance_fut")
        )
    except Exception as e:
        log.error(f"golden_score err {sym}: {e}")
        return None

def _run_golden_scan(syms, max_results, progress_cb, relaxed=False):
    results = []; total = len(syms)
    def scan_one(sym):
        try: return golden_score(sym, debug=False, relaxed=relaxed)
        except Exception: return None
    processed = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        futures = {executor.submit(scan_one, s): s for s in syms}
        for future in concurrent.futures.as_completed(futures):
            processed += 1
            res = future.result()
            if res: results.append(res)
            if progress_cb and processed % 50 == 0:
                try: progress_cb(processed, total, len(results))
                except Exception: pass
    results.sort(key=lambda x: -x["score"])
    return results[:max_results]

def golden_scan(max_results=2, progress_cb=None):
    try:
        rows = binance_futures_all_tickers()
        if rows:
            rows_sorted = sorted(rows, key=lambda x: -x["volume"])
            syms = [x["symbol"] for x in rows_sorted if x["volume"] > 2_000_000 and x["symbol"] not in EXCLUDE_TICKERS][:600]
        else:
            syms = top_symbols(600, binance_only=True)
    except Exception:
        syms = top_symbols(600, binance_only=True)
    if not syms: return []
    results = _run_golden_scan(syms, max_results, progress_cb, relaxed=False)
    if not results:
        log.info("golden_scan: no strict results, trying relaxed mode on top 200")
        results = _run_golden_scan(syms[:200], max_results, progress_cb, relaxed=True)
    return results

def format_golden_post(g, tier="vip"):
    side_word = "🟢 شراء (LONG)" if g["side"] == 1 else "🔴 بيع (SHORT)"
    decision = "🟢 شراء" if g["side"] == 1 else "🔴 بيع"
    cl = coin_link(g['sym'], g.get("source", "binance_fut"))
    lines = [
        "🏆✨ <b>الفرصة الذهبية</b> ✨🏆",
        "━━━━━━━━━━━━━━━━━━━━━",
        f"📌 <a href='{cl}'>#{g['sym']}/USDT</a> 👆 اضغط هنا",
        f"📡 المصدر: <b>{display_source(g.get('source','binance_fut'))}</b>",
        f"🏆 التقييم: <b>{g['score']} نقطة</b>",
        "━━━━━━━━━━━━━━━━━━━━━", "",
        f"🎯 الاتجاه: <b>{side_word}</b>",
        f"💵 الدخول: <code>{fmt(g['price'])}</code>",
        f"🛑 وقف الخسارة: <code>{fmt(g['sl'])}</code>", "",
        "🎯 <b>الأهداف (الأسعار):</b>",
    ]
    for i, tp in enumerate(g["tps"], 1):
        emoji = ["🥇", "🥈", "🥉", "🏅"][i-1]
        lines.append(f"{emoji} هدف {i}: <code>{fmt(tp)}</code>")
    lines += ["", "━━━━━━━━━━━━━━━━━━━━━", f"🎯 <b>القرار المتوقع: {decision}</b>",
              "", "🔍 <b>لماذا الفرصة الذهبية؟</b>"]
    for r in g["reasons"][:8]: lines.append(f"  • {r}")
    lines += ["", "━━━━━━━━━━━━━━━━━━━━━", RISK_WARNING, f"👤 {BRAND}"]
    return "\n".join(lines)

def build_whale_tweet(sym, side, notional, price):
    side_emoji = "🟢 شراء" if side == "buy" else "🔴 بيع"
    name, ref = referral_for("binance_fut")
    text = (
        f"🚨 عاجل: رصد حوت في السوق! 🐋\n\n"
        f"💰 صفقة {side_emoji} ضخمة على #{sym}/USDT\n"
        f"💵 بقيمة: ${notional:,.0f}\n"
        f"📊 السعر: {fmt(price)}\n\n"
        f"⚡️ تحرك الحيتان يعني حركة قوية قادمة!\n"
        f"🔗 للتداول اللحظي وسحب أرباحك من حركة الحوت:\n"
        f"{ref}\n\n"
        f"👤 Ryma Crypto"
    )
    share_url = f"https://twitter.com/intent/tweet?text={urllib.parse.quote(text)}"
    return text, share_url

def build_social_text(res, platform="binance"):
    side = res["side"]; lv = res["lv"]; sym = res["sym"]
    side_word = "LONG 🟢" if side == 1 else "SHORT 🔴"
    meta = PLATFORM_META.get(platform, PLATFORM_META["binance"])
    emoji = meta["emoji"]
    
    sym_u = sym.upper()
    if platform == "binance":
        trade_link = f"https://www.binance.com/futures/{sym_u}USDT?ref=WELCOMBAKC"
    elif platform == "bybit":
        trade_link = f"https://www.bybit.com/trade/usdt/{sym_u}USDT"
    elif platform == "okx":
        trade_link = f"https://www.okx.com/trade-swap/{sym.lower()}-usdt-swap"
    elif platform == "bitget":
        trade_link = f"https://www.bitget.com/futures/usdt/{sym_u}USDT"
    elif platform == "kucoin":
        trade_link = f"https://www.kucoin.com/futures/trade/{sym_u}USDTM"
    elif platform == "gateio":
        trade_link = f"https://www.gate.com/futures/USDT/{sym_u}_USDT"
    else:
        trade_link = coin_link(sym, res.get("source", "binance_fut"))

    # هاشتاجين فقط
    coins = hot_coins_for(sym)
    other_coin = next((c for c in coins if c != sym), "BTC")
    hashtags = f"#{sym} #{other_coin}"

    if platform == "twitter":
        text = (
            f"{emoji} #{sym}/USDT {side_word}\n"
            f"💵 {fmt(lv['entry'])}\n"
            f"🎯 {fmt(lv['tps'][0]['px'])} | {fmt(lv['tps'][1]['px'])} | {fmt(lv['tps'][2]['px'])}\n"
            f"🛑 {fmt(lv['sl'])}\n\n"
            f"👉 Trade here: {trade_link}\n\n"
            f"{hashtags}\n"
            f"⚠️ High risk / NFA"
        )
    else:
        text = (
            f"{emoji} #{sym}/USDT  {side_word}\n"
            f"💵 Entry: {fmt(lv['entry'])}\n"
            f"🎯 TP1: {fmt(lv['tps'][0]['px'])}\n"
            f"🎯 TP2: {fmt(lv['tps'][1]['px'])}\n"
            f"🎯 TP3: {fmt(lv['tps'][2]['px'])}\n"
            f"🎯 TP4: {fmt(lv['tps'][3]['px'])}\n"
            f"🛑 SL: {fmt(lv['sl'])}\n\n"
            f"👉 Trade here: {trade_link}\n\n"
            f"👍 Like | 🔁 Share | 👤 Follow\n\n"
            f"{hashtags}\n\n"
            f"⚠️ High risk / Not financial advice"
        )
    return text

def keyboard_share_post(res):
    sym = res["sym"]
    
    platform_urls = {
        "binance": "https://www.binance.com/en/square/post/create",
        "bybit":   "https://www.bybit.com/en/copy-trading/",
        "okx":     "https://www.okx.com/feed",
        "bitget":  "https://www.bitget.com/en/copy-trading",
        "kucoin":  "https://www.kucoin.com/copy-trade",
        "gateio":  "https://www.gate.io/copy-trading",
    }
    
    trade_meta = [
        ("binance", "🟡", "Binance"),
        ("bybit",   "🔵", "Bybit"),
        ("okx",     "⚫", "OKX"),
        ("bitget",  "🟢", "Bitget"),
        ("kucoin",  "🟢", "KuCoin"),
        ("gateio",  "⚪", "Gate"),
    ]
    
    trade_rows = []
    for key, emoji, name in trade_meta:
        # نص مختصر جداً لزر النسخ (أقل من 256 حرف)
        short_text = (
            f"#{sym} {'LONG🟢' if res['side']==1 else 'SHORT🔴'} | 💵 {fmt(res['lv']['entry'])} | "
            f"🎯 {fmt(res['lv']['tps'][0]['px'])} | {fmt(res['lv']['tps'][1]['px'])} | {fmt(res['lv']['tps'][2]['px'])} | "
            f"🛑 {fmt(res['lv']['sl'])} | 👉 {coin_link(sym, key)}"
        )
        trade_rows.append([
            InlineKeyboardButton(f"{emoji} {name} — افتح", url=platform_urls[key]),
            InlineKeyboardButton(f"📋 نسخ لـ {name}", copy_text=CopyTextButton(text=short_text)),
        ])

    texts = {p: build_social_text(res, p) for p in PLATFORM_META}
    ref = REFERRAL_LINKS["binance"][1]
    tw_url = f"https://twitter.com/intent/tweet?text={urllib.parse.quote(texts['twitter'])}"
    tg_url = f"https://t.me/share/url?url={urllib.parse.quote(ref)}&text={urllib.parse.quote(texts['telegram'])}"
    fb_url = f"https://www.facebook.com/sharer/sharer.php?u={urllib.parse.quote(ref)}&quote={urllib.parse.quote(texts['facebook'])}"
    wa_url = f"https://wa.me/?text={urllib.parse.quote(texts['whatsapp'])}"
    rd_url = f"https://www.reddit.com/submit?title={urllib.parse.quote(f'#{sym}/USDT Signal')}&text={urllib.parse.quote(texts['reddit'])}"

    social_rows = [
        [InlineKeyboardButton("🐦 Twitter (X) — جاهز", url=tw_url),
         InlineKeyboardButton("📱 Telegram — جاهز", url=tg_url)],
        [InlineKeyboardButton("📘 Facebook — جاهز", url=fb_url),
         InlineKeyboardButton("💬 WhatsApp — جاهز", url=wa_url)],
        [InlineKeyboardButton("🔴 Reddit — جاهز", url=rd_url),
         InlineKeyboardButton("🎮 Discord (نسخ)", copy_text=CopyTextButton(text=texts["discord"][:250]))],
    ]

    return InlineKeyboardMarkup(
        social_rows + trade_rows + [
            [InlineKeyboardButton("❌ إغلاق", callback_data="close_menu")],
        ]
    )

async def notify_both(bot, text, protect=True):
    for chat in (VIP_CHANNEL_ID, ADMIN_ID or None):
        if not chat: continue
        try:
            await bot.send_message(chat, text, parse_mode=ParseMode.HTML,
                                   protect_content=PROTECT if chat != ADMIN_ID else False,
                                   link_preview_options=NO_PREVIEW)
        except Exception as e:
            log.warning(f"notify_both {chat}: {str(e)[:80]}")

async def notify_free(bot, text):
    if CHANNEL_ID:
        try:
            await bot.send_message(CHANNEL_ID, text, parse_mode=ParseMode.HTML,
                                   protect_content=PROTECT, link_preview_options=NO_PREVIEW)
        except Exception as e:
            log.warning(f"notify_free: {str(e)[:80]}")

def render_chart(res, tier="free", published=False):
    df = res["df"].tail(CHART_BARS).reset_index(drop=True)
    lv, side, tf = res["lv"], res["side"], res["tf"]
    n_tps = 4 if tier in ("admin","vip") else 2
    bg, fg = "#ffffff", "#111111"; navy = "#1e3a8a"; blue = "#1565c0"
    fig = plt.figure(figsize=(11,7.5), facecolor=bg)
    gs = fig.add_gridspec(2,1, height_ratios=[4.2,1], hspace=0.06)
    ax = fig.add_subplot(gs[0], facecolor=bg); axr = fig.add_subplot(gs[1], facecolor=bg, sharex=ax)
    x = np.arange(len(df))
    ax.fill_between(x, df["bb_lo"], df["bb_up"], color=navy, alpha=0.25)
    ax.plot(x, df["bb_up"], color=navy, lw=1.5, ls="--")
    ax.plot(x, df["bb_mid"], color=navy, lw=1.1)
    ax.plot(x, df["bb_lo"], color=navy, lw=1.5, ls="--")
    if tier in ("admin","vip"):
        ax.plot(x, df["ema20"], color="#f5c518", lw=1.3)
        ax.plot(x, df["ema50"], color="#1976d2", lw=1.3)
        ax.plot(x, df["ema200"], color="#7b1fa2", lw=1.3)
    ax.plot(x, df["close"], color=blue, lw=2.2)
    levels = [(lv["entry"], "دخول", "#0d47a1", "-."), (lv["sl"], "وقف", "#d32f2f", "--")]
    for j,tp in enumerate(lv["tps"][:n_tps], 1):
        levels.append((tp["px"], f"هدف{j}", "#2e7d32", "--"))
    xe = len(df)-1
    for p_, name, col, ls in levels:
        ax.axhline(p_, color=col, lw=1.1, ls=ls)
        ax.text(xe+0.6, p_, f" {name} {fmt(p_)}", color=col, fontsize=8, va="center",
                fontweight="bold", bbox=dict(boxstyle="round,pad=0.15", fc=bg, ec=col, lw=0.6))
    lo = min([float(df["low"].min())]+[v[0] for v in levels])
    hi = max([float(df["high"].max())]+[v[0] for v in levels])
    pad = (hi-lo)*0.05
    ax.set_ylim(lo-pad, hi+pad); ax.set_xlim(-1, len(df)+17)
    axr.plot(x, df["rsi"], color="#7b1fa2", lw=1.3)
    axr.axhline(70, color="#ef5350", lw=0.7, ls="--"); axr.axhline(30, color="#26a69a", lw=0.7, ls="--")
    axr.fill_between(x, 30, 70, color="#f8bbd0", alpha=0.15); axr.set_ylim(10, 90)
    for a_ in (ax, axr):
        a_.tick_params(colors=fg, labelsize=8); a_.grid(color="#e8e8e8", lw=0.6)
        for sp in a_.spines.values(): sp.set_color("#888888")
        a_.yaxis.tick_right()
    plt.setp(ax.get_xticklabels(), visible=False)
    step = max(len(df)//6, 1); ts = pd.to_datetime(df["t"], unit="ms")
    axr.set_xticks(range(0, len(df), step))
    axr.set_xticklabels([ts.iloc[i].strftime("%Y-%m-%d") for i in range(0,len(df),step)], fontsize=8)
    col = "#26a69a" if side==1 else "#ef5350"
    ax.set_title(f"{res['sym']}/USDT  -  {tf.upper()}  -  {'شراء' if side==1 else 'بيع'}",
                 color=col, fontsize=13, fontweight="bold", loc="left")
    fig.text(0.5, 0.5, "Ryma Crypto", fontsize=58, color=navy, alpha=0.12, ha="center", va="center",
             fontweight="bold", rotation=25)
    fig.text(0.985, 0.012, "Ryma Crypto", fontsize=10, color="#c9a227", ha="right", va="bottom", fontweight="bold")
    buf = io.BytesIO(); fig.savefig(buf, format="png", dpi=130, facecolor=bg, bbox_inches="tight")
    plt.close(fig); buf.seek(0); return buf

def caption_core(res, tier="free"):
    side, lv, sym = res["side"], res["lv"], res["sym"]
    side_word = "🟢 شراء (LONG)" if side == 1 else "🔴 بيع (SHORT)"
    decision = "🟢 شراء" if side == 1 else "🔴 بيع"
    now_ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
    source = display_source(res.get("source", "binance_fut"))
    cl = coin_link(sym, res.get("source", "binance_fut"))
    grade = res.get("grade", "?")
    lines = [
        "╭━━━ 💎 RYMA CRYPTO 💎 ━━━╮",
        f"┃ 🎯 التوصية: {side_word}",
        f"┃ 📌 <a href='{cl}'>#{sym}/USDT</a> 👆 اضغط هنا",
        f"┃ 📡 المصدر: {source}",
        f"┃ 🏆 الدرجة: {grade}",
        "╰━━━━━━━━━━━━━━━━━━━━━━━╯", "",
        f"💵 الدخول: {fmt(lv['entry'])}",
        f"🛑 وقف الخسارة: {fmt(lv['sl'])}", "",
    ]
    n_tps = 4 if tier in ("admin","vip") else 2
    for i,tp in enumerate(lv["tps"][:n_tps], 1):
        emoji = "🎯" if i <= 2 else "🏆"
        lines.append(f"{emoji} الهدف {i}: {fmt(tp['px'])}")
    if tier == "free" and len(lv["tps"]) > 2:
        lines += ["", "💎 للأهداف الإضافية → VIP"]
    lines += ["", "━━━━━━━━━━━━━━━━", f"🎯 <b>القرار المتوقع: {decision}</b>"]
    lines += ["", "╔═══════ ⚠️ تنبيهات ═══════╗",
              "║ 📌 ليست كل صفقاتنا ناجحة",
              "║ 📌 إدارة المخاطر سر النجاح",
              "║ 📌 إدارة حجم الصفقة مطلوبة",
              "║ 📌 إجراء بحث خاص قبل الدخول",
              "╚═══════════════════════════╝",
              "", f"📅 {now_ts}", f"👤 {BRAND}"]
    return "\n".join(lines)

def caption_free(res): return caption_core(res, "free")
def caption_vip(res): return caption_core(res, "vip")

def build_copy_post(res):
    side, lv, sym = res["side"], res["lv"], res["sym"]
    side_icon = "🟢" if side == 1 else "🔴"
    side_word = "LONG" if side == 1 else "SHORT"
    lines = [f"#{sym}/USDT {side_icon} {side_word}", "", f"💵 {fmt(lv['entry'])}"]
    for i, tp in enumerate(lv["tps"][:3], 1):
        lines.append(f"🎯 TP{i}: {fmt(tp['px'])}")
    lines.append(f"🛑 SL: {fmt(lv['sl'])}")
    return "\n".join(lines)

def keyboard_free(sym, tf, source="binance_fut"):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🌐 جميع منصات التداول", callback_data=f"refs:{source}")],
        [InlineKeyboardButton("💎 اشترك في VIP ✨", url=VIP_INVITE_LINK)],
        [InlineKeyboardButton("🆓 قناة التوصيات المجانية", url=CHANNEL_LINK)],
        [InlineKeyboardButton("📞 تواصل معنا", url=f"https://t.me/{CONTACT_LINK.lstrip('@')}")],
        [InlineKeyboardButton("🔄 تحديث", callback_data=f"tf:{sym}:{tf}")],
    ])

def keyboard_vip(sym, tf, source="binance_fut"):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🌐 جميع المنصات", callback_data=f"refs:{source}")],
        [InlineKeyboardButton("📋 القائمة", callback_data="vip_menu")],
        [InlineKeyboardButton("🔄 تحديث", callback_data=f"tf:{sym}:{tf}")],
    ])

def keyboard_admin(sym, tf, source="binance_fut"):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🌐 جميع المنصات", callback_data=f"refs:{source}")],
        [InlineKeyboardButton("📊 المؤشرات", callback_data=f"ind:{sym}:{tf}"),
         InlineKeyboardButton("💧 السيولة", callback_data=f"liq:{sym}")],
        [InlineKeyboardButton("🐋 الحيتان", callback_data=f"whale:{sym}"),
         InlineKeyboardButton("🧪 Backtest", callback_data=f"btcb:{sym}")],
        [InlineKeyboardButton("📋 قائمة الأوامر الكاملة", callback_data="admin_menu")],
        [InlineKeyboardButton("🎯 صفقة اليوم", callback_data="adm:/daily")],
        [InlineKeyboardButton("🔄 تحديث", callback_data=f"tf:{sym}:{tf}")],
    ])

def keyboard(sym, tf, tier, source="binance_fut"):
    if tier == "admin": return keyboard_admin(sym, tf, source)
    if tier == "vip": return keyboard_vip(sym, tf, source)
    return keyboard_free(sym, tf, source)

def keyboard_refs(current="binance_fut"):
    cur = (current or "binance").lower()
    if cur.startswith("binance"): cur = "binance"
    rows = []
    name, url = REFERRAL_LINKS["binance"]
    rows.append([InlineKeyboardButton(f"🚀 {name} Futures (رسوم أقل)", url=url)])
    for key, (name, url) in REFERRAL_LINKS.items():
        if key == "binance": continue
        label = f"✅ {name}" if key == cur else f"🔗 {name}"
        rows.append([InlineKeyboardButton(label, url=url)])
    rows.append([InlineKeyboardButton("❌ إغلاق", callback_data="close_menu")])
    return InlineKeyboardMarkup(rows)

def keyboard_golden_config():
    cfg = get_golden_config()
    def ic(b): return "✅" if b else "⬜"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"{ic(cfg.get('require_1d_trend',False))} اتجاه 1D", callback_data="gc:require_1d_trend")],
        [InlineKeyboardButton(f"{ic(cfg.get('require_mtf',False))} توافق MTF", callback_data="gc:require_mtf")],
        [InlineKeyboardButton(f"{ic(cfg.get('require_rsi',True))} RSI صحي", callback_data="gc:require_rsi")],
        [InlineKeyboardButton(f"{ic(cfg.get('require_volume',False))} حجم قوي", callback_data="gc:require_volume")],
        [InlineKeyboardButton(f"{ic(cfg.get('require_breakout',False))} اختراق/كسر", callback_data="gc:require_breakout")],
        [InlineKeyboardButton(f"{ic(cfg.get('require_liquidity',False))} سيولة >1M$", callback_data="gc:require_liquidity")],
        [InlineKeyboardButton(f"{ic(cfg.get('require_alpha',False))} عملة Alpha فقط", callback_data="gc:require_alpha")],
        [InlineKeyboardButton(f"📊 الحد الأدنى للنقاط: {cfg.get('min_score',1)}", callback_data="gc:min_score")],
        [InlineKeyboardButton("🔄 تحديث", callback_data="gc:refresh")],
        [InlineKeyboardButton("❌ إغلاق", callback_data="close_menu")],
    ])

def build_golden_config_text():
    cfg = get_golden_config()
    return (
        "💎✨ <b>إعدادات الفرصة الذهبية</b> ✨💎\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🔧 اضغط على أي شرط لتفعيله/تعطيله\n\n"
        "✅ = مطلوب اجتيازه\n"
        "⬜ = غير مطلوب\n\n"
        f"📊 الحد الأدنى للنقاط: <b>{cfg.get('min_score',1)}</b>\n"
        f"⏱ الدورة: كل 5 ساعات\n"
        f"🔄 وضع Relaxed تلقائي عند عدم وجود نتائج\n"
        f"📌 ستظهر الفرصة عند تحقيق الشروط المفعّلة"
    )

def keyboard_vip_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💎✨ الفرصة الذهبية", callback_data="vip_cmd:/golden")],
        [InlineKeyboardButton("🎯 صفقة اليوم", callback_data="vip_cmd:/daily")],
        [InlineKeyboardButton("🔮 التنبؤ بالانفجارات", callback_data="vip_cmd:/predict")],
        [InlineKeyboardButton("🐋 رادار الحيتان", callback_data="vip_cmd:/whales")],
        [InlineKeyboardButton("💥 رادار الانفجارات", callback_data="vip_cmd:/explosions")],
        [InlineKeyboardButton("🚀 عملات صاعدة", callback_data="vip_cmd:/pump")],
        [InlineKeyboardButton("📉 رادار القيعان الذكي", callback_data="vip_cmd:/bottom")],
        [InlineKeyboardButton("🔻 رادار فرص الشورت", callback_data="vip_cmd:/shortsradar")],
        [InlineKeyboardButton("⭐ رادار Alpha الأسبوعي", callback_data="vip_cmd:/alpha")],
        [InlineKeyboardButton("💧 رادار السيولة", callback_data="vip_cmd_args:liquidity")],
        [InlineKeyboardButton("📊 بيانات الفيوتشر", callback_data="vip_cmd_args:futures")],
        [InlineKeyboardButton("📈 الصفقات النشطة", callback_data="vip_cmd:/active")],
        [InlineKeyboardButton("🌐 منصات التداول", callback_data="refs:binance_fut")],
        [InlineKeyboardButton("🆓 قناة التوصيات المجانية", url=CHANNEL_LINK)],
        [InlineKeyboardButton("❌ إغلاق القائمة", callback_data="close_menu")],
    ])

def build_vip_menu_text():
    return ("╭━━━ 💎 قائمة VIP الكاملة 💎 ━━━╮\n"
            "┃ 🔥 كل الخدمات متاحة 🔥\n"
            "╰━━━━━━━━━━━━━━━━━━━━━━━━━━╯\n\n"
            "🎯 الأزرار متاحة للوصول للخدمة مباشرة\n\n"
            "📌 ملاحظات:\n"
            "• 💧 السيولة و 📊 الفيوتشر يتطلبان كتابة الرمز\n"
            "• 📈 الصفقات النشطة تعرض الصفقات الحالية\n"
            "• 📢 القناة المجانية للتنبيهات العامة\n\n"
            "╰━━━━━━━━━━━━━━━━━━━━━━━━━━╯")

def build_vip_page():
    return ("╭━━━ 💎 RYMA CRYPTO VIP 💎 ━━━╮\n"
            "┃ 🔥 حصري · جودة · أرباح 🔥\n"
            "╰━━━━━━━━━━━━━━━━━━━━━━━━━━╯\n\n"
            "✨ لماذا VIP؟\n\n"
            "🔥 💎✨ الفرصة الذهبية\n"
            "🔥 🎯 صفقة اليوم\n"
            "🔥 🎯 4 أهداف كاملة (بدل 2)\n"
            "🔥 📡 تتبع لحظي لـ TP + SL\n"
            "🔥 ⚡ تنبيهات فورية\n"
            "🔥 🔮 رادار تنبؤي للانفجارات\n"
            "🔥 🐋 رادار الحيتان\n"
            "🔥 💧 رادار السيولة\n"
            "🔥 💥 رادار الانفجارات\n"
            "🔥 📉 رادار القيعان\n"
            "🔥 🔻 رادار الفرص الشورت\n"
            "🔥 ⭐ رادار عملات Alpha\n"
            "🔥 🌟 تنبيهات التقاطع الذهبي\n\n"
            "💎 3 باقات VIP متاحة\n"
            "💰 الأسعار عند الطلب\n\n"
            f"📞 للاستفسار والاشتراك: {CONTACT_LINK}\n\n"
            "👇 للانضمام")

def build_admin_menu_text():
    return "👑 <b>قائمة أوامر الأدمن الكاملة</b>\n\nالأزرار متاحة للتنفيذ المباشر.\n"

def keyboard_admin_menu():
    multi_icon = "✅" if is_multi_exchange() else "❌"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⚡ تشغيل النشر", callback_data="adm:/forcepost"),
         InlineKeyboardButton("🧹 تفريغ الصفقات", callback_data="adm:/clearsignals")],
        [InlineKeyboardButton("🛠 وضع الصيانة", callback_data="toggle_maintenance"),
         InlineKeyboardButton(f"🌐 تعدد المنصات: {multi_icon}", callback_data="toggle_multi_exchange")],
        [InlineKeyboardButton("💎 إعدادات الذهبية", callback_data="adm:/goldenconfig")],
        [InlineKeyboardButton("📤 مشاركة صفقة اجتماعياً", callback_data="adm_args:share")],
        [InlineKeyboardButton("💎✨ الفرصة الذهبية", callback_data="adm:/golden"),
         InlineKeyboardButton("🎯 صفقة اليوم", callback_data="adm:/daily")],
        [InlineKeyboardButton("🌟 التقاطعات", callback_data="adm:/cross"),
         InlineKeyboardButton("🔍 تشخيص", callback_data="adm:/debug")],
        [InlineKeyboardButton("📊 فحص السوق", callback_data="adm:/scan"),
         InlineKeyboardButton("🔻 فرص شورت", callback_data="adm:/short")],
        [InlineKeyboardButton("🔮 التنبؤ", callback_data="adm:/predict"),
         InlineKeyboardButton("🐋 الحيتان", callback_data="adm:/whales")],
        [InlineKeyboardButton("💥 الانفجارات", callback_data="adm:/explosions"),
         InlineKeyboardButton("🚀 الصعود", callback_data="adm:/pump")],
        [InlineKeyboardButton("📉 القيعان", callback_data="adm:/bottom"),
         InlineKeyboardButton("🔻 شورت رادار", callback_data="adm:/shortsradar")],
        [InlineKeyboardButton("⭐ Alpha", callback_data="adm:/alpha"),
         InlineKeyboardButton("🧪 Backtest", callback_data="adm:/bt")],
        [InlineKeyboardButton("🧪 Backtest TOP50", callback_data="adm:/bt50"),
         InlineKeyboardButton("👥 المستخدمون", callback_data="adm:/users")],
        [InlineKeyboardButton("💎 قائمة VIP", callback_data="adm:/viplist"),
         InlineKeyboardButton("📈 إحصائيات", callback_data="adm:/stats")],
        [InlineKeyboardButton("🗂 السجل", callback_data="adm:/history"),
         InlineKeyboardButton("🎛 لوحة التحكم", callback_data="adm:/dashboard")],
        [InlineKeyboardButton("📢 اختبار القنوات", callback_data="adm:/testchannels"),
         InlineKeyboardButton("🗑 قائمة الحذف", callback_data="adm:/delist")],
        [InlineKeyboardButton("💧 السيولة", callback_data="adm_args:liquidity"),
         InlineKeyboardButton("📊 فيوتشر", callback_data="adm_args:futures")],
        [InlineKeyboardButton("💲 أسعار", callback_data="adm_args:price"),
         InlineKeyboardButton("🔎 لماذا", callback_data="adm_args:why")],
        [InlineKeyboardButton("📝 نشر توصية", callback_data="adm_args:post"),
         InlineKeyboardButton("➕ إضافة VIP", callback_data="adm_args:addvip")],
        [InlineKeyboardButton("➖ إزالة VIP", callback_data="adm_args:removevip"),
         InlineKeyboardButton("❌ إغلاق القائمة", callback_data="close_menu")],
    ])

def build_admin_extras(res):
    trend_ar = {"strong_up":"صاعد قوي","up":"صاعد","strong_down":"هابط قوي","down":"هابط",
                "range":"عرضي","unknown":"—"}.get(res.get("trend",""), "—")
    lines = [f"📊 المؤشرات - {res['sym']}/{res['tf']}", "",
             f"📈 RSI: {res['rsi']:.1f}", f"📊 ATR: {res['lv']['atr']:.4f}",
             f"📉 MACD: {res['macd']:.4f}", f"🎯 الاستراتيجية: {res['strategy']}",
             f"🏆 الدرجة: {res['grade']}", f"📊 اتجاه: {trend_ar}"]
    if res.get("candle"): lines.append(f"🕯 الشمعة: {res['candle']}")
    lines.append("")
    for i,tp in enumerate(res['lv']['tps'], 1):
        lines.append(f"🎯 هدف {i}: {tp['pct']}%")
    return "\n".join(lines)

def active_signals():
    with store.lock:
        return [p for p in store.data["signals"] if p.get("state", "open") in ("pending","open")]

def can_track(res):
    if res["sym"] in WEAK_COINS: return False
    act = active_signals()
    if len(act) >= MAX_OPEN: return False
    if any(p["coin"]==res["sym"] and p["tf"]==res["tf"] for p in act): return False
    return True

def track_signal(res):
    sz = RISK_SIZE.get(res["grade"], 0.02)
    p = new_trade(res["sym"], res["tf"], res["side"], res["lv"], res["bar_t"],
                  res["grade"], res["setup"], size_pct=sz)
    p["source"] = res.get("source", "binance_fut")
    with store.lock:
        store.data["signals"].append(p); store.save("signals")
    return p

async def publish(bot, res, vip=True, free=False, admin_copy=False):
    sent = []; copy_text = build_copy_post(res); source = res.get("source", "binance_fut")
    try:
        if vip and VIP_CHANNEL_ID:
            img = await asyncio.to_thread(render_chart, res, "vip", True)
            await bot.send_photo(VIP_CHANNEL_ID, img, caption=caption_vip(res),
                                 parse_mode=ParseMode.HTML, protect_content=PROTECT,
                                 reply_markup=keyboard_vip(res["sym"], res["tf"], source))
            sent.append("VIP")
        if free and CHANNEL_ID:
            img = await asyncio.to_thread(render_chart, res, "free", True)
            await bot.send_photo(CHANNEL_ID, img, caption=caption_free(res),
                                 parse_mode=ParseMode.HTML, protect_content=PROTECT,
                                 reply_markup=keyboard_free(res["sym"], res["tf"], source))
            sent.append("FREE")
        if admin_copy and ADMIN_ID:
            try: await bot.send_message(ADMIN_ID, copy_text)
            except Exception: pass
    except Exception as e:
        sent.append(f"err:{str(e)[:40]}")
    return sent

def build_market_summary(symbols):
    now_ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = ["📊 تنبيهات السوق", f"🕐 {now_ts}", "━━━━━━━━━━━━━━━━", ""]
    ok_count = 0
    for s in symbols:
        try:
            price, ch24, ch7 = ticker_change(s)
            if price <= 0: continue
            lines.append(f"💠 {s} — {fmt(price)}")
            lines.append(f"{'🔺' if ch24 >= 0 else '🔻'} 24h: {ch24:+.2f}%")
            lines.append(f"{'🔺' if ch7 >= 0 else '🔻'} 7d: {ch7:+.2f}%")
            lines.append(""); ok_count += 1
        except Exception: continue
    if ok_count == 0: return None
    lines.append(f"📣 {CHANNEL_LINK}")
    return "\n".join(lines)

def pick_short_opportunities(n=5):
    syms = top_symbols(50); rows = []
    for s in syms[:40]:
        r = short_radar(s)
        if r: rows.append((s, r))
    rows.sort(key=lambda x: -x[1]["rsi"])
    return rows[:n]

_free_day = {"d":"","n":0}

async def run_autopost(bot):
    log.info("run_autopost: بدء المسح")
    syms = await asyncio.to_thread(top_symbols, 60)
    results = await asyncio.to_thread(scan_sync, syms[:40])
    cands = [r for r in results if r["rank"] >= 0 and r["sym"] not in WEAK_COINS and r["sym"] not in EXCLUDE_TICKERS]
    cands.sort(key=lambda r: (-r["rank"], -r.get("ok",0)))
    today = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d")
    if _free_day["d"] != today: _free_day.update(d=today, n=0)
    a_candidates = [r for r in cands if r["grade"] in ("A+","A")]
    a_candidates.sort(key=lambda r: (-r.get("fut_score", 0), -r["rank"]))
    b_candidates = [r for r in cands if r["grade"] == "B"]
    vip_pool = a_candidates if a_candidates else b_candidates
    if len(active_signals()) >= MAX_OPEN:
        log.warning(f"MAX_OPEN {len(active_signals())}/{MAX_OPEN}"); return dict(vip=0,free=0,total=0)
    vip_n = free_n = 0; used = set()
    for r in vip_pool:
        if vip_n >= VIP_POSTS_PER_CYCLE: break
        key = f"{r['sym']}:{r['tf']}:{r['bar_t']}"
        if key in store.data["posted"]: continue
        if len(active_signals()) >= MAX_OPEN: break
        if any(p["coin"]==r["sym"] and p["tf"]==r["tf"] for p in active_signals()): continue
        vip_liq_ok = check_liquidity(r["sym"], min_usd=VIP_MIN_LIQUIDITY)
        vip_mtf_ok = True
        if REQUIRE_MTF_FOR_SMALL and r["sym"] not in MAJORS and r["sym"] not in MEME_COINS:
            mtf_c, mtf_t = mtf_score(r["sym"], r["side"])
            if mtf_t >= 2 and mtf_c < mtf_t: vip_mtf_ok = False
        if not (vip_liq_ok and vip_mtf_ok): continue
        try: sent = await publish(bot, r, vip=True, free=False, admin_copy=True)
        except Exception: continue
        if sent and not any(s.startswith("err") for s in sent):
            track_signal(r)
            with store.lock:
                store.data["posted"][key] = now_s()
                cut = now_s() - 14*DAY
                store.data["posted"] = {k:v for k,v in store.data["posted"].items() if v > cut}
                store.save("posted")
            vip_n += 1; used.add(r["sym"]); await asyncio.sleep(2)
    if vip_n == 0:
        fallback = [r for r in cands if r["grade"] in ("B","C") and r["sym"] not in used]
        for r in fallback:
            if vip_n >= 2: break
            key = f"{r['sym']}:{r['tf']}:{r['bar_t']}"
            if key in store.data["posted"]: continue
            if len(active_signals()) >= MAX_OPEN: break
            if any(p["coin"]==r["sym"] and p["tf"]==r["tf"] for p in active_signals()): continue
            try: sent = await publish(bot, r, vip=True, free=False, admin_copy=False)
            except Exception: continue
            if sent and not any(s.startswith("err") for s in sent):
                track_signal(r)
                with store.lock: store.data["posted"][key] = now_s(); store.save("posted")
                vip_n += 1; used.add(r["sym"]); await asyncio.sleep(2)
    free_pool = [r for r in cands if r["grade"] in ("B","C") and r["sym"] not in used]
    for r in free_pool:
        if free_n >= FREE_POSTS_PER_CYCLE or _free_day["n"] >= FREE_MAX_PER_DAY: break
        key = f"{r['sym']}:{r['tf']}:{r['bar_t']}"
        if key in store.data["posted"]: continue
        if len(active_signals()) >= MAX_OPEN: break
        if any(p["coin"]==r["sym"] and p["tf"]==r["tf"] for p in active_signals()): continue
        try: sent = await publish(bot, r, vip=False, free=True, admin_copy=False)
        except Exception: continue
        if sent and not any(s.startswith("err") for s in sent):
            track_signal(r)
            with store.lock:
                store.data["posted"][key] = now_s()
                cut = now_s() - 14*DAY
                store.data["posted"] = {k:v for k,v in store.data["posted"].items() if v > cut}
                store.save("posted")
            free_n += 1; _free_day["n"] += 1; used.add(r["sym"]); await asyncio.sleep(2)
    log.info(f"run_autopost: VIP={vip_n}, FREE={free_n}")
    return dict(vip=vip_n, free=free_n, total=vip_n+free_n)

def build_promo_post():
    lines = ["💎✨ <b>لماذا نتداول على هذه المنصات؟</b> ✨💎",
             "━━━━━━━━━━━━━━━━━━━━━", "", "🚀 <b>مزايا المنصات المعتمدة:</b>", ""]
    for key, (name, url) in REFERRAL_LINKS.items():
        lines += [f"🔗 <b>{name}</b>", f"   <a href='{url}'>سجّل الآن واحصل على مكافآت</a>", ""]
    lines += ["━━━━━━━━━━━━━━━━━━━━━", "✅ رسوم منخفضة", "✅ سيولة عالية",
              "✅ عقود فيوتشر احترافية", "✅ دعم فني 24/7", 
              "", "🎁 <b>للحصول على تجربة مجانية:</b> اكتب كلمة (تجربة) في الردود", 
              "", RISK_WARNING, f"👤 {BRAND}"]
    return "\n".join(lines)

async def autopost_loop(app):
    await asyncio.sleep(20)
    while True:
        try:
            now = time.time()
            nxt = (int(now//CYCLE_AUTOPOST)+1)*CYCLE_AUTOPOST + 60
            await asyncio.sleep(max(5, nxt-now))
            await run_autopost(app.bot)
        except asyncio.CancelledError: raise
        except Exception: log.exception("autopost")
        await asyncio.sleep(60)

async def tracker_loop(app):
    await asyncio.sleep(30); cycle = 0
    while True:
        try:
            cycle += 1
            if cycle % CLEANUP_EVERY_CYCLES == 0: await asyncio.to_thread(smart_cleanup_trades)
            notes = await asyncio.to_thread(check_signals_sync)
            for coin, tf, side, e, R in notes:
                trade_obj = None
                with store.lock:
                    for p in store.data["signals"]:
                        if p["coin"] == coin and p["tf"] == tf and p["side"] == side: trade_obj = p; break
                if not trade_obj: continue
                full_msg = event_text(coin, tf, side, e, R, "vip", trade=trade_obj)
                if full_msg:
                    await notify_both(app.bot, full_msg, protect=True)
                    await notify_free(app.bot, full_msg)
        except asyncio.CancelledError: raise
        except Exception: log.exception("tracker")
        await asyncio.sleep(300)

async def cross_loop(app):
    await asyncio.sleep(300); _cross_posted = {}
    while True:
        try:
            syms = await asyncio.to_thread(top_symbols, 60)
            syms = [s for s in syms if s not in CROSS_EXCLUDE and s not in WEAK_COINS and s not in EXCLUDE_TICKERS]
            for sym in syms[:50]:
                try:
                    df = await asyncio.to_thread(get_candles, sym, "4h", 450, 600, 200)
                    if df.empty or len(df) < 210: continue
                    df = add_indicators(df, 100); cross = detect_cross(df)
                    if not cross: continue
                    key = f"{sym}:{cross['type']}:{cross['bar_t']}"
                    if key in _cross_posted: continue
                    _cross_posted[key] = now_s()
                    if cross["type"] == "golden":
                        emoji = "🌟"; title = "تقاطع ذهبي (Golden Cross)"
                        side_word = "🟢 إشارة شراء قوية"; decision = "🟢 شراء"
                    else:
                        emoji = "💀"; title = "تقاطع الموت (Death Cross)"
                        side_word = "🔴 إشارة بيع قوية"; decision = "🔴 بيع"
                    now_ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
                    cl = coin_link(sym, "binance_fut")
                    msg = (f"{emoji} <b>{title}</b>\n━━━━━━━━━━━━━━━━━━━\n\n"
                           f"📌 <a href='{cl}'>#{sym}/USDT</a> 👆\n⏱ الفريم: <b>4H</b>\n\n"
                           f"💵 السعر: <code>{fmt(cross['price'])}</code>\n"
                           f"📈 EMA 50: <code>{fmt(cross['ema50'])}</code>\n"
                           f"📉 EMA 200: <code>{fmt(cross['ema200'])}</code>\n\n"
                           f"{side_word}\n🎯 <b>القرار المتوقع: {decision}</b>\n\n"
                           f"📅 {now_ts}\n👤 {BRAND}")
                    try: await notify_both(app.bot, msg, protect=True)
                    except Exception: pass
                    try: await notify_free(app.bot, msg)
                    except Exception: pass
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
                rows = await asyncio.to_thread(pick_short_opportunities, 5)
                if rows:
                    lines = ["🔻 فرص الشورت المجانية",
                             f"🕐 {dt.datetime.now().strftime('%Y-%m-%d %H:%M')}",
                             "━━━━━━━━━━━━━━━━", ""]
                    for s, r in rows:
                        cl = coin_link(s, "binance_fut")
                        lines += [f"💠 <a href='{cl}'>#{s}</a> 👆 — RSI {r['rsi']}",
                                  f"💵 {fmt(r['price'])}", f"🎯 القرار المتوقع: 🔴 بيع", ""]
                    lines.append("💎 للأدق → VIP"); lines.append(RISK_WARNING)
                    try: await app.bot.send_message(CHANNEL_ID, "\n".join(lines), protect_content=PROTECT, link_preview_options=NO_PREVIEW)
                    except Exception: pass
        except asyncio.CancelledError: raise
        except Exception: log.exception("short_free")
        await asyncio.sleep(CYCLE_SHORT_FREE)

async def market_summary_loop(app):
    await asyncio.sleep(180)
    FREE_SYMS = ["BTC","ETH","BNB","SOL","XRP","ADA","DOGE","DOT","LINK","AVAX"]
    while True:
        try:
            if CHANNEL_ID:
                txt = await asyncio.to_thread(build_market_summary, FREE_SYMS)
                if txt:
                    try: await app.bot.send_message(CHANNEL_ID, txt, protect_content=PROTECT, link_preview_options=NO_PREVIEW)
                    except Exception: pass
            if VIP_CHANNEL_ID:
                txt = await asyncio.to_thread(build_market_summary, ALPHA_COINS[:10])
                if txt:
                    try: await app.bot.send_message(VIP_CHANNEL_ID, txt, protect_content=PROTECT, link_preview_options=NO_PREVIEW)
                    except Exception: pass
        except asyncio.CancelledError: raise
        except Exception: log.exception("market_summary")
        await asyncio.sleep(CYCLE_MARKET_SUMMARY)

async def predict_loop(app):
    await asyncio.sleep(120)
    while True:
        try:
            if VIP_CHANNEL_ID:
                syms = await asyncio.to_thread(top_symbols, 40)
                preds = await asyncio.to_thread(predictive_scan, syms[:30])
                if preds:
                    lines = ["🔮 رادار التنبؤ بالانفجارات", "━━━━━━━━━━━━━━━━", ""]
                    for p in preds[:7]:
                        icon = "🟢" if p["direction"] == "LONG" else "🔴"
                        decision = "🟢 شراء" if p["direction"] == "LONG" else "🔴 بيع"
                        reasons = " | ".join(p["reasons"][:3])
                        lines += [f"{icon} #{p['sym']} ({p['score']}/15)", f"   {reasons}",
                                  f"   🎯 القرار المتوقع: {decision}"]
                    await app.bot.send_message(VIP_CHANNEL_ID, "\n".join(lines), protect_content=PROTECT, link_preview_options=NO_PREVIEW)
        except asyncio.CancelledError: raise
        except Exception: log.exception("predict")
        await asyncio.sleep(CYCLE_PREDICT)

async def alpha_summary_loop(app):
    await asyncio.sleep(240)
    while True:
        try:
            if VIP_CHANNEL_ID:
                syms = await asyncio.to_thread(top_symbols, 60); rows = []
                for s in syms[:50]:
                    r = await asyncio.to_thread(alpha_radar, s)
                    if r: rows.append((s, r))
                if rows:
                    rows.sort(key=lambda x: -x[1]["change_7d"])
                    lines = ["⭐ رادار عملات Alpha", f"🕐 {dt.datetime.now().strftime('%Y-%m-%d %H:%M')}",
                             "━━━━━━━━━━━━━━━━", ""]
                    for s, r in rows[:10]:
                        lines += [f"⭐ #{s}", f"   7ي {r['change_7d']:+.2f}% | 30ي {r['change_30d']:+.2f}%",
                                  f"   RSI {r['rsi']}", f"   🎯 القرار المتوقع: 🟢 شراء"]
                    await app.bot.send_message(VIP_CHANNEL_ID, "\n".join(lines), protect_content=PROTECT, link_preview_options=NO_PREVIEW)
        except asyncio.CancelledError: raise
        except Exception: log.exception("alpha_summary")
        await asyncio.sleep(CYCLE_ALPHA_SUMMARY)

async def bottoms_auto_loop(app):
    await asyncio.sleep(300)
    while True:
        try:
            if VIP_CHANNEL_ID:
                syms = await asyncio.to_thread(top_symbols, 70); rows = []
                for s in syms[:55]:
                    r = await asyncio.to_thread(bottom_radar, s)
                    if r: rows.append((s, r))
                if rows:
                    rows.sort(key=lambda x: x[1]["diff"])
                    lines = ["📉 رادار القيعان الذكي", f"🕐 {dt.datetime.now().strftime('%Y-%m-%d %H:%M')}",
                             "━━━━━━━━━━━━━━━━", ""]
                    for s, r in rows[:10]:
                        lines += [f"{r['severity']} #{s}",
                                  f"   {r['diff']}% تحت EMA200 | RSI {r['rsi']}",
                                  f"   تعافي: +{r['rebound']}% | حجم: x{r['vol_ratio']}",
                                  f"   💵 {fmt(r['price'])}", f"   🎯 القرار المتوقع: 🟢 شراء"]
                    await app.bot.send_message(VIP_CHANNEL_ID, "\n".join(lines), protect_content=PROTECT, link_preview_options=NO_PREVIEW)
        except asyncio.CancelledError: raise
        except Exception: log.exception("bottoms_auto")
        await asyncio.sleep(CYCLE_BOTTOMS_AUTO)

async def golden_loop(app):
    await asyncio.sleep(600); _golden_posted = {}
    while True:
        try:
            log.info("golden_loop: بدء البحث الشامل")
            results = await asyncio.to_thread(golden_scan, 2, None)
            for g in results:
                key = f"{g['sym']}:{g['side']}"
                if key in _golden_posted and time.time() - _golden_posted[key] < 48*3600: continue
                _golden_posted[key] = time.time()
                txt = format_golden_post(g, "vip")
                try: await notify_both(app.bot, txt, protect=True)
                except Exception: pass
                log.info(f"الفرصة الذهبية: {g['sym']} (score={g['score']}) - VIP فقط")
                await asyncio.sleep(3)
        except asyncio.CancelledError: raise
        except Exception: log.exception("golden_loop")
        await asyncio.sleep(CYCLE_GOLDEN)

async def whale_hunter_loop(app):
    await asyncio.sleep(60)
    log.info("whale_hunter_loop: بدء مراقبة الحيتان")
    _seen_whales = set()
    while True:
        try:
            syms = await asyncio.to_thread(top_symbols, 20)
            for s in syms[:10]:
                try:
                    ex = get_ex("binance")
                    with _ex_lock["binance"]:
                        trades = ex.fetch_trades(f"{s}/USDT", limit=50)
                    for tr in trades:
                        notional = float(tr.get("price", 0)) * float(tr.get("amount", 0))
                        if notional >= 50000:
                            trade_id = f"{s}_{tr.get('id')}"
                            if trade_id in _seen_whales: continue
                            _seen_whales.add(trade_id)
                            if len(_seen_whales) > 1000: _seen_whales.clear()
                            side = tr.get("side", "buy")
                            price = float(tr.get("price", 0))
                            text, share_url = build_whale_tweet(s, side, notional, price)
                            kb = InlineKeyboardMarkup([
                                [InlineKeyboardButton("🚀 انشر على تويتر (X)", url=share_url)],
                                [InlineKeyboardButton("📋 رابط التسجيل فقط", url=REFERRAL_LINKS["binance"][1])]
                            ])
                            await app.bot.send_message(
                                ADMIN_ID,
                                f"🚨 <b>صيد جديد (جاهز للنشر):</b>\n\n{text}",
                                parse_mode=ParseMode.HTML,
                                reply_markup=kb,
                                link_preview_options=NO_PREVIEW
                            )
                except Exception:
                    continue
        except asyncio.CancelledError: raise
        except Exception: log.exception("whale_hunter")
        await asyncio.sleep(120)

async def promo_loop(app):
    await asyncio.sleep(3600)
    while True:
        try:
            txt = build_promo_post()
            try: await notify_both(app.bot, txt)
            except Exception: pass
            try: await notify_free(app.bot, txt)
            except Exception: pass
            log.info("promo_loop: نشر المنشور الترويجي")
        except asyncio.CancelledError: raise
        except Exception: log.exception("promo")
        await asyncio.sleep(CYCLE_PROMO)

def predictive_scan(symbols):
    out = []
    for s in symbols:
        try:
            df = get_candles(s, PREDICT_TF, 250, ttl=600, min_bars=100)
            if df.empty or len(df) < 100: continue
            df = add_indicators(df, 100); last = df.iloc[-1]
            if pd.isna(last["bb_up"]) or pd.isna(last["bb_lo"]): continue
            bbw = float(last["bb_up"]) - float(last["bb_lo"])
            bbw_avg = float((df["bb_up"] - df["bb_lo"]).tail(50).mean())
            if bbw_avg <= 0: continue
            bb_sqz = bbw / bbw_avg
            rvol = float(last["rvol"]) if not pd.isna(last["rvol"]) else 0
            rsi_v = float(last["rsi"]) if not pd.isna(last["rsi"]) else 50
            score = 0; reasons = []
            if bb_sqz < PREDICT_BB_SQZ: score += 4; reasons.append(f"ضغط BB {bb_sqz:.2f}")
            if rvol >= PREDICT_VOL_BUILDUP: score += 3; reasons.append(f"حجم x{rvol:.2f}")
            if PREDICT_RSI_NEUTRAL[0] <= rsi_v <= PREDICT_RSI_NEUTRAL[1]:
                score += 2; reasons.append(f"RSI محايد {rsi_v:.0f}")
            try:
                oi = futures_oi_history(s, "4h", 20)
                if oi and abs(oi.get("change_pct", 0)) >= PREDICT_OI_CHANGE_MIN:
                    score += 3; reasons.append(f"OI {oi['change_pct']:+.1f}%")
            except Exception: pass
            if score < 5: continue
            direction = "LONG" if rsi_v < 55 else "SHORT"
            out.append(dict(sym=s, score=score, direction=direction, reasons=reasons))
        except Exception: continue
    out.sort(key=lambda x: -x["score"])
    return out[:PREDICT_TOP_N]

def simulate(df, sym, tf, start=210):
    n = len(df); o,h,l,c = (df[k].values for k in ("open","high","low","close"))
    atr,t = df["atr"].values, df["t"].values
    trades, i = [], start; is_meme = sym in MEME_COINS
    while i < n-1:
        if is_meme: long_ok = detect_meme_conditions(df, i, 1); short_ok = detect_meme_conditions(df, i, -1)
        else: long_ok = detect_core_conditions(df, i, 1); short_ok = detect_core_conditions(df, i, -1)
        if long_ok and not short_ok: side = 1
        elif short_ok and not long_ok: side = -1
        elif long_ok and short_ok: side = 1 if df["rsi"].iloc[i] < 50 else -1
        else: i += 1; continue
        grade = grade_setup(df, i, side)
        lv = compute_meme_levels(df, i, side) if is_meme else compute_core_levels(df, i, side)
        if lv is None: i += 1; continue
        p = new_trade(sym, tf, side, lv, int(t[i]), grade, "Core" if not is_meme else "Meme")
        j = i+1
        while j < n:
            trade_step(p, o[j], h[j], l[j], c[j], atr[j])
            if p["state"] in ("closed","expired"): break
            j += 1
        if p["state"] == "open": close_at(p, float(c[n-1]), "TIME"); j = n-1
        if p["state"] == "closed":
            trades.append(dict(coin=sym, t=int(t[min(j,n-1)]), R=p["R"], exit=p["result"],
                               setup=p["setup"], grade=p["grade"], side=side, bars=p["bars"],
                               tps=sum(1 for x in p["tps"] if x["hit"])))
        i = j+1
    return trades

def backtest_symbol(sym, years=3.0):
    tf = default_tf(sym)
    df = get_candles(sym, tf, int(years*(6*365 if tf=="4h" else 365))+260, ttl=900)
    n = len(df)
    if n < 250: return [], int(df["t"].iloc[0]), int(df["t"].iloc[-1])
    df = add_indicators(df, 100)
    trades = simulate(df, sym, tf, 210)
    return trades, int(df["t"].iloc[210]), int(df["t"].iloc[-1])

def metrics(trs):
    if not trs: return dict(n=0,wr=0.0,pf=0.0,total=0.0,avg=0.0,dd=0.0,tp1=0.0)
    srt = sorted(trs, key=lambda z: z["t"]); R = np.array([x["R"] for x in srt], dtype=float)
    g,l = R[R>0].sum(), -R[R<=0].sum(); eq = np.cumsum(R)
    return dict(n=len(R), wr=round(100*float((R>0).mean()),1), pf=round(float(g/l),2) if l>0 else 999.0,
                total=round(float(R.sum()),1), avg=round(float(R.mean()),3),
                dd=round(float((np.maximum.accumulate(eq)-eq).max()),1),
                tp1=round(100*float(np.mean([x.get("tps",0)>=1 for x in srt])),1))

def bt_report(per, tmin, tmax, label):
    allt = [x for tr in per.values() for x in tr]
    cut = tmin + 0.65*(tmax-tmin)
    tr = [x for x in allt if x["t"] < cut]; te = [x for x in allt if x["t"] >= cut]
    mt, me = metrics(tr), metrics(te)
    lines = [f"📊 Backtest {label} ({len(per)} عملة)", "",
             f"📈 TRAIN: n={mt['n']} | WR {mt['wr']}% | PF {mt['pf']} | {mt['total']:+}R",
             f"📊 TEST : n={me['n']} | WR {me['wr']}% | PF {me['pf']} | {me['total']:+}R",
             f"🎯 وصول TP1: {me['tp1']}%", ""]
    v = "🔴 ضعيف" if me["pf"]<1.1 else ("🟡 مقبول" if me["pf"]<1.5 else ("✅ جيد" if me["pf"]<1.8 else "🏆 ممتاز"))
    lines.append(f"الحكم: {v}")
    return "\n".join(lines), ""

def backtest_many(symbols, years):
    per, tmin, tmax = {}, None, None
    for s in symbols:
        try:
            trs, a, b = backtest_symbol(s, years); per[s] = trs
            tmin = a if tmin is None else min(tmin,a); tmax = b if tmax is None else max(tmax,b)
        except Exception: pass
    return per, tmin, tmax

def admin_only(fn):
    @functools.wraps(fn)
    async def wrapper(update, ctx):
        if not ADMIN_ID or not update.effective_user or update.effective_user.id != ADMIN_ID:
            await update.message.reply_text("⛔ للأدمن فقط"); return
        try: return await fn(update, ctx)
        except Exception as e:
            log.exception("admin"); await update.message.reply_text(f"⚠️ {str(e)[:200]}")
    return wrapper

def vip_only(fn):
    @functools.wraps(fn)
    async def wrapper(update, ctx):
        u = update.effective_user
        if not u: return
        rec = store.data["users"].get(str(u.id), {})
        st, _ = user_status(u.id, rec)
        if st not in ("admin","vip","trial","warning"):
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("💎 اشترك في VIP", url=VIP_INVITE_LINK)]])
            await update.message.reply_text("🔒 VIP فقط", reply_markup=kb); return
        try: return await fn(update, ctx)
        except Exception as e:
            log.exception("vip"); await update.message.reply_text(f"⚠️ {str(e)[:200]}")
    return wrapper

def clean_symbol(text):
    s = (text or "").upper().strip().replace("$","").replace("/","").replace("-","")
    for suf in ("USDT","USDC","PERP","USD"):
        if s.endswith(suf) and len(s)>len(suf): s = s[:-len(suf)]
    return s if re.fullmatch(r"[A-Z0-9]{1,12}", s) else ""

_cooldown = {}

async def gate(update):
    user = update.effective_user
    if is_maintenance() and user.id != ADMIN_ID:
        await update.message.reply_text(f"🔧 صيانة.\n📞 {CONTACT_LINK}"); return "maintenance", False
    rec = touch_user(user); st, day = user_status(user.id, rec)
    if st == "blocked":
        await update.message.reply_text(f"⛔ انتهت التجربة.\n📞 {CONTACT_LINK}"); return st, False
    if st == "warning":
        left = VIP_FORCE_DAY - day
        await update.message.reply_text(f"⚠️ تنبيه: التجربة تنتهي خلال {left} يوم.")
    if st != "admin":
        if time.time() - _cooldown.get(user.id, 0) < 3:
            await update.message.reply_text("⏳ انتظار 3 ثوان"); return st, False
        _cooldown[user.id] = time.time()
    return st, True

async def handle_symbol(update, raw, to_channel=False):
    st, ok = await gate(update)
    if not ok: return
    sym = clean_symbol(raw)
    if not sym: await update.message.reply_text("📝 إدخال رمز عملة صحيح"); return
    if sym in EXCLUDE_TICKERS:
        await update.message.reply_text("⛔ هذا الرمز غير مدعوم (أسهم أو غير متاح)."); return
    wait = await update.message.reply_text(f"⏳ جارٍ تحليل {sym}...")
    try:
        res = await asyncio.to_thread(get_analysis, sym)
        tier = "admin" if st=="admin" else ("vip" if st=="vip" else "free")
        source = res.get("source", "binance_fut")
        kb = keyboard(sym, res["tf"], tier, source)
        img = await asyncio.to_thread(render_chart, res, tier)
        cap = caption_vip(res) if tier in ("admin","vip") else caption_free(res)
        await update.message.reply_photo(photo=img, caption=cap, parse_mode=ParseMode.HTML,
                                         protect_content=(st!="admin" and PROTECT), reply_markup=kb)
        if st == "admin":
            try: await update.message.reply_text(build_copy_post(res))
            except Exception: pass
    except PairNotFound: await update.message.reply_text(f"❌ لم يتم العثور على العملة: {sym}")
    except NoData: await update.message.reply_text("⚠️ البيانات غير متوفرة")
    except Exception: log.exception("handle"); await update.message.reply_text("⚠️ حدث خطأ")
    finally:
        try: await wait.delete()
        except Exception: pass

async def cmd_start(update, ctx):
    if is_maintenance() and update.effective_user.id != ADMIN_ID:
        await update.message.reply_text(f"🔧 صيانة.\n📞 {CONTACT_LINK}"); return
    touch_user(update.effective_user, "/start")
    if update.effective_user.id == ADMIN_ID:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📋 قائمة الأوامر الكاملة", callback_data="admin_menu")],
            [InlineKeyboardButton("💎 إعدادات الذهبية", callback_data="adm:/goldenconfig")],
        ])
        await update.message.reply_text("👑 لوحة تحكم الأدمن\n\nاستخدام /menu للقائمة الكاملة", reply_markup=kb)
        return
    u = update.effective_user; rec = touch_user(u, "/start"); st, day = user_status(u.id, rec)
    if st == "vip":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📋 قائمة VIP الكاملة", callback_data="vip_menu")],
            [InlineKeyboardButton("🆓 قناة التوصيات المجانية", url=CHANNEL_LINK)],
        ])
        await update.message.reply_text("💎 حساب VIP نشط\n\nإرسال رمز أي عملة للحصول على تحليل كامل", reply_markup=kb)
        return
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("💎 اشترك في VIP ✨", url=VIP_INVITE_LINK)],
        [InlineKeyboardButton("🆓 قناة التوصيات المجانية", url=CHANNEL_LINK)],
        [InlineKeyboardButton("📞 تواصل معنا", url=f"https://t.me/{CONTACT_LINK.lstrip('@')}")],
        [InlineKeyboardButton("🎁 تجربة يوم مجاناً", callback_data="vip_trial")],
    ])
    await update.message.reply_text(build_vip_page(), reply_markup=kb)

async def cmd_help(update, ctx):
    touch_user(update.effective_user, "/help")
    u = update.effective_user; rec = store.data["users"].get(str(u.id), {})
    st, _ = user_status(u.id, rec)
    if st in ("admin","vip"):
        await update.message.reply_text(
            "ℹ️ <b>مساعدة VIP</b>\n\n📝 إرسال رمز عملة → تحليل + 4 أهداف\n"
            "📋 /menu — قائمة VIP\n💎✨ /golden — الفرصة الذهبية\n"
            "🎯 /daily — صفقة اليوم\n📊 /futures BTC — الفيوتشر",
            parse_mode=ParseMode.HTML); return
    await update.message.reply_text("ℹ️ مساعدة Ryma Crypto\n\n📝 إرسال رمز عملة للتحليل\n🆔 /myid\n💎 /vip")

async def cmd_myid(update, ctx):
    touch_user(update.effective_user, "/myid")
    await update.message.reply_text(f"🆔 رقم الحساب: {update.effective_user.id}")

async def cmd_vip(update, ctx):
    u = update.effective_user; touch_user(u, "/vip")
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("💎 اشترك الآن ✨", url=VIP_INVITE_LINK)],
        [InlineKeyboardButton("🆓 قناة التوصيات المجانية", url=CHANNEL_LINK)],
        [InlineKeyboardButton("📞 تواصل معنا", url=f"https://t.me/{CONTACT_LINK.lstrip('@')}")],
    ])
    await update.message.reply_text(build_vip_page(), reply_markup=kb)

async def cmd_menu(update, ctx):
    touch_user(update.effective_user, "/menu")
    u = update.effective_user; rec = store.data["users"].get(str(u.id), {})
    st, _ = user_status(u.id, rec)
    if st == "admin":
        await update.message.reply_text(build_admin_menu_text(),
                                        reply_markup=keyboard_admin_menu(), parse_mode=ParseMode.HTML); return
    if st not in ("vip","trial","warning"):
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("💎 اشترك في VIP", url=VIP_INVITE_LINK)]])
        await update.message.reply_text("🔒 القائمة لمشتركي VIP فقط", reply_markup=kb); return
    await update.message.reply_text(build_vip_menu_text(), reply_markup=keyboard_vip_menu())

async def cmd_daily(update, ctx):
    u = update.effective_user
    if not u: return
    rec = store.data["users"].get(str(u.id), {}); st, _ = user_status(u.id, rec)
    if st not in ("admin", "vip", "trial", "warning"):
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("💎 اشترك في VIP", url=VIP_INVITE_LINK)]])
        await update.message.reply_text("🔒 الأمر حصري للمشتركين", reply_markup=kb); return
    touch_user(u, "/daily")
    wait = await update.message.reply_text("🎯 جلب Top Movers...")
    try:
        longs, shorts = await asyncio.to_thread(_build_daily_lists)
        if not longs and not shorts:
            await wait.edit_text("🎯 لا توجد صفقات مطابقة حالياً بمعايير العقل المركزي."); return
        await wait.delete()
        for idx, item in enumerate(longs, 1):
            txt = _format_daily_card(item["res"], f"🟢 LONG {idx}", item.get("change", 0), item.get("volume", 0))
            await update.message.reply_text(txt, parse_mode=ParseMode.HTML, link_preview_options=NO_PREVIEW)
            await asyncio.sleep(0.7)
        for idx, item in enumerate(shorts, 1):
            txt = _format_daily_card(item["res"], f"🔴 SHORT {idx}", item.get("change", 0), item.get("volume", 0))
            await update.message.reply_text(txt, parse_mode=ParseMode.HTML, link_preview_options=NO_PREVIEW)
            await asyncio.sleep(0.7)
        await update.message.reply_text(f"📊 النتيجة: {len(longs)} LONG + {len(shorts)} SHORT\n\n"
                                        f"💡 الصفقات جاهزة للنشر يدوياً\n\n{RISK_WARNING}",
                                        parse_mode=ParseMode.HTML)
    except Exception as e:
        log.exception("daily"); await wait.edit_text(f"⚠️ {str(e)[:200]}")

@admin_only
async def cmd_goldenconfig(update, ctx):
    await update.message.reply_text(build_golden_config_text(),
                                    reply_markup=keyboard_golden_config(),
                                    parse_mode=ParseMode.HTML)

@admin_only
async def cmd_cross(update, ctx):
    wait = await update.message.reply_text("🌟 جارٍ فحص التقاطعات...")
    try:
        syms = await asyncio.to_thread(top_symbols, 60)
        syms = [s for s in syms if s not in CROSS_EXCLUDE and s not in WEAK_COINS and s not in EXCLUDE_TICKERS]
        found = []
        for sym in syms[:50]:
            try:
                df = await asyncio.to_thread(get_candles, sym, "4h", 450, 600, 200)
                if df.empty or len(df) < 210: continue
                df = add_indicators(df, 100); cross = detect_cross(df)
                if cross: found.append((sym, cross))
            except Exception: continue
        if not found: await wait.edit_text("🌟 لا توجد تقاطعات"); return
        lines = ["🌟 التقاطعات:", ""]
        for sym, c in found:
            t = "🌟 ذهبي" if c["type"]=="golden" else "💀 موت"
            dec = "🟢 شراء" if c["type"]=="golden" else "🔴 بيع"
            lines += [f"{t} #{sym} — {fmt(c['price'])}", f"   🎯 القرار المتوقع: {dec}"]
        await wait.edit_text("\n".join(lines)[:4000], link_preview_options=NO_PREVIEW)
    except Exception as e:
        log.exception("cross"); await wait.edit_text(f"⚠️ {str(e)[:200]}")

@vip_only
async def cmd_active(update, ctx):
    act = active_signals()
    if not act: await update.message.reply_text("📊 لا توجد صفقات نشطة"); return
    act.sort(key=lambda p: -p.get("created", 0))
    lines = [f"📊 الصفقات النشطة ({len(act)}/{MAX_OPEN})", ""]
    for p in act[:20]:
        side_icon = "🟢" if p["side"] == 1 else "🔴"
        decision = "🟢 شراء" if p["side"] == 1 else "🔴 بيع"
        cur_r = p.get("realized", 0.0); tp_hits = sum(1 for t in p["tps"] if t["hit"])
        lines += [f"{side_icon} #{p['coin']}/{p['tf']} R:{cur_r:+.2f} TP:{tp_hits}/4",
                  f"   🎯 القرار المتوقع: {decision}"]
    await update.message.reply_text("\n".join(lines)[:4000], link_preview_options=NO_PREVIEW)

@vip_only
async def cmd_predict(update, ctx):
    wait = await update.message.reply_text("🔮 جارٍ الفحص...")
    try:
        syms = await asyncio.to_thread(top_symbols, 40)
        preds = await asyncio.to_thread(predictive_scan, syms[:30])
        if not preds: await wait.edit_text("🔮 لا توجد عملات"); return
        lines = ["🔮 رادار التنبؤ:", ""]
        for p in preds[:7]:
            icon = "🟢" if p["direction"] == "LONG" else "🔴"
            decision = "🟢 شراء" if p["direction"] == "LONG" else "🔴 بيع"
            lines.append(f"{icon} #{p['sym']} — {p['score']}/15")
            for r in p["reasons"][:3]: lines.append(f"   ✅ {r}")
            lines += [f"   🎯 القرار المتوقع: {decision}", ""]
        await wait.edit_text("\n".join(lines), link_preview_options=NO_PREVIEW)
    except Exception as e: await wait.edit_text(f"⚠️ {str(e)[:150]}")

@vip_only
async def cmd_whales(update, ctx):
    wait = await update.message.reply_text("🐋 فحص الحيتان...")
    syms = await asyncio.to_thread(top_symbols, 30); rows = []
    for s in syms[:15]:
        w, largest, bv, sv = await asyncio.to_thread(whale_radar, s)
        if w and w >= 1: rows.append((s, w, largest, bv, sv))
        
    if not rows:
        fallback_syms = await asyncio.to_thread(top_symbols, 5)
        lines = ["🐋 <b>السوق هادئ جداً الآن (لا توجد تحركات حيتان)</b>", "👇 إليك أنشط العملات حالياً للمراقبة:", ""]
        for s in fallback_syms:
            lines.append(f"• #{s} — 🎯 القرار المتوقع: مراقبة")
        await wait.edit_text("\n".join(lines), parse_mode=ParseMode.HTML, link_preview_options=NO_PREVIEW)
        return

    rows.sort(key=lambda x: -x[2])
    lines = ["🐋 <b>رادار الحيتان (فوق 5 آلاف دولار)</b>", "━━━━━━━━━━━━━━━━", ""]
    for s, w, largest, bv, sv in rows[:10]:
        total = bv + sv; buy_pct = (bv / total * 100) if total > 0 else 0
        direction = "🟢 شراء" if buy_pct > 55 else ("🔴 بيع" if buy_pct < 45 else "⚖️ متذبذب")
        lines += [f"🐋 <b>#{s}</b>", f"   🔢 {w} صفقة كبيرة",
                  f"   💰 أكبر صفقة: <b>${largest:,.0f}</b>",
                  f"   📊 الاتجاه: <b>{direction}</b> ({buy_pct:.0f}%)",
                  f"   🎯 القرار المتوقع: {direction}", ""]
    await wait.edit_text("\n".join(lines), parse_mode=ParseMode.HTML, link_preview_options=NO_PREVIEW)

@vip_only
async def cmd_liquidity(update, ctx):
    if not ctx.args: await update.message.reply_text("مثال: /liquidity BTC"); return
    sym = clean_symbol(ctx.args[0]); wait = await update.message.reply_text(f"💧 تحليل {sym}...")
    liq = await asyncio.to_thread(liquidity_radar, sym)
    if not liq: await wait.edit_text("💧 لا توجد بيانات"); return
    bias = "🟢 ضغط شراء" if liq["ratio"] > 0.55 else ("🔴 ضغط بيع" if liq["ratio"] < 0.45 else "⚖️")
    decision = "🟢 شراء" if liq["ratio"] > 0.55 else ("🔴 بيع" if liq["ratio"] < 0.45 else "⚖️ حياد")
    await wait.edit_text(f"💧 {sym}\n🟢 ${liq['bids']:,.0f}\n🔴 ${liq['asks']:,.0f}\n"
                         f"📊 {liq['ratio']*100:.1f}% | {bias}\n\n🎯 القرار المتوقع: {decision}")

@vip_only
async def cmd_futures(update, ctx):
    if not ctx.args: await update.message.reply_text("مثال: /futures BTC"); return
    sym = clean_symbol(ctx.args[0]); wait = await update.message.reply_text(f"⏳ {sym}...")
    try:
        oi = await asyncio.to_thread(futures_oi_history, sym, "4h", 20)
        fr = await asyncio.to_thread(futures_funding_rate, sym)
        ls = await asyncio.to_thread(futures_long_short_ratio, sym)
        lines = [f"📊 <b>الفيوتشر - {sym}/USDT</b>", "━━━━━━━━━━━━━━━━", ""]
        if oi and oi.get("current", 0) > 0:
            lines.append(f"📈 OI: <code>{oi['current']:,.0f}</code>")
            if oi.get("change_pct", 0) != 0:
                icon = "🟢" if oi["change_pct"] > 0 else "🔴"
                lines.append(f"{icon} تغير OI: <b>{oi['change_pct']:+.2f}%</b>")
        else: lines.append("📈 OI: <i>لا توجد بيانات</i>")
        if fr is not None:
            icon = "🟢" if fr > 0 else "🔴"
            lines.append(f"{icon} Funding: <b>{fr:+.4f}%</b>")
        else: lines.append("💰 Funding: <i>لا توجد بيانات</i>")
        if ls is not None:
            icon = "🟢" if ls > 1.2 else ("🔴" if ls < 0.83 else "⚖️")
            lines.append(f"{icon} L/S: <b>{ls}</b>")
        else: lines.append("⚖️ L/S: <i>لا توجد بيانات</i>")
        sig = await asyncio.to_thread(futures_signal, sym)
        if sig and sig["direction"]:
            decision = "🟢 شراء" if sig['direction']=='LONG' else "🔴 بيع"
            lines += ["", "━━━━━━━━━━━━━━━━",
                      f"🎯 <b>الإشارة: {'🟢 شراء (LONG)' if sig['direction']=='LONG' else '🔴 بيع (SHORT)'}</b>",
                      f"💪 القوة: {sig['confidence']} (Score: {sig['score']:+d})",
                      f"🎯 <b>القرار المتوقع: {decision}</b>", "", "🔍 <b>الأسباب:</b>"]
            for r in sig["reasons"][:5]: lines.append(f"  • {r}")
        else:
            lines += ["", "━━━━━━━━━━━━━━━━", "⚖️ <b>لا توجد إشارة واضحة</b>"]
        await wait.edit_text("\n".join(lines), parse_mode=ParseMode.HTML, link_preview_options=NO_PREVIEW)
    except Exception as e:
        log.exception("futures")
        try: await wait.edit_text(f"⚠️ {str(e)[:150]}")
        except Exception: pass

@vip_only
async def cmd_golden(update, ctx):
    wait = await update.message.reply_text("💎 جارٍ البحث...\n⏳ فحص 500 عملة")
    try:
        running_loop = asyncio.get_running_loop()
    except RuntimeError:
        running_loop = asyncio.new_event_loop()
    async def update_progress(processed, total, found):
        try:
            await wait.edit_text(
                f"💎 <b>جارٍ المسح الشامل...</b>\n\n"
                f"📊 تم فحص: <b>{processed}</b> من <b>{total}</b> عملة\n"
                f"🎯 تم إيجاد: <b>{found}</b> فرصة حتى الآن\n\n"
                f"⏳ يرجى الانتظار...",
                parse_mode=ParseMode.HTML
            )
        except Exception: pass
    def sync_progress(proc, tot, fnd):
        try: asyncio.run_coroutine_threadsafe(update_progress(proc, tot, fnd), running_loop)
        except Exception: pass
    try:
        results = await asyncio.to_thread(golden_scan, 2, sync_progress)
        if not results:
            syms = await asyncio.to_thread(top_symbols, 20, True)
            near_misses = []
            for s in syms[:8]:
                try:
                    r = await asyncio.to_thread(golden_score, s, True, False)
                    if r: near_misses.append(r)
                except Exception: continue
            near_misses.sort(key=lambda x: -x["score"])
            msg = "💎 <b>لا توجد فرصة ذهبية حالياً</b>\n\n"
            if near_misses:
                msg += "🔍 <b>أقرب العملات للفرصة (Score):</b>\n"
                for nm in near_misses[:3]:
                    msg += f"<b>#{nm['sym']}</b> — النقاط: {nm['score']}\n"
                    msg += f"  ↳ {', '.join(nm['reasons'][:4])}\n\n"
            cfg = get_golden_config()
            msg += f"📊 <b>الحد الأدنى المطلوب: {cfg.get('min_score',1)}</b>\n"
            msg += "💡 <b>ملاحظة:</b> البوت جرب الوضع Relaxed تلقائياً ولم يجد فرصة كافية."
            await wait.edit_text(msg, parse_mode=ParseMode.HTML, link_preview_options=NO_PREVIEW)
            return
        await wait.delete()
        for g in results:
            txt = format_golden_post(g, "vip")
            await update.message.reply_text(txt, parse_mode=ParseMode.HTML, link_preview_options=NO_PREVIEW)
            await asyncio.sleep(1)
    except Exception as e:
        log.exception("golden")
        try: await wait.edit_text(f"⚠️ {str(e)[:200]}")
        except Exception: pass

@vip_only
async def cmd_explosions(update, ctx):
    wait = await update.message.reply_text("💥 رادار الانفجارات...")
    syms = await asyncio.to_thread(top_symbols, 40); rows = []
    for s in syms[:30]:
        r = await asyncio.to_thread(explosion_radar, s)
        if r: rows.append((s, r))
        
    if not rows:
        fallback_syms = await asyncio.to_thread(top_symbols, 5)
        lines = ["💥 <b>لا توجد انفجارات حالياً (السوق هادئ)</b>", "👇 إليك أنشط العملات للمراقبة:", ""]
        for s in fallback_syms:
            lines.append(f"• #{s} — 🎯 القرار المتوقع: مراقبة")
        await wait.edit_text("\n".join(lines), parse_mode=ParseMode.HTML, link_preview_options=NO_PREVIEW)
        return

    rows.sort(key=lambda x: -x[1]["vol_ratio"])
    lines = ["💥 رادار الانفجارات:", ""]
    for s, r in rows[:10]:
        decision = "🟢 شراء" if r["price_change"] > 0 else "🔴 بيع"
        lines += [f"💥 #{s}: حجم x{r['vol_ratio']} | {r['price_change']:+.2f}%",
                  f"   🎯 القرار المتوقع: {decision}"]
    await wait.edit_text("\n".join(lines), link_preview_options=NO_PREVIEW)

@vip_only
async def cmd_pump(update, ctx):
    wait = await update.message.reply_text("🚀 عملات صاعدة...")
    syms = await asyncio.to_thread(top_symbols, 40); rows = []
    for s in syms[:30]:
        r = await asyncio.to_thread(pump_radar, s)
        if r: rows.append((s, r))
        
    if not rows:
        fallback_syms = await asyncio.to_thread(top_symbols, 5)
        lines = ["🚀 <b>لا توجد عملات صاعدة حالياً (السوق هادئ)</b>", "👇 إليك أنشط العملات للمراقبة:", ""]
        for s in fallback_syms:
            lines.append(f"• #{s} — 🎯 القرار المتوقع: مراقبة")
        await wait.edit_text("\n".join(lines), parse_mode=ParseMode.HTML, link_preview_options=NO_PREVIEW)
        return

    rows.sort(key=lambda x: -x[1]["change_1h"])
    lines = ["🚀 عملات في صعود:", ""]
    for s, r in rows[:10]:
        lines += [f"🚀 #{s}: 1س {r['change_1h']:+.2f}% | 4س {r['change_4h']:+.2f}%",
                  f"   🎯 القرار المتوقع: 🟢 شراء"]
    await wait.edit_text("\n".join(lines), link_preview_options=NO_PREVIEW)

@vip_only
async def cmd_bottoms(update, ctx):
    wait = await update.message.reply_text("📉 رادار القيعان...")
    syms = await asyncio.to_thread(top_symbols, 70); rows = []
    for s in syms[:55]:
        r = await asyncio.to_thread(bottom_radar, s)
        if r: rows.append((s, r))
    if not rows: await wait.edit_text("📉 لا توجد قيعان حالياً (السوق في حالة استقرار)."); return
    rows.sort(key=lambda x: x[1]["diff"])
    lines = ["📉 رادار القيعان:", ""]
    for s, r in rows[:10]:
        lines += [f"{r['severity']} #{s}",
                  f"   {r['diff']}% تحت EMA200 | RSI {r['rsi']} | تعافي +{r['rebound']}%",
                  f"   🎯 القرار المتوقع: 🟢 شراء"]
    await wait.edit_text("\n".join(lines)[:4000], link_preview_options=NO_PREVIEW)

@vip_only
async def cmd_shorts_radar(update, ctx):
    wait = await update.message.reply_text("🔻 فرص الشورت...")
    syms = await asyncio.to_thread(top_symbols, 40); rows = []
    for s in syms[:30]:
        r = await asyncio.to_thread(short_radar, s)
        if r: rows.append((s, r))
        
    if not rows:
        fallback_syms = await asyncio.to_thread(top_symbols, 5)
        lines = ["🔻 <b>لا توجد فرص شورت حالياً (السوق يميل للصعود)</b>", "👇 إليك أنشط العملات للمراقبة:", ""]
        for s in fallback_syms:
            lines.append(f"• #{s} — 🎯 القرار المتوقع: مراقبة")
        await wait.edit_text("\n".join(lines), parse_mode=ParseMode.HTML, link_preview_options=NO_PREVIEW)
        return

    lines = ["🔻 رادار الشورت:", ""]
    for s, r in rows[:10]:
        lines += [f"🔻 #{s}: RSI {r['rsi']} | {fmt(r['price'])}", f"   🎯 القرار المتوقع: 🔴 بيع"]
    await wait.edit_text("\n".join(lines), link_preview_options=NO_PREVIEW)

@vip_only
async def cmd_alpha(update, ctx):
    wait = await update.message.reply_text("⭐ رادار Alpha...")
    syms = await asyncio.to_thread(top_symbols, 50); rows = []
    for s in syms[:35]:
        r = await asyncio.to_thread(alpha_radar, s)
        if r: rows.append((s, r))
    if not rows: await wait.edit_text("⭐ لا توجد عملات Alpha قوية حالياً."); return
    rows.sort(key=lambda x: -x[1]["change_7d"])
    lines = ["⭐ رادار Alpha:", ""]
    for s, r in rows[:10]:
        lines += [f"⭐ #{s} 7ي {r['change_7d']:+.2f}% | 30ي {r['change_30d']:+.2f}%",
                  f"   🎯 القرار المتوقع: 🟢 شراء"]
    await wait.edit_text("\n".join(lines), link_preview_options=NO_PREVIEW)

@admin_only
async def cmd_share(update, ctx):
    if not ctx.args: await update.message.reply_text("مثال: /share BTC"); return
    sym = clean_symbol(ctx.args[0])
    wait = await update.message.reply_text(f"⏳ جارٍ تجهيز {sym}...")
    try:
        res = await asyncio.to_thread(get_analysis, sym, True)
        await wait.delete()
        img = await asyncio.to_thread(render_chart, res, "vip")
        await update.message.reply_photo(
            photo=img,
            caption=f"🖼️ شارت #{sym} — احفظه لإرفاقه مع منشورك"
        )
        await update.message.reply_text(
            "👇 اختر المنصة:",
            reply_markup=keyboard_share_post(res),
            link_preview_options=NO_PREVIEW
        )
    except Exception as e:
        log.exception("share")
        try: await wait.edit_text(f"⚠️ {str(e)[:200]}")
        except Exception: pass

@admin_only
async def cmd_forcepost(update, ctx):
    wait = await update.message.reply_text("⏳ جارٍ المسح والنشر...")
    try:
        res = await run_autopost(ctx.bot); stats = res or {}
        await wait.edit_text(f"✅ انتهى\n\nVIP: {stats.get('vip',0)} | FREE: {stats.get('free',0)}\nإجمالي: {stats.get('total',0)}")
    except Exception as e:
        log.exception("forcepost"); await wait.edit_text(f"⚠️ {str(e)[:200]}")

@admin_only
async def cmd_clearsignals(update, ctx):
    with store.lock:
        n = len(store.data["signals"]); store.data["signals"] = []; store.save("signals")
    await update.message.reply_text(f"✅ تم حذف {n} صفقة")

@admin_only
async def cmd_smartcleanup(update, ctx):
    wait = await update.message.reply_text("🧹 جارٍ التنظيف...")
    n = await asyncio.to_thread(smart_cleanup_trades)
    await wait.edit_text(f"✅ تم إغلاق {n} صفقة")

@admin_only
async def cmd_debug(update, ctx):
    lines = ["🔍 تشخيص", "", f"💾 Storage: {store.remote_msg}",
             f"🔧 Maintenance: {'ON' if is_maintenance() else 'OFF'}",
             f"🌐 Multi-Exchange: {'ON' if is_multi_exchange() else 'OFF'}",
             f"📢 FREE: {'✅' if CHANNEL_ID else '❌'} | VIP: {'✅' if VIP_CHANNEL_ID else '❌'}",
             f"📊 MAX_OPEN: {MAX_OPEN} | نشطة: {len(active_signals())}",
             f"📝 posted: {len(store.data['posted'])}",
             f"📡 Binance Futures API: {BINANCE_FAPI_BASE}",
             f"💎 Golden: كل {CYCLE_GOLDEN//3600}س | Min score: {get_golden_config().get('min_score',1)}"]
    syms = await asyncio.to_thread(top_symbols, 60)
    lines.append(f"🎯 top_symbols: {len(syms)}")
    if syms: lines.append(f"   {', '.join(syms[:10])}")
    g, l = await asyncio.to_thread(get_top_movers, True)
    lines.append(f"🟢 Gainers: {len(g)} | 🔴 Losers: {len(l)}")
    lines.append(f"🔗 روابط الإحالة: {len(REFERRAL_LINKS)} منصة")
    await update.message.reply_text("\n".join(lines)[:4000], link_preview_options=NO_PREVIEW)

@admin_only
async def cmd_post(update, ctx):
    if not ctx.args: await update.message.reply_text("مثال: /post BTC"); return
    sym = clean_symbol(ctx.args[0])
    res = await asyncio.to_thread(get_analysis, sym, True)
    sent = await publish(ctx.bot, res, vip=True, free=True, admin_copy=True)
    if res["rank"] >= 0: track_signal(res)
    await update.message.reply_text(f"✅ نُشر: {', '.join(sent)}")

@admin_only
async def cmd_bt(update, ctx):
    arg = ctx.args[0].upper() if ctx.args else "ALL"
    try: years = float(ctx.args[1]) if len(ctx.args) > 1 else 3.0
    except ValueError: years = 3.0
    syms = COINS if arg in ("ALL","") else [clean_symbol(arg)]
    wait = await update.message.reply_text("⏳ Backtest...")
    per, a, b = await asyncio.to_thread(backtest_many, syms, years)
    if not per: await wait.edit_text("❌"); return
    text, _ = bt_report(per, a, b, arg)
    await wait.edit_text(text[:4000], link_preview_options=NO_PREVIEW)

@admin_only
async def cmd_bt50(update, ctx):
    try: years = float(ctx.args[0]) if ctx.args else 3.0
    except ValueError: years = 3.0
    wait = await update.message.reply_text("⏳ Top 50...")
    syms = await asyncio.to_thread(top_symbols, 50)
    per, a, b = await asyncio.to_thread(backtest_many, syms, years)
    if not per: await wait.edit_text("❌"); return
    text, _ = bt_report(per, a, b, "TOP50")
    await wait.edit_text(text[:4000], link_preview_options=NO_PREVIEW)

@admin_only
async def cmd_scan(update, ctx):
    wait = await update.message.reply_text("⏳ فحص...")
    syms = await asyncio.to_thread(top_symbols, 60, True)
    res = await asyncio.to_thread(scan_sync, syms[:30])
    res = [r for r in res if r["rank"] >= 0]; res.sort(key=lambda r: -r["rank"])
    if not res: await wait.edit_text("لا شيء"); return
    lines = ["🎯 أفضل الإعدادات:", ""]
    for r in res[:10]:
        s = "🟢" if r["side"]==1 else "🔴"; dec = "🟢 شراء" if r["side"]==1 else "🔴 بيع"
        lines += [f"{r['grade']} | #{r['sym']} {s}", f"   🎯 القرار المتوقع: {dec}"]
    await wait.edit_text("\n".join(lines), link_preview_options=NO_PREVIEW)

@admin_only
async def cmd_short(update, ctx):
    wait = await update.message.reply_text("⏳...")
    res = await asyncio.to_thread(scan_sync, COINS)
    res = [r for r in res if r["side"]==-1 and r["rank"]>=0]
    if not res: await wait.edit_text("لا شورت"); return
    lines = ["🔻 فرص شورت:"] + [f"{r['grade']} | #{r['sym']} | 🎯 شراء: بيع" for r in res[:15]]
    await wait.edit_text("\n".join(lines), link_preview_options=NO_PREVIEW)

@admin_only
async def cmd_delist(update, ctx):
    wait = await update.message.reply_text("⏳...")
    try:
        r = requests.get("https://www.binance.com/bapi/composite/v1/public/cms/article/list/query",
                         params=dict(type=1, catalogId=161, pageNo=1, pageSize=20), timeout=10)
        arts = r.json()["data"]["catalogs"][0]["articles"]
        skip = {"BINANCE","WILL","DELIST","ON","AND","THE","FOR","USD","USDT","FROM","SPOT","TRADING",
                "PAIRS","PAIR","TOKEN","TOKENS","ANNOUNCEMENT","API","BNB","LISTING","MARGIN","FUTURES",
                "MONITORING","DELISTING"}
        now_ms = int(time.time() * 1000); rows = []
        for a in arts[:40]:
            title = a.get("title", ""); release_ts = a.get("releaseDate", 0)
            if release_ts and release_ts > now_ms:
                dt_str = dt.datetime.utcfromtimestamp(release_ts / 1000).strftime("%Y-%m-%d")
                for w in re.findall(r'\b[A-Z0-9]{2,10}\b', title):
                    if w in skip or w.isdigit() or len(w) < 2: continue
                    rows.append((w, dt_str))
        if not rows: await wait.edit_text("✅ لا توجد"); return
        seen = set(); lines = ["⚠️ للحذف قريباً:", ""]
        for c, d in rows:
            if c in seen: continue
            seen.add(c); lines.append(f"🔻 {c} — 📅 {d}")
        await wait.edit_text("\n".join(lines[:40]), link_preview_options=NO_PREVIEW)
    except Exception as e: await wait.edit_text(f"⚠️ {str(e)[:150]}")

@admin_only
async def cmd_price(update, ctx):
    if not ctx.args: await update.message.reply_text("مثال: /price BTC"); return
    sym = clean_symbol(ctx.args[0]); prices = await asyncio.to_thread(all_prices, sym)
    if not prices: await update.message.reply_text("لا بيانات"); return
    lo, hi = min(prices.values()), max(prices.values())
    lines = [f"💲 {sym}/USDT"] + [f"• {n}: {fmt(p)}" for n,p in sorted(prices.items(), key=lambda z:z[1])]
    lines.append(f"\n📊 الفرق: {100*(hi/lo-1):.2f}%")
    await update.message.reply_text("\n".join(lines), link_preview_options=NO_PREVIEW)

@admin_only
async def cmd_why(update, ctx):
    if not ctx.args: await update.message.reply_text("مثال: /why BTC"); return
    sym = clean_symbol(ctx.args[0])
    try:
        res = await asyncio.to_thread(get_analysis, sym, True); df = res["df"]; r = df.iloc[-1]
        side = "شراء" if res["side"] == 1 else "بيع"
        lines = [f"🔎 {sym} · {res['tf']} · {res.get('source')}",
                 f"القرار: {side} | {res['trend']}",
                 f"الإغلاق {fmt(r['close'])} | EMA20 {fmt(r['ema20'])} | RSI {r['rsi']:.0f}"]
        await update.message.reply_text("\n".join(lines))
    except Exception as e: await update.message.reply_text(f"⚠️ {str(e)[:150]}")

@admin_only
async def cmd_users(update, ctx):
    with store.lock: users = list(store.data["users"].values())
    users.sort(key=lambda u: -u.get("last_seen",0))
    lines = [f"👥 ({len(users)})", ""]
    for u in users[:30]:
        n = (u.get("name") or "").strip(); un = (u.get("username") or "").strip()
        label = f"{n} (@{un})" if n and un else (n or (f"@{un}" if un else f"ID:{u['id']}"))
        st, _ = user_status(int(u["id"]), u)
        icon = {"admin":"👑","vip":"💎","trial":"🆓","warning":"⚠️","blocked":"⛔"}.get(st,"•")
        lines += [f"{icon} {label}", f"    📊 {u.get('requests',0)} | آخر: {time_ago(u.get('last_seen'))}"]
    await update.message.reply_text("\n".join(lines)[:4000], link_preview_options=NO_PREVIEW)

@admin_only
async def cmd_testchannels(update, ctx):
    out = []
    for name, chat in (("FREE", CHANNEL_ID), ("VIP", VIP_CHANNEL_ID)):
        if not chat: out.append(f"❌ {name}"); continue
        try:
            await ctx.bot.send_message(chat, f"✅ اختبار - {BRAND}"); out.append(f"✅ {name}")
        except Exception as e: out.append(f"❌ {name}: {str(e)[:80]}")
    await update.message.reply_text("\n".join(out))

@admin_only
async def cmd_stats(update, ctx):
    since = (time.time()-7*DAY)*1000
    with store.lock: h = [x for x in store.data["history"] if (x.get("closed") or 0) >= since]
    if not h: await update.message.reply_text("لا صفقات"); return
    R = np.array([x["R"] for x in h]); g,l = R[R>0].sum(), -R[R<=0].sum()
    pf = round(float(g/l),2) if l>0 else 999
    await update.message.reply_text(
        f"📈 آخر 7 أيام\n\nصفقات: {len(R)} | WR: {100*(R>0).mean():.1f}%\nPF: {pf} | {R.sum():+.1f}R")

@admin_only
async def cmd_history(update, ctx):
    with store.lock: h = store.data["history"][-15:][::-1]
    if not h: await update.message.reply_text("لا يوجد"); return
    await update.message.reply_text("🗂 آخر 15:\n" + "\n".join(
        f"{'✅' if x['R']>0 else '❌'} #{x['coin']} {x['R']:+.2f}R | {x['result']}" for x in h))

@admin_only
async def cmd_dashboard(update, ctx):
    act = active_signals()
    with store.lock:
        nu = len(store.data["users"]); nv = sum(1 for k in store.data["vip"] if is_vip(int(k)))
    maint = "🔧 صيانة" if is_maintenance() else "✅ يعمل"
    msg = (f"🎛 لوحة التحكم — {maint}\n\n👥 {nu} | 💎 VIP: {nv}\n"
           f"📊 نشطة: {len(act)}/{MAX_OPEN}\n💾 {store.remote_msg}\n"
           f"📡 Binance Futures: ✅\n"
           f"📢 FREE {'✅' if CHANNEL_ID else '—'} | VIP {'✅' if VIP_CHANNEL_ID else '—'}\n\n"
           f"🔗 روابط إحالة: {len(REFERRAL_LINKS)}\n"
           f"💎 الذهبية: كل {CYCLE_GOLDEN//3600}س | Min: {get_golden_config().get('min_score',1)}")
    await update.message.reply_text(msg)

@admin_only
async def cmd_addvip(update, ctx):
    try: uid, days = int(ctx.args[0]), int(ctx.args[1])
    except Exception: await update.message.reply_text("/addvip ID DAYS"); return
    exp = add_vip(uid, days)
    e = dt.datetime.utcfromtimestamp(exp).strftime("%Y-%m-%d")
    await update.message.reply_text(f"✅ VIP {uid} حتى {e}")

@admin_only
async def cmd_removevip(update, ctx):
    try: uid = int(ctx.args[0])
    except Exception: await update.message.reply_text("/removevip ID"); return
    await update.message.reply_text("✅" if remove_vip(uid) else "ليس VIP")

@admin_only
async def cmd_viplist(update, ctx):
    with store.lock: rows = sorted(store.data["vip"].items(), key=lambda kv: kv[1].get("expires",0))
    lines = [f"👑 VIP ({len(rows)})"]
    for uid, v in rows[:40]:
        left = (int(v.get("expires",0)) - now_s()) // DAY
        lines.append(f"{uid} - {'متبقي '+str(left)+' يوم' if left>=0 else 'منتهي'}")
    await update.message.reply_text("\n".join(lines))

class _FakeUpdate:
    def __init__(self, msg, user):
        self.message = msg; self.effective_user = user; self.callback_query = None

ADMIN_CMD_MAP = {
    "/forcepost": cmd_forcepost, "/clearsignals": cmd_clearsignals,
    "/smartcleanup": cmd_smartcleanup, "/debug": cmd_debug, "/daily": cmd_daily,
    "/cross": cmd_cross, "/predict": cmd_predict, "/whales": cmd_whales,
    "/explosions": cmd_explosions, "/pump": cmd_pump, "/bottom": cmd_bottoms,
    "/shortsradar": cmd_shorts_radar, "/alpha": cmd_alpha, "/scan": cmd_scan,
    "/short": cmd_short, "/bt": cmd_bt, "/bt50": cmd_bt50, "/users": cmd_users,
    "/viplist": cmd_viplist, "/stats": cmd_stats, "/history": cmd_history,
    "/dashboard": cmd_dashboard, "/testchannels": cmd_testchannels, "/delist": cmd_delist,
    "/golden": cmd_golden, "/goldenconfig": cmd_goldenconfig, "/share": cmd_share,
}

ADMIN_ARGS_HINTS = {
    "liquidity": "أرسل: /liquidity BTC", "futures": "أرسل: /futures BTC",
    "price": "أرسل: /price BTC", "why": "أرسل: /why BTC", "post": "أرسل: /post BTC",
    "addvip": "أرسل: /addvip ID DAYS", "removevip": "أرسل: /removevip ID",
    "share": "أرسل: /share BTC",
}

async def cb_handler(update, ctx):
    q = update.callback_query
    try: await q.answer()
    except Exception: pass

    if q.data.startswith("gc:"):
        if q.from_user.id != ADMIN_ID: return
        key = q.data.split(":", 1)[1]
        if key == "refresh":
            try:
                await q.message.edit_text(build_golden_config_text(),
                                          reply_markup=keyboard_golden_config(),
                                          parse_mode=ParseMode.HTML)
            except Exception: pass
            return
        if key == "min_score":
            cfg = get_golden_config()
            current = cfg.get("min_score", 1)
            cycle = {0:1, 1:2, 2:3, 3:4, 4:5, 5:6, 6:7, 7:8, 8:9, 9:0}
            set_golden_config("min_score", cycle.get(current, 1))
            try:
                await q.message.edit_text(build_golden_config_text(),
                                          reply_markup=keyboard_golden_config(),
                                          parse_mode=ParseMode.HTML)
            except Exception: pass
            return
        if key in DEFAULT_GOLDEN_CONFIG:
            cfg = get_golden_config(); current = cfg.get(key, False)
            set_golden_config(key, not current)
            try:
                await q.message.edit_text(build_golden_config_text(),
                                          reply_markup=keyboard_golden_config(),
                                          parse_mode=ParseMode.HTML)
            except Exception: pass
        return

    if q.data == "toggle_maintenance":
        if q.from_user.id != ADMIN_ID:
            await q.message.reply_text("⛔ للأدمن فقط"); return
        new_state = set_maintenance(not is_maintenance())
        msg = "🔧 <b>وضع الصيانة مفعّل</b>" if new_state else "✅ <b>وضع الصيانة معطّل</b>"
        await q.message.reply_text(msg, parse_mode=ParseMode.HTML)
        return

    if q.data == "toggle_multi_exchange":
        if q.from_user.id != ADMIN_ID:
            await q.message.reply_text("⛔ للأدمن فقط"); return
        new_state = set_multi_exchange(not is_multi_exchange())
        if new_state:
            msg = ("🌐 <b>تعدد المنصات: مفعّل</b>\n\n"
                   "✅ سيبحث البوت في جميع المنصات (Binance, OKX, Bybit, MEXC...)\n"
                   "✅ سيظهر اسم المنصة المصدر في كل توصية ورادار.")
        else:
            msg = ("🛑 <b>تعدد المنصات: معطّل</b>\n\n"
                   "✅ سيعتمد البوت على Binance فقط.\n"
                   "⚠️ إذا لم يجد العملة في بينانس، سيعتبرها غير موجودة.")
        await q.message.reply_text(msg, parse_mode=ParseMode.HTML)
        try: await q.message.edit_reply_markup(reply_markup=keyboard_admin_menu())
        except Exception: pass
        return

    if q.data == "admin_menu":
        if q.from_user.id != ADMIN_ID: return
        try:
            await q.message.reply_text(build_admin_menu_text(),
                                       reply_markup=keyboard_admin_menu(), parse_mode=ParseMode.HTML)
        except Exception: pass
        return

    if q.data == "close_menu":
        try: await q.message.delete()
        except Exception: pass
        return

    if q.data.startswith("refs:"):
        try:
            msg = (
                "🌐 <b>منصات التداول المعتمدة</b>\n\n"
                "💡 <b>لماذا التسجيل عبر البوت؟</b>\n"
                "✅ رسوم تداول أقل\n"
                "✅ دعم فني من فريقنا\n"
                "✅ دعم استمرارية القناة المجانية\n\n"
                "👇 اختر منصتك للتسجيل:"
            )
            await q.message.reply_text(msg, reply_markup=keyboard_refs("binance_fut"),
                                       parse_mode=ParseMode.HTML,
                                       link_preview_options=NO_PREVIEW)
        except Exception: pass
        return

    if q.data.startswith("adm_args:"):
        if q.from_user.id != ADMIN_ID: return
        key = q.data.split(":", 1)[1]
        await q.message.reply_text(f"✏️ {ADMIN_ARGS_HINTS.get(key, 'الأمر مع الوسيط')}")
        return

    if q.data.startswith("adm:"):
        if q.from_user.id != ADMIN_ID: return
        cmd_name = q.data.split(":", 1)[1]
        fn = ADMIN_CMD_MAP.get(cmd_name)
        if not fn:
            await q.message.reply_text("⚠️ غير معروف"); return
        fake = _FakeUpdate(q.message, q.from_user)
        try: await fn(fake, ctx)
        except Exception as e:
            try: await q.message.reply_text(f"⚠️ {str(e)[:150]}")
            except Exception: pass
        return

    if q.data in ("vip", "vip_page"):
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("💎 الانضمام لقناة VIP ✨", url=VIP_INVITE_LINK)],
            [InlineKeyboardButton("🆓 قناة التوصيات المجانية", url=CHANNEL_LINK)],
            [InlineKeyboardButton("🎁 تجربة يوم", callback_data="vip_trial")],
        ])
        try: await q.message.reply_text(build_vip_page(), reply_markup=kb)
        except Exception: pass
        return

    if q.data == "vip_trial":
        u = q.from_user; uid = str(u.id)
        with store.lock:
            u_rec = store.data["users"].get(uid, {})
            if u_rec.get("trial_used"):
                await q.message.reply_text("⛔ استخدمت التجربة",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("💎 اشترك", url=VIP_INVITE_LINK)]]))
                return
            u_rec["trial_used"] = True
            store.data["users"][uid] = u_rec; store.save("users")
        add_vip(u.id, 1)
        await q.message.reply_text("🎉 تجربة VIP 24 ساعة!",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📋 البدء", callback_data="vip_menu")]]))
        return

    if q.data == "vip_menu":
        u = q.from_user; rec = store.data["users"].get(str(u.id), {})
        st, _ = user_status(u.id, rec)
        if st not in ("admin","vip","trial","warning"):
            await q.message.reply_text("🔒 VIP فقط"); return
        await q.message.reply_text(build_vip_menu_text(), reply_markup=keyboard_vip_menu())
        return

    if q.data.startswith("vip_cmd_args:"):
        key = q.data.split(":", 1)[1]
        u = q.from_user; rec = store.data["users"].get(str(u.id), {})
        st, _ = user_status(u.id, rec)
        if st not in ("admin","vip","trial","warning"):
            await q.message.reply_text("🔒 VIP فقط"); return
        hints = {"liquidity": "💧 /liquidity BTC", "futures": "📊 /futures BTC"}
        await q.message.reply_text(f"✏️ {hints.get(key, 'الأمر مع الرمز')}")
        return

    if q.data.startswith("vip_cmd:"):
        cmd = q.data.split(":",1)[1]
        u = q.from_user; rec = store.data["users"].get(str(u.id), {})
        st, _ = user_status(u.id, rec)
        if st not in ("admin","vip","trial","warning"):
            await q.message.reply_text("🔒 VIP فقط"); return
        try:
            fake = _FakeUpdate(q.message, q.from_user)
            handlers = {"/predict": cmd_predict, "/whales": cmd_whales, "/explosions": cmd_explosions,
                        "/pump": cmd_pump, "/bottom": cmd_bottoms, "/shortsradar": cmd_shorts_radar,
                        "/alpha": cmd_alpha, "/active": cmd_active, "/golden": cmd_golden, "/daily": cmd_daily}
            fn = handlers.get(cmd)
            if fn: await fn(fake, ctx)
        except Exception as e:
            try: await q.message.reply_text(f"⚠️ {str(e)[:150]}")
            except Exception: pass
        return

    if q.data.startswith("ind:"):
        if q.from_user.id != ADMIN_ID: return
        try:
            sym = q.data.split(":")[1]
            res = await asyncio.to_thread(get_analysis, sym)
            await q.message.reply_text(build_admin_extras(res))
        except Exception as e: await q.message.reply_text(f"⚠️ {str(e)[:200]}")
        return

    if q.data.startswith("liq:"):
        if q.from_user.id != ADMIN_ID: return
        try:
            sym = q.data.split(":")[1]; liq = await asyncio.to_thread(liquidity_radar, sym)
            if not liq: await q.message.reply_text("لا بيانات"); return
            bias = "🟢" if liq["ratio"] > 0.55 else ("🔴" if liq["ratio"] < 0.45 else "⚖️")
            await q.message.reply_text(
                f"💧 {sym}\n🟢 ${liq['bids']:,.0f}\n🔴 ${liq['asks']:,.0f}\n{liq['ratio']*100:.1f}% | {bias}")
        except Exception: pass
        return

    if q.data.startswith("whale:"):
        if q.from_user.id != ADMIN_ID: return
        try:
            sym = q.data.split(":")[1]; w, largest, bv, sv = await asyncio.to_thread(whale_radar, sym)
            if w is None: await q.message.reply_text("لا بيانات"); return
            total = bv + sv; buy_pct = (bv / total * 100) if total > 0 else 0
            direction = "🟢" if buy_pct > 55 else ("🔴" if buy_pct < 45 else "⚖️")
            await q.message.reply_text(f"🐋 {sym}\n📊 {w} صفقة\n💰 ${largest:,.0f}\n{direction} {buy_pct:.0f}%")
        except Exception: pass
        return

    if q.data.startswith("btcb:"):
        if q.from_user.id != ADMIN_ID: return
        sym = q.data.split(":")[1]
        try:
            per, a, b = await asyncio.to_thread(backtest_many, [sym], 3.0)
            if not per: await q.message.reply_text("لا بيانات"); return
            text, _ = bt_report(per, a, b, sym)
            await q.message.reply_text(text[:4000])
        except Exception: pass
        return

    if q.data.startswith("tf:"):
        parts = q.data.split(":"); sym, tf = parts[1], parts[2]
        user = touch_user(q.from_user, "refresh")
        st, _ = user_status(q.from_user.id, user)
        tier = "admin" if st=="admin" else ("vip" if st=="vip" else "free")
        _candle_cache.pop((sym, tf, 450, 215, False), None); _analysis_cache.pop(sym, None)
        try:
            res = await asyncio.to_thread(get_analysis, sym)
            source = res.get("source", "binance_fut")
            kb = keyboard(sym, res["tf"], tier, source)
            img = await asyncio.to_thread(render_chart, res, tier)
            cap = caption_vip(res) if tier in ("admin","vip") else caption_free(res)
            await ctx.bot.send_photo(q.message.chat.id, photo=img, caption=cap,
                                     protect_content=(st!="admin" and PROTECT), reply_markup=kb)
        except Exception: pass

async def on_error(update, ctx):
    log.error("err: %s", ctx.error, exc_info=ctx.error)

async def on_text(update, ctx):
    if update.message and update.message.text:
        await handle_symbol(update, update.message.text.split()[0])

PUBLIC_COMMANDS = [("start","ابدأ"),("help","المساعدة"),("myid","رقمي"),
                   ("vip","VIP"),("menu","قائمتي 📋")]

async def post_init(app):
    try: await app.bot.set_my_commands([BotCommand(c,d) for c,d in PUBLIC_COMMANDS])
    except Exception: pass
    try:
        n = await asyncio.to_thread(smart_cleanup_trades)
        log.info(f"startup: smart_cleanup نظّفت {n} صفقة")
    except Exception as e: log.warning(f"startup cleanup: {e}")
    app.bot_data["tasks"] = [
        asyncio.create_task(autopost_loop(app)), asyncio.create_task(tracker_loop(app)),
        asyncio.create_task(predict_loop(app)), asyncio.create_task(market_summary_loop(app)),
        asyncio.create_task(short_free_loop(app)), asyncio.create_task(alpha_summary_loop(app)),
        asyncio.create_task(bottoms_auto_loop(app)), asyncio.create_task(cross_loop(app)),
        asyncio.create_task(golden_loop(app)), asyncio.create_task(promo_loop(app)),
        asyncio.create_task(whale_hunter_loop(app)),
    ]
    log.info("ryma v96 | max_open=%d | golden_cycle=%ds | min_score=%d | multi=%s | maint=%s",
             MAX_OPEN, CYCLE_GOLDEN, get_golden_config().get("min_score",1), is_multi_exchange(), is_maintenance())

async def post_shutdown(app):
    for t in app.bot_data.get("tasks",[]): t.cancel()
    try: store.flush()
    except Exception: pass

def main():
    if not BOT_TOKEN: raise SystemExit("BOT_TOKEN not set")
    store.start_flusher()
    app = (ApplicationBuilder().token(BOT_TOKEN)
           .post_init(post_init).post_shutdown(post_shutdown).build())
    handlers = [
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
    ]
    for name, fn in handlers: app.add_handler(CommandHandler(name, fn))
    app.add_handler(CallbackQueryHandler(cb_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, on_text))
    app.add_error_handler(on_error)
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
