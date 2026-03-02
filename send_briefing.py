import os
import io
import json
import time
import zipfile
import logging
import datetime as dt
import xml.etree.ElementTree as ET
from pathlib import Path
from functools import lru_cache
from zoneinfo import ZoneInfo

import requests
import feedparser
import pandas as pd
import yfinance as yf

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pykrx import stock
from openai import OpenAI


# =========================
# ENV / CONFIG
# =========================
KST = ZoneInfo("Asia/Seoul")
BASE_DIR = Path(__file__).resolve().parent if "__file__" in globals() else Path(".").resolve()
CACHE_DIR = BASE_DIR / ".cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
DART_API_KEY = os.environ.get("DART_API_KEY", "")
HTTP_USER_AGENT = os.environ.get(
    "HTTP_USER_AGENT",
    "stock-telegram-bot/1.0 (contact: your_email@example.com)",
)

PYKRX_SLEEP_SEC = float(os.environ.get("PYKRX_SLEEP_SEC", "0.05"))
KR_UNIVERSE_EACH = int(os.environ.get("KR_UNIVERSE_EACH", "60"))
KR_TOP_PRE_FLOW = int(os.environ.get("KR_TOP_PRE_FLOW", "12"))

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

US_TICKERS = {
    "엔비디아(NVDA)": "NVDA",
    "테슬라(TSLA)": "TSLA",
    "팔란티어(PLTR)": "PLTR",
}

KR_CORE = {
    "SK하이닉스(000660)": "000660",
}

SEC_8K_ATOM = {
    "NVDA": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0001045810&type=8-K&owner=exclude&count=20&output=atom",
    "TSLA": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0001318605&type=8-K&owner=exclude&count=20&output=atom",
    "PLTR": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0001321655&type=8-K&owner=exclude&count=20&output=atom",
}


# =========================
# Logging
# =========================
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("daily-briefing")


# =========================
# HTTP session
# =========================
def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET", "POST"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": HTTP_USER_AGENT,
            "Accept-Encoding": "gzip, deflate",
            "Accept": "*/*",
        }
    )
    return session


SESSION = build_session()


# =========================
# Utils
# =========================
def kst_now() -> dt.datetime:
    return dt.datetime.now(KST)


def is_nan(x) -> bool:
    try:
        return pd.isna(x)
    except Exception:
        return x != x


def safe_float(x, default=float("nan")) -> float:
    try:
        return float(x)
    except Exception:
        return default


def fmt_pct(x) -> str:
    return "N/A" if is_nan(x) else f"{x:+.2f}%"


def fmt_int(x) -> str:
    try:
        return f"{int(round(float(x))):,}"
    except Exception:
        return "N/A"


def fmt_bn_krw(x) -> str:
    try:
        return f"{float(x) / 1_000_000_000.0:+.1f}십억"
    except Exception:
        return "N/A"


def uniq_keep_order(items: list[str]) -> list[str]:
    seen = set()
    out = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


# =========================
# Telegram
# =========================
def telegram_send(text: str) -> None:
    max_len = 3800
    parts = []
    buf = text.strip()

    while len(buf) > max_len:
        cut = buf.rfind("\n", 0, max_len)
        if cut < 800:
            cut = max_len
        parts.append(buf[:cut])
        buf = buf[cut:].lstrip("\n")

    if buf:
        parts.append(buf)

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

    for i, part in enumerate(parts, start=1):
        try:
            r = SESSION.post(
                url,
                json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": part,
                    "disable_web_page_preview": True,
                },
                timeout=30,
            )
            logger.info("telegram send part=%s status=%s", i, r.status_code)
            r.raise_for_status()
        except Exception:
            logger.exception("telegram send failed at part=%s", i)
            raise


# =========================
# RSS / NEWS
# =========================
def google_news_rss(query: str) -> str:
    q = requests.utils.quote(query)
    return f"https://news.google.com/rss/search?q={q}&hl=ko&gl=KR&ceid=KR:ko"


def fetch_rss(url: str, limit: int = 3) -> list[tuple[str, str]]:
    try:
        r = SESSION.get(url, timeout=15)
        r.raise_for_status()
        feed = feedparser.parse(r.content)

        out = []
        for e in (feed.entries or [])[:limit]:
            title = (getattr(e, "title", "") or "").strip()
            link = (getattr(e, "link", "") or "").strip()
            if title:
                out.append((title, link))
        return out
    except Exception:
        logger.exception("rss fetch failed: %s", url)
        return []


# =========================
# OpenAI summary
# =========================
def ai_summarize(bundle_title: str, bullets: list[str]) -> str:
    if not client:
        return "AI 요약: (OPENAI_API_KEY가 없어 요약을 생략했어요.)"
    if not bullets:
        return "AI 요약: (요약할 뉴스/공시가 부족했어요.)"

    prompt = (
        f"다음은 '{bundle_title}' 관련 최신 뉴스/공시 헤드라인 목록이야.\n"
        f"한국어로, 투자 초보도 이해할 수 있게 요약해줘.\n"
        f"규칙:\n"
        f"- 4~6줄 요약\n"
        f"- 긍정 1줄, 리스크 1줄, 오늘 체크포인트 1줄 포함\n"
        f"- 과장/확정적 예언 금지, 가능성 표현 사용\n"
        f"- 링크는 요약문에 넣지 말고, 아래 원문 목록으로만 유지\n\n"
        f"헤드라인:\n" + "\n".join(bullets[:12])
    )

    try:
        resp = client.responses.create(
            model="gpt-5-mini",
            input=[
                {"role": "system", "content": "너는 신중하고 사실 기반의 투자 뉴스 요약가야."},
                {"role": "user", "content": prompt},
            ],
        )
        text = getattr(resp, "output_text", None)
        if text:
            return "AI 요약:\n" + text.strip()

        data = resp.to_dict() if hasattr(resp, "to_dict") else {}
        chunks = []
        for item in data.get("output", []):
            for c in item.get("content", []):
                if c.get("type") in ("output_text", "text"):
                    chunks.append(c.get("text", ""))
        merged = "\n".join(x.strip() for x in chunks if x and x.strip()).strip()
        return "AI 요약:\n" + (merged if merged else "(요약 결과를 읽지 못했어요.)")
    except Exception as e:
        logger.exception("ai summarize failed: %s", bundle_title)
        return f"AI 요약: (요약 중 오류로 생략했어요: {type(e).__name__})"


# =========================
# Market dates (KRX)
# =========================
def fallback_weekday_ymd(base_date: dt.date | None = None) -> str:
    d = base_date or kst_now().date()
    while d.weekday() >= 5:
        d -= dt.timedelta(days=1)
    return d.strftime("%Y%m%d")


@lru_cache(maxsize=128)
def business_days_between(from_ymd: str, to_ymd: str) -> list[str]:
    try:
        days = stock.get_previous_business_days(fromdate=from_ymd, todate=to_ymd)
        return [pd.Timestamp(x).strftime("%Y%m%d") for x in days]
    except Exception:
        logger.exception("business day lookup failed: %s ~ %s", from_ymd, to_ymd)
        return []


@lru_cache(maxsize=128)
def recent_business_days(end_ymd: str, n: int) -> list[str]:
    start = (pd.Timestamp(end_ymd) - pd.Timedelta(days=max(n * 3, 10))).strftime("%Y%m%d")
    days = business_days_between(start, end_ymd)
    return days[-n:] if days else []


def effective_krx_close_date(now: dt.datetime | None = None, final_hour: int = 18) -> str:
    now = now or kst_now()
    today_ymd = now.strftime("%Y%m%d")

    try:
        nearest = stock.get_nearest_business_day_in_a_week(today_ymd)
    except Exception:
        logger.exception("nearest business day lookup failed")
        nearest = fallback_weekday_ymd(now.date())

    days = recent_business_days(nearest, 5)
    if not days:
        return nearest

    last_bd = days[-1]
    if today_ymd == last_bd and now.hour < final_hour and len(days) >= 2:
        return days[-2]
    return last_bd


# =========================
# Market / FX
# =========================
def get_usdkrw(default=1350.0) -> float:
    try:
        fx = yf.Ticker("KRW=X").history(period="10d").dropna()
        if fx.empty:
            return default
        return float(fx["Close"].iloc[-1])
    except Exception:
        logger.exception("usdkrw fetch failed")
        return default


def get_index_return(ticker: str) -> float:
    try:
        h = yf.Ticker(ticker).history(period="10d").dropna()
        if len(h) < 2:
            return float("nan")
        close = float(h["Close"].iloc[-1])
        prev = float(h["Close"].iloc[-2])
        return (close / prev - 1.0) * 100.0
    except Exception:
        logger.exception("index return fetch failed: %s", ticker)
        return float("nan")


def parse_investor_flow_df(df: pd.DataFrame) -> dict | None:
    if df is None or df.empty:
        return None

    try:
        idx_as_str = [str(x) for x in df.index]

        if "순매수" in df.columns:
            def pick(names: list[str]) -> float:
                for nm in names:
                    for idx in idx_as_str:
                        if nm in idx:
                            return safe_float(df.loc[idx, "순매수"])
                return float("nan")

            return {
                "foreign": pick(["외국인합계", "외국인"]),
                "inst": pick(["기관합계", "기관"]),
            }

        for investor_col in ["외국인합계", "외국인"]:
            if investor_col in df.columns:
                inst_col = "기관합계" if "기관합계" in df.columns else ("기관" if "기관" in df.columns else None)
                if not inst_col:
                    break
                for idx in idx_as_str:
                    if "순매수" in idx:
                        return {
                            "foreign": safe_float(df.loc[idx, investor_col]),
                            "inst": safe_float(df.loc[idx, inst_col]),
                        }
    except Exception:
        logger.exception("parse investor flow failed")

    return None


def kospi_flow(date: str) -> dict | None:
    try:
        df = stock.get_market_trading_value_by_investor(date, date, "KOSPI")
        return parse_investor_flow_df(df)
    except Exception:
        logger.exception("kospi flow failed: %s", date)
        return None


def market_brief() -> tuple[str, str]:
    nasdaq = get_index_return("^IXIC")
    spx = get_index_return("^GSPC")
    kospi = get_index_return("^KS11")
    kosdaq = get_index_return("^KQ11")
    vix = get_index_return("^VIX")

    krx_date = effective_krx_close_date()
    flow = kospi_flow(krx_date)

    risk_hits = 0
    if not is_nan(kospi) and kospi <= -1.5:
        risk_hits += 1
    if not is_nan(kosdaq) and kosdaq <= -1.8:
        risk_hits += 1
    if not is_nan(vix) and vix >= 6.0:
        risk_hits += 1

    flow_line = ""
    if flow:
        f = flow.get("foreign", float("nan"))
        i = flow.get("inst", float("nan"))
        flow_line = f"- KOSPI 수급(확정 기준일, {krx_date}): 외국인 {fmt_bn_krw(f)} / 기관 {fmt_bn_krw(i)}"
        if (not is_nan(f)) and (not is_nan(i)) and f < 0 and i < 0:
            risk_hits += 1

    if risk_hits >= 2:
        level = "bad"
        comment = "오늘은 시장이 방어적으로 보여요. 국내 추천은 쉬고, 신규 진입은 보수적으로 가는 게 좋아요."
        why = []
        if not is_nan(kospi) and kospi <= -1.5:
            why.append("코스피 급락")
        if not is_nan(kosdaq) and kosdaq <= -1.8:
            why.append("코스닥 급락")
        if not is_nan(vix) and vix >= 6.0:
            why.append("VIX 급등")
        if flow and (not is_nan(flow.get("foreign", float("nan")))) and (not is_nan(flow.get("inst", float("nan")))):
            if flow["foreign"] < 0 and flow["inst"] < 0:
                why.append("외국인·기관 동반 순매도")
        reason = f"추천 보류 이유: {', '.join(why) if why else '복합 리스크'}"
    elif risk_hits == 1:
        level = "meh"
        comment = "시장 분위기가 예민할 수 있어요. 국내 추천은 0~2개로 엄선하거나 대기하는 게 편합니다."
        reason = "추천 제한 이유: 시장 신호가 일부 불안정"
    else:
        level = "good"
        comment = "전반 분위기는 무난해요. 조건 맞는 종목은 선별적으로 접근 가능합니다."
        reason = ""

    lines = [
        "📈 시장 요약",
        f"- 나스닥: {fmt_pct(nasdaq)} | S&P500: {fmt_pct(spx)}",
        f"- 코스피: {fmt_pct(kospi)} | 코스닥: {fmt_pct(kosdaq)}",
        f"- VIX: {fmt_pct(vix)}",
    ]
    if flow_line:
        lines.append(flow_line)
    lines.append(f"🧭 코멘트: {comment}")
    if reason:
        lines.append(f"🧾 {reason}")
    return "\n".join(lines), level


# =========================
# Technical / Signals
# =========================
def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / (loss.replace(0, 1e-9))
    return 100 - (100 / (1 + rs))


def rolling_high(series: pd.Series, window: int = 63) -> float:
    try:
        return float(series.tail(window).max())
    except Exception:
        return float("nan")


def exit_signals_302020(close, ma20, rsi_v, chg5d, high_3m):
    flags = []
    dist20 = float("nan")

    if (not is_nan(rsi_v)) and rsi_v >= 70:
        flags.append("RSI≥70")

    if (not is_nan(ma20)) and ma20 != 0 and (not is_nan(close)):
        dist20 = (close / ma20 - 1.0) * 100.0
        if dist20 >= 6:
            flags.append("20일선+6%↑")

    if (not is_nan(chg5d)) and chg5d >= 12:
        flags.append("5D+12%↑")

    if (not is_nan(high_3m)) and (not is_nan(close)) and high_3m != 0:
        if close >= high_3m * 0.98:
            flags.append("3개월고점근처")

    n = len(flags)

    if n >= 3 and (
        ((not is_nan(rsi_v)) and rsi_v >= 80)
        or ((not is_nan(chg5d)) and chg5d >= 15)
        or ("3개월고점근처" in flags and (not is_nan(dist20)) and dist20 >= 9)
    ):
        action = "✅ 3차 익절(추가20%, 총80%) 후보"
    elif n >= 3:
        action = "✅ 2차 익절(추가30%, 총60%) 후보"
    elif n >= 2:
        action = "✅ 1차 익절(30%) 후보"
    else:
        action = "⏸ 보유/대기(익절 신호 부족)"

    return action, (", ".join(flags) if flags else "해당 없음"), dist20


def entry_plan_by_ma(close, ma20, market_level, currency="$"):
    if market_level == "bad":
        return "신규: 시장 bad → 신규 진입은 쉬어가는 게 확률이 좋아요."
    if is_nan(close) or is_nan(ma20) or ma20 == 0:
        return "신규: 데이터 부족(지표 계산 불가) → 무리하지 말고 흐름만 확인해요."

    low = ma20 * 0.98
    high = ma20 * 1.02

    if close > high:
        return f"신규: 20일선 위로 멀어요 → 추격보단 {currency}{low:.2f}~{currency}{high:.2f}(20일선 근처) 대기가 편해요."
    if close < low:
        return f"신규: 20일선 아래예요 → 들어가도 {currency}{low:.2f}~{currency}{high:.2f} 구간 분할로 천천히가 좋아요."
    return f"신규: 20일선 근처({currency}{low:.2f}~{currency}{high:.2f}) → 분할 진입 후보입니다."


# =========================
# US snapshot + news
# =========================
def us_snapshot(ticker: str) -> dict | None:
    try:
        data = yf.Ticker(ticker).history(period="3mo").dropna()
        if data.empty or len(data) < 25:
            return None

        close = float(data["Close"].iloc[-1])
        prev = float(data["Close"].iloc[-2])
        chg1d = (close / prev - 1.0) * 100.0 if prev != 0 else float("nan")

        chg5d = float("nan")
        if len(data) >= 6:
            c5 = float(data["Close"].iloc[-6])
            chg5d = (close / c5 - 1.0) * 100.0 if c5 != 0 else float("nan")

        ma20 = float(data["Close"].rolling(20).mean().iloc[-1])
        rsi_v = float(rsi(data["Close"]).iloc[-1])
        high_3m = rolling_high(data["Close"], window=63)

        return {
            "close": close,
            "chg1d": chg1d,
            "chg5d": chg5d,
            "ma20": ma20,
            "rsi": rsi_v,
            "high_3m": high_3m,
        }
    except Exception:
        logger.exception("us snapshot failed: %s", ticker)
        return None


def build_us_news_bullets(name: str, tkr: str, limit_google=3, limit_sec=2) -> list[str]:
    bullets = []

    for title, link in fetch_rss(google_news_rss(f"{tkr} {name}"), limit=limit_google):
        bullets.append(f"[GOOGLE] {title} - {link}")

    sec_url = SEC_8K_ATOM.get(tkr)
    if sec_url:
        for title, link in fetch_rss(sec_url, limit=limit_sec):
            bullets.append(f"[SEC 8-K] {title} - {link}")

    return uniq_keep_order(bullets)


# =========================
# KR helpers
# =========================
@lru_cache(maxsize=1024)
def kr_name(code: str) -> str:
    try:
        return stock.get_market_ticker_name(code)
    except Exception:
        logger.exception("kr name lookup failed: %s", code)
        return code


def kr_price_batch(date: str, market: str) -> pd.DataFrame:
    try:
        df = stock.get_market_ohlcv(date, market=market)
        if df is None or df.empty:
            return pd.DataFrame()
        return df
    except Exception:
        logger.exception("kr price batch failed: %s %s", date, market)
        return pd.DataFrame()


def kr_fundamental_batch(date: str, market: str) -> pd.DataFrame:
    try:
        df = stock.get_market_fundamental(date, market=market)
        if df is None or df.empty:
            return pd.DataFrame()
        return df
    except Exception:
        logger.exception("kr fundamental batch failed: %s %s", date, market)
        return pd.DataFrame()


def kr_recent_history(code: str, end_date: str, days: int = 65) -> pd.DataFrame:
    try:
        bdays = recent_business_days(end_date, days)
        if not bdays:
            return pd.DataFrame()
        start = bdays[0]
        df = stock.get_market_ohlcv_by_date(start, end_date, code)
        if df is None or df.empty:
            return pd.DataFrame()
        return df
    except Exception:
        logger.exception("kr recent history failed: %s", code)
        return pd.DataFrame()


def kr_one_day_change(code: str, date: str) -> tuple[float, float]:
    hist = kr_recent_history(code, date, days=3)
    if hist.empty or len(hist) < 2:
        return float("nan"), float("nan")

    close = safe_float(hist["종가"].iloc[-1])
    prev = safe_float(hist["종가"].iloc[-2])
    chg1d = (close / prev - 1.0) * 100.0 if prev and prev == prev else float("nan")
    return close, chg1d


def kr_core_block(market_level: str):
    date = effective_krx_close_date()
    blocks = ["🇰🇷 국내 핵심(보유/관심)"]

    markets = {
        "KOSPI": kr_price_batch(date, "KOSPI"),
        "KOSDAQ": kr_price_batch(date, "KOSDAQ"),
    }
    funds = {
        "KOSPI": kr_fundamental_batch(date, "KOSPI"),
        "KOSDAQ": kr_fundamental_batch(date, "KOSDAQ"),
    }

    for label, code in KR_CORE.items():
        market = "KOSPI" if code in markets["KOSPI"].index else "KOSDAQ"
        price_df = markets.get(market, pd.DataFrame())
        fund_df = funds.get(market, pd.DataFrame())

        close = float("nan")
        chg1d = float("nan")
        if code in price_df.index:
            close = safe_float(price_df.loc[code, "종가"])
            _, chg1d = kr_one_day_change(code, date)

        per = float("nan")
        eps = float("nan")
        if code in fund_df.index:
            per = safe_float(fund_df.loc[code, "PER"])
            eps = safe_float(fund_df.loc[code, "EPS"])

        if is_nan(close):
            blocks.append(f"\n• {label}\n  - 데이터 수신이 불안정해서 오늘은 국내 가격을 못 불러왔어요.")
            continue

        blocks.append(
            f"\n• {label}\n"
            f"  - 종가: ₩{fmt_int(close)} | 1D: {fmt_pct(chg1d)}\n"
            f"  - PER: {('N/A' if is_nan(per) else f'{per:.1f}')} | EPS: {('N/A' if is_nan(eps) else f'{eps:,.0f}')}"
        )

    return "\n".join(blocks), date


# =========================
# DART (optional)
# =========================
def corp_code_cache_path() -> Path:
    return CACHE_DIR / "dart_corp_codes.json"


def load_corp_code_cache(max_age_days: int = 30) -> dict[str, str]:
    path = corp_code_cache_path()
    if not path.exists():
        return {}
    try:
        age = (dt.datetime.now() - dt.datetime.fromtimestamp(path.stat().st_mtime)).days
        if age > max_age_days:
            return {}
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.exception("corp code cache load failed")
        return {}


def save_corp_code_cache(data: dict[str, str]) -> None:
    try:
        corp_code_cache_path().write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception:
        logger.exception("corp code cache save failed")


def get_dart_corp_code_map() -> dict[str, str]:
    if not DART_API_KEY:
        return {}

    cached = load_corp_code_cache()
    if cached:
        return cached

    try:
        url = f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={DART_API_KEY}"
        r = SESSION.get(url, timeout=30)
        r.raise_for_status()

        z = zipfile.ZipFile(io.BytesIO(r.content))
        xml_bytes = z.read("CORPCODE.xml")
        root = ET.fromstring(xml_bytes)

        mapping = {}
        for item in root.findall("list"):
            stock_code = (item.findtext("stock_code") or "").strip()
            corp_code = (item.findtext("corp_code") or "").strip()
            if stock_code and corp_code:
                mapping[stock_code] = corp_code

        if mapping:
            save_corp_code_cache(mapping)
        return mapping
    except Exception:
        logger.exception("dart corp code map failed")
        return {}


def dart_find_corp_code(stock_code: str) -> str | None:
    return get_dart_corp_code_map().get(stock_code)


def dart_recent_disclosures(corp_code: str, limit=3) -> list[tuple[str, str]]:
    if not DART_API_KEY or not corp_code:
        return []
    try:
        end = kst_now().strftime("%Y%m%d")
        start = (kst_now() - dt.timedelta(days=7)).strftime("%Y%m%d")
        url = (
            "https://opendart.fss.or.kr/api/list.json"
            f"?crtfc_key={DART_API_KEY}&corp_code={corp_code}"
            f"&bgn_de={start}&end_de={end}&page_no=1&page_count=10"
        )
        r = SESSION.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()
        items = data.get("list", [])[:limit]

        out = []
        for it in items:
            title = (it.get("report_nm") or "").strip()
            rcp_no = (it.get("rcept_no") or "").strip()
            if title and rcp_no:
                link = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcp_no}"
                out.append((title, link))
        return out
    except Exception:
        logger.exception("dart disclosure fetch failed: %s", corp_code)
        return []


def build_dart_bullets_for_core() -> list[str]:
    if not DART_API_KEY:
        return []

    bullets = []
    for _, code in KR_CORE.items():
        corp = kr_name(code)
        corp_code = dart_find_corp_code(code)
        if not corp_code:
            continue
        for title, link in dart_recent_disclosures(corp_code, limit=3):
            bullets.append(f"[DART] {corp}: {title} - {link}")
    return uniq_keep_order(bullets)


# =========================
# KR recommendations
# =========================
def build_universe_top_caps(date: str, n_each: int = 60) -> pd.DataFrame:
    frames = []
    for market in ["KOSPI", "KOSDAQ"]:
        try:
            cap = stock.get_market_cap(date, market=market)
            if cap is None or cap.empty:
                continue
            cap = cap.sort_values("시가총액", ascending=False).head(n_each).copy()
            cap["market"] = market
            frames.append(cap)
        except Exception:
            logger.exception("market cap batch failed: %s %s", date, market)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, axis=0)


def candidate_base_score(code: str, date: str, fund_row: pd.Series | None) -> dict | None:
    hist = kr_recent_history(code, date, days=65)
    time.sleep(PYKRX_SLEEP_SEC)

    if hist.empty or len(hist) < 20:
        return None

    close = safe_float(hist["종가"].iloc[-1])
    ma20 = safe_float(hist["종가"].rolling(20).mean().iloc[-1])

    c5 = safe_float(hist["종가"].iloc[-6]) if len(hist) >= 6 else float("nan")
    mom5 = (close / c5 - 1.0) * 100.0 if (not is_nan(c5)) and c5 != 0 else float("nan")
    dist20 = (close / ma20 - 1.0) * 100.0 if (not is_nan(ma20)) and ma20 != 0 else float("nan")

    if (not is_nan(dist20)) and dist20 >= 12:
        return None
    if (not is_nan(mom5)) and mom5 >= 18:
        return None

    eps = safe_float(fund_row.get("EPS")) if fund_row is not None else float("nan")
    per = safe_float(fund_row.get("PER")) if fund_row is not None else float("nan")
    pbr = safe_float(fund_row.get("PBR")) if fund_row is not None else float("nan")

    if is_nan(eps) or eps <= 0:
        return None

    score = 0.0
    if not is_nan(mom5):
        score += mom5 * 1.1
    if not is_nan(dist20):
        score += max(min(dist20, 6), -6) * 0.7

    if not is_nan(per):
        if per >= 60:
            score -= 5.0
        elif per >= 35:
            score -= 2.0

    if not is_nan(pbr) and pbr >= 8:
        score -= 1.5

    return {
        "code": code,
        "close": close,
        "mom5": mom5,
        "dist20": dist20,
        "eps": eps,
        "per": per,
        "pbr": pbr,
        "score": score,
        "ma20": ma20,
        "flow": None,
    }


def kr_investor_flow_by_ticker(code: str, date: str) -> dict | None:
    try:
        df = stock.get_market_trading_value_by_investor(date, date, code)
        return parse_investor_flow_df(df)
    except Exception:
        logger.exception("ticker investor flow failed: %s %s", code, date)
        return None


def apply_flow_adjustment(cands: list[dict], date: str) -> list[dict]:
    out = []
    for c in cands:
        flow = kr_investor_flow_by_ticker(c["code"], date)
        time.sleep(PYKRX_SLEEP_SEC)

        bonus = 0.0
        if flow:
            foreign = flow.get("foreign", float("nan"))
            inst = flow.get("inst", float("nan"))
            if (not is_nan(foreign)) and (not is_nan(inst)):
                if foreign < 0 and inst < 0:
                    bonus -= 3.5
                elif foreign > 0 and inst > 0:
                    bonus += 1.5

        c = dict(c)
        c["flow"] = flow
        c["score"] = safe_float(c["score"], 0.0) + bonus
        out.append(c)

    return out


def kr_recommendations(market_level: str, max_picks=3):
    date = effective_krx_close_date()

    if market_level == "bad":
        return {
            "date": date,
            "picks": [],
            "text": (
                "❌ 오늘 국내 추천 없음\n"
                "시장 신호가 방어적이라 신규 진입 확률이 낮습니다.\n"
                "→ 선배 전략: 신규는 쉬고, 보유 종목은 과열이면 일부 정리로 편하게 가요."
            ),
        }

    uni = build_universe_top_caps(date, n_each=KR_UNIVERSE_EACH)
    if uni.empty:
        return {
            "date": date,
            "picks": [],
            "text": "📌 국내 추천: 오늘은 데이터 수신이 불안정해서 쉬어갈게요.",
        }

    fund_all = pd.concat(
        [
            kr_fundamental_batch(date, "KOSPI"),
            kr_fundamental_batch(date, "KOSDAQ"),
        ],
        axis=0,
    )

    candidates = []
    for code in uni.index.tolist():
        fund_row = fund_all.loc[code] if code in fund_all.index else None
        c = candidate_base_score(code, date, fund_row)
        if c:
            candidates.append(c)

    if not candidates:
        return {
            "date": date,
            "picks": [],
            "text": (
                "📌 국내 추천: 오늘은 조건을 만족하는 후보가 없어서 쉬어갈게요.\n"
                "(흑자 + 과열아님 + 추세 조건을 동시에 만족하는 종목이 부족했습니다.)"
            ),
        }

    pre_top = (
        pd.DataFrame(candidates)
        .sort_values(["score", "mom5"], ascending=[False, False])
        .head(KR_TOP_PRE_FLOW)
        .to_dict(orient="records")
    )

    rescored = apply_flow_adjustment(pre_top, date)
    df = pd.DataFrame(rescored).sort_values(["score", "mom5"], ascending=[False, False])

    pick_n = 2 if market_level == "meh" else max_picks
    picks = df.head(pick_n).to_dict(orient="records")

    if not picks:
        return {
            "date": date,
            "picks": [],
            "text": "📌 국내 추천: 오늘은 최종 후보가 부족해서 쉬어갈게요.",
        }

    lines = ["🔥 오늘의 국내 추천 (조건 충족 시만)"]
    for i, p in enumerate(picks, start=1):
        code = p["code"]
        name = kr_name(code)

        flow_note = ""
        flow = p.get("flow")
        if flow:
            f = flow.get("foreign", float("nan"))
            inst = flow.get("inst", float("nan"))
            flow_note = f" | 수급(확정 기준일): 외국인 {fmt_bn_krw(f)}, 기관 {fmt_bn_krw(inst)}"

        close = p["close"]
        ma20 = p.get("ma20", float("nan"))
        entry = ""
        if (not is_nan(close)) and (not is_nan(ma20)) and ma20 != 0:
            entry_low = ma20 * 0.98
            entry_high = ma20 * 1.02
            stop = ma20 * 0.96
            tp1 = close * 1.10
            tp2 = close * 1.18
            entry = (
                f"  - 진입(가이드): ₩{fmt_int(entry_low)}~₩{fmt_int(entry_high)} 분할 | "
                f"리스크: ₩{fmt_int(stop)} 이탈 시 보수적 | "
                f"익절(가이드): 1차 ₩{fmt_int(tp1)}, 2차 ₩{fmt_int(tp2)}"
            )

        lines.append(
            f"\n{i}. {name} ({code})\n"
            f"  - 종가: ₩{fmt_int(close)} | 5D: {fmt_pct(p['mom5'])} | 20일선 대비: {fmt_pct(p['dist20'])}\n"
            f"  - PER: {('N/A' if is_nan(p['per']) else format(p['per'], '.1f'))} | "
            f"EPS: {('N/A' if is_nan(p['eps']) else format(p['eps'], ',.0f'))}{flow_note}\n"
            f"{entry}"
        )

    return {"date": date, "picks": picks, "text": "\n".join(lines)}


def kr_reco_news_bullets(picks, limit_each=2) -> list[str]:
    bullets = []
    for p in picks:
        code = p["code"]
        name = kr_name(code)
        for title, link in fetch_rss(google_news_rss(f"{name} {code}"), limit=limit_each):
            bullets.append(f"[GOOGLE] {name}: {title} - {link}")
    return uniq_keep_order(bullets)


# =========================
# Main
# =========================
def main():
    now = kst_now()
    if "your_email@example.com" in HTTP_USER_AGENT:
        logger.warning("SEC 호출용 HTTP_USER_AGENT에 실제 연락처를 넣는 것을 권장합니다.")

    header = f"📌 데일리 브리핑 (KST {now:%Y-%m-%d %H:%M})"
    usdkrw = get_usdkrw()
    fxline = f"💱 USD/KRW: {usdkrw:,.2f}"

    market_text, market_level = market_brief()

    # ---------- US section ----------
    us_lines = ["🇺🇸 미국 3종목 (원화환산 + 기술지표 + 오늘 액션)"]
    us_ai_blocks = []

    for name, tkr in US_TICKERS.items():
        s = us_snapshot(tkr)
        if not s:
            us_lines.append(f"\n• {name}\n  - 데이터 수신이 불안정해서 오늘은 가격을 못 불러왔어요.")
            continue

        close = s["close"]
        krw_price = close * usdkrw
        action, flags_txt, dist20 = exit_signals_302020(
            close=close,
            ma20=s["ma20"],
            rsi_v=s["rsi"],
            chg5d=s["chg5d"],
            high_3m=s["high_3m"],
        )
        entry_txt = entry_plan_by_ma(close, s["ma20"], market_level, currency="$")

        us_lines.append(
            f"\n• {name}\n"
            f"  - 종가: ${close:.2f} (₩{krw_price:,.0f})\n"
            f"  - 1D: {fmt_pct(s['chg1d'])} | 5D: {fmt_pct(s['chg5d'])}\n"
            f"  - 20일선 대비: {fmt_pct(dist20)} | RSI: {('N/A' if is_nan(s['rsi']) else format(s['rsi'], '.0f'))}\n"
            f"  - 익절 신호: {flags_txt}\n"
            f"  - 오늘 액션: {action}\n"
            f"  - {entry_txt}"
        )

        bullets = build_us_news_bullets(name, tkr, limit_google=3, limit_sec=2)
        if bullets:
            us_lines.append("  - 뉴스 링크:")
            for b in bullets[:5]:
                us_lines.append("    • " + b.split("] ", 1)[-1])
            us_ai_blocks.append(ai_summarize(f"{name} (미국)", bullets))

    us_text = "\n".join(us_lines)
    us_ai_text = (
        "\n\n".join(["🤖 미국 뉴스/공시 AI 요약"] + us_ai_blocks)
        if us_ai_blocks
        else "🤖 미국 뉴스/공시 AI 요약\nAI 요약: (요약할 자료가 부족했어요.)"
    )

    # ---------- KR core ----------
    kr_core_text, kr_date = kr_core_block(market_level)

    # ---------- KR DART ----------
    dart_bullets = build_dart_bullets_for_core() if DART_API_KEY else []
    dart_ai_text = "🤖 국내 공시(DART) AI 요약\n" + ai_summarize("국내 공시(DART)", dart_bullets)

    # ---------- KR recommendations ----------
    reco = kr_recommendations(market_level, max_picks=3)
    kr_reco_text = reco["text"]
    kr_picks = reco["picks"]

    kr_reco_ai_text = ""
    if kr_picks:
        kr_news_bullets = kr_reco_news_bullets(kr_picks, limit_each=2)
        links_lines = ["\n📰 추천 종목 뉴스 링크"]
        for b in kr_news_bullets[:8]:
            links_lines.append("• " + b.split("] ", 1)[-1])
        kr_reco_ai_text = (
            "\n\n🤖 국내 추천주 뉴스 AI 요약\n"
            + ai_summarize("국내 추천주(뉴스)", kr_news_bullets)
            + "\n"
            + "\n".join(links_lines)
        )

    guide = (
        "\n\n🧭 선배 익절 전략(확정)\n"
        "- 신호 2개↑: 1차 익절 30%\n"
        "- 신호 3개↑: 2차 익절 추가 30%(총 60%)\n"
        "- 신호 3개 + 강과열: 3차 익절 추가 20%(총 80%)\n"
        "- 잔여 20%는 추세 추종(무리한 추격 금지)"
    )

    edu = (
        "\n\n📚 오늘의 매매 타이밍 원칙\n"
        "- 시장 bad면 신규는 쉬는 게 확률이 좋아요.\n"
        "- 종목은 ‘20일선 근처(±2%)’에서 분할 진입이 가장 편합니다.\n"
        "- 익절은 수익률이 아니라 ‘과열 신호’로 판단하면 흔들림이 줄어요."
    )

    footer = f"\n\n🗓 국내 데이터 기준일: {kr_date} (오후 6시 이전 실행 시 전 영업일 기준)"

    msg = (
        header + "\n\n"
        + fxline + "\n\n"
        + market_text + "\n\n"
        + us_text + "\n\n"
        + us_ai_text + "\n\n"
        + kr_core_text + "\n\n"
        + dart_ai_text + "\n\n"
        + kr_reco_text
        + (kr_reco_ai_text if kr_reco_ai_text else "")
        + guide
        + edu
        + footer
    )

    telegram_send(msg)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("daily briefing failed")
        raise
