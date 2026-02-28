import os
import datetime as dt
import requests
import feedparser
import pandas as pd
import yfinance as yf
from pykrx import stock

from openai import OpenAI

# =========================
# ENV
# =========================
TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
DART_API_KEY = os.environ.get("DART_API_KEY", "")  # 없으면 국내 공시 요약 생략

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# =========================
# CONFIG
# =========================
US_TICKERS = {
    "엔비디아(NVDA)": "NVDA",
    "테슬라(TSLA)": "TSLA",
    "팔란티어(PLTR)": "PLTR",
}

KR_CORE = {
    "SK하이닉스(000660)": "000660",
}

# SEC 8-K Atom (미국 공시)
SEC_8K_ATOM = {
    "NVDA": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0001045810&type=8-K&owner=exclude&count=20&output=atom",
    "TSLA": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0001318605&type=8-K&owner=exclude&count=20&output=atom",
    "PLTR": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0001321655&type=8-K&owner=exclude&count=20&output=atom",
}

# =========================
# Utils
# =========================
def kst_now():
    return dt.datetime.utcnow() + dt.timedelta(hours=9)

def is_nan(x):
    return x != x

def safe_float(x, default=float("nan")):
    try:
        return float(x)
    except Exception:
        return default

def fmt_pct(x):
    return "N/A" if is_nan(x) else f"{x:+.2f}%"

def fmt_int(x):
    try:
        return f"{int(round(float(x))):,}"
    except Exception:
        return "N/A"

def fmt_bn_krw(x):
    # 원 단위 -> 십억 원
    try:
        v = float(x) / 1_000_000_000.0
        return f"{v:+.1f}십억"
    except Exception:
        return "N/A"

# =========================
# Telegram (split)
# =========================
def telegram_send(text: str):
    # Telegram limit ~4096. Use safe split.
    MAX_LEN = 3800
    parts = []
    t = text

    while len(t) > MAX_LEN:
        cut = t.rfind("\n", 0, MAX_LEN)
        if cut < 800:
            cut = MAX_LEN
        parts.append(t[:cut])
        t = t[cut:].lstrip("\n")

    parts.append(t)

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    for p in parts:
        r = requests.post(
            url,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": p, "disable_web_page_preview": True},
            timeout=30,
        )
        print("TG_STATUS:", r.status_code)
        print("TG_RESP:", (r.text or "")[:300])
        r.raise_for_status()

# =========================
# RSS helpers
# =========================
def google_news_rss(query: str):
    q = requests.utils.quote(query)
    return f"https://news.google.com/rss/search?q={q}&hl=ko&gl=KR&ceid=KR:ko"

def fetch_rss(url: str, limit: int = 3):
    try:
        feed = feedparser.parse(url)
        out = []
        for e in (feed.entries or [])[:limit]:
            title = (getattr(e, "title", "") or "").strip()
            link = (getattr(e, "link", "") or "").strip()
            if title:
                out.append((title, link))
        return out
    except Exception:
        return []

# =========================
# OpenAI summarizer (news only)
# =========================
def ai_summarize_news(bundle_title: str, bullets: list[str]) -> str:
    """
    bullets: ["[SRC] title - link", ...]
    """
    if not client:
        return "AI 요약: (OPENAI_API_KEY가 없어 요약을 생략했어요.)"

    model = "gpt-5-mini"

    prompt = (
        f"다음은 '{bundle_title}' 관련 최신 뉴스/공시 헤드라인 목록이야.\n"
        f"한국어로, 투자 초보도 이해할 수 있게 요약해줘.\n"
        f"규칙:\n"
        f"- 4~6줄 요약\n"
        f"- 긍정 1줄, 리스크 1줄, 오늘 체크포인트 1줄 포함\n"
        f"- 과장/확정적 예언 금지, 가능성 표현 사용\n\n"
        f"헤드라인:\n" + "\n".join(bullets)
    )

    try:
        resp = client.responses.create(
            model=model,
            input=[
                {"role": "system", "content": "너는 신중하고 사실 기반의 투자 뉴스 요약가야."},
                {"role": "user", "content": prompt},
            ],
        )
        text = getattr(resp, "output_text", None)
        if text:
            return "AI 요약:\n" + text.strip()

        # fallback
        d = resp.to_dict() if hasattr(resp, "to_dict") else {}
        out_text = ""
        for item in d.get("output", []):
            for c in item.get("content", []):
                if c.get("type") in ("output_text", "text"):
                    out_text += c.get("text", "")
        out_text = out_text.strip()
        return "AI 요약:\n" + (out_text if out_text else "(요약 결과를 읽지 못했어요.)")
    except Exception as e:
        return f"AI 요약: (요약 중 오류로 생략했어요: {type(e).__name__})"

# =========================
# Market & FX
# =========================
def get_usdkrw(default=1350.0):
    try:
        fx = yf.Ticker("KRW=X").history(period="10d").dropna()
        if fx.empty:
            return default
        return float(fx["Close"].iloc[-1])
    except Exception:
        return default

def get_index_return(ticker: str):
    try:
        h = yf.Ticker(ticker).history(period="10d").dropna()
        if len(h) < 2:
            return float("nan")
        close = float(h["Close"].iloc[-1])
        prev = float(h["Close"].iloc[-2])
        return (close / prev - 1.0) * 100.0
    except Exception:
        return float("nan")

def nearest_krx_day():
    today = kst_now().strftime("%Y%m%d")
    return stock.get_nearest_business_day_in_a_week(today)

def market_flow_kospi(date: str):
    """KOSPI 전체 외국인/기관 순매수(거래대금). 실패하면 None."""
    try:
        df = stock.get_market_trading_value_by_investor(date, date, market="KOSPI")
        if df is None or df.empty:
            return None

        # case 1: index investor, columns include '순매수'
        if "순매수" in df.columns:
            def pick_row(names):
                for n in names:
                    for idx in df.index:
                        if n in str(idx):
                            return safe_float(df.loc[idx, "순매수"])
                return float("nan")

            foreign = pick_row(["외국인"])
            inst = pick_row(["기관합계", "기관"])
            if not is_nan(foreign) or not is_nan(inst):
                return {"foreign": foreign, "inst": inst}

        # case 2: columns investor, index includes 순매수
        if "외국인" in df.columns and "기관합계" in df.columns:
            for idx in df.index:
                if "순매수" in str(idx):
                    return {
                        "foreign": safe_float(df.loc[idx, "외국인"]),
                        "inst": safe_float(df.loc[idx, "기관합계"]),
                    }
        return None
    except Exception:
        return None

def market_brief():
    nasdaq = get_index_return("^IXIC")
    spx = get_index_return("^GSPC")
    kospi = get_index_return("^KS11")
    kosdaq = get_index_return("^KQ11")
    vix = get_index_return("^VIX")

    date = nearest_krx_day()
    flow = market_flow_kospi(date)

    risk_hits = 0
    if (not is_nan(kospi)) and kospi <= -1.5:
        risk_hits += 1
    if (not is_nan(kosdaq)) and kosdaq <= -1.8:
        risk_hits += 1
    if (not is_nan(vix)) and vix >= 6.0:
        risk_hits += 1

    flow_line = ""
    if flow:
        f = flow.get("foreign", float("nan"))
        i = flow.get("inst", float("nan"))
        flow_line = f"- KOSPI 수급(전일, {date}): 외국인 {fmt_bn_krw(f)} / 기관 {fmt_bn_krw(i)}"
        if (not is_nan(f)) and (not is_nan(i)) and (f < 0) and (i < 0):
            risk_hits += 1

    if risk_hits >= 2:
        level = "bad"
        comment = "오늘은 시장이 방어적으로 보여요. 국내 신규 추천은 쉬고, 현금 비중이 유리한 날입니다."
    elif risk_hits == 1:
        level = "meh"
        comment = "시장 분위기가 예민할 수 있어요. 신규는 소수만(또는 대기), 분할 접근이 편합니다."
    else:
        level = "good"
        comment = "전반 분위기는 무난해요. 조건 맞는 종목은 선별적으로 접근 가능합니다."

    lines = [
        "📈 시장 요약",
        f"- 나스닥: {fmt_pct(nasdaq)} | S&P500: {fmt_pct(spx)}",
        f"- 코스피: {fmt_pct(kospi)} | 코스닥: {fmt_pct(kosdaq)}",
        f"- VIX: {fmt_pct(vix)}",
    ]
    if flow_line:
        lines.append(flow_line)
    lines.append(f"🧭 코멘트: {comment}")
    return "\n".join(lines), level

# =========================
# Technical indicators + exit strategy (30/30/20/20)
# =========================
def rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / (loss.replace(0, 1e-9))
    return 100 - (100 / (1 + rs))

def rolling_high(series, window=63):  # ~3개월
    try:
        return float(series.tail(window).max())
    except Exception:
        return float("nan")

def exit_signals(close, ma20, rsi_v, chg5d, high_3m):
    """
    선배 확정 규칙(신호 기반):
    - 1차 익절(30%): 신호 2개↑
    - 2차(추가 30%): 신호 3개↑
    - 3차(추가 20%): 신호 3개↑ + 강과열 조건
    신호:
      1) RSI>=70
      2) 20일선 대비 +6%↑
      3) 5D>=+12%
      4) 3개월 고점 근처(고점의 -2% 이내)
    """
    flags = []

    # 1) RSI
    if (not is_nan(rsi_v)) and rsi_v >= 70:
        flags.append("RSI≥70")

    # 2) dist20
    dist20 = float("nan")
    if (not is_nan(ma20)) and ma20 != 0 and (not is_nan(close)):
        dist20 = (close / ma20 - 1.0) * 100.0
        if dist20 >= 6:
            flags.append("20일선+6%↑")

    # 3) 5D
    if (not is_nan(chg5d)) and chg5d >= 12:
        flags.append("5D+12%↑")

    # 4) near 3m high
    if (not is_nan(high_3m)) and (not is_nan(close)) and high_3m != 0:
        if close >= high_3m * 0.98:
            flags.append("3개월고점근처")

    n = len(flags)

    action = "대기/보유"
    stage = "—"
    if n >= 2:
        action = "1차 익절(30%) 후보"
        stage = "1차"
    if n >= 3:
        action = "2차 익절(추가30%, 총60%) 후보"
        stage = "2차"
    if n >= 3 and (
        ((not is_nan(rsi_v)) and rsi_v >= 80) or
        ((not is_nan(chg5d)) and chg5d >= 15) or
        ("3개월고점근처" in flags and (not is_nan(dist20)) and dist20 >= 9)
    ):
        action = "3차 익절(추가20%, 총80%) 후보"
        stage = "3차"

    flags_txt = ", ".join(flags) if flags else "해당 없음"
    return stage, action, flags_txt, dist20

def entry_plan_us(close, ma20, market_level):
    """
    진입은 20일선 근처(±2%) 중심의 분할 진입 가이드.
    시장 bad면 신규는 보류.
    """
    if market_level == "bad":
        return "신규: 시장 bad → 신규 진입은 쉬어가는 게 확률이 좋아요."
    if is_nan(close) or is_nan(ma20) or ma20 == 0:
        return "신규: 데이터 부족으로 오늘은 무리하지 말고 흐름만 확인해요."

    low = ma20 * 0.98
    high = ma20 * 1.02
    if close > high:
        return f"신규: 20일선 위로 멀어요 → 추격보단 ${low:.2f}~${high:.2f}(20일선 근처) 대기가 편해요."
    if close < low:
        return f"신규: 20일선 아래예요 → 들어가도 ${low:.2f}~${high:.2f} 구간 분할로 천천히가 좋아요."
    return f"신규: 20일선 근처(${low:.2f}~${high:.2f}) → 분할 진입 후보입니다."

# =========================
# US snapshot + news bullets
# =========================
def us_snapshot(ticker: str):
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
        dist20 = (close / ma20 - 1.0) * 100.0 if ma20 != 0 else float("nan")
        rsi_v = float(rsi(data["Close"]).iloc[-1])

        high_3m = rolling_high(data["Close"], window=63)

        return {
            "close": close,
            "chg1d": chg1d,
            "chg5d": chg5d,
            "ma20": ma20,
            "dist20": dist20,
            "rsi": rsi_v,
            "high_3m": high_3m,
        }
    except Exception:
        return None

def build_us_news_bullets(name: str, tkr: str, limit_news=3, limit_sec=2):
    bullets = []
    for title, link in fetch_rss(google_news_rss(f"{tkr} {name}"), limit=limit_news):
        bullets.append(f"[GOOGLE] {title} - {link}")
    sec_url = SEC_8K_ATOM.get(tkr)
    if sec_url:
        for title, link in fetch_rss(sec_url, limit=limit_sec):
            bullets.append(f"[SEC 8-K] {title} - {link}")
    return bullets

# =========================
# KR core
# =========================
def kr_name(code: str):
    try:
        return stock.get_market_ticker_name(code)
    except Exception:
        return code

def kr_ohlcv(code: str, date: str):
    try:
        df = stock.get_market_ohlcv_by_date(date, date, code)
        if df is None or df.empty:
            return None
        close = safe_float(df.iloc[-1]["종가"])

        prev_days = stock.get_previous_business_days(date, 1)
        if not prev_days:
            return {"close": close, "chg1d": float("nan")}
        prev_day = prev_days[0]
        df2 = stock.get_market_ohlcv_by_date(prev_day, prev_day, code)
        if df2 is None or df2.empty:
            return {"close": close, "chg1d": float("nan")}
        prev = safe_float(df2.iloc[-1]["종가"])
        chg1d = (close / prev - 1.0) * 100.0 if prev and prev == prev else float("nan")
        return {"close": close, "chg1d": chg1d}
    except Exception:
        return None

def kr_fundamental(code: str, date: str):
    try:
        f = stock.get_market_fundamental_by_date(date, date, code)
        if f is None or f.empty:
            return None
        row = f.iloc[-1]
        return {
            "per": safe_float(row.get("PER", float("nan"))),
            "eps": safe_float(row.get("EPS", float("nan"))),
            "pbr": safe_float(row.get("PBR", float("nan"))),
        }
    except Exception:
        return None

def kr_core_block():
    date = nearest_krx_day()
    blocks = ["🇰🇷 국내 핵심(보유/관심)"]
    for label, code in KR_CORE.items():
        o = kr_ohlcv(code, date)
        f = kr_fundamental(code, date)
        if not o:
            blocks.append(f"\n• {label}\n  - 데이터 수신이 불안정해서 오늘은 가격을 못 불러왔어요.")
            continue
        per = f["per"] if f else float("nan")
        eps = f["eps"] if f else float("nan")
        blocks.append(
            f"\n• {label}\n"
            f"  - 종가: ₩{fmt_int(o['close'])} | 1D: {fmt_pct(o['chg1d'])}\n"
            f"  - PER: {('N/A' if is_nan(per) else f'{per:.1f}')} | EPS: {('N/A' if is_nan(eps) else f'{eps:,.0f}')}"
        )
    return "\n".join(blocks)

# =========================
# DART (optional) for KR core news bullets
# =========================
def dart_find_corp_code_by_stock_code(stock_code: str):
    if not DART_API_KEY:
        return None
    try:
        url = f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={DART_API_KEY}"
        r = requests.get(url, timeout=30)
        r.raise_for_status()

        import zipfile, io, xml.etree.ElementTree as ET
        z = zipfile.ZipFile(io.BytesIO(r.content))
        xml_bytes = z.read("CORPCODE.xml")
        root = ET.fromstring(xml_bytes)

        for item in root.findall("list"):
            sc = (item.findtext("stock_code") or "").strip()
            if sc == stock_code:
                return (item.findtext("corp_code") or "").strip() or None
        return None
    except Exception:
        return None

def dart_recent_disclosures(corp_code: str, limit=3):
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
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()
        items = data.get("list", [])[:limit]

        out = []
        for it in items:
            title = (it.get("report_nm") or "").strip()
            rcp_no = (it.get("rcept_no") or "").strip()
            if not title or not rcp_no:
                continue
            link = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcp_no}"
            out.append((title, link))
        return out
    except Exception:
        return []

def build_kr_dart_bullets(stock_code: str, corp_name: str, limit=3):
    corp_code = dart_find_corp_code_by_stock_code(stock_code)
    if not corp_code:
        return []
    items = dart_recent_disclosures(corp_code, limit=limit)
    bullets = []
    for title, link in items:
        bullets.append(f"[DART] {corp_name}: {title} - {link}")
    return bullets

# =========================
# KR recommendations (rule-based)
# =========================
def build_universe_top_caps(date: str, n_each=200):
    frames = []
    for m in ["KOSPI", "KOSDAQ"]:
        cap = stock.get_market_cap_by_ticker(date, market=m)
        if cap is None or cap.empty:
            continue
        cap = cap.sort_values("시가총액", ascending=False).head(n_each)
        cap["market"] = m
        frames.append(cap)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, axis=0)

def kr_investor_flow_by_ticker(code: str, date: str):
    try:
        df = stock.get_market_trading_value_by_investor(date, date, code)
        if df is None or df.empty:
            return None

        if "순매수" in df.columns:
            def pick(names):
                for nm in names:
                    for idx in df.index:
                        if nm in str(idx):
                            return safe_float(df.loc[idx, "순매수"])
                return float("nan")
            foreign = pick(["외국인"])
            inst = pick(["기관합계", "기관"])
            if not is_nan(foreign) or not is_nan(inst):
                return {"foreign": foreign, "inst": inst}

        if "외국인" in df.columns and "기관합계" in df.columns:
            for idx in df.index:
                if "순매수" in str(idx):
                    return {
                        "foreign": safe_float(df.loc[idx, "외국인"]),
                        "inst": safe_float(df.loc[idx, "기관합계"]),
                    }
        return None
    except Exception:
        return None

def score_candidate(code: str, date: str):
    """
    추천 후보(흑자 + 과열 제외 + 추세/모멘텀 + 수급 반영)
    """
    try:
        prev_days = stock.get_previous_business_days(date, 25)
        if not prev_days:
            return None
        start = prev_days[-1]
        df = stock.get_market_ohlcv_by_date(start, date, code)
        if df is None or df.empty or len(df) < 10:
            return None

        close = safe_float(df["종가"].iloc[-1])
        ma20 = safe_float(df["종가"].rolling(20).mean().iloc[-1])
        dist20 = (close / ma20 - 1.0) * 100.0 if (ma20 == ma20 and ma20 != 0) else float("nan")

        mom5 = float("nan")
        if len(df) >= 6:
            c5 = safe_float(df["종가"].iloc[-6])
            mom5 = (close / c5 - 1.0) * 100.0 if (c5 == c5 and c5 != 0) else float("nan")

        # 과열 제외
        if (not is_nan(dist20)) and dist20 >= 12:
            return None
        if (not is_nan(mom5)) and mom5 >= 18:
            return None

        f = kr_fundamental(code, date)
        if not f:
            return None
        eps = f["eps"]
        per = f["per"]
        if is_nan(eps) or eps <= 0:
            return None

        flow = kr_investor_flow_by_ticker(code, date)
        flow_penalty = 0.0
        if flow:
            foreign = flow.get("foreign", float("nan"))
            inst = flow.get("inst", float("nan"))
            if (not is_nan(foreign)) and (not is_nan(inst)) and foreign < 0 and inst < 0:
                flow_penalty = -3.5

        score = 0.0
        if not is_nan(mom5):
            score += mom5
        if not is_nan(dist20):
            score += max(min(dist20, 6), -6) * 0.7

        if not is_nan(per):
            if per >= 60:
                score -= 5
            elif per >= 35:
                score -= 2

        score += flow_penalty

        return {
            "code": code,
            "close": close,
            "mom5": mom5,
            "dist20": dist20,
            "eps": eps,
            "per": per,
            "score": score,
            "ma20": ma20,
            "flow": flow,
        }
    except Exception:
        return None

def kr_reco_block(risk_level: str, limit=3):
    date = nearest_krx_day()

    if risk_level == "bad":
        return (
            "❌ 오늘 국내 추천 없음\n"
            "- 지수/변동성/수급 위험 신호가 겹쳤어요.\n"
            "- 이런 날은 좋은 종목도 같이 흔들릴 확률이 높아서 신규 진입 확률이 떨어집니다.\n"
            "🧭 선배 전략: 신규는 대기, 보유는 과열이면 일부 정리로 편하게 가요."
        ), [], date

    uni = build_universe_top_caps(date, n_each=200)
    if uni.empty:
        return "📌 국내 추천: 오늘은 데이터 수신이 불안정해서 쉬어갈게요.", [], date

    candidates = []
    for code in uni.index.tolist():
        c = score_candidate(code, date)
        if c:
            candidates.append(c)

    if not candidates:
        return (
            "📌 국내 추천: 오늘은 조건을 만족하는 후보가 없어서 쉬어갈게요.\n"
            "(흑자 + 과열아님 + 추세 + 수급 조건을 동시에 만족하는 종목이 부족했습니다.)"
        ), [], date

    df = pd.DataFrame(candidates).sort_values("score", ascending=False)

    pick_n = 2 if risk_level == "meh" else limit
    picks = df.head(pick_n).to_dict(orient="records")

    lines = ["🔥 오늘의 국내 추천 (조건 충족 시만)"]
    for i, p in enumerate(picks, start=1):
        code = p["code"]
        name = kr_name(code)
        per = p["per"]
        per_s = "N/A" if is_nan(per) else f"{per:.1f}"
        mom5_s = "N/A" if is_nan(p["mom5"]) else f"{p['mom5']:+.1f}%"
        dist20_s = "N/A" if is_nan(p["dist20"]) else f"{p['dist20']:+.1f}%"

        flow_note = ""
        flow = p.get("flow")
        if flow:
            f = flow.get("foreign", float("nan"))
            inst = flow.get("inst", float("nan"))
            flow_note = f" | 수급(전일): 외국인 {fmt_bn_krw(f)}, 기관 {fmt_bn_krw(inst)}"

        close = p["close"]
        ma20 = p.get("ma20", float("nan"))
        plan = ""
        if not is_nan(close) and not is_nan(ma20) and ma20 != 0:
            entry_low = ma20 * 0.98
            entry_high = ma20 * 1.02
            stop = ma20 * 0.96
            tp1 = close * 1.10
            tp2 = close * 1.18
            plan = (
                f"  - 진입(가이드): ₩{fmt_int(entry_low)}~₩{fmt_int(entry_high)} 분할 | "
                f"리스크: ₩{fmt_int(stop)} 이탈 시 보수적 | "
                f"익절(가이드): 1차 ₩{fmt_int(tp1)}, 2차 ₩{fmt_int(tp2)}"
            )

        lines.append(
            f"\n{i}. {name} ({code})\n"
            f"  - 종가: ₩{fmt_int(close)} | 5D: {mom5_s} | 20일선 대비: {dist20_s}\n"
            f"  - PER: {per_s} | EPS: {p['eps']:,.0f}{flow_note}\n"
            f"{plan}"
        )

    return "\n".join(lines), picks, date

def kr_reco_news_bullets(picks, limit_each=2):
    bullets = []
    for p in picks:
        code = p["code"]
        name = kr_name(code)
        for title, link in fetch_rss(google_news_rss(f"{name} {code}"), limit=limit_each):
            bullets.append(f"[GOOGLE] {name}: {title} - {link}")
    return bullets

# =========================
# MAIN
# =========================
def main():
    now = kst_now()
    usdkrw = get_usdkrw()

    header = f"📌 데일리 브리핑 (KST {now:%Y-%m-%d %H:%M})"
    fxline = f"💱 USD/KRW: {usdkrw:,.2f}"

    market_text, risk_level = market_brief()

    # ---------- US section ----------
    us_lines = ["🇺🇸 미국 관심종목 (분석 + 오늘 액션)"]
    us_ai_summaries = []

    for name, tkr in US_TICKERS.items():
        s = us_snapshot(tkr)
        if not s:
            us_lines.append(f"\n• {name}\n  - 데이터 수신이 불안정해서 오늘은 가격을 못 불러왔어요.")
            continue

        close = s["close"]
        krw_price = close * usdkrw
        rsi_v = s["rsi"]
        high_3m = s["high_3m"]

        stage, action, flags_txt, dist20_calc = exit_signals(
            close=close,
            ma20=s["ma20"],
            rsi_v=rsi_v,
            chg5d=s["chg5d"],
            high_3m=high_3m,
        )

        dist20_s = "N/A" if is_nan(dist20_calc) else f"{dist20_calc:+.1f}%"
        rsi_s = "N/A" if is_nan(rsi_v) else f"{rsi_v:.0f}"

        entry_txt = entry_plan_us(close, s["ma20"], risk_level)

        us_lines.append(
            f"\n• {name}\n"
            f"  - 종가: ${close:.2f} (₩{krw_price:,.0f})\n"
            f"  - 1D: {fmt_pct(s['chg1d'])} | 5D: {fmt_pct(s['chg5d'])}\n"
            f"  - 20일선 대비: {dist20_s} | RSI: {rsi_s}\n"
            f"  - 익절 신호: {flags_txt}\n"
            f"  - 오늘 액션: {action}\n"
            f"  - {entry_txt}"
        )

        # 뉴스 bullets (Google + SEC 8-K)
        bullets = build_us_news_bullets(name, tkr, limit_news=3, limit_sec=2)
        if bullets:
            # AI 요약(뉴스만)
            us_ai_summaries.append("\n" + ai_summarize_news(f"{name} (미국)", bullets))

            # 링크도 같이(선배 요청)
            us_lines.append("  - 뉴스 링크:")
            for b in bullets[:5]:
                us_lines.append("    • " + b.split("] ", 1)[-1])

    us_block_text = "\n".join(us_lines)
    us_ai_text = "\n".join(us_ai_summaries) if us_ai_summaries else "AI 요약: (오늘은 요약할 뉴스가 부족했어요.)"

    # ---------- KR core ----------
    kr_core_text = kr_core_block()

    # 국내 공시(DART) AI 요약 (선택)
    dart_ai_text = ""
    if DART_API_KEY:
        dart_bullets_all = []
        for _, code in KR_CORE.items():
            corp_name = kr_name(code)
            dart_bullets_all.extend(build_kr_dart_bullets(code, corp_name, limit=3))
        if dart_bullets_all:
            dart_ai_text = ai_summarize_news("국내 공시(DART)", dart_bullets_all)
        else:
            dart_ai_text = "AI 요약: (최근 7일 내 공시가 없거나 수집이 어려웠어요.)"
    else:
        dart_ai_text = "AI 요약: (DART_API_KEY가 없어 국내 공시 요약은 생략했어요.)"

    # ---------- KR recommendations ----------
    kr_reco_text, picks, kr_date = kr_reco_block(risk_level, limit=3)

    kr_reco_ai = ""
    if picks:
        bullets = kr_reco_news_bullets(picks, limit_each=2)
        if bullets:
            kr_reco_ai = ai_summarize_news("국내 추천주(뉴스)", bullets)

    # ---------- Guide + Education ----------
    guide = (
        "\n\n🧭 선배 익절 전략(확정: 30/30/20/잔여20)\n"
        "- 신호 2개↑: 1차(30%) 후보\n"
        "- 신호 3개↑: 2차(추가30%, 총60%) 후보\n"
        "- 신호 3개 + 강과열: 3차(추가20%, 총80%) 후보\n"
        "- 잔여는 추세 추종(무리한 추격 금지)"
    )
    edu = (
        "\n\n📚 오늘의 매매 타이밍 원칙\n"
        "- 시장 bad면: 신규는 쉬는 게 확률이 좋아요.\n"
        "- 종목은 ‘20일선 근처(±2%)’에서 분할 진입이 가장 편합니다.\n"
        "- 익절은 수익률이 아니라 ‘과열 신호’로 판단하면 흔들림이 줄어요."
    )

    msg = (
        header + "\n\n"
        + fxline + "\n\n"
        + market_text + "\n\n"
        + us_block_text + "\n\n"
        + "🤖 미국 뉴스/공시 AI 요약\n" + us_ai_text + "\n\n"
        + kr_core_text + "\n\n"
        + "🤖 국내 공시 AI 요약\n" + dart_ai_text + "\n\n"
        + kr_reco_text
        + ("\n\n🤖 국내 추천주 뉴스 AI 요약\n" + kr_reco_ai if kr_reco_ai else "")
        + guide
        + edu
    )

    telegram_send(msg)

if __name__ == "__main__":
    main()
