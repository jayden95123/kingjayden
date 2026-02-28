import os
import datetime as dt
import requests
import feedparser
import yfinance as yf
from openai import OpenAI

# =========================
# ENV
# =========================
TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

US_TICKERS = {
    "엔비디아(NVDA)": "NVDA",
    "테슬라(TSLA)": "TSLA",
    "팔란티어(PLTR)": "PLTR",
}

# =========================
# 공통 유틸
# =========================
def kst_now():
    return dt.datetime.utcnow() + dt.timedelta(hours=9)

def is_nan(x):
    return x != x

def safe_float(x, default=float("nan")):
    try:
        return float(x)
    except:
        return default

def fmt_pct(x):
    return "N/A" if is_nan(x) else f"{x:+.2f}%"

def fmt_int(x):
    try:
        return f"{int(round(float(x))):,}"
    except:
        return "N/A"

# =========================
# 텔레그램 분할 전송
# =========================
def telegram_send(text):
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
            timeout=20,
        )
        r.raise_for_status()

# =========================
# 환율
# =========================
def get_usdkrw():
    try:
        df = yf.Ticker("KRW=X").history(period="7d").dropna()
        if df.empty:
            return 1350.0
        return float(df["Close"].iloc[-1])
    except:
        return 1350.0

# =========================
# 시장 요약
# =========================
def get_index_return(ticker):
    try:
        df = yf.Ticker(ticker).history(period="7d").dropna()
        if len(df) < 2:
            return float("nan")
        close = float(df["Close"].iloc[-1])
        prev = float(df["Close"].iloc[-2])
        return (close/prev - 1) * 100
    except:
        return float("nan")

def market_summary():
    kospi = get_index_return("^KS11")
    nasdaq = get_index_return("^IXIC")
    vix = get_index_return("^VIX")

    risk = 0
    if not is_nan(kospi) and kospi <= -1.5:
        risk += 1
    if not is_nan(vix) and vix >= 6:
        risk += 1

    if risk >= 2:
        level = "bad"
        comment = "시장 변동성이 높습니다. 신규 진입은 쉬는 것이 유리합니다."
    else:
        level = "good"
        comment = "시장 분위기는 비교적 안정적입니다."

    text = (
        "📈 시장 요약\n"
        f"- 코스피: {fmt_pct(kospi)}\n"
        f"- 나스닥: {fmt_pct(nasdaq)}\n"
        f"- VIX: {fmt_pct(vix)}\n"
        f"🧭 코멘트: {comment}"
    )
    return text, level

# =========================
# 기술적 신호 (30/30/20 전략)
# =========================
def rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / (loss.replace(0, 1e-9))
    return 100 - (100 / (1 + rs))

def exit_signals(close, ma20, rsi_v, chg5d):
    signals = 0

    if rsi_v >= 70:
        signals += 1
    if ma20 > 0 and (close/ma20 - 1)*100 >= 6:
        signals += 1
    if chg5d >= 12:
        signals += 1

    if signals >= 3:
        return "2차 익절(총 60%) 후보"
    elif signals >= 2:
        return "1차 익절(30%) 후보"
    else:
        return "보유 유지"

# =========================
# 뉴스 RSS
# =========================
def google_news_rss(query):
    q = requests.utils.quote(query)
    return f"https://news.google.com/rss/search?q={q}&hl=ko&gl=KR&ceid=KR:ko"

def fetch_rss(url, limit=3):
    try:
        feed = feedparser.parse(url)
        out = []
        for e in feed.entries[:limit]:
            out.append((e.title, e.link))
        return out
    except:
        return []

# =========================
# AI 요약 (뉴스만)
# =========================
def ai_summary(title, bullets):
    if not client:
        return "AI 요약: (OPENAI 키 없음)"

    prompt = (
        f"{title} 관련 뉴스입니다.\n"
        "4~6줄로 요약하고 긍정/리스크/체크포인트 포함하세요.\n\n"
        + "\n".join(bullets)
    )

    try:
        resp = client.responses.create(
            model="gpt-5-mini",
            input=prompt
        )
        return "AI 요약:\n" + resp.output_text.strip()
    except:
        return "AI 요약 실패"

# =========================
# 메인
# =========================
def main():
    now = kst_now()
    usdkrw = get_usdkrw()

    header = f"📌 데일리 브리핑 (KST {now:%Y-%m-%d %H:%M})"
    fx = f"💱 USD/KRW: {usdkrw:,.2f}"

    market_text, level = market_summary()

    us_text = "🇺🇸 미국 종목\n"

    for name, ticker in US_TICKERS.items():
        try:
            df = yf.Ticker(ticker).history(period="3mo").dropna()
            close = float(df["Close"].iloc[-1])
            prev = float(df["Close"].iloc[-2])
            chg1d = (close/prev - 1)*100

            chg5d = 0
            if len(df) >= 6:
                c5 = float(df["Close"].iloc[-6])
                chg5d = (close/c5 - 1)*100

            ma20 = float(df["Close"].rolling(20).mean().iloc[-1])
            rsi_v = float(rsi(df["Close"]).iloc[-1])

            action = exit_signals(close, ma20, rsi_v, chg5d)

            us_text += (
                f"\n• {name}\n"
                f"  - ${close:.2f} (₩{fmt_int(close*usdkrw)})\n"
                f"  - 1D: {fmt_pct(chg1d)} | 5D: {fmt_pct(chg5d)}\n"
                f"  - RSI: {int(rsi_v)}\n"
                f"  - 오늘 액션: {action}\n"
            )

            news = fetch_rss(google_news_rss(ticker), 3)
            bullets = [f"{t} - {l}" for t,l in news]
            if bullets:
                us_text += "\n" + ai_summary(name, bullets) + "\n"

        except:
            us_text += f"\n• {name} 데이터 오류\n"

    final_msg = (
        header + "\n\n"
        + fx + "\n\n"
        + market_text + "\n\n"
        + us_text +
        "\n📚 원칙: 신호 2개↑ → 1차 익절, 3개↑ → 2차 익절\n"
    )

    telegram_send(final_msg)

if __name__ == "__main__":
    main()
