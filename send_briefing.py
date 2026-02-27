import os
import datetime as dt
import requests
import yfinance as yf

TOKEN = os.environ["TELEGRAM_TOKEN"]
CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

US_TICKERS = {
    "엔비디아(NVDA)": "NVDA",
    "테슬라(TSLA)": "TSLA",
    "팔란티어(PLTR)": "PLTR",
}

# --- helpers ---
def get_usdkrw():
    # yfinance 환율 티커
    fx = yf.Ticker("KRW=X").history(period="5d").dropna()
    rate = float(fx["Close"].iloc[-1])
    return rate

def rsi(series, period=14):
    # 간단 RSI
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / (loss.replace(0, 1e-9))
    return 100 - (100 / (1 + rs))

def get_snapshot(ticker: str):
    data = yf.Ticker(ticker).history(period="3mo").dropna()
    close = float(data["Close"].iloc[-1])
    prev = float(data["Close"].iloc[-2])
    chg1d = (close / prev - 1.0) * 100.0

    # 추세 지표(초보도 이해 쉬운 것만)
    ma20 = float(data["Close"].rolling(20).mean().iloc[-1])
    ma60 = float(data["Close"].rolling(60).mean().iloc[-1]) if len(data) >= 60 else float("nan")
    dist20 = (close / ma20 - 1.0) * 100.0 if ma20 else 0.0

    r = rsi(data["Close"]).iloc[-1]
    r = float(r) if r == r else float("nan")  # NaN 처리

    # 최근 5거래일 변화
    if len(data) >= 6:
        close_5d_ago = float(data["Close"].iloc[-6])
        chg5d = (close / close_5d_ago - 1.0) * 100.0
    else:
        chg5d = float("nan")

    return {
        "close": close,
        "chg1d": chg1d,
        "chg5d": chg5d,
        "ma20": ma20,
        "ma60": ma60,
        "dist20": dist20,
        "rsi": r,
    }

def get_news(ticker: str, limit=3):
    # Yahoo Finance 뉴스(가끔 빈 리스트일 수 있음)
    items = []
    try:
        raw = yf.Ticker(ticker).news or []
        for n in raw[: max(limit * 2, 6)]:  # 중복 대비 여유
            title = (n.get("title") or "").strip()
            pub = (n.get("publisher") or "").strip()
            t = n.get("providerPublishTime")
            link = n.get("link") or n.get("url") or ""
            if not title:
                continue
            when = ""
            if isinstance(t, int):
                kst = dt.datetime.fromtimestamp(t, tz=dt.timezone.utc) + dt.timedelta(hours=9)
                when = kst.strftime("%m/%d %H:%M")
            items.append((title, pub, when, link))
    except Exception:
        pass

    # 제목 중복 제거
    seen = set()
    out = []
    for title, pub, when, link in items:
        key = title.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append((title, pub, when, link))
        if len(out) >= limit:
            break
    return out

def classify(snapshot):
    # 아주 단순하지만 실전에서 유용한 “체감” 분류
    r = snapshot["rsi"]
    dist20 = snapshot["dist20"]

    if r == r and r >= 70:
        return "과열(추격매수 주의)"
    if r == r and r <= 35:
        return "과매도(급반등/변동 주의)"
    if dist20 >= 6:
        return "단기 과열권(분할익절 유리)"
    if dist20 <= -4:
        return "단기 눌림권(분할 접근 유리)"
    return "추세 구간(계획대로 대응)"

def send_telegram(text: str):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    r = requests.post(url, json={"chat_id": CHAT_ID, "text": text, "disable_web_page_preview": True})
    r.raise_for_status()

def fmt_pct(x):
    return "N/A" if x != x else f"{x:+.2f}%"

def main():
    now_kst = dt.datetime.utcnow() + dt.timedelta(hours=9)
    usdkrw = get_usdkrw()

    lines = []
    news_lines = []

    for name, tkr in US_TICKERS.items():
        s = get_snapshot(tkr)
        krw_price = s["close"] * usdkrw

        vibe = classify(s)

        lines.append(
            f"• {name}\n"
            f"  - 종가: ${s['close']:.2f} (₩{krw_price:,.0f})\n"
            f"  - 1D: {fmt_pct(s['chg1d'])} | 5D: {fmt_pct(s['chg5d'])}\n"
            f"  - 20일선 대비: {s['dist20']:+.1f}% | RSI: {('N/A' if s['rsi']!=s['rsi'] else f'{s['rsi']:.0f}')}\n"
            f"  - 코멘트: {vibe}"
        )

        news = get_news(tkr, limit=3)
        if news:
            news_lines.append(f"\n📰 {name} 최근 뉴스")
            for title, pub, when, link in news:
                stamp = f"({when}) " if when else ""
                pubtxt = f" - {pub}" if pub else ""
                # 링크는 길어질 수 있어도 텔레그램에서 클릭 가능
                news_lines.append(f"- {stamp}{title}{pubtxt}\n  {link}")

    header = f"📌 데일리 주식 브리핑 (KST {now_kst:%Y-%m-%d %H:%M})"
    fxline = f"💱 환율(USD/KRW): {usdkrw:,.2f}"

    # 선배 스타일(중간 수익실현형) 요약 한 줄
    guide = (
        "\n\n🧭 선배 체크포인트\n"
        "• 과열 표시가 뜬 종목은 ‘추격’보다 ‘분할익절/비중조절’이 편해요.\n"
        "• 눌림 표시가 뜬 종목은 ‘한 번에’ 말고 ‘분할’이 안정적이에요."
    )

    msg = header + "\n" + fxline + "\n\n" + "\n\n".join(lines) + "".join(news_lines) + guide
    send_telegram(msg)

if __name__ == "__main__":
    main()
