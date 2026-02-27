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

# -----------------------
# Helpers
# -----------------------
def safe_float(x, default=float("nan")):
    try:
        return float(x)
    except Exception:
        return default

def fmt_pct(x):
    return "N/A" if x != x else f"{x:+.2f}%"

def get_usdkrw(default=1350.0):
    """USD/KRW 환율(종가)을 가져오되, 실패하면 default 사용"""
    try:
        fx = yf.Ticker("KRW=X").history(period="7d").dropna()
        if fx.empty:
            return default
        return float(fx["Close"].iloc[-1])
    except Exception:
        return default

def rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / (loss.replace(0, 1e-9))
    return 100 - (100 / (1 + rs))

def classify(dist20, r):
    """초보도 이해 쉬운 단순 분류"""
    if r == r and r >= 70:
        return "과열(추격매수 주의)"
    if r == r and r <= 35:
        return "과매도(급반등/변동 주의)"
    if dist20 >= 6:
        return "단기 과열권(분할익절 유리)"
    if dist20 <= -4:
        return "단기 눌림권(분할 접근 유리)"
    return "추세 구간(계획대로 대응)"

def get_snapshot(ticker: str):
    """가격/추세/RSI 스냅샷. 실패해도 예외 던지지 않게 처리."""
    try:
        data = yf.Ticker(ticker).history(period="3mo").dropna()
        if data.empty or len(data) < 3:
            return None

        close = safe_float(data["Close"].iloc[-1])
        prev = safe_float(data["Close"].iloc[-2])
        chg1d = (close / prev - 1.0) * 100.0 if prev == prev and prev != 0 else float("nan")

        ma20 = safe_float(data["Close"].rolling(20).mean().iloc[-1])
        dist20 = (close / ma20 - 1.0) * 100.0 if ma20 == ma20 and ma20 != 0 else float("nan")

        r = rsi(data["Close"]).iloc[-1]
        r = safe_float(r)

        # 5거래일 변화(대충 6행 전 = 5거래일 전)
        if len(data) >= 6:
            close_5d_ago = safe_float(data["Close"].iloc[-6])
            chg5d = (close / close_5d_ago - 1.0) * 100.0 if close_5d_ago == close_5d_ago and close_5d_ago != 0 else float("nan")
        else:
            chg5d = float("nan")

        return {
            "close": close,
            "chg1d": chg1d,
            "chg5d": chg5d,
            "dist20": dist20,
            "rsi": r,
        }
    except Exception:
        return None

def get_news(ticker: str, limit=3):
    """Yahoo 뉴스(있으면) 타이틀/퍼블리셔/시간/링크"""
    out = []
    try:
        raw = yf.Ticker(ticker).news or []
        seen = set()
        for n in raw:
            title = (n.get("title") or "").strip()
            if not title:
                continue
            key = title.lower()
            if key in seen:
                continue
            seen.add(key)

            pub = (n.get("publisher") or "").strip()
            t = n.get("providerPublishTime")
            link = n.get("link") or n.get("url") or ""

            when = ""
            if isinstance(t, int):
                kst = dt.datetime.fromtimestamp(t, tz=dt.timezone.utc) + dt.timedelta(hours=9)
                when = kst.strftime("%m/%d %H:%M")

            out.append((title, pub, when, link))
            if len(out) >= limit:
                break
    except Exception:
        pass
    return out

def send_telegram(text: str):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    r = requests.post(
        url,
        json={"chat_id": CHAT_ID, "text": text, "disable_web_page_preview": True},
        timeout=20,
    )
    print("STATUS:", r.status_code)
    print("RESPONSE:", r.text[:500])  # 로그 너무 길어지는 것 방지
    r.raise_for_status()

# -----------------------
# Main
# -----------------------
def main():
    now_kst = dt.datetime.utcnow() + dt.timedelta(hours=9)
    usdkrw = get_usdkrw()

    header = f"📌 데일리 주식 브리핑 (KST {now_kst:%Y-%m-%d %H:%M})"
    fxline = f"💱 환율(USD/KRW): {usdkrw:,.2f}"

    blocks = []
    news_blocks = []

    for name, tkr in US_TICKERS.items():
        s = get_snapshot(tkr)
        if not s:
            blocks.append(f"• {name}\n  - 데이터 수신이 불안정해서 오늘은 가격을 못 불러왔어요.")
            continue

        close = s["close"]
        krw_price = close * usdkrw if close == close else float("nan")

        rsi_val = s["rsi"]
        rsi_str = "N/A" if rsi_val != rsi_val else f"{rsi_val:.0f}"

        dist20 = s["dist20"]
        dist20_str = "N/A" if dist20 != dist20 else f"{dist20:+.1f}%"

        vibe = classify(dist20 if dist20 == dist20 else 0.0, rsi_val)

        blocks.append(
            f"• {name}\n"
            f"  - 종가: ${close:.2f} (₩{krw_price:,.0f})\n"
            f"  - 1D: {fmt_pct(s['chg1d'])} | 5D: {fmt_pct(s['chg5d'])}\n"
            f"  - 20일선 대비: {dist20_str} | RSI: {rsi_str}\n"
            f"  - 코멘트: {vibe}"
        )

        news = get_news(tkr, limit=3)
        if news:
            news_blocks.append(f"\n📰 {name} 최근 뉴스")
            for title, pub, when, link in news:
                stamp = f"({when}) " if when else ""
                pubtxt = f" - {pub}" if pub else ""
                if link:
                    news_blocks.append(f"- {stamp}{title}{pubtxt}\n  {link}")
                else:
                    news_blocks.append(f"- {stamp}{title}{pubtxt}")

    guide = (
        "\n\n🧭 선배 체크포인트\n"
        "• 과열이면 ‘추격’보다 ‘분할익절/비중조절’이 편해요.\n"
        "• 눌림이면 ‘한 번에’보다 ‘분할’이 안정적이에요."
    )

    msg = header + "\n" + fxline + "\n\n" + "\n\n".join(blocks) + "".join(news_blocks) + guide
    send_telegram(msg)

if __name__ == "__main__":
    main()
