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

def get_close_and_change(ticker: str):
    df = yf.download(ticker, period="5d", interval="1d", progress=False).dropna()
    close = float(df["Close"].iloc[-1])
    prev = float(df["Close"].iloc[-2])
    chg = (close / prev - 1.0) * 100.0
    return close, chg

def send_telegram(text: str):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    r = requests.post(url, json={"chat_id": CHAT_ID, "text": text})
    print("STATUS:", r.status_code)
    print("RESPONSE:", r.text)
    r.raise_for_status()

def main():
    now_kst = dt.datetime.utcnow() + dt.timedelta(hours=9)

    lines = []
    for name, tkr in US_TICKERS.items():
        close, chg = get_close_and_change(tkr)
        lines.append(f"- {name}: {close:.2f}달러 ({chg:+.2f}%)")

    msg = (
        f"📌 데일리 주식 브리핑 (KST {now_kst:%Y-%m-%d %H:%M})\n\n"
        f"🇺🇸 미국주식\n" + "\n".join(lines) +
        "\n\n선배 😊\n"
        "오늘도 무리하지 말고, 수익 난 구간이면 일부 정리로 편하게 가요."
    )

    send_telegram(msg)

if __name__ == "__main__":
    main()
