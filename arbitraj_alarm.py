"""
Kripto Arbitraj Alarm Botu
Binance, Gate, MEXC, OKX, Bybit, Paribu, BTCTürk fiyatlarını karşılaştırır
%0.5 fark olunca Telegram'a bildirim atar
"""

import requests
import time
import os
import threading
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

ESIK = 0.5  # % fark eşiği

son_bildirim = {}  # Aynı fırsatı tekrar tekrar bildirmesin


def telegram_gonder(mesaj):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": mesaj,
            "parse_mode": "HTML"
        }, timeout=10)
    except Exception as e:
        print(f"Telegram hata: {e}")


def binance_fiyat(coin):
    try:
        r = requests.get(f"https://api.binance.com/api/v3/ticker/price",
                         params={"symbol": f"{coin}USDT"}, timeout=5)
        return float(r.json()["price"])
    except:
        return None


def gate_fiyat(coin):
    try:
        r = requests.get(f"https://api.gateio.ws/api/v4/spot/tickers",
                         params={"currency_pair": f"{coin}_USDT"}, timeout=5)
        veri = r.json()
        if veri:
            return float(veri[0]["last"])
    except:
        pass
    return None


def mexc_fiyat(coin):
    try:
        r = requests.get(f"https://api.mexc.com/api/v3/ticker/price",
                         params={"symbol": f"{coin}USDT"}, timeout=5)
        return float(r.json()["price"])
    except:
        return None


def okx_fiyat(coin):
    try:
        r = requests.get(f"https://www.okx.com/api/v5/market/ticker",
                         params={"instId": f"{coin}-USDT"}, timeout=5)
        veri = r.json()
        if veri.get("data"):
            return float(veri["data"][0]["last"])
    except:
        pass
    return None


def bybit_fiyat(coin):
    try:
        r = requests.get(f"https://api.bybit.com/v5/market/tickers",
                         params={"category": "spot", "symbol": f"{coin}USDT"}, timeout=5)
        veri = r.json()
        if veri.get("result", {}).get("list"):
            return float(veri["result"]["list"][0]["lastPrice"])
    except:
        pass
    return None


def paribu_fiyat(coin):
    try:
        parite = f"{coin.lower()}_tl"
        r = requests.get("https://api.paribu.com/orderbook",
                         params={"market": parite, "depth": 1}, timeout=5)
        veri = r.json()
        bids = veri.get("bids", [])
        asks = veri.get("asks", [])
        if bids and asks:
            fiyat_tl = (float(bids[0][0]) + float(asks[0][0])) / 2
            # TL'yi USD'ye çevir
            usdt_r = requests.get("https://api.paribu.com/orderbook",
                                   params={"market": "usdt_tl", "depth": 1}, timeout=5)
            usdt_v = usdt_r.json()
            usdt_bids = usdt_v.get("bids", [])
            usdt_asks = usdt_v.get("asks", [])
            if usdt_bids and usdt_asks:
                usdt_tl = (float(usdt_bids[0][0]) + float(usdt_asks[0][0])) / 2
                return fiyat_tl / usdt_tl
    except:
        pass
    return None


def btcturk_fiyat(coin):
    try:
        parite = f"{coin}TRY"
        r = requests.get("https://api.btcturk.com/api/v2/ticker",
                         params={"pairSymbol": parite}, timeout=5)
        veri = r.json().get("data", [])
        if veri:
            fiyat_tl = float(veri[0]["last"])
            # TL'yi USD'ye çevir
            usdt_r = requests.get("https://api.btcturk.com/api/v2/ticker",
                                   params={"pairSymbol": "USDTTRY"}, timeout=5)
            usdt_v = usdt_r.json().get("data", [])
            if usdt_v:
                usdt_tl = float(usdt_v[0]["last"])
                return fiyat_tl / usdt_tl
    except:
        pass
    return None


def fiyatlari_al(coin):
    fonksiyonlar = {
        "Binance": binance_fiyat,
        "Gate":    gate_fiyat,
        "MEXC":    mexc_fiyat,
        "OKX":     okx_fiyat,
        "Bybit":   bybit_fiyat,
        "Paribu":  paribu_fiyat,
        "BTCTürk": btcturk_fiyat,
    }
    fiyatlar = {}
    for borsa, fn in fonksiyonlar.items():
        f = fn(coin)
        if f and f > 0:
            fiyatlar[borsa] = f
    return fiyatlar


def kontrol_et(coin):
    fiyatlar = fiyatlari_al(coin)
    if len(fiyatlar) < 2:
        return

    borsalar = list(fiyatlar.items())
    for i in range(len(borsalar)):
        for j in range(i+1, len(borsalar)):
            borsa1, fiyat1 = borsalar[i]
            borsa2, fiyat2 = borsalar[j]

            dusuk_borsa  = borsa1 if fiyat1 < fiyat2 else borsa2
            yuksek_borsa = borsa2 if fiyat1 < fiyat2 else borsa1
            dusuk_fiyat  = min(fiyat1, fiyat2)
            yuksek_fiyat = max(fiyat1, fiyat2)

            fark_yuzde = ((yuksek_fiyat - dusuk_fiyat) / dusuk_fiyat) * 100

            if fark_yuzde >= ESIK:
                anahtar = f"{coin}_{dusuk_borsa}_{yuksek_borsa}"
                son = son_bildirim.get(anahtar, 0)
                # Aynı fırsatı 5 dakikada bir bildir
                if time.time() - son > 300:
                    son_bildirim[anahtar] = time.time()
                    zaman = datetime.now().strftime("%H:%M:%S")
                    mesaj = (
                        f"🚨 <b>ARBİTRAJ FIRSATI</b>\n\n"
                        f"🪙 <b>{coin}</b>\n"
                        f"📉 <b>{dusuk_borsa}</b>: ${dusuk_fiyat:.6f}\n"
                        f"📈 <b>{yuksek_borsa}</b>: ${yuksek_fiyat:.6f}\n"
                        f"💰 <b>Fark: %{fark_yuzde:.2f}</b>\n"
                        f"🕐 {zaman}"
                    )
                    print(mesaj)
                    telegram_gonder(mesaj)


def bot_calistir(coinler, aralik):
    print(f"Bot başladı — {len(coinler)} coin, {aralik}s aralik, eşik: %{ESIK}")
    telegram_gonder(f"✅ Arbitraj Alarm Botu başladı!\n{len(coinler)} coin takip ediliyor.\nEşik: %{ESIK}")
    while True:
        for coin in coinler:
            try:
                kontrol_et(coin)
            except Exception as e:
                print(f"{coin} hata: {e}")
            time.sleep(0.5)
        time.sleep(aralik)


if __name__ == "__main__":
    COINLER = [
        "BTC", "ETH", "BNB", "SOL", "XRP",
        "DOGE", "ADA", "AVAX", "LINK", "DOT",
        "MATIC", "UNI", "ATOM", "LTC", "XLM",
        "NEAR", "ARB", "OP", "SAND", "MANA"
    ]
    ARALIK = 30  # saniye

    bot_calistir(COINLER, ARALIK)
