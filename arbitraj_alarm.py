"""
Kripto Arbitraj Alarm Botu
Yabancı borsalar (USDT) ↔ Türk borsalar (TL) fiyat farkı
"""

import requests
import time
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID_06     = os.getenv("CHAT_ID_06")
CHAT_ID_15     = os.getenv("CHAT_ID_15", os.getenv("CHAT_ID_06"))
CHAT_ID_40     = os.getenv("CHAT_ID_40", os.getenv("CHAT_ID_06"))

GRUPLAR = [
    (4.0, "🔴", "BÜYÜK FIRSAT",  CHAT_ID_40, 120),
    (1.5, "🟠", "İYİ FIRSAT",    CHAT_ID_15, 180),
    (0.6, "🟡", "KÜÇÜK FIRSAT",  CHAT_ID_06, 300),
]

son_bildirim = {}


def telegram_gonder(chat_id, mesaj):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, json={
            "chat_id": chat_id,
            "text": mesaj,
            "parse_mode": "HTML"
        }, timeout=10)
    except Exception as e:
        print(f"Telegram hata: {e}")


def coinleri_getir():
    paribu_coinler  = set()
    btcturk_coinler = set()
    try:
        r = requests.get("https://api.paribu.com/ticker", timeout=10)
        for parite in r.json().keys():
            if parite.endswith("_tl"):
                paribu_coinler.add(parite.replace("_tl", "").upper())
        print(f"Paribu: {len(paribu_coinler)} coin")
    except Exception as e:
        print(f"Paribu liste hata: {e}")
    try:
        r = requests.get("https://api.btcturk.com/api/v2/ticker", timeout=10)
        for item in r.json().get("data", []):
            parite = item.get("pair", "")
            if parite.endswith("TRY"):
                btcturk_coinler.add(parite.replace("TRY", ""))
        print(f"BTCTürk: {len(btcturk_coinler)} coin")
    except Exception as e:
        print(f"BTCTürk liste hata: {e}")
    tum = paribu_coinler | btcturk_coinler
    hariç = {"USDT", "USDC", "BUSD", "DAI", "TRY", "TUSD", "USDP"}
    tum -= hariç
    print(f"Toplam: {len(tum)} coin takip edilecek")
    return sorted(tum)


def usdt_tl_kuru():
    """USDT/TL kurunu Paribu ve BTCTürk ortalamasından al"""
    kurlar = []
    try:
        r = requests.get("https://api.paribu.com/orderbook",
                         params={"market": "usdt_tl", "depth": 1}, timeout=5)
        veri = r.json()
        bids, asks = veri.get("bids", []), veri.get("asks", [])
        if bids and asks:
            kurlar.append((float(bids[0][0]) + float(asks[0][0])) / 2)
    except: pass
    try:
        r = requests.get("https://api.btcturk.com/api/v2/ticker",
                         params={"pairSymbol": "USDTTRY"}, timeout=5)
        veri = r.json().get("data", [])
        if veri:
            kurlar.append(float(veri[0]["last"]))
    except: pass
    return sum(kurlar) / len(kurlar) if kurlar else None


def yabanci_fiyat_usdt(coin):
    """Yabancı borsalardan USDT fiyatı al, en iyi fiyatı döndür"""
    fiyatlar = {}
    # Binance
    try:
        r = requests.get("https://api.binance.com/api/v3/ticker/price",
                         params={"symbol": f"{coin}USDT"}, timeout=5)
        if r.status_code == 200:
            fiyatlar["Binance"] = float(r.json()["price"])
    except: pass
    # Gate
    try:
        r = requests.get("https://api.gateio.ws/api/v4/spot/tickers",
                         params={"currency_pair": f"{coin}_USDT"}, timeout=5)
        veri = r.json()
        if veri and float(veri[0].get("last", 0)) > 0:
            fiyatlar["Gate"] = float(veri[0]["last"])
    except: pass
    # MEXC
    try:
        r = requests.get("https://api.mexc.com/api/v3/ticker/price",
                         params={"symbol": f"{coin}USDT"}, timeout=5)
        if r.status_code == 200:
            fiyatlar["MEXC"] = float(r.json()["price"])
    except: pass
    # OKX
    try:
        r = requests.get("https://www.okx.com/api/v5/market/ticker",
                         params={"instId": f"{coin}-USDT"}, timeout=5)
        veri = r.json()
        if veri.get("data"):
            fiyatlar["OKX"] = float(veri["data"][0]["last"])
    except: pass
    # Bybit
    try:
        r = requests.get("https://api.bybit.com/v5/market/tickers",
                         params={"category": "spot", "symbol": f"{coin}USDT"}, timeout=5)
        veri = r.json()
        if veri.get("result", {}).get("list"):
            fiyatlar["Bybit"] = float(veri["result"]["list"][0]["lastPrice"])
    except: pass
    return fiyatlar


def turk_fiyat_tl(coin):
    """Türk borsalardan TL fiyatı al"""
    fiyatlar = {}
    # Paribu
    try:
        r = requests.get("https://api.paribu.com/orderbook",
                         params={"market": f"{coin.lower()}_tl", "depth": 1}, timeout=5)
        veri = r.json()
        bids, asks = veri.get("bids", []), veri.get("asks", [])
        if bids and asks:
            fiyatlar["Paribu"] = (float(bids[0][0]) + float(asks[0][0])) / 2
    except: pass
    # BTCTürk
    try:
        r = requests.get("https://api.btcturk.com/api/v2/ticker",
                         params={"pairSymbol": f"{coin}TRY"}, timeout=5)
        veri = r.json().get("data", [])
        if veri:
            fiyatlar["BTCTürk"] = float(veri[0]["last"])
    except: pass
    return fiyatlar


def bildirim_gonder(coin, al_borsa, sat_borsa, al_fiyat, sat_fiyat, fark_yuzde, yon):
    for esik, emoji, etiket, chat_id, bekleme in GRUPLAR:
        if fark_yuzde >= esik:
            anahtar = f"{coin}_{al_borsa}_{sat_borsa}_{etiket}"
            son = son_bildirim.get(anahtar, 0)
            if time.time() - son > bekleme:
                son_bildirim[anahtar] = time.time()
                zaman = datetime.now().strftime("%H:%M:%S")
                mesaj = (
                    f"{emoji} <b>{etiket}</b>\n\n"
                    f"🪙 <b>{coin}</b>  {yon}\n"
                    f"📉 Al: <b>{al_borsa}</b> → {al_fiyat}\n"
                    f"📈 Sat: <b>{sat_borsa}</b> → {sat_fiyat}\n"
                    f"💰 Fark: <b>%{fark_yuzde:.2f}</b>\n"
                    f"🕐 {zaman}"
                )
                print(f"[{zaman}] {emoji} {coin} {al_borsa}→{sat_borsa} %{fark_yuzde:.2f}")
                telegram_gonder(chat_id, mesaj)
            break


def kontrol_et(coin, kur):
    if not kur or kur <= 0:
        return

    yabanci = yabanci_fiyat_usdt(coin)
    turk    = turk_fiyat_tl(coin)

    if not yabanci or not turk:
        return

    # En ucuz yabancı ve en pahalı yabancı
    en_ucuz_yab  = min(yabanci, key=yabanci.get)
    en_pahali_yab = max(yabanci, key=yabanci.get)

    # En ucuz Türk ve en pahalı Türk
    en_ucuz_turk  = min(turk, key=turk.get)
    en_pahali_turk = max(turk, key=turk.get)

    yab_ucuz_usdt  = yabanci[en_ucuz_yab]
    yab_pahali_usdt = yabanci[en_pahali_yab]
    turk_ucuz_tl   = turk[en_ucuz_turk]
    turk_pahali_tl  = turk[en_pahali_turk]

    # Yabancı'dan al (USDT), Türk'te sat (TL)
    # Yabancı fiyatını TL'ye çevir
    yab_ucuz_tl = yab_ucuz_usdt * kur
    if turk_pahali_tl > yab_ucuz_tl:
        fark = ((turk_pahali_tl - yab_ucuz_tl) / yab_ucuz_tl) * 100
        bildirim_gonder(
            coin,
            en_ucuz_yab, en_pahali_turk,
            f"${yab_ucuz_usdt:.6f} (≈₺{yab_ucuz_tl:.4f})",
            f"₺{turk_pahali_tl:.4f}",
            fark,
            "🌍→🇹🇷"
        )

    # Türk'ten al (TL), Yabancı'da sat (USDT)
    turk_ucuz_usdt = turk_ucuz_tl / kur
    if yab_pahali_usdt > turk_ucuz_usdt:
        fark = ((yab_pahali_usdt - turk_ucuz_usdt) / turk_ucuz_usdt) * 100
        bildirim_gonder(
            coin,
            en_ucuz_turk, en_pahali_yab,
            f"₺{turk_ucuz_tl:.4f} (≈${turk_ucuz_usdt:.6f})",
            f"${yab_pahali_usdt:.6f}",
            fark,
            "🇹🇷→🌍"
        )


def bot_calistir():
    print("Coin listesi alınıyor...")
    coinler = coinleri_getir()
    if not coinler:
        print("Coin listesi alınamadı!")
        return

    telegram_gonder(CHAT_ID_06,
        f"✅ <b>Arbitraj Alarm Botu Başladı</b>\n"
        f"📊 {len(coinler)} coin takip ediliyor\n"
        f"🌍↔🇹🇷 Yabancı ↔ Türk borsa\n"
        f"🟡 %0.6 / 🟠 %1.5 / 🔴 %4.0"
    )

    print(f"Bot başladı! {len(coinler)} coin taranıyor...")

    while True:
        kur = usdt_tl_kuru()
        if kur:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] USDT/TL: {kur:.2f}")

        for coin in coinler:
            try:
                kontrol_et(coin, kur)
            except Exception as e:
                print(f"{coin} hata: {e}")
            time.sleep(1 / len(coinler))

        print(f"[{datetime.now().strftime('%H:%M:%S')}] Tur tamamlandı")


if __name__ == "__main__":
    bot_calistir()
