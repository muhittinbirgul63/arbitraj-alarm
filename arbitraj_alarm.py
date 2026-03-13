"""
Kripto Arbitraj Alarm Botu v2
- Her borsa için tek sorguda tüm fiyatlar
- Paribu ↔ BTCTürk arası da karşılaştırılır
- 3 grup: %0.6 / %1.5 / %4
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


def binance_tumfiyatlar():
    try:
        r = requests.get("https://api.binance.com/api/v3/ticker/price", timeout=10)
        sonuc = {}
        for item in r.json():
            if item["symbol"].endswith("USDT"):
                coin = item["symbol"][:-4]
                sonuc[coin] = float(item["price"])
        return sonuc
    except Exception as e:
        print(f"Binance hata: {e}")
        return {}


def gate_tumfiyatlar():
    try:
        r = requests.get("https://api.gateio.ws/api/v4/spot/tickers", timeout=10)
        sonuc = {}
        for item in r.json():
            if item["currency_pair"].endswith("_USDT") and float(item.get("last", 0)) > 0:
                coin = item["currency_pair"][:-5]
                sonuc[coin] = float(item["last"])
        return sonuc
    except Exception as e:
        print(f"Gate hata: {e}")
        return {}


def mexc_tumfiyatlar():
    try:
        r = requests.get("https://api.mexc.com/api/v3/ticker/price", timeout=10)
        sonuc = {}
        for item in r.json():
            if item["symbol"].endswith("USDT"):
                coin = item["symbol"][:-4]
                sonuc[coin] = float(item["price"])
        return sonuc
    except Exception as e:
        print(f"MEXC hata: {e}")
        return {}


def okx_tumfiyatlar():
    try:
        r = requests.get("https://www.okx.com/api/v5/market/tickers",
                         params={"instType": "SPOT"}, timeout=10)
        sonuc = {}
        for item in r.json().get("data", []):
            if item["instId"].endswith("-USDT"):
                coin = item["instId"][:-5]
                sonuc[coin] = float(item["last"])
        return sonuc
    except Exception as e:
        print(f"OKX hata: {e}")
        return {}


def bybit_tumfiyatlar():
    try:
        r = requests.get("https://api.bybit.com/v5/market/tickers",
                         params={"category": "spot"}, timeout=10)
        sonuc = {}
        for item in r.json().get("result", {}).get("list", []):
            if item["symbol"].endswith("USDT"):
                coin = item["symbol"][:-4]
                sonuc[coin] = float(item["lastPrice"])
        return sonuc
    except Exception as e:
        print(f"Bybit hata: {e}")
        return {}


def paribu_tumfiyatlar():
    try:
        r = requests.get("https://api.paribu.com/ticker", timeout=10)
        sonuc = {}
        for parite, veri in r.json().items():
            if parite.endswith("_tl"):
                coin = parite[:-3].upper()
                fiyat = float(veri.get("last", 0))
                if fiyat > 0:
                    sonuc[coin] = fiyat
        return sonuc
    except Exception as e:
        print(f"Paribu hata: {e}")
        return {}


def btcturk_tumfiyatlar():
    try:
        r = requests.get("https://api.btcturk.com/api/v2/ticker", timeout=10)
        sonuc = {}
        for item in r.json().get("data", []):
            if item.get("pair", "").endswith("TRY"):
                coin = item["pair"][:-3]
                fiyat = float(item.get("last", 0))
                if fiyat > 0:
                    sonuc[coin] = fiyat
        return sonuc
    except Exception as e:
        print(f"BTCTürk hata: {e}")
        return {}


def usdt_tl_kuru(paribu_tl, btcturk_tl):
    kurlar = []
    if "USDT" in paribu_tl:
        kurlar.append(paribu_tl["USDT"])
    if "USDT" in btcturk_tl:
        kurlar.append(btcturk_tl["USDT"])
    return sum(kurlar) / len(kurlar) if kurlar else None


def bildirim_gonder(coin, al_borsa, sat_borsa, al_fiyat_str, sat_fiyat_str, fark_yuzde, yon):
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
                    f"📉 Al: <b>{al_borsa}</b> → {al_fiyat_str}\n"
                    f"📈 Sat: <b>{sat_borsa}</b> → {sat_fiyat_str}\n"
                    f"💰 Fark: <b>%{fark_yuzde:.2f}</b>\n"
                    f"🕐 {zaman}"
                )
                print(f"[{zaman}] {emoji} {coin} {al_borsa}→{sat_borsa} %{fark_yuzde:.2f}")
                telegram_gonder(chat_id, mesaj)
            break


def karsilastir(coin, usdt_fiyat, tl_fiyat, borsa_usdt, borsa_tl, kur):
    if not kur or kur <= 0:
        return
    # USDT borsadan al → TL borsada sat
    usdt_tl = usdt_fiyat * kur
    if tl_fiyat > usdt_tl:
        fark = ((tl_fiyat - usdt_tl) / usdt_tl) * 100
        bildirim_gonder(
            coin, borsa_usdt, borsa_tl,
            f"${usdt_fiyat:.6f} (≈₺{usdt_tl:.4f})",
            f"₺{tl_fiyat:.4f}",
            fark, "🌍→🇹🇷"
        )
    # TL borsadan al → USDT borsada sat
    tl_usdt = tl_fiyat / kur
    if usdt_fiyat > tl_usdt:
        fark = ((usdt_fiyat - tl_usdt) / tl_usdt) * 100
        bildirim_gonder(
            coin, borsa_tl, borsa_usdt,
            f"₺{tl_fiyat:.4f} (≈${tl_usdt:.6f})",
            f"${usdt_fiyat:.6f}",
            fark, "🇹🇷→🌍"
        )


def bot_calistir():
    print("Bot başlatılıyor...")

    while True:
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Fiyatlar çekiliyor...")

        # Tüm borsalardan tek sorguda fiyat al
        binance  = binance_tumfiyatlar()
        gate     = gate_tumfiyatlar()
        mexc     = mexc_tumfiyatlar()
        okx      = okx_tumfiyatlar()
        bybit    = bybit_tumfiyatlar()
        paribu   = paribu_tumfiyatlar()
        btcturk  = btcturk_tumfiyatlar()

        # USDT/TL kuru
        kur = usdt_tl_kuru(paribu, btcturk)
        if not kur:
            print("USDT/TL kuru alınamadı, bekleniyor...")
            time.sleep(10)
            continue

        print(f"USDT/TL: {kur:.2f} | Paribu: {len(paribu)} | BTCTürk: {len(btcturk)} coin")

        # Tüm TL coinleri
        tl_coinler = set(paribu.keys()) | set(btcturk.keys())
        tl_coinler.discard("USDT")

        usdt_borsalar = {
            "Binance": binance,
            "Gate":    gate,
            "MEXC":    mexc,
            "OKX":     okx,
            "Bybit":   bybit,
        }

        for coin in tl_coinler:
            # Yabancı borsalar ↔ Türk borsalar
            for borsa_usdt, fiyatlar_usdt in usdt_borsalar.items():
                if coin not in fiyatlar_usdt:
                    continue
                usdt_fiyat = fiyatlar_usdt[coin]

                if coin in paribu:
                    karsilastir(coin, usdt_fiyat, paribu[coin], borsa_usdt, "Paribu", kur)
                if coin in btcturk:
                    karsilastir(coin, usdt_fiyat, btcturk[coin], borsa_usdt, "BTCTürk", kur)

            # Paribu ↔ BTCTürk arası
            if coin in paribu and coin in btcturk:
                p_fiyat = paribu[coin]
                b_fiyat = btcturk[coin]
                if p_fiyat > 0 and b_fiyat > 0:
                    if p_fiyat > b_fiyat:
                        fark = ((p_fiyat - b_fiyat) / b_fiyat) * 100
                        bildirim_gonder(coin, "BTCTürk", "Paribu",
                                       f"₺{b_fiyat:.4f}", f"₺{p_fiyat:.4f}", fark, "🇹🇷↔🇹🇷")
                    elif b_fiyat > p_fiyat:
                        fark = ((b_fiyat - p_fiyat) / p_fiyat) * 100
                        bildirim_gonder(coin, "Paribu", "BTCTürk",
                                       f"₺{p_fiyat:.4f}", f"₺{b_fiyat:.4f}", fark, "🇹🇷↔🇹🇷")

        print(f"[{datetime.now().strftime('%H:%M:%S')}] Tur tamamlandı, 60s bekleniyor...")
        time.sleep(60)


if __name__ == "__main__":
    telegram_gonder(CHAT_ID_06,
        f"✅ <b>Arbitraj Alarm Botu v2 Başladı</b>\n"
        f"🌍↔🇹🇷 Yabancı ↔ Türk borsa\n"
        f"🇹🇷↔🇹🇷 Paribu ↔ BTCTürk\n"
        f"🟡 %0.6 / 🟠 %1.5 / 🔴 %4.0"
    )
    bot_calistir()
