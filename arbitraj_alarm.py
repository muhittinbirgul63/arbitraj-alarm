"""
Kripto Arbitraj Alarm Botu v5
- Binance, Gate, MEXC, OKX, KuCoin (USDT)
- Paribu, BTCTürk (TL)
- Orderbook doğrulama (tüm yönler)
- Kademeli ban sistemi
- Hacim kontrolü
"""

import requests
import time
import os
import json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

# Borsalarda farklı token olan coinler
MEXC_HARIC    = {"FB"}
GATE_HARIC    = {"FB"}
BINANCE_HARIC = {"GAL"}
OKX_HARIC     = set()
KUCOIN_HARIC  = {"FB"}

# Minimum 24s hacim (USDT)
MIN_HACIM_USDT = 100_000

# Tekrar süresi (saniye)
TEKRAR_SURE = {
    4.0: 120,
    1.5: 300,
    0.6: 600,
}

# Ban sistemi
son_bildirim = {}
coin_sayac   = {}
coin_ban     = {}
ban_seviye   = {}

BAN_SURELER = [600, 3600, 21600, 86400]  # 10dk, 1sa, 6sa, 24sa
SPAM_LIMIT  = 30
SPAM_SURE   = 600  # 10 dakika pencere

# Grup emojileri
GRUP_EMOJI = {
    4.0: "🚀",
    1.5: "📈",
    0.6: "📊",
}

# Manuel ban listesi (Telegram'dan /ban komutuyla eklenir)
BAN_DOSYA = "/app/manuel_ban.json"

def ban_yukle():
    try:
        if os.path.exists(BAN_DOSYA):
            with open(BAN_DOSYA, "r") as f:
                return set(json.load(f))
    except: pass
    return set()

def ban_kaydet(ban_set):
    try:
        with open(BAN_DOSYA, "w") as f:
            json.dump(list(ban_set), f)
    except Exception as e:
        print(f"Ban kayıt hata: {e}")

MANUEL_BAN = ban_yukle()

# Çekim/yatırma durum takibi
onceki_durum = {}  # {borsa_coin: True/False}
son_durum_kontrol = 0
DURUM_KONTROL_SURESI = 30  # saniye

# Yetkili kullanıcı ID
ADMIN_ID = os.getenv("ADMIN_ID", "1072335473")

# Hata sayaçları
hata_sayac = {}
HATA_LIMIT = 10

# Telegram update offset
update_offset = 0


def paribu_durum_kontrol():
    """Paribu status sayfasından çekim/yatırma durumunu çek"""
    try:
        r = requests.get("https://status.paribu.com/api/v2/components.json", timeout=10)
        sonuc = {}
        for item in r.json().get("components", []):
            isim = item.get("name", "")
            durum = item.get("status", "")  # operational, degraded_performance, partial_outage, major_outage
            aktif = durum == "operational"
            sonuc[f"Paribu_{isim}"] = {"aktif": aktif, "durum": durum, "isim": isim}
        return sonuc
    except Exception as e:
        print(f"Paribu durum hata: {e}")
        return {}


def btcturk_durum_kontrol():
    """BTCTürk exchangeinfo'dan çekim/yatırma durumunu çek"""
    try:
        r = requests.get("https://api.btcturk.com/api/v2/server/exchangeinfo", timeout=10)
        sonuc = {}
        for item in r.json().get("data", {}).get("currencies", []):
            isim = item.get("name", "")
            yatirma = item.get("depositEnable", True)
            cekim = item.get("withdrawEnable", True)
            sonuc[f"BTCTurk_{isim}_yatirma"] = {"aktif": yatirma, "isim": f"{isim} Yatırma"}
            sonuc[f"BTCTurk_{isim}_cekim"] = {"aktif": cekim, "isim": f"{isim} Çekim"}
        return sonuc
    except Exception as e:
        print(f"BTCTürk durum hata: {e}")
        return {}


def durum_kontrol_et():
    """Çekim/yatırma durumlarını kontrol et, değişiklik varsa bildir"""
    global onceki_durum
    cid = os.getenv("CHAT_ID_06")

    tum_durum = {}
    tum_durum.update(paribu_durum_kontrol())
    tum_durum.update(btcturk_durum_kontrol())

    for anahtar, bilgi in tum_durum.items():
        aktif = bilgi["aktif"]
        isim = bilgi["isim"]
        onceki = onceki_durum.get(anahtar)

        if onceki is None:
            onceki_durum[anahtar] = aktif
            continue

        if onceki != aktif:
            onceki_durum[anahtar] = aktif
            if not aktif:
                telegram_gonder(cid, f"🔴 <b>{isim}</b> kapatıldı!")
                print(f"[DURUM] 🔴 {isim} kapatıldı!")
            else:
                telegram_gonder(cid, f"🟢 <b>{isim}</b> tekrar açıldı!")
                print(f"[DURUM] 🟢 {isim} açıldı!")


def telegram_komutlari_isle():
    """Telegram'dan gelen /ban, /unban, /banlist komutlarını işle"""
    global update_offset, MANUEL_BAN
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
        r = requests.get(url, params={"offset": update_offset, "timeout": 1}, timeout=5)
        guncellemeler = r.json().get("result", [])
        for guncelleme in guncellemeler:
            update_offset = guncelleme["update_id"] + 1
            mesaj = guncelleme.get("message", {})
            chat_id = str(mesaj.get("chat", {}).get("id", ""))
            metin = mesaj.get("text", "").strip()

            print(f"[DEBUG] Mesaj geldi - chat_id: {chat_id}, metin: {metin[:30]}")
            izinli = [ADMIN_ID, os.getenv("CHAT_ID_06",""), os.getenv("CHAT_ID_15",""), os.getenv("CHAT_ID_40","")]
            if chat_id not in izinli:
                print(f"[DEBUG] Yetkisiz: {chat_id}")
                continue

            if metin.startswith("/ban "):
                coin = metin[5:].strip().upper()
                MANUEL_BAN.add(coin)
                ban_kaydet(MANUEL_BAN)
                telegram_gonder(chat_id, f"🚫 <b>{coin}</b> banlı listeye eklendi.")
                print(f"[KOMUT] /ban {coin}")

            elif metin.startswith("/unban "):
                coin = metin[7:].strip().upper()
                MANUEL_BAN.discard(coin)
                ban_kaydet(MANUEL_BAN)
                telegram_gonder(chat_id, f"✅ <b>{coin}</b> ban listesinden çıkarıldı.")
                print(f"[KOMUT] /unban {coin}")

            elif metin == "/banlist":
                if MANUEL_BAN:
                    liste = ", ".join(sorted(MANUEL_BAN))
                    telegram_gonder(chat_id, "🚫 <b>Banlı Coinler:</b>\n" + liste)
                else:
                    telegram_gonder(chat_id, "✅ Banlı coin yok.")
                print(f"[KOMUT] /banlist")

    except Exception as e:
        print(f"[KOMUT HATA] {e}")


def get_gruplar():
    cid_06 = os.getenv("CHAT_ID_06")
    cid_15 = os.getenv("CHAT_ID_15", cid_06)
    cid_40 = os.getenv("CHAT_ID_40", cid_06)
    return [
        (4.0, cid_40),
        (1.5, cid_15),
        (0.6, cid_06),
    ]


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


def borsa_hata_kontrol(borsa, basarili):
    global hata_sayac
    if basarili:
        hata_sayac[borsa] = 0
        return
    hata_sayac[borsa] = hata_sayac.get(borsa, 0) + 1
    if hata_sayac[borsa] == HATA_LIMIT:
        cid = os.getenv("CHAT_ID_06")
        telegram_gonder(cid, f"⚠️ <b>{borsa}</b> {HATA_LIMIT} turda üst üste hata veriyor!")
        print(f"[UYARI] {borsa} {HATA_LIMIT} tur üst üste hata!")


def fiyat_formatla(fiyat):
    if fiyat >= 1000:
        return f"{fiyat:,.2f}"
    elif fiyat >= 1:
        return f"{fiyat:.4f}"
    elif fiyat >= 0.01:
        return f"{fiyat:.4f}"
    elif fiyat >= 0.001:
        return f"{fiyat:.5f}"
    else:
        return f"{fiyat:.6f}"


# ─── BORSA FİYAT FONKSİYONLARI ───────────────────────────────────────────────

def binance_tek_fiyat(coin):
    """Tek coin için Binance fiyatı al"""
    try:
        r = requests.get("https://api.binance.com/api/v3/ticker/price",
                        params={"symbol": f"{coin}USDT"}, timeout=5)
        if r.status_code == 200:
            return float(r.json()["price"])
    except: pass
    return None


def binance_tek_hacim(coin):
    """Tek coin için Binance hacmi al"""
    try:
        r = requests.get("https://api.binance.com/api/v3/ticker/24hr",
                        params={"symbol": f"{coin}USDT"}, timeout=5)
        if r.status_code == 200:
            return float(r.json()["quoteVolume"])
    except: pass
    return 0


def binance_tumfiyatlar():
    # Binance tek sorguda 451 veriyor, tl_coinler listesi sonradan doluyor
    # Bu yüzden boş döndürüyoruz, karsilastir içinde coin bazlı sorgu atılacak
    return {}


def gate_tumfiyatlar():
    try:
        r = requests.get("https://api.gateio.ws/api/v4/spot/tickers", timeout=10)
        r.raise_for_status()
        sonuc = {}
        for item in r.json():
            if item["currency_pair"].endswith("_USDT"):
                coin = item["currency_pair"][:-5]
                if coin in GATE_HARIC: continue
                try:
                    fiyat = float(item.get("last", 0))
                    hacim = float(item.get("quote_volume", 0))
                    if fiyat > 0:
                        sonuc[coin] = {"fiyat": fiyat, "hacim": hacim}
                except: pass
        borsa_hata_kontrol("Gate", True)
        return sonuc
    except Exception as e:
        print(f"Gate hata: {e}")
        borsa_hata_kontrol("Gate", False)
        return {}


def mexc_tumfiyatlar():
    try:
        r = requests.get("https://api.mexc.com/api/v3/ticker/24hr", timeout=15)
        r.raise_for_status()
        sonuc = {}
        for item in r.json():
            if not isinstance(item, dict): continue
            sym = item.get("symbol", "")
            if sym.endswith("USDT"):
                coin = sym[:-4]
                if coin in MEXC_HARIC: continue
                try:
                    fiyat = float(item.get("lastPrice", 0))
                    hacim = float(item.get("quoteVolume", 0))
                    if fiyat > 0:
                        sonuc[coin] = {"fiyat": fiyat, "hacim": hacim}
                except: pass
        borsa_hata_kontrol("MEXC", True)
        return sonuc
    except Exception as e:
        print(f"MEXC hata: {e}")
        borsa_hata_kontrol("MEXC", False)
        return {}


def okx_tumfiyatlar():
    try:
        r = requests.get("https://www.okx.com/api/v5/market/tickers",
                         params={"instType": "SPOT"}, timeout=10)
        r.raise_for_status()
        sonuc = {}
        for item in r.json().get("data", []):
            if item["instId"].endswith("-USDT"):
                coin = item["instId"][:-5]
                if coin in OKX_HARIC: continue
                try:
                    fiyat = float(item.get("last", 0))
                    hacim = float(item.get("volCcy24h", 0))
                    if fiyat > 0:
                        sonuc[coin] = {"fiyat": fiyat, "hacim": hacim}
                except: pass
        borsa_hata_kontrol("OKX", True)
        return sonuc
    except Exception as e:
        print(f"OKX hata: {e}")
        borsa_hata_kontrol("OKX", False)
        return {}


def kucoin_tumfiyatlar():
    try:
        r = requests.get("https://api.kucoin.com/api/v1/market/allTickers", timeout=15)
        r.raise_for_status()
        sonuc = {}
        for item in r.json().get("data", {}).get("ticker", []):
            sym = item.get("symbol", "")
            if sym.endswith("-USDT"):
                coin = sym[:-5]
                if coin in KUCOIN_HARIC: continue
                try:
                    fiyat = float(item.get("last", 0) or 0)
                    hacim = float(item.get("volValue", 0) or 0)
                    if fiyat > 0:
                        sonuc[coin] = {"fiyat": fiyat, "hacim": hacim}
                except: pass
        borsa_hata_kontrol("KuCoin", True)
        print(f"KuCoin: {len(sonuc)} coin")
        return sonuc
    except Exception as e:
        print(f"KuCoin hata: {e}")
        borsa_hata_kontrol("KuCoin", False)
        return {}


def paribu_tumfiyatlar():
    try:
        r = requests.get("https://www.paribu.com/ticker", timeout=10)
        r.raise_for_status()
        sonuc = {}
        veri = r.json()
        if isinstance(veri, dict):
            for parite, bilgi in veri.items():
                if parite.endswith("_tl") or parite.endswith("TL"):
                    coin = parite.replace("_tl","").replace("TL","").upper()
                    try:
                        fiyat = float(bilgi.get("last", 0))
                        ask   = float(bilgi.get("lowestAsk", fiyat))
                        bid   = float(bilgi.get("highestBid", fiyat))
                        hacim = float(bilgi.get("volume", 0)) * fiyat
                        if fiyat > 0:
                            sonuc[coin] = {"fiyat": fiyat, "ask": ask, "bid": bid, "hacim": hacim}
                    except: pass
        borsa_hata_kontrol("Paribu", True)
        return sonuc
    except Exception as e:
        print(f"Paribu hata: {e}")
        borsa_hata_kontrol("Paribu", False)
        return {}


def btcturk_tumfiyatlar():
    try:
        r = requests.get("https://api.btcturk.com/api/v2/ticker", timeout=10)
        r.raise_for_status()
        sonuc = {}
        for item in r.json().get("data", []):
            if item.get("pair", "").endswith("TRY"):
                coin = item["pair"][:-3]
                try:
                    fiyat = float(item.get("last", 0))
                    ask   = float(item.get("ask", fiyat))
                    bid   = float(item.get("bid", fiyat))
                    hacim = float(item.get("volume", 0)) * fiyat
                    if fiyat > 0:
                        sonuc[coin] = {"fiyat": fiyat, "ask": ask, "bid": bid, "hacim": hacim}
                except: pass
        borsa_hata_kontrol("BTCTurk", True)
        return sonuc
    except Exception as e:
        print(f"BTCTurk hata: {e}")
        borsa_hata_kontrol("BTCTurk", False)
        return {}


# ─── ORDERBOOK FONKSİYONLARI ─────────────────────────────────────────────────

def orderbook_ask(borsa, coin):
    """Yabancı borsadan en iyi ask fiyatı — biz buradan alacağız"""
    try:
        if borsa == "Binance":
            for base in ["api","api1","api2","api3"]:
                try:
                    r = requests.get(f"https://{base}.binance.com/api/v3/ticker/bookTicker",
                                   params={"symbol": f"{coin}USDT"}, timeout=5)
                    if r.status_code == 200:
                        return float(r.json()["askPrice"])
                except: continue
        elif borsa == "Gate":
            r = requests.get("https://api.gateio.ws/api/v4/spot/order_book",
                           params={"currency_pair": f"{coin}_USDT", "limit": 1}, timeout=5)
            return float(r.json()["asks"][0][0])
        elif borsa == "MEXC":
            r = requests.get("https://api.mexc.com/api/v3/ticker/bookTicker",
                           params={"symbol": f"{coin}USDT"}, timeout=5)
            return float(r.json()["askPrice"])
        elif borsa == "OKX":
            r = requests.get("https://www.okx.com/api/v5/market/ticker",
                           params={"instId": f"{coin}-USDT"}, timeout=5)
            return float(r.json()["data"][0]["askPx"])
        elif borsa == "KuCoin":
            r = requests.get(f"https://api.kucoin.com/api/v1/market/orderbook/level1",
                           params={"symbol": f"{coin}-USDT"}, timeout=5)
            return float(r.json()["data"]["bestAsk"])
    except: pass
    return None


def orderbook_bid(borsa, coin):
    """Yabancı borsadan en iyi bid fiyatı — biz buraya satacağız"""
    try:
        if borsa == "Binance":
            for base in ["api","api1","api2","api3"]:
                try:
                    r = requests.get(f"https://{base}.binance.com/api/v3/ticker/bookTicker",
                                   params={"symbol": f"{coin}USDT"}, timeout=5)
                    if r.status_code == 200:
                        return float(r.json()["bidPrice"])
                except: continue
        elif borsa == "Gate":
            r = requests.get("https://api.gateio.ws/api/v4/spot/order_book",
                           params={"currency_pair": f"{coin}_USDT", "limit": 1}, timeout=5)
            return float(r.json()["bids"][0][0])
        elif borsa == "MEXC":
            r = requests.get("https://api.mexc.com/api/v3/ticker/bookTicker",
                           params={"symbol": f"{coin}USDT"}, timeout=5)
            return float(r.json()["bidPrice"])
        elif borsa == "OKX":
            r = requests.get("https://www.okx.com/api/v5/market/ticker",
                           params={"instId": f"{coin}-USDT"}, timeout=5)
            return float(r.json()["data"][0]["bidPx"])
        elif borsa == "KuCoin":
            r = requests.get(f"https://api.kucoin.com/api/v1/market/orderbook/level1",
                           params={"symbol": f"{coin}-USDT"}, timeout=5)
            return float(r.json()["data"]["bestBid"])
    except: pass
    return None


def paribu_bid(coin):
    try:
        r = requests.get("https://api.paribu.com/orderbook",
                        params={"market": f"{coin.lower()}_tl", "depth": 1}, timeout=5)
        veri = r.json()
        bids = veri.get("bids", [])
        if bids:
            ilk = bids[0]
            if isinstance(ilk, list): return float(ilk[0])
            elif isinstance(ilk, (int, float, str)): return float(ilk)
            elif isinstance(ilk, dict): return float(ilk.get("price", ilk.get("p", 0)))
    except: pass
    return None


def paribu_ask(coin):
    try:
        r = requests.get("https://api.paribu.com/orderbook",
                        params={"market": f"{coin.lower()}_tl", "depth": 1}, timeout=5)
        veri = r.json()
        asks = veri.get("asks", [])
        if asks:
            ilk = asks[0]
            if isinstance(ilk, list): return float(ilk[0])
            elif isinstance(ilk, (int, float, str)): return float(ilk)
            elif isinstance(ilk, dict): return float(ilk.get("price", ilk.get("p", 0)))
    except: pass
    return None


def btcturk_bid(coin):
    try:
        r = requests.get("https://api.btcturk.com/api/v2/orderbook",
                        params={"pairSymbol": f"{coin}TRY"}, timeout=5)
        veri = r.json()
        bids = veri.get("data", {}).get("bids", [])
        if bids:
            ilk = bids[0]
            if isinstance(ilk, list): return float(ilk[0])
            elif isinstance(ilk, dict): return float(ilk.get("price", ilk.get("P", 0)))
    except: pass
    return None


def btcturk_ask(coin):
    try:
        r = requests.get("https://api.btcturk.com/api/v2/orderbook",
                        params={"pairSymbol": f"{coin}TRY"}, timeout=5)
        veri = r.json()
        asks = veri.get("data", {}).get("asks", [])
        if asks:
            ilk = asks[0]
            if isinstance(ilk, list): return float(ilk[0])
            elif isinstance(ilk, dict): return float(ilk.get("price", ilk.get("P", 0)))
    except: pass
    return None


# ─── YARDIMCI FONKSİYONLAR ───────────────────────────────────────────────────

def usdt_tl_kuru(paribu, btcturk):
    kurlar = []
    if "USDT" in paribu:
        kurlar.append(paribu["USDT"]["fiyat"])
    if "USDT" in btcturk:
        kurlar.append(btcturk["USDT"]["fiyat"])
    return sum(kurlar) / len(kurlar) if kurlar else None


def bildirim_gonder(coin, al_borsa, sat_borsa, al_fiyat_str, sat_fiyat_str, fark_yuzde, hacim_usdt, kur):
    for esik, chat_id in get_gruplar():
        if fark_yuzde >= esik:
            anahtar = f"{coin}_{esik}"
            simdi = time.time()

            # Ban kontrolü
            if anahtar in coin_ban:
                if simdi < coin_ban[anahtar]:
                    break
                else:
                    del coin_ban[anahtar]
                    coin_sayac[anahtar] = []

            # Sayaç güncelle
            if anahtar not in coin_sayac:
                coin_sayac[anahtar] = []
            coin_sayac[anahtar] = [t for t in coin_sayac[anahtar] if simdi - t < SPAM_SURE]
            coin_sayac[anahtar].append(simdi)

            # Spam kontrolü
            if len(coin_sayac[anahtar]) > SPAM_LIMIT:
                seviye = ban_seviye.get(anahtar, 0)
                ban_sure = BAN_SURELER[min(seviye, len(BAN_SURELER)-1)]
                coin_ban[anahtar] = simdi + ban_sure
                ban_seviye[anahtar] = seviye + 1
                coin_sayac[anahtar] = []
                ban_dk = ban_sure // 60
                ban_sa = ban_dk // 60
                ban_str = f"{ban_sa} saat" if ban_sa > 0 else f"{ban_dk} dakika"
                print(f"[BAN] {coin} %{esik} - {ban_str} ban (seviye {seviye+1})")
                telegram_gonder(chat_id,
                    f"🚫 <b>{coin}</b> — {ban_str} ban\n"
                    f"10 dakikada {SPAM_LIMIT}+ bildirim gönderildi."
                )
                break

            # Tekrar süresi kontrolü
            son = son_bildirim.get(anahtar, 0)
            bekleme = TEKRAR_SURE.get(esik, 600)
            if simdi - son > bekleme:
                son_bildirim[anahtar] = simdi
                zaman = datetime.now().strftime("%H:%M:%S")
                grup_emoji = GRUP_EMOJI.get(esik, "📊")
                hacim_str = f"${hacim_usdt:,.0f}" if hacim_usdt >= MIN_HACIM_USDT else "⚠️ Yetersiz"
                mesaj = (
                    f"━━━━━━━━━━━━━━━\n"
                    f"🚨 <b>{coin}</b> {grup_emoji} %{fark_yuzde:.2f}\n"
                    f"━━━━━━━━━━━━━━━\n"
                    f"🟢 <b>{al_borsa}</b> → {al_fiyat_str}\n"
                    f"🔴 <b>{sat_borsa}</b> → {sat_fiyat_str}\n"
                    f"📊 {hacim_str} | ₺{kur:.2f} | {zaman}"
                )
                print(f"[{zaman}] {grup_emoji} {coin} {al_borsa}→{sat_borsa} %{fark_yuzde:.2f}")
                telegram_gonder(chat_id, mesaj)
            break


def karsilastir(coin, usdt_veri, tl_veri, borsa_usdt, borsa_tl, kur):
    if not kur or kur <= 0:
        return
    if coin in MANUEL_BAN:
        return
    usdt_fiyat = usdt_veri["fiyat"]
    tl_fiyat   = tl_veri["fiyat"]
    usdt_hacim = usdt_veri["hacim"]
    tl_hacim   = tl_veri["hacim"] / kur
    min_hacim  = min(usdt_hacim, tl_hacim)

    usdt_tl = usdt_fiyat * kur

    # Yabancıdan al → TL'de sat
    # Biz yabancıdan ask fiyatından alacağız, TL borsasında bid fiyatından satacağız
    tl_bid = tl_veri.get("bid", tl_fiyat)  # BTCTürk/Paribu bid
    if tl_bid > usdt_tl:
        fark = ((tl_bid - usdt_tl) / usdt_tl) * 100
        if fark > 50:
            return
        if fark >= 0.6:
            ask = orderbook_ask(borsa_usdt, coin)
            if ask and ask > 0:
                ask_tl = ask * kur
                gercek_fark = ((tl_bid - ask_tl) / ask_tl) * 100
                if gercek_fark > 0:
                    bildirim_gonder(
                        coin, borsa_usdt, borsa_tl,
                        f"${fiyat_formatla(ask)} (≈₺{fiyat_formatla(ask_tl)})",
                        f"₺{fiyat_formatla(tl_bid)} (≈${fiyat_formatla(tl_bid/kur)})",
                        gercek_fark, min_hacim, kur
                    )
            else:
                bildirim_gonder(
                    coin, borsa_usdt, borsa_tl,
                    f"${fiyat_formatla(usdt_fiyat)} (≈₺{fiyat_formatla(usdt_tl)})",
                    f"₺{fiyat_formatla(tl_bid)} (≈${fiyat_formatla(tl_bid/kur)})",
                    fark, min_hacim, kur
                )

    # TL'den al → Yabancıda sat
    # Biz TL borsasından ask fiyatından alacağız, yabancıda bid fiyatından satacağız
    tl_ask = tl_veri.get("ask", tl_fiyat)  # BTCTürk/Paribu ask
    tl_ask_usdt = tl_ask / kur
    if usdt_fiyat > tl_ask_usdt:
        fark = ((usdt_fiyat - tl_ask_usdt) / tl_ask_usdt) * 100
        if fark > 50:
            return
        if fark >= 0.6:
            bid = orderbook_bid(borsa_usdt, coin)
            if bid and bid > 0:
                gercek_fark = ((bid - tl_ask_usdt) / tl_ask_usdt) * 100
                if gercek_fark > 0:
                    bildirim_gonder(
                        coin, borsa_tl, borsa_usdt,
                        f"₺{fiyat_formatla(tl_ask)} (≈${fiyat_formatla(tl_ask_usdt)})",
                        f"${fiyat_formatla(bid)} (≈₺{fiyat_formatla(bid*kur)})",
                        gercek_fark, min_hacim, kur
                    )
            else:
                bildirim_gonder(
                    coin, borsa_tl, borsa_usdt,
                    f"₺{fiyat_formatla(tl_ask)} (≈${fiyat_formatla(tl_ask_usdt)})",
                    f"${fiyat_formatla(usdt_fiyat)} (≈₺{fiyat_formatla(usdt_fiyat*kur)})",
                    fark, min_hacim, kur
                )


def karsilastir_tl(coin, paribu_veri, btcturk_veri, kur):
    """Paribu ↔ BTCTürk orderbook ile karşılaştır"""
    if coin in MANUEL_BAN:
        return
    p_fiyat  = paribu_veri["fiyat"]
    b_fiyat  = btcturk_veri["fiyat"]
    p_hacim  = paribu_veri["hacim"] / kur
    b_hacim  = btcturk_veri["hacim"] / kur
    min_hacim = min(p_hacim, b_hacim)

    if p_fiyat <= 0 or b_fiyat <= 0:
        return

    # Paribu'dan al → BTCTürk'te sat
    if b_fiyat > p_fiyat:
        fark = ((b_fiyat - p_fiyat) / p_fiyat) * 100
        if 0 < fark <= 50:
            ask = paribu_ask(coin)
            bid = btcturk_bid(coin)
            if ask and bid and ask > 0 and bid > 0:
                gercek_fark = ((bid - ask) / ask) * 100
                if gercek_fark > 0:
                    bildirim_gonder(coin, "Paribu", "BTCTürk",
                        f"₺{fiyat_formatla(ask)}", f"₺{fiyat_formatla(bid)}",
                        gercek_fark, min_hacim, kur)
            else:
                bildirim_gonder(coin, "Paribu", "BTCTürk",
                    f"₺{fiyat_formatla(p_fiyat)}", f"₺{fiyat_formatla(b_fiyat)}",
                    fark, min_hacim, kur)

    # BTCTürk'ten al → Paribu'da sat
    elif p_fiyat > b_fiyat:
        fark = ((p_fiyat - b_fiyat) / b_fiyat) * 100
        if 0 < fark <= 50:
            ask = btcturk_ask(coin)
            bid = paribu_bid(coin)
            if ask and bid and ask > 0 and bid > 0:
                gercek_fark = ((bid - ask) / ask) * 100
                if gercek_fark > 0:
                    bildirim_gonder(coin, "BTCTürk", "Paribu",
                        f"₺{fiyat_formatla(ask)}", f"₺{fiyat_formatla(bid)}",
                        gercek_fark, min_hacim, kur)
            else:
                bildirim_gonder(coin, "BTCTürk", "Paribu",
                    f"₺{fiyat_formatla(b_fiyat)}", f"₺{fiyat_formatla(p_fiyat)}",
                    fark, min_hacim, kur)


# ─── ANA DÖNGÜ ───────────────────────────────────────────────────────────────

def bot_calistir():
    print("Arbitraj Alarm Botu v5 başlatılıyor...")

    telegram_gonder(os.getenv("CHAT_ID_06"),
        f"✅ <b>Arbitraj Alarm Botu v5 Başladı</b>\n"
        f"🏦 Binance, Gate, MEXC, OKX, KuCoin\n"
        f"🇹🇷 Paribu ↔ BTCTürk\n"
        f"📊 %0.6 / 📈 %1.5 / 🚀 %4.0\n"
        f"🛡 Kademeli ban sistemi aktif\n"
        f"💱 Min hacim: ${MIN_HACIM_USDT:,}"
    )

    while True:
        # Telegram komutlarını kontrol et
        telegram_komutlari_isle()

        # Çekim/yatırma durum kontrolü (30 saniyede bir)
        global son_durum_kontrol
        if time.time() - son_durum_kontrol >= DURUM_KONTROL_SURESI:
            durum_kontrol_et()
            son_durum_kontrol = time.time()

        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Fiyatlar çekiliyor...")

        binance = binance_tumfiyatlar()
        gate    = gate_tumfiyatlar()
        mexc    = mexc_tumfiyatlar()
        okx     = okx_tumfiyatlar()
        kucoin  = kucoin_tumfiyatlar()
        paribu  = paribu_tumfiyatlar()
        btcturk = btcturk_tumfiyatlar()

        kur = usdt_tl_kuru(paribu, btcturk)
        if not kur:
            print("USDT/TL kuru alınamadı, bekleniyor...")
            time.sleep(10)
            continue

        print(f"USDT/TL: {kur:.2f} | Paribu: {len(paribu)} | BTCTürk: {len(btcturk)} | KuCoin: {len(kucoin)} coin")

        tl_coinler = set(paribu.keys()) | set(btcturk.keys())
        tl_coinler.discard("USDT")

        usdt_borsalar = {
            "Binance": binance,
            "Gate":    gate,
            "MEXC":    mexc,
            "OKX":     okx,
            "KuCoin":  kucoin,
        }

        for coin in tl_coinler:
            # Manuel ban kontrolü
            if coin in MANUEL_BAN:
                continue

            # Binance coin bazlı sorgu (EU bölgede çalışıyor)
            if coin not in BINANCE_HARIC:
                b_fiyat = binance_tek_fiyat(coin)
                if b_fiyat and b_fiyat > 0:
                    b_hacim = binance_tek_hacim(coin)
                    b_veri = {"fiyat": b_fiyat, "hacim": b_hacim}
                    if coin in paribu:
                        karsilastir(coin, b_veri, paribu[coin], "Binance", "Paribu", kur)
                    if coin in btcturk:
                        karsilastir(coin, b_veri, btcturk[coin], "Binance", "BTCTürk", kur)

            for borsa_usdt, fiyatlar_usdt in usdt_borsalar.items():
                if coin not in fiyatlar_usdt:
                    continue
                if coin in paribu:
                    karsilastir(coin, fiyatlar_usdt[coin], paribu[coin], borsa_usdt, "Paribu", kur)
                if coin in btcturk:
                    karsilastir(coin, fiyatlar_usdt[coin], btcturk[coin], borsa_usdt, "BTCTürk", kur)

            # Paribu ↔ BTCTürk (orderbook ile)
            if coin in paribu and coin in btcturk:
                karsilastir_tl(coin, paribu[coin], btcturk[coin], kur)

        print(f"[{datetime.now().strftime('%H:%M:%S')}] Tur tamamlandı, 10s bekleniyor...")
        time.sleep(10)


if __name__ == "__main__":
    bot_calistir()
