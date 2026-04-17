"""
Kripto Arbitraj Alarm Botu v5
- Binance, Gate, MEXC, OKX, KuCoin (USDT)
- Paribu, BTCTürk (TL)
- Orderbook doğrulama
- Kademeli ban sistemi
- Hacim kontrolü
- Telegram komutları: /ban /unban /banlist
- Çekim/yatırma durum takibi
"""

import requests
import time
import os
import json
import threading
import websocket  # pip install websocket-client
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()

_session = requests.Session()

TZ_TR = timezone(timedelta(hours=3))

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
ADMIN_ID       = str(os.getenv("ADMIN_ID", "1072335473"))

# Borsalarda farklı token olan coinler
MEXC_HARIC    = {"FB"}
GATE_HARIC    = {"FB"}
BINANCE_HARIC = {"GAL"}
OKX_HARIC     = set()
KUCOIN_HARIC  = {"FB"}
BYBIT_HARIC   = set()

MIN_HACIM_USDT = 100_000

TEKRAR_SURE = {
    4.0: 30,
    1.5: 30,
    0.6: 30,
}

son_bildirim = {}
coin_sayac   = {}
coin_ban     = {}
ban_seviye   = {}

BAN_SURELER = [600, 3600, 21600, 86400]
SPAM_LIMIT  = 15
SPAM_SURE   = 60

GRUP_EMOJI = {
    4.0: "🚀",
    1.5: "📈",
    0.6: "📊",
}

hata_sayac = {}
HATA_LIMIT = 10

MANUEL_BAN = set()

onceki_durum     = {}
son_durum_kontrol = 0
DURUM_KONTROL_SURESI = 30

sonuclar_lock = threading.Lock()
okx_cache     = {}
binance_cache = {}
gate_cache    = {}
mexc_cache    = {}
kucoin_cache  = {}
bybit_cache   = {}

# Orderbook adayları için sabit worker havuzu (her tur 1000+ thread yaratmamak için)
ORDERBOOK_POOL = ThreadPoolExecutor(max_workers=50, thread_name_prefix="ob")
# Telegram mesajları için ayrı havuz — orderbook worker'ları Telegram'ı beklemesin
TELEGRAM_POOL  = ThreadPoolExecutor(max_workers=5,  thread_name_prefix="tg")

# ─── PARIBU WEBSOCKET ───────────────────────────────────────────────────────
PARIBU_WS_URL     = "wss://api.paribu.com/stream"
paribu_ws_cache   = {}              # {"BTC": {"fiyat": ..., "ask": ..., "bid": ..., "hacim": ...}, ...}
paribu_ws_son     = {}              # {"BTC": timestamp} - son güncelleme
paribu_ws_lock    = threading.Lock()
paribu_markets    = []              # ["btc_tl", "eth_tl", ...]
paribu_ws_bagli   = False
paribu_ws_msg_sayac = 0             # Debug: ilk mesajları log'la


# ─── TELEGRAM ────────────────────────────────────────────────────────────────

def get_gruplar():
    cid_06 = os.getenv("CHAT_ID_06")
    cid_15 = os.getenv("CHAT_ID_15", cid_06)
    cid_40 = os.getenv("CHAT_ID_40", cid_06)
    return [
        (4.0, cid_40),
        (1.5, cid_15),
        (0.6, cid_06),
    ]


def _telegram_gonder_blocking(chat_id, mesaj):
    """Gerçek Telegram HTTP çağrısı — arka plan thread'inde çalışır"""
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        _session.post(url, json={
            "chat_id": chat_id,
            "text": mesaj,
            "parse_mode": "HTML"
        }, timeout=10)
    except Exception as e:
        print(f"Telegram hata: {e}")


def telegram_gonder(chat_id, mesaj):
    """Non-blocking: Telegram çağrısını havuza atıp hemen döner.
    Orderbook worker'ları Telegram API'sini beklemez."""
    try:
        TELEGRAM_POOL.submit(_telegram_gonder_blocking, chat_id, mesaj)
    except Exception as e:
        print(f"Telegram pool hata: {e}")


def komut_dinleyici():
    offset = 0
    print("[KOMUT] Dinleyici başladı")
    while True:
        try:
            r = _session.get(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
                params={"offset": offset, "timeout": 30},
                timeout=35
            )
            veri = r.json()
            if not veri.get("ok"):
                time.sleep(5)
                continue

            for guncelleme in veri.get("result", []):
                offset = guncelleme["update_id"] + 1
                mesaj  = guncelleme.get("message", {})
                chat_id = str(mesaj.get("chat", {}).get("id", ""))
                metin   = mesaj.get("text", "").strip()

                if not metin or chat_id != ADMIN_ID:
                    continue

                print(f"[KOMUT] Gelen: chat_id={chat_id} metin={metin}")

                if metin.startswith("/ban "):
                    coin = metin[5:].strip().upper()
                    MANUEL_BAN.add(coin)
                    telegram_gonder(chat_id, f"🚫 <b>{coin}</b> banlı listeye eklendi.")
                    print(f"[KOMUT] /ban {coin} - Banlı: {MANUEL_BAN}")

                elif metin.startswith("/unban "):
                    coin = metin[7:].strip().upper()
                    MANUEL_BAN.discard(coin)
                    telegram_gonder(chat_id, f"✅ <b>{coin}</b> ban listesinden çıkarıldı.")
                    print(f"[KOMUT] /unban {coin}")

                elif metin == "/banlist":
                    if MANUEL_BAN:
                        telegram_gonder(chat_id, "🚫 <b>Banlı Coinler:</b>\n" + ", ".join(sorted(MANUEL_BAN)))
                    else:
                        telegram_gonder(chat_id, "✅ Banlı coin yok.")

        except Exception as e:
            print(f"[KOMUT HATA] {e}")
            time.sleep(5)


# ─── BORSA HATA KONTROLÜ ─────────────────────────────────────────────────────

def borsa_hata_kontrol(borsa, basarili):
    if basarili:
        hata_sayac[borsa] = 0
        return
    hata_sayac[borsa] = hata_sayac.get(borsa, 0) + 1
    if hata_sayac[borsa] == HATA_LIMIT:
        telegram_gonder(os.getenv("CHAT_ID_06"), f"⚠️ <b>{borsa}</b> {HATA_LIMIT} turda üst üste hata veriyor!")


# ─── FİYAT FORMATI ───────────────────────────────────────────────────────────

def fiyat_formatla(fiyat):
    if fiyat >= 1000:  return f"{fiyat:,.2f}"
    elif fiyat >= 1:   return f"{fiyat:.4f}"
    elif fiyat >= 0.01: return f"{fiyat:.4f}"
    elif fiyat >= 0.001: return f"{fiyat:.5f}"
    else:              return f"{fiyat:.6f}"


# ─── BORSA FİYAT FONKSİYONLARI ───────────────────────────────────────────────

def binance_tumfiyatlar():
    # Binance birden çok endpoint'i var, Türkiye/bazı IP'lerden erişim için fallback
    endpoints = [
        "https://api.binance.com/api/v3/ticker/24hr",
        "https://api1.binance.com/api/v3/ticker/24hr",
        "https://api2.binance.com/api/v3/ticker/24hr",
        "https://api3.binance.com/api/v3/ticker/24hr",
        "https://api4.binance.com/api/v3/ticker/24hr",
        "https://api-gcp.binance.com/api/v3/ticker/24hr",
    ]
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/131.0.0.0 Safari/537.36",
        "Accept": "application/json",
    }
    son_hata = None

    for url in endpoints:
        try:
            r = _session.get(url, headers=headers, timeout=15)
            if r.status_code != 200:
                son_hata = f"{url.split('//')[1].split('/')[0]} HTTP {r.status_code} body:{r.text[:120]}"
                continue
            try:
                data = r.json()
            except Exception:
                son_hata = f"{url.split('//')[1].split('/')[0]} JSON değil body:{r.text[:120]}"
                continue

            sonuc = {}
            for item in data:
                if not isinstance(item, dict): continue
                sym = item.get("symbol", "")
                if sym.endswith("USDT"):
                    coin = sym[:-4]
                    if coin in BINANCE_HARIC: continue
                    try:
                        fiyat = float(item["lastPrice"])
                        hacim = float(item["quoteVolume"])
                        ask   = float(item.get("askPrice", 0) or 0)
                        bid   = float(item.get("bidPrice", 0) or 0)
                        if fiyat > 0:
                            sonuc[coin] = {
                                "fiyat": fiyat,
                                "hacim": hacim,
                                "ask":   ask if ask > 0 else fiyat,
                                "bid":   bid if bid > 0 else fiyat,
                            }
                    except: pass
            if sonuc:
                borsa_hata_kontrol("Binance", True)
                return sonuc
        except Exception as e:
            son_hata = f"{url.split('//')[1].split('/')[0]} exception: {e}"
            continue

    print(f"Binance hata: {son_hata}")
    borsa_hata_kontrol("Binance", False)
    return {}


def gate_tumfiyatlar():
    try:
        r = _session.get("https://api.gateio.ws/api/v4/spot/tickers", timeout=10)
        sonuc = {}
        for item in r.json():
            if item["currency_pair"].endswith("_USDT"):
                coin = item["currency_pair"][:-5]
                if coin in GATE_HARIC: continue
                try:
                    fiyat = float(item.get("last", 0))
                    hacim = float(item.get("quote_volume", 0))
                    ask   = float(item.get("lowest_ask", 0) or 0)
                    bid   = float(item.get("highest_bid", 0) or 0)
                    if fiyat > 0:
                        sonuc[coin] = {
                            "fiyat": fiyat,
                            "hacim": hacim,
                            "ask":   ask if ask > 0 else fiyat,
                            "bid":   bid if bid > 0 else fiyat,
                        }
                except: pass
        borsa_hata_kontrol("Gate", True)
        return sonuc
    except Exception as e:
        print(f"Gate hata: {e}")
        borsa_hata_kontrol("Gate", False)
        return {}


def mexc_tumfiyatlar():
    try:
        r = _session.get("https://api.mexc.com/api/v3/ticker/24hr", timeout=15)
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
                    ask   = float(item.get("askPrice", 0) or 0)
                    bid   = float(item.get("bidPrice", 0) or 0)
                    if fiyat > 0:
                        sonuc[coin] = {
                            "fiyat": fiyat,
                            "hacim": hacim,
                            "ask":   ask if ask > 0 else fiyat,
                            "bid":   bid if bid > 0 else fiyat,
                        }
                except: pass
        borsa_hata_kontrol("MEXC", True)
        return sonuc
    except Exception as e:
        print(f"MEXC hata: {e}")
        borsa_hata_kontrol("MEXC", False)
        return {}


def okx_tumfiyatlar():
    try:
        r = _session.get("https://www.okx.com/api/v5/market/tickers",
                         params={"instType": "SPOT"}, timeout=10)
        sonuc = {}
        for item in r.json().get("data", []):
            if item["instId"].endswith("-USDT"):
                coin = item["instId"][:-5]
                if coin in OKX_HARIC: continue
                try:
                    fiyat = float(item.get("last", 0))
                    ask   = float(item.get("askPx", 0) or 0)
                    bid   = float(item.get("bidPx", 0) or 0)
                    hacim = float(item.get("volCcy24h", 0))
                    if fiyat > 0:
                        sonuc[coin] = {
                            "fiyat": fiyat,
                            "hacim": hacim,
                            "ask":   ask if ask > 0 else fiyat,
                            "bid":   bid if bid > 0 else fiyat,
                        }
                except: pass
        borsa_hata_kontrol("OKX", True)
        return sonuc
    except Exception as e:
        print(f"OKX hata: {e}")
        borsa_hata_kontrol("OKX", False)
        return {}


def kucoin_tumfiyatlar():
    try:
        r = _session.get("https://api.kucoin.com/api/v1/market/allTickers", timeout=15)
        sonuc = {}
        for item in r.json().get("data", {}).get("ticker", []):
            sym = item.get("symbol", "")
            if sym.endswith("-USDT"):
                coin = sym[:-5]
                if coin in KUCOIN_HARIC: continue
                try:
                    fiyat = float(item.get("last", 0) or 0)
                    hacim = float(item.get("volValue", 0) or 0)
                    ask   = float(item.get("sell", 0) or 0)
                    bid   = float(item.get("buy",  0) or 0)
                    if fiyat > 0:
                        sonuc[coin] = {
                            "fiyat": fiyat,
                            "hacim": hacim,
                            "ask":   ask if ask > 0 else fiyat,
                            "bid":   bid if bid > 0 else fiyat,
                        }
                except: pass
        borsa_hata_kontrol("KuCoin", True)
        return sonuc
    except Exception as e:
        print(f"KuCoin hata: {e}")
        borsa_hata_kontrol("KuCoin", False)
        return {}


def paribu_market_listesi_cek():
    """TL marketlerin listesini çek. Önce resmi API, olmazsa www fallback."""
    global paribu_markets
    markets = []

    # 1. Önce resmi endpoint: api.paribu.com/market/ticker (array format)
    try:
        url = "https://api.paribu.com/market/ticker"
        r = _session.get(url, timeout=10)
        data = r.json()
        if isinstance(data, list):
            for item in data:
                m = (item.get("market") or "").lower()
                if m.endswith("_tl"):
                    markets.append(m)
            if markets:
                paribu_markets = markets
                print(f"Paribu: {len(markets)} TL marketi bulundu (resmi API)")
                return True
    except Exception as e:
        print(f"Paribu resmi API market listesi hata: {e}")

    # 2. Fallback: www.paribu.com/ticker (eski dict format)
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/131.0.0.0 Safari/537.36",
            "Cache-Control": "no-cache",
        }
        url = f"https://www.paribu.com/ticker?_={int(time.time() * 1000)}"
        r = _session.get(url, headers=headers, timeout=10)
        data = r.json()
        if isinstance(data, dict):
            for parite in data.keys():
                pl = parite.lower()
                if pl.endswith("_tl"):
                    markets.append(pl)
            if markets:
                paribu_markets = markets
                print(f"Paribu: {len(markets)} TL marketi bulundu (www fallback)")
                return True
    except Exception as e:
        print(f"Paribu www fallback hata: {e}")

    print(f"Paribu market listesi alınamadı (0 market)")
    return False


def paribu_ws_on_open(ws):
    global paribu_ws_bagli
    paribu_ws_bagli = True
    print(f"✅ Paribu WebSocket bağlandı, {len(paribu_markets)} kanala abone olunuyor...")
    # Sunucuyu boğmamak için 50'şerli batch'ler halinde abone ol
    BATCH = 50
    for i in range(0, len(paribu_markets), BATCH):
        batch = paribu_markets[i:i+BATCH]
        channels = [f"ticker24h:{m}@1000ms" for m in batch]
        msg = {
            "method": "subscribe",
            "channels": channels,
            "id": f"sub_{i}",
        }
        try:
            ws.send(json.dumps(msg))
        except Exception as e:
            print(f"Paribu WS subscribe hata: {e}")
        time.sleep(0.1)
    print(f"✅ Paribu WS abone olundu")


def paribu_ws_on_message(ws, message):
    global paribu_ws_msg_sayac
    try:
        # Debug: ilk 3 mesajı log'la (sonra susar)
        paribu_ws_msg_sayac += 1
        if paribu_ws_msg_sayac <= 3:
            print(f"📨 Paribu WS mesaj #{paribu_ws_msg_sayac}: {message[:300]}")

        data = json.loads(message)
        if data.get("e") != "ticker24h":
            return
        symbol = data.get("s", "")
        if not symbol.endswith("_tl"):
            return
        coin = symbol[:-3].upper()
        r = data.get("r", {})
        fiyat = float(r.get("c", 0))
        if fiyat <= 0:
            return
        hacim_tl = float(r.get("q", 0))  # quote asset volume (TL cinsinden)
        with paribu_ws_lock:
            paribu_ws_cache[coin] = {
                "fiyat": fiyat,
                "ask":   fiyat,  # WS ticker'da bid/ask yok, son fiyatı kullan
                "bid":   fiyat,
                "hacim": hacim_tl,
            }
            paribu_ws_son[coin] = time.time()
    except Exception as e:
        pass  # Parse hataları çok sık log basmasın


def paribu_ws_on_error(ws, error):
    print(f"Paribu WS hata: {error}")


def paribu_ws_on_close(ws, close_status_code, close_msg):
    global paribu_ws_bagli
    paribu_ws_bagli = False
    print(f"Paribu WS kapandı: {close_status_code} {close_msg}")


def paribu_ws_thread():
    """Arka plan thread'i — disconnect olursa otomatik yeniden bağlanır"""
    while True:
        # Market listesini tazele (ilk açılış ve her reconnect'te)
        if not paribu_market_listesi_cek():
            print("Paribu market listesi alınamadı, 30sn sonra tekrar denenecek...")
            time.sleep(30)
            continue

        try:
            ws = websocket.WebSocketApp(
                PARIBU_WS_URL,
                on_open=paribu_ws_on_open,
                on_message=paribu_ws_on_message,
                on_error=paribu_ws_on_error,
                on_close=paribu_ws_on_close,
            )
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            print(f"Paribu WS exception: {e}")
        print("Paribu WS 5sn sonra yeniden bağlanıyor...")
        time.sleep(5)


def paribu_tumfiyatlar():
    """WebSocket cache'ten okur — REST çağrısı YOK"""
    try:
        now = time.time()
        sonuc = {}
        with paribu_ws_lock:
            for coin, veri in paribu_ws_cache.items():
                # Son 90sn içinde güncellenmiş coinleri dahil et (stale guard)
                if now - paribu_ws_son.get(coin, 0) < 90:
                    sonuc[coin] = dict(veri)
        if sonuc:
            borsa_hata_kontrol("Paribu", True)
        else:
            # WS bağlı ama henüz data gelmemiş olabilir (başlangıç)
            borsa_hata_kontrol("Paribu", paribu_ws_bagli)
        return sonuc
    except Exception as e:
        print(f"Paribu cache okuma hata: {e}")
        borsa_hata_kontrol("Paribu", False)
        return {}


def bybit_tumfiyatlar():
    # Bybit V5 API - tüm spot tickers tek çağrıda
    # Bazı bölgelerde api.bybit.com bloklanıyor, api.bytick.com fallback
    endpoints = [
        "https://api.bybit.com/v5/market/tickers",
        "https://api.bytick.com/v5/market/tickers",
    ]
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/131.0.0.0 Safari/537.36",
        "Accept": "application/json",
    }
    son_hata = None

    for url in endpoints:
        try:
            r = _session.get(url, params={"category": "spot"}, headers=headers, timeout=10)
            # HTTP status kontrolü
            if r.status_code != 200:
                son_hata = f"{url} HTTP {r.status_code} body:{r.text[:150]}"
                continue
            # JSON parse dene
            try:
                data = r.json()
            except Exception:
                son_hata = f"{url} JSON değil, body:{r.text[:150]}"
                continue
            if data.get("retCode") != 0:
                son_hata = f"{url} retCode={data.get('retCode')} msg={data.get('retMsg')}"
                continue

            sonuc = {}
            for item in data.get("result", {}).get("list", []):
                sym = item.get("symbol", "")
                if sym.endswith("USDT"):
                    coin = sym[:-4]
                    if coin in BYBIT_HARIC: continue
                    try:
                        fiyat = float(item.get("lastPrice", 0) or 0)
                        hacim = float(item.get("turnover24h", 0) or 0)
                        ask   = float(item.get("ask1Price", 0) or 0)
                        bid   = float(item.get("bid1Price", 0) or 0)
                        if fiyat > 0:
                            sonuc[coin] = {
                                "fiyat": fiyat,
                                "hacim": hacim,
                                "ask":   ask if ask > 0 else fiyat,
                                "bid":   bid if bid > 0 else fiyat,
                            }
                    except: pass
            borsa_hata_kontrol("Bybit", True)
            return sonuc
        except Exception as e:
            son_hata = f"{url} exception: {e}"
            continue

    # Tüm endpoint'ler başarısız
    print(f"Bybit hata: {son_hata}")
    borsa_hata_kontrol("Bybit", False)
    return {}


def btcturk_tumfiyatlar():
    try:
        r = _session.get("https://api.btcturk.com/api/v2/ticker", timeout=10)
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
        borsa_hata_kontrol("BTCTürk", True)
        return sonuc
    except Exception as e:
        print(f"BTCTürk hata: {e}")
        borsa_hata_kontrol("BTCTürk", False)
        return {}


# ─── ORDERBOOK FONKSİYONLARI ─────────────────────────────────────────────────

def orderbook_ask(borsa, coin):
    try:
        if borsa == "Binance":
            veri = binance_cache.get(coin)
            if veri and veri.get("ask", 0) > 0:
                return veri["ask"]
            # Fallback: direkt istek
            r = _session.get("https://api.binance.com/api/v3/ticker/bookTicker",
                           params={"symbol": f"{coin}USDT"}, timeout=5)
            if r.status_code == 200:
                return float(r.json()["askPrice"])
        elif borsa == "Gate":
            veri = gate_cache.get(coin)
            if veri and veri.get("ask", 0) > 0:
                return veri["ask"]
            r = _session.get("https://api.gateio.ws/api/v4/spot/order_book",
                           params={"currency_pair": f"{coin}_USDT", "limit": 1}, timeout=5)
            return float(r.json()["asks"][0][0])
        elif borsa == "MEXC":
            veri = mexc_cache.get(coin)
            if veri and veri.get("ask", 0) > 0:
                return veri["ask"]
            r = _session.get("https://api.mexc.com/api/v3/ticker/bookTicker",
                           params={"symbol": f"{coin}USDT"}, timeout=5)
            return float(r.json()["askPrice"])
        elif borsa == "OKX":
            veri = okx_cache.get(coin)
            if veri and veri.get("ask", 0) > 0:
                return veri["ask"]
            return None
        elif borsa == "KuCoin":
            veri = kucoin_cache.get(coin)
            if veri and veri.get("ask", 0) > 0:
                return veri["ask"]
            r = _session.get("https://api.kucoin.com/api/v1/market/orderbook/level1",
                           params={"symbol": f"{coin}-USDT"}, timeout=5)
            return float(r.json()["data"]["bestAsk"])
        elif borsa == "Bybit":
            veri = bybit_cache.get(coin)
            if veri and veri.get("ask", 0) > 0:
                return veri["ask"]
            r = _session.get("https://api.bybit.com/v5/market/orderbook",
                           params={"category": "spot", "symbol": f"{coin}USDT", "limit": 1}, timeout=5)
            return float(r.json()["result"]["a"][0][0])
    except: pass
    return None


def orderbook_bid(borsa, coin):
    try:
        if borsa == "Binance":
            veri = binance_cache.get(coin)
            if veri and veri.get("bid", 0) > 0:
                return veri["bid"]
            r = _session.get("https://api.binance.com/api/v3/ticker/bookTicker",
                           params={"symbol": f"{coin}USDT"}, timeout=5)
            if r.status_code == 200:
                return float(r.json()["bidPrice"])
        elif borsa == "Gate":
            veri = gate_cache.get(coin)
            if veri and veri.get("bid", 0) > 0:
                return veri["bid"]
            r = _session.get("https://api.gateio.ws/api/v4/spot/order_book",
                           params={"currency_pair": f"{coin}_USDT", "limit": 1}, timeout=5)
            return float(r.json()["bids"][0][0])
        elif borsa == "MEXC":
            veri = mexc_cache.get(coin)
            if veri and veri.get("bid", 0) > 0:
                return veri["bid"]
            r = _session.get("https://api.mexc.com/api/v3/ticker/bookTicker",
                           params={"symbol": f"{coin}USDT"}, timeout=5)
            return float(r.json()["bidPrice"])
        elif borsa == "OKX":
            veri = okx_cache.get(coin)
            if veri and veri.get("bid", 0) > 0:
                return veri["bid"]
            return None
        elif borsa == "KuCoin":
            veri = kucoin_cache.get(coin)
            if veri and veri.get("bid", 0) > 0:
                return veri["bid"]
            r = _session.get("https://api.kucoin.com/api/v1/market/orderbook/level1",
                           params={"symbol": f"{coin}-USDT"}, timeout=5)
            return float(r.json()["data"]["bestBid"])
        elif borsa == "Bybit":
            veri = bybit_cache.get(coin)
            if veri and veri.get("bid", 0) > 0:
                return veri["bid"]
            r = _session.get("https://api.bybit.com/v5/market/orderbook",
                           params={"category": "spot", "symbol": f"{coin}USDT", "limit": 1}, timeout=5)
            return float(r.json()["result"]["b"][0][0])
    except: pass
    return None


# ─── YARDIMCI ────────────────────────────────────────────────────────────────

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
            simdi   = time.time()

            if anahtar in coin_ban:
                if simdi < coin_ban[anahtar]:
                    break
                else:
                    del coin_ban[anahtar]
                    coin_sayac[anahtar] = []

            son     = son_bildirim.get(anahtar, 0)
            bekleme = TEKRAR_SURE.get(esik, 600)
            if simdi - son > bekleme:
                # Mesaj gönder
                son_bildirim[anahtar] = simdi
                zaman      = datetime.now(TZ_TR).strftime("%H:%M:%S")
                grup_emoji = GRUP_EMOJI.get(esik, "📊")
                hacim_str  = f"${hacim_usdt:,.0f}" if hacim_usdt >= MIN_HACIM_USDT else "⚠️ Yetersiz"
                mesaj = (
                    f"🚨 <b>{coin}</b> {grup_emoji} %{fark_yuzde:.2f}\n"
                    f"🟢 <b>{al_borsa}</b> → {al_fiyat_str}\n"
                    f"🔴 <b>{sat_borsa}</b> → {sat_fiyat_str}\n"
                    f"📊 {hacim_str} | ₺{kur:.2f} | {zaman}"
                )
                print(f"[{zaman}] {grup_emoji} {coin} {al_borsa}→{sat_borsa} %{fark_yuzde:.2f}")
                telegram_gonder(chat_id, mesaj)

                # Mesaj atıldıktan sonra sayaca ekle
                if anahtar not in coin_sayac:
                    coin_sayac[anahtar] = []
                coin_sayac[anahtar] = [t for t in coin_sayac[anahtar] if simdi - t < SPAM_SURE]
                coin_sayac[anahtar].append(simdi)

                if len(coin_sayac[anahtar]) > SPAM_LIMIT:
                    seviye   = ban_seviye.get(anahtar, 0)
                    ban_sure = BAN_SURELER[min(seviye, len(BAN_SURELER)-1)]
                    coin_ban[anahtar]   = simdi + ban_sure
                    ban_seviye[anahtar] = seviye + 1
                    coin_sayac[anahtar] = []
                    ban_dk  = ban_sure // 60
                    ban_sa  = ban_dk // 60
                    ban_str = f"{ban_sa} saat" if ban_sa > 0 else f"{ban_dk} dakika"
                    print(f"[BAN] {coin} %{esik} - {ban_str} ban (seviye {seviye+1})")
                    telegram_gonder(chat_id,
                        f"🚫 <b>{coin}</b> — {ban_str} ban\n"
                        f"10 dakikada {SPAM_LIMIT}+ bildirim gönderildi.")
            break


def karsilastir(coin, usdt_veri, tl_veri, borsa_usdt, borsa_tl, kur):
    """Hızlı ön tarama — orderbook olmadan fark var mı kontrol eder.
    Fark varsa (coin, borsa_usdt, yön) tuple'ı döner, yoksa None."""
    if not kur or kur <= 0 or coin in MANUEL_BAN:
        return None

    usdt_fiyat = usdt_veri["fiyat"]
    tl_bid     = tl_veri.get("bid", tl_veri["fiyat"])
    tl_ask     = tl_veri.get("ask", tl_veri["fiyat"])
    usdt_hacim = usdt_veri["hacim"]
    tl_hacim   = tl_veri["hacim"] / kur
    min_hacim  = min(usdt_hacim, tl_hacim)
    usdt_tl    = usdt_fiyat * kur

    sonuclar = []

    # Yabancıdan al → TL'de sat
    if tl_bid > usdt_tl:
        fark = ((tl_bid - usdt_tl) / usdt_tl) * 100
        if 0 < fark <= 50:
            sonuclar.append({
                "yon": "usdt_al",
                "coin": coin, "borsa_usdt": borsa_usdt, "borsa_tl": borsa_tl,
                "usdt_fiyat": usdt_fiyat, "tl_bid": tl_bid,
                "usdt_tl": usdt_tl, "fark": fark,
                "min_hacim": min_hacim, "kur": kur,
            })

    # TL'den al → Yabancıda sat
    tl_ask_usdt = tl_ask / kur
    if usdt_fiyat > tl_ask_usdt:
        fark = ((usdt_fiyat - tl_ask_usdt) / tl_ask_usdt) * 100
        if 0 < fark <= 50:
            sonuclar.append({
                "yon": "tl_al",
                "coin": coin, "borsa_usdt": borsa_usdt, "borsa_tl": borsa_tl,
                "usdt_fiyat": usdt_fiyat, "tl_ask": tl_ask,
                "tl_ask_usdt": tl_ask_usdt, "fark": fark,
                "min_hacim": min_hacim, "kur": kur,
            })

    return sonuclar if sonuclar else None


def karsilastir_orderbook(aday):
    """Tek bir aday için orderbook çekip gerçek farkı hesaplar ve alarm gönderir."""
    kur       = aday["kur"]
    coin      = aday["coin"]
    borsa_usdt = aday["borsa_usdt"]
    borsa_tl   = aday["borsa_tl"]
    min_hacim  = aday["min_hacim"]

    if aday["yon"] == "usdt_al":
        tl_bid  = aday["tl_bid"]
        ask = orderbook_ask(borsa_usdt, coin)
        if not ask or ask <= 0:
            print(f"[ORDERBOOK] {coin} {borsa_usdt} ask alınamadı, alarm atlandı")
            return
        ask_tl      = ask * kur
        gercek_fark = ((tl_bid - ask_tl) / ask_tl) * 100
        if gercek_fark > 0:
            bildirim_gonder(coin, borsa_usdt, borsa_tl,
                f"${fiyat_formatla(ask)} (≈₺{fiyat_formatla(ask_tl)})",
                f"₺{fiyat_formatla(tl_bid)} (≈${fiyat_formatla(tl_bid/kur)})",
                gercek_fark, min_hacim, kur)

    elif aday["yon"] == "tl_al":
        tl_ask      = aday["tl_ask"]
        tl_ask_usdt = aday["tl_ask_usdt"]
        fark        = aday["fark"]
        bid = orderbook_bid(borsa_usdt, coin)
        if not bid or bid <= 0:
            print(f"[ORDERBOOK] {coin} {borsa_usdt} bid alınamadı, alarm atlandı")
            return
        gercek_fark = ((bid - tl_ask_usdt) / tl_ask_usdt) * 100
        if gercek_fark > 0:
            bildirim_gonder(coin, borsa_tl, borsa_usdt,
                f"₺{fiyat_formatla(tl_ask)} (≈${fiyat_formatla(tl_ask_usdt)})",
                f"${fiyat_formatla(bid)} (≈₺{fiyat_formatla(bid*kur)})",
                gercek_fark, min_hacim, kur)


def karsilastir_tl(coin, paribu_veri, btcturk_veri, kur):
    if coin in MANUEL_BAN:
        return

    simdi = time.time()
    for esik, _ in get_gruplar():
        anahtar = f"{coin}_{esik}"
        if anahtar in coin_ban and simdi < coin_ban[anahtar]:
            return

    p_ask  = paribu_veri.get("ask",  paribu_veri["fiyat"])
    p_bid  = paribu_veri.get("bid",  paribu_veri["fiyat"])
    b_ask  = btcturk_veri.get("ask", btcturk_veri["fiyat"])
    b_bid  = btcturk_veri.get("bid", btcturk_veri["fiyat"])

    p_hacim   = paribu_veri["hacim"]  / kur
    b_hacim   = btcturk_veri["hacim"] / kur
    min_hacim = min(p_hacim, b_hacim)

    if p_ask <= 0 or b_ask <= 0:
        return

    # Paribu'dan al → BTCTürk'te sat
    if b_bid > p_ask:
        fark = ((b_bid - p_ask) / p_ask) * 100
        if 0 < fark <= 50:
            bildirim_gonder(coin, "Paribu", "BTCTürk",
                f"₺{fiyat_formatla(p_ask)}", f"₺{fiyat_formatla(b_bid)}",
                fark, min_hacim, kur)

    # BTCTürk'ten al → Paribu'da sat
    elif p_bid > b_ask:
        fark = ((p_bid - b_ask) / b_ask) * 100
        if 0 < fark <= 50:
            bildirim_gonder(coin, "BTCTürk", "Paribu",
                f"₺{fiyat_formatla(b_ask)}", f"₺{fiyat_formatla(p_bid)}",
                fark, min_hacim, kur)


# ─── DURUM KONTROLÜ ──────────────────────────────────────────────────────────

def paribu_durum_kontrol():
    try:
        r = _session.get("https://status.paribu.com/api/v2/components.json", timeout=10)
        sonuc = {}
        for item in r.json().get("components", []):
            isim  = item.get("name", "")
            durum = item.get("status", "")
            sonuc[f"Paribu_{isim}"] = {"aktif": durum == "operational", "isim": f"Paribu {isim}"}
        return sonuc
    except: return {}


def btcturk_durum_kontrol():
    try:
        r = _session.get("https://api.btcturk.com/api/v2/server/exchangeinfo", timeout=10)
        sonuc = {}
        for item in r.json().get("data", {}).get("currencies", []):
            isim = item.get("name", "")
            sonuc[f"BTCTurk_{isim}_yatirma"] = {"aktif": item.get("depositEnable",  True), "isim": f"BTCTürk {isim} Yatırma"}
            sonuc[f"BTCTurk_{isim}_cekim"]   = {"aktif": item.get("withdrawEnable", True), "isim": f"BTCTürk {isim} Çekim"}
        return sonuc
    except: return {}


def durum_kontrol_et():
    global onceki_durum
    cid = os.getenv("CHAT_ID_06")
    tum_durum = {**paribu_durum_kontrol(), **btcturk_durum_kontrol()}

    for anahtar, bilgi in tum_durum.items():
        aktif  = bilgi["aktif"]
        isim   = bilgi["isim"]
        onceki = onceki_durum.get(anahtar)
        if onceki is None:
            onceki_durum[anahtar] = aktif
            continue
        if onceki != aktif:
            onceki_durum[anahtar] = aktif
            if not aktif:
                telegram_gonder(cid, f"🔴 <b>{isim}</b> kapatıldı!")
            else:
                telegram_gonder(cid, f"🟢 <b>{isim}</b> tekrar açıldı!")


# ─── ANA DÖNGÜ ───────────────────────────────────────────────────────────────

def bot_calistir():
    global son_durum_kontrol

    print("Arbitraj Alarm Botu v5 başlatılıyor...")

    # Paribu WebSocket thread'i (arka planda sürekli çalışır)
    ws_thread = threading.Thread(target=paribu_ws_thread, daemon=True)
    ws_thread.start()

    # İlk Paribu datası gelene kadar bekle (max 20sn)
    print("Paribu WebSocket ilk veriler bekleniyor...")
    bekleme_basladi = time.time()
    while time.time() - bekleme_basladi < 20:
        with paribu_ws_lock:
            if len(paribu_ws_cache) > 10:
                break
        time.sleep(0.5)
    with paribu_ws_lock:
        print(f"Paribu WS: {len(paribu_ws_cache)} coin hazır, ana döngü başlıyor")

    komut_thread = threading.Thread(target=komut_dinleyici, daemon=True)
    komut_thread.start()

    telegram_gonder(os.getenv("CHAT_ID_06"),
        f"✅ <b>Arbitraj Alarm Botu v5 Başladı</b>\n"
        f"🏦 Binance, Gate, MEXC, OKX, KuCoin, Bybit\n"
        f"🇹🇷 Paribu ↔ BTCTürk\n"
        f"📊 %0.6 / 📈 %1.5 / 🚀 %4.0\n"
        f"🛡 Kademeli ban sistemi aktif\n"
        f"💱 Min hacim: ${MIN_HACIM_USDT:,}"
    )

    while True:
        # Durum kontrolü
        if time.time() - son_durum_kontrol >= DURUM_KONTROL_SURESI:
            durum_kontrol_et()
            son_durum_kontrol = time.time()

        tur_baslangic = time.time()
        print(f"\n[{datetime.now(TZ_TR).strftime('%H:%M:%S')}] Fiyatlar çekiliyor...")

        # Tüm borsaları paralel çek
        sonuclar = {}
        def cek(isim, fn):
            veri = fn()
            with sonuclar_lock:
                sonuclar[isim] = veri

        threadler = [
            threading.Thread(target=cek, args=("binance", binance_tumfiyatlar)),
            threading.Thread(target=cek, args=("gate",    gate_tumfiyatlar)),
            threading.Thread(target=cek, args=("mexc",    mexc_tumfiyatlar)),
            threading.Thread(target=cek, args=("okx",     okx_tumfiyatlar)),
            threading.Thread(target=cek, args=("kucoin",  kucoin_tumfiyatlar)),
            threading.Thread(target=cek, args=("bybit",   bybit_tumfiyatlar)),
            threading.Thread(target=cek, args=("paribu",  paribu_tumfiyatlar)),
            threading.Thread(target=cek, args=("btcturk", btcturk_tumfiyatlar)),
        ]
        for t in threadler: t.start()
        for t in threadler: t.join(timeout=20)

        binance = sonuclar.get("binance", {})
        gate    = sonuclar.get("gate",    {})
        mexc    = sonuclar.get("mexc",    {})
        okx     = sonuclar.get("okx",     {})
        kucoin  = sonuclar.get("kucoin",  {})
        bybit   = sonuclar.get("bybit",   {})
        paribu  = sonuclar.get("paribu",  {})
        btcturk = sonuclar.get("btcturk", {})

        # Cache güncelle (orderbook isteği atmamak için)
        okx_cache.clear();     okx_cache.update(okx)
        binance_cache.clear(); binance_cache.update(binance)
        gate_cache.clear();    gate_cache.update(gate)
        mexc_cache.clear();    mexc_cache.update(mexc)
        kucoin_cache.clear();  kucoin_cache.update(kucoin)
        bybit_cache.clear();   bybit_cache.update(bybit)

        kur = usdt_tl_kuru(paribu, btcturk)
        if not kur:
            print("USDT/TL kuru alınamadı, bekleniyor...")
            time.sleep(10)
            continue

        print(f"USDT/TL: {kur:.2f} | Paribu: {len(paribu)} | BTCTürk: {len(btcturk)} | Binance: {len(binance)} | Bybit: {len(bybit)} coin")

        tl_coinler = (set(paribu.keys()) | set(btcturk.keys())) - {"USDT"}

        usdt_borsalar = {
            "Binance": binance,
            "Gate":    gate,
            "MEXC":    mexc,
            "OKX":     okx,
            "KuCoin":  kucoin,
            "Bybit":   bybit,
        }

        # ── 1. Hızlı ön tarama (orderbook yok) ──
        adaylar = []
        for coin in tl_coinler:
            if coin in MANUEL_BAN:
                continue
            for borsa_usdt, fiyatlar_usdt in usdt_borsalar.items():
                if coin not in fiyatlar_usdt:
                    continue
                if coin in paribu:
                    sonuc = karsilastir(coin, fiyatlar_usdt[coin], paribu[coin], borsa_usdt, "Paribu", kur)
                    if sonuc:
                        adaylar.extend(sonuc)
                if coin in btcturk:
                    sonuc = karsilastir(coin, fiyatlar_usdt[coin], btcturk[coin], borsa_usdt, "BTCTürk", kur)
                    if sonuc:
                        adaylar.extend(sonuc)
            if coin in paribu and coin in btcturk:
                karsilastir_tl(coin, paribu[coin], btcturk[coin], kur)

        # ── 2. Adaylar için orderbook'ları paralel çek ──
        if adaylar:
            print(f"[{datetime.now(TZ_TR).strftime('%H:%M:%S')}] {len(adaylar)} aday bulundu, orderbook çekiliyor...")
            # ThreadPool ile 1000+ thread yaratma maliyetini ortadan kaldır
            futures = [ORDERBOOK_POOL.submit(karsilastir_orderbook, aday) for aday in adaylar]
            # Max 10sn bekle, uzun süren thread'leri bırak (bir sonraki turda yine denenir)
            wait(futures, timeout=10)

        tur_suresi = time.time() - tur_baslangic
        print(f"[{datetime.now(TZ_TR).strftime('%H:%M:%S')}] Tur tamamlandı. ({tur_suresi:.1f}sn)")
        time.sleep(0.3)


if __name__ == "__main__":
    bot_calistir()
