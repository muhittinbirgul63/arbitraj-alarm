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
import signal
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

# Borsalarda farklı token olan veya delisted coinler
MEXC_HARIC    = {"FB"}
GATE_HARIC    = {"FB"}
# 1 Nisan 2026'da Binance'ten delisted: A2Z, FORTH, HOOK, IDEX, LRC, NTRN, RDNT, SXP
# Nisan 2026'da Binance delist: OXT
# GAL: farklı token
BINANCE_HARIC = {"GAL", "A2Z", "FORTH", "HOOK", "IDEX", "LRC", "NTRN", "RDNT", "SXP", "OXT"}
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
MANUEL_BAN_DOSYA = "manuel_ban.json"


def manuel_ban_yukle():
    """Restart sonrası MANUEL_BAN'i dosyadan geri yükle."""
    global MANUEL_BAN
    try:
        if os.path.exists(MANUEL_BAN_DOSYA):
            with open(MANUEL_BAN_DOSYA, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    MANUEL_BAN = set(c.upper() for c in data)
                    print(f"[BAN] {len(MANUEL_BAN)} manuel ban yüklendi: "
                          f"{', '.join(sorted(MANUEL_BAN)) if MANUEL_BAN else '(boş)'}")
    except Exception as e:
        print(f"[BAN YÜKLE] Hata: {e}")


def manuel_ban_kaydet():
    """MANUEL_BAN'i dosyaya kaydet — /ban, /unban sonrası çağrılmalı."""
    try:
        with open(MANUEL_BAN_DOSYA, "w", encoding="utf-8") as f:
            json.dump(sorted(MANUEL_BAN), f, ensure_ascii=False)
    except Exception as e:
        print(f"[BAN KAYIT] Hata: {e}")

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

# Graceful shutdown — SIGTERM/SIGINT alındığında set edilir, ana döngü çıkar
_shutdown = threading.Event()

# Orderbook adayları için sabit worker havuzu.
# 50 worker → 10 worker: Binance'e aynı anda 50 paralel istek atmak rate limit
# aşıyordu. 10 worker yeterli; adaylar sıralı işlenir ama rate limit'i aşmaz.
ORDERBOOK_POOL = ThreadPoolExecutor(max_workers=10, thread_name_prefix="ob")
# Telegram mesajları için ayrı havuz — orderbook worker'ları Telegram'ı beklemesin
TELEGRAM_POOL  = ThreadPoolExecutor(max_workers=5,  thread_name_prefix="tg")

# TTL cache — 5sn → 15sn: Binance /ticker/24hr weight=80. Dakikada 12 çağrı
# (5sn TTL) 960 weight yapıyor; orderbook isteklerle toplam 1200'ü aşıp BAN
# yiyorduk. 15sn TTL ile dakikada 4 çağrı = 320 weight → rahat alan.
CACHE_TTL = 15.0


def ttl_cache(ttl=CACHE_TTL):
    """Borsa fiyat fonksiyonlarına TTL cache uygular.
    - Dönen dict boş değilse cache'ler
    - Fonksiyon boş dict dönerse (hata), son başarılı cache'i döner"""
    def decorator(func):
        state = {"data": {}, "time": 0}
        def wrapper(*args, **kwargs):
            simdi = time.time()
            if state["data"] and (simdi - state["time"]) < ttl:
                return state["data"]
            sonuc = func(*args, **kwargs)
            if sonuc:
                state["data"] = sonuc
                state["time"] = simdi
                return sonuc
            return state["data"]  # Hata: eski cache'i dön (başta {} olur)
        return wrapper
    return decorator


# ─── CONFIG KONTROLÜ ────────────────────────────────────────────────────────

def _config_kontrol_et():
    """Başlangıçta kritik env var'ların varlığını kontrol eder.
    Eksik varsa net hata verip çıkar — sessiz çalışıp mesaj kaybetmek yerine."""
    zorunlu = {
        "TELEGRAM_TOKEN": "Telegram bot token (BotFather'dan al)",
        "CHAT_ID_06":     "%0.6 alarm grubu chat ID'si",
    }
    eksik = [f"  - {k}: {a}" for k, a in zorunlu.items() if not os.getenv(k)]
    if eksik:
        print("❌ Eksik environment variable'lar:")
        for line in eksik:
            print(line)
        print("\nRailway → Variables sekmesinden eklemen gerekiyor.")
        raise SystemExit(1)

    # Opsiyonel — uyarı ver ama durdurmazsın
    if not os.getenv("CHAT_ID_15"):
        print("⚠️  CHAT_ID_15 yok, %1.5 alarmları CHAT_ID_06'ya gidecek")
    if not os.getenv("CHAT_ID_40"):
        print("⚠️  CHAT_ID_40 yok, %4.0 alarmları CHAT_ID_06'ya gidecek")
    print("✅ Config kontrolü tamam")


# ─── GRACEFUL SHUTDOWN ──────────────────────────────────────────────────────

def _sinyal_handler(signum, frame):
    """SIGTERM (Railway deploy) veya SIGINT (Ctrl+C) alındığında tetiklenir"""
    if not _shutdown.is_set():
        print(f"\n🛑 Sinyal {signum} alındı, temiz kapanma başlıyor...")
        _shutdown.set()


signal.signal(signal.SIGTERM, _sinyal_handler)
signal.signal(signal.SIGINT,  _sinyal_handler)


# ─── PERİYODİK TEMİZLİK (memory leak önleme) ────────────────────────────────

def periyodik_temizlik():
    """Her saatte bir eski kayıtları siler — RAM şişmesini önler.
    Haftalarca çalışan bot için önemli."""
    while not _shutdown.is_set():
        if _shutdown.wait(3600):  # 1 saat bekle, shutdown sinyalinde hemen çık
            break
        simdi = time.time()
        silinen = 0

        # son_bildirim: 1 günden eski kayıtlar (artık hiç gelmeyen coinler)
        for k in list(son_bildirim.keys()):
            if simdi - son_bildirim[k] > 86400:
                del son_bildirim[k]
                silinen += 1

        # coin_sayac: liste boşaldıysa veya hepsi eski ise
        for k in list(coin_sayac.keys()):
            coin_sayac[k] = [t for t in coin_sayac[k] if simdi - t < SPAM_SURE]
            if not coin_sayac[k]:
                del coin_sayac[k]
                silinen += 1

        # coin_ban: süresi dolmuş ban'ler
        for k in list(coin_ban.keys()):
            if coin_ban[k] < simdi:
                del coin_ban[k]
                silinen += 1

        # ban_seviye: artık aktif olmayan coinler
        for k in list(ban_seviye.keys()):
            if k not in coin_ban and k not in son_bildirim:
                del ban_seviye[k]
                silinen += 1

        if silinen > 0:
            print(f"[TEMİZLİK] {silinen} eski kayıt silindi, "
                  f"aktif: son_bildirim={len(son_bildirim)} "
                  f"coin_sayac={len(coin_sayac)} coin_ban={len(coin_ban)}")


# ─── PARIBU WEBSOCKET ───────────────────────────────────────────────────────
PARIBU_WS_URL     = "wss://api.paribu.com/stream"
paribu_ws_cache   = {}              # {"BTC": {"fiyat": ..., "ask": ..., "bid": ..., "hacim": ...}, ...}
paribu_ws_son     = {}              # {"BTC": timestamp} - son güncelleme
paribu_ws_lock    = threading.Lock()
paribu_markets    = []              # ["btc_tl", "eth_tl", ...]
paribu_ws_bagli   = False


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
                    manuel_ban_kaydet()  # Kalıcı sakla
                    telegram_gonder(chat_id, f"🚫 <b>{coin}</b> banlı listeye eklendi.")
                    print(f"[KOMUT] /ban {coin} - Banlı: {MANUEL_BAN}")

                elif metin.startswith("/unban "):
                    coin = metin[7:].strip().upper()
                    MANUEL_BAN.discard(coin)
                    manuel_ban_kaydet()  # Kalıcı sakla
                    telegram_gonder(chat_id, f"✅ <b>{coin}</b> ban listesinden çıkarıldı.")
                    print(f"[KOMUT] /unban {coin}")

                elif metin == "/banlist":
                    if MANUEL_BAN:
                        telegram_gonder(chat_id, "🚫 <b>Banlı Coinler:</b>\n" + ", ".join(sorted(MANUEL_BAN)))
                    else:
                        telegram_gonder(chat_id, "✅ Banlı coin yok.")

                elif metin == "/stat":
                    # Bot sağlık durumu — Binance cooldown, borsa hata sayaçları vs.
                    zaman = datetime.now(TZ_TR).strftime("%H:%M:%S")
                    satirlar = [f"📊 <b>Bot Durumu</b> [{zaman}]"]

                    # Binance rate limit durumu
                    if binance_rate_limit_aktif():
                        kalan = int(_binance_ban_kadar - time.time())
                        satirlar.append(f"🔴 Binance COOLDOWN: {kalan//60}dk {kalan%60}sn")
                    else:
                        satirlar.append(f"🟢 Binance: normal")

                    # MEXC cooldown
                    if time.time() < _mexc_devre_disi_kadar:
                        kalan = int(_mexc_devre_disi_kadar - time.time())
                        satirlar.append(f"🔴 MEXC COOLDOWN: {kalan//60}dk {kalan%60}sn")
                    else:
                        satirlar.append(f"🟢 MEXC: normal")

                    # Borsa hata sayaçları
                    if hata_sayac:
                        hatali = [(k, v) for k, v in hata_sayac.items() if v > 0]
                        if hatali:
                            hata_str = ", ".join(f"{b}:{v}" for b, v in hatali)
                            satirlar.append(f"⚠️ Hata sayaçları: {hata_str}")

                    # Aktif ban'ler
                    aktif_ban = len([k for k, v in coin_ban.items() if v > time.time()])
                    satirlar.append(f"🚫 Aktif otomatik ban: {aktif_ban} coin")
                    satirlar.append(f"🚫 Manuel ban: {len(MANUEL_BAN)} coin")

                    # Paribu WS durumu
                    if paribu_ws_bagli:
                        with paribu_ws_lock:
                            satirlar.append(f"🟢 Paribu WS: {len(paribu_ws_cache)} coin")
                    else:
                        satirlar.append(f"🔴 Paribu WS: BAĞLANTI YOK")

                    telegram_gonder(chat_id, "\n".join(satirlar))

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

# Binance rate limit cooldown — 418 (IP ban) veya 429 (rate limit) alınca
# bir süre tüm Binance isteklerini durdur. Aksi takdirde retry'larla ban
# süremiz uzar. 5 dakika güvenli default; Binance ban süreleri 2dk-3gün arası.
_binance_ban_kadar = 0
BINANCE_COOLDOWN = 300  # 5 dakika


def binance_rate_limit_aktif():
    """Binance'te aktif rate limit/ban cooldown var mı?"""
    return time.time() < _binance_ban_kadar


def _binance_ban_ayarla(sebep):
    """Binance'i belli bir süre devre dışı bırak."""
    global _binance_ban_kadar
    _binance_ban_kadar = time.time() + BINANCE_COOLDOWN
    kalan = BINANCE_COOLDOWN
    print(f"[BINANCE COOLDOWN] {sebep} → {kalan//60}dk tüm istekler durduruldu")


@ttl_cache()
def binance_tumfiyatlar():
    """Binance /ticker/24hr — weight=80. 15sn TTL ile dakikada 4 çağrı = 320 weight.
    Orderbook'larla birlikte 1200 weight/dakika limiti içinde kalırız."""
    # Ban cooldown aktifse → boş dön, ttl_cache önceki başarılı cache'i kullanır
    if binance_rate_limit_aktif():
        kalan = int(_binance_ban_kadar - time.time())
        print(f"[BINANCE] Cooldown aktif, {kalan}sn daha isteği atla")
        return {}

    # Türkiye/bazı IP'lerden erişim için çoklu endpoint fallback
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
            # 418 (IP ban) veya 429 (rate limit) → TÜM endpoint'lere dokunma
            if r.status_code in (418, 429):
                _binance_ban_ayarla(f"HTTP {r.status_code} from {url.split('//')[1].split('/')[0]}")
                borsa_hata_kontrol("Binance", False)
                return {}
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
                    # Delisted coinleri otomatik atla: count = 24h işlem sayısı
                    # Delisted olunca count=0 olur, fiyat eski kalır (zombi entry)
                    count = item.get("count")
                    if count is not None and int(count) == 0:
                        continue
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


@ttl_cache()
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
                    # Delisted/dormant filtre: hacim 0 ise atla (zombi entry)
                    if fiyat > 0 and hacim > 0:
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


# MEXC cool-down — peş peşe hata alınca bir süre taramayı atla (Türkiye'den
# zaman zaman 403 dönüyor, bunu fark edip MEXC'yi geçici olarak pas geçeriz)
_mexc_devre_disi_kadar = 0  # unix timestamp — bu zamana kadar MEXC atlanır
_mexc_hata_sayac = 0
MEXC_HATA_ESIGI = 3          # 3 ardışık hatadan sonra
MEXC_COOLDOWN   = 600        # 10 dakika devre dışı


@ttl_cache()
def mexc_tumfiyatlar():
    global _mexc_devre_disi_kadar, _mexc_hata_sayac
    # Cool-down aktifse → boş dön, ttl_cache önceki başarılı cache'i kullanır
    simdi = time.time()
    if simdi < _mexc_devre_disi_kadar:
        return {}

    try:
        r = _session.get("https://api.mexc.com/api/v3/ticker/24hr", timeout=15)
        if r.status_code != 200:
            _mexc_hata_sayac += 1
            if _mexc_hata_sayac >= MEXC_HATA_ESIGI:
                _mexc_devre_disi_kadar = simdi + MEXC_COOLDOWN
                print(f"[MEXC] {MEXC_HATA_ESIGI} ardışık hata (HTTP {r.status_code}) — "
                      f"{MEXC_COOLDOWN//60}dk devre dışı")
                _mexc_hata_sayac = 0
            borsa_hata_kontrol("MEXC", False)
            return {}

        sonuc = {}
        for item in r.json():
            if not isinstance(item, dict): continue
            sym = item.get("symbol", "")
            if sym.endswith("USDT"):
                coin = sym[:-4]
                if coin in MEXC_HARIC: continue
                # Delisted coinleri otomatik atla (count = 0)
                count = item.get("count")
                if count is not None and int(count) == 0:
                    continue
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
        _mexc_hata_sayac = 0  # Başarılı tur, sayacı sıfırla
        borsa_hata_kontrol("MEXC", True)
        return sonuc
    except Exception as e:
        _mexc_hata_sayac += 1
        if _mexc_hata_sayac >= MEXC_HATA_ESIGI:
            _mexc_devre_disi_kadar = simdi + MEXC_COOLDOWN
            print(f"[MEXC] {MEXC_HATA_ESIGI} ardışık hata ({e}) — "
                  f"{MEXC_COOLDOWN//60}dk devre dışı")
            _mexc_hata_sayac = 0
        else:
            print(f"MEXC hata: {e}")
        borsa_hata_kontrol("MEXC", False)
        return {}


@ttl_cache()
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
                    # Delisted/dormant filtre: hacim 0 ise atla
                    if fiyat > 0 and hacim > 0:
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


@ttl_cache()
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
                    # Delisted/dormant filtre: hacim 0 ise atla
                    if fiyat > 0 and hacim > 0:
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
    print(f"✅ Paribu WebSocket bağlandı, {len(paribu_markets)*2} kanala abone olunuyor...")
    # Hem ticker24h (last fiyat + hacim) hem orderbook (gerçek bid/ask) aboneliği
    BATCH = 50  # market başına 2 kanal = batch başına 100 kanal
    for i in range(0, len(paribu_markets), BATCH):
        batch = paribu_markets[i:i+BATCH]
        channels = []
        for m in batch:
            channels.append(f"ticker24h:{m}@1000ms")
            channels.append(f"orderbook:{m}@1000ms")
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
    try:
        data = json.loads(message)
        event = data.get("e")
        symbol = data.get("s", "")
        if not symbol.endswith("_tl"):
            return
        coin = symbol[:-3].upper()
        r = data.get("r", {})

        if event == "ticker24h":
            # Last fiyat ve 24h hacim bilgisi
            fiyat = float(r.get("c", 0))
            if fiyat <= 0:
                return
            hacim_tl = float(r.get("q", 0))
            with paribu_ws_lock:
                if coin in paribu_ws_cache:
                    # Mevcut bid/ask'ı koru (orderbook'tan gelmişti), sadece fiyat/hacim güncelle
                    paribu_ws_cache[coin]["fiyat"] = fiyat
                    paribu_ws_cache[coin]["hacim"] = hacim_tl
                else:
                    # Henüz orderbook gelmemiş → ask/bid None → karşılaştırmaya dahil edilmez.
                    # Sahte spread (ask=bid=last) VERMEK YANLIŞ ALARM ÜRETİR.
                    paribu_ws_cache[coin] = {
                        "fiyat": fiyat, "ask": None, "bid": None, "hacim": hacim_tl,
                        "bids": [], "asks": [],
                    }
                paribu_ws_son[coin] = time.time()

        elif event == "orderbook":
            # Gerçek best bid/ask — arbitraj için bu kritik
            bids_raw = r.get("b", [])
            asks_raw = r.get("a", [])
            if not bids_raw or not asks_raw:
                return
            try:
                # Tüm seviyeleri parse et (arbitraj hacim hesabı için)
                # Format: [[fiyat_str, miktar_str], ...]
                bids = []
                for seviye in bids_raw:
                    try:
                        p = float(seviye[0])
                        m = float(seviye[1])
                        if p > 0 and m > 0:
                            bids.append((p, m))
                    except (ValueError, IndexError, TypeError):
                        continue
                asks = []
                for seviye in asks_raw:
                    try:
                        p = float(seviye[0])
                        m = float(seviye[1])
                        if p > 0 and m > 0:
                            asks.append((p, m))
                    except (ValueError, IndexError, TypeError):
                        continue

                if not bids or not asks:
                    return

                # Bids: yüksekten düşüğe sırala (en yüksek alıcı başta)
                bids.sort(key=lambda x: x[0], reverse=True)
                # Asks: düşükten yükseğe sırala (en ucuz satıcı başta)
                asks.sort(key=lambda x: x[0])

                bid = bids[0][0]
                ask = asks[0][0]
            except Exception:
                return
            if bid <= 0 or ask <= 0:
                return
            with paribu_ws_lock:
                if coin in paribu_ws_cache:
                    paribu_ws_cache[coin]["ask"] = ask
                    paribu_ws_cache[coin]["bid"] = bid
                    paribu_ws_cache[coin]["bids"] = bids  # tüm bid seviyeleri
                    paribu_ws_cache[coin]["asks"] = asks  # tüm ask seviyeleri
                else:
                    # Henüz ticker gelmemiş → fiyat None → karşılaştırmaya dahil edilmez.
                    paribu_ws_cache[coin] = {
                        "fiyat": None, "ask": ask, "bid": bid, "hacim": 0,
                        "bids": bids, "asks": asks,
                    }
                paribu_ws_son[coin] = time.time()
    except Exception:
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
    """WebSocket cache'ten okur — REST çağrısı YOK.
    Sadece hem ticker HEM orderbook verisi gelmiş olan coinleri döner:
    eksik bid/ask ile yapılan karşılaştırmalar yanlış alarm üretir."""
    try:
        now = time.time()
        sonuc = {}
        with paribu_ws_lock:
            for coin, veri in paribu_ws_cache.items():
                # Son 90sn içinde güncellenmiş coinleri dahil et (stale guard)
                if now - paribu_ws_son.get(coin, 0) >= 90:
                    continue
                # Eksik veri filtresi: fiyat, bid, ask hepsi olmalı ve 0'dan büyük olmalı.
                # İlk ticker/orderbook gelip de diğeri gelmemiş coinler bu filtreye takılır.
                fiyat = veri.get("fiyat")
                bid = veri.get("bid")
                ask = veri.get("ask")
                if not fiyat or not bid or not ask:
                    continue
                if fiyat <= 0 or bid <= 0 or ask <= 0:
                    continue
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


@ttl_cache()
def bybit_tumfiyatlar():
    """Bybit V5 spot tickers. api.bybit.com bloklu bölgelerde api.bytick.com fallback."""
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
                        # Delisted/dormant filtre: hacim 0 ise atla
                        if fiyat > 0 and hacim > 0:
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

    print(f"Bybit hata: {son_hata}")
    borsa_hata_kontrol("Bybit", False)
    return {}


@ttl_cache()
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

# BTCTürk orderbook cache — alarm anında fetch edilir, 20sn TTL.
# Aynı coin için her turda tekrar istek atmamak için cache şart.
_btcturk_ob_cache = {}      # {"BTC": (timestamp, bids_list, asks_list)}
_btcturk_ob_lock = threading.Lock()
BTCTURK_OB_TTL = 20.0       # saniye


def btcturk_orderbook_al(coin):
    """BTCTürk /orderbook endpoint'ten tam orderbook çek. Cache'li.
    Returns: (bids, asks) — her biri [(fiyat, miktar), ...] TL cinsinden. None dönebilir.

    NOT: Sadece alarm tetiklendiğinde çağrılır (her taramada değil). Yani
    BTCTürk'e atılan ek istek sayısı = ilgili coin'in alarm sayısı. Genelde
    saniyede 1-2 coin alarm verdiği için rate limit riski düşük.
    """
    simdi = time.time()
    # Cache kontrolü
    with _btcturk_ob_lock:
        if coin in _btcturk_ob_cache:
            ts, bids, asks = _btcturk_ob_cache[coin]
            if simdi - ts < BTCTURK_OB_TTL:
                return bids, asks

    try:
        r = _session.get("https://api.btcturk.com/api/v2/orderbook",
                         params={"pairSymbol": f"{coin}_TRY", "limit": 25},
                         timeout=5)
        if r.status_code != 200:
            return None, None
        data = r.json().get("data", {})
        bids_raw = data.get("bids", [])
        asks_raw = data.get("asks", [])

        bids = []
        for seviye in bids_raw:
            try:
                # BTCTürk format: [fiyat_str, miktar_str]
                p = float(seviye[0])
                m = float(seviye[1])
                if p > 0 and m > 0:
                    bids.append((p, m))
            except (ValueError, IndexError, TypeError):
                continue
        asks = []
        for seviye in asks_raw:
            try:
                p = float(seviye[0])
                m = float(seviye[1])
                if p > 0 and m > 0:
                    asks.append((p, m))
            except (ValueError, IndexError, TypeError):
                continue

        if not bids or not asks:
            return None, None

        bids.sort(key=lambda x: x[0], reverse=True)
        asks.sort(key=lambda x: x[0])

        with _btcturk_ob_lock:
            _btcturk_ob_cache[coin] = (simdi, bids, asks)
        return bids, asks
    except Exception as e:
        print(f"[BTCTÜRK ORDERBOOK] {coin} hata: {e}")
        return None, None


def arb_hacim_hesapla_tl_sat(usdt_ask, bids_tl, kur):
    """
    TL borsasında BID tarafındaki kârlı emirleri topla.

    Senaryosu: Yurtdışı borsasından USDT ile coin alacaksın (usdt_ask fiyatından),
    TL borsasında satacaksın. TL borsasının bid tarafında, `usdt_ask × kur`'un
    ÜSTÜNDEKİ tüm emirler kârlıdır — onları topla.

    Args:
        usdt_ask: Yurtdışı borsasında alış fiyatı (USDT)
        bids_tl:  TL borsası bid listesi [(fiyat_tl, miktar), ...] — yüksekten düşüğe sıralı
        kur:      TL borsasının USDT/TL kuru

    Returns:
        {"coin_miktar": float, "tl_toplam": float, "usdt_toplam": float}
        Kârlı bölge yoksa miktar/toplam 0 döner.
    """
    if not bids_tl or not usdt_ask or usdt_ask <= 0 or not kur or kur <= 0:
        return {"coin_miktar": 0, "tl_toplam": 0, "usdt_toplam": 0}

    # Alış fiyatının TL karşılığı — bid bu değerin üstünde ise kârlı
    esik_tl = usdt_ask * kur

    toplam_coin = 0.0
    toplam_tl = 0.0
    for fiyat_tl, miktar in bids_tl:
        if fiyat_tl <= esik_tl:
            break  # bids yüksekten düşüğe sıralı, daha alta bakmaya gerek yok
        toplam_coin += miktar
        toplam_tl += fiyat_tl * miktar

    return {
        "coin_miktar": toplam_coin,
        "tl_toplam": toplam_tl,
        "usdt_toplam": toplam_tl / kur if kur > 0 else 0,
    }


def arb_hacim_hesapla_tl_al(usdt_bid, asks_tl, kur):
    """
    TL borsasında ASK tarafındaki kârlı emirleri topla (ters yön).

    Senaryosu: TL borsasından coin alacaksın (TL ask fiyatından),
    yurtdışı borsasında satacaksın (usdt_bid fiyatından). TL borsasının ask
    tarafında, `usdt_bid × kur`'un ALTINDAKİ tüm emirler kârlıdır — onları topla.

    Args:
        usdt_bid: Yurtdışı borsasında satış fiyatı (USDT)
        asks_tl:  TL borsası ask listesi [(fiyat_tl, miktar), ...] — düşükten yükseğe sıralı
        kur:      TL borsasının USDT/TL kuru

    Returns:
        {"coin_miktar": float, "tl_toplam": float, "usdt_toplam": float}
    """
    if not asks_tl or not usdt_bid or usdt_bid <= 0 or not kur or kur <= 0:
        return {"coin_miktar": 0, "tl_toplam": 0, "usdt_toplam": 0}

    esik_tl = usdt_bid * kur

    toplam_coin = 0.0
    toplam_tl = 0.0
    for fiyat_tl, miktar in asks_tl:
        if fiyat_tl >= esik_tl:
            break  # asks düşükten yükseğe sıralı, daha üste bakmaya gerek yok
        toplam_coin += miktar
        toplam_tl += fiyat_tl * miktar

    return {
        "coin_miktar": toplam_coin,
        "tl_toplam": toplam_tl,
        "usdt_toplam": toplam_tl / kur if kur > 0 else 0,
    }


def format_arb_hacim(arb, coin_sembol=""):
    """Arbitraj hacim sonucunu iki satır olarak formatlar:
      '₺890,000 ($19,786)\n🪙 Miktar   175,800 ITA'
    coin_sembol boşsa miktar satırı 'Miktar' yazar."""
    if not arb or arb["coin_miktar"] <= 0:
        return None
    usdt = arb["usdt_toplam"]
    tl   = arb.get("tl_toplam", 0)
    coin_m = arb["coin_miktar"]

    # Coin miktarı format
    if coin_m >= 10000:
        coin_str = f"{coin_m:,.0f}"
    elif coin_m >= 100:
        coin_str = f"{coin_m:.1f}"
    else:
        coin_str = f"{coin_m:.3f}".rstrip("0").rstrip(".")

    # USDT format
    if usdt >= 1000:
        usdt_str = f"${usdt:,.0f}"
    else:
        usdt_str = f"${usdt:.1f}"

    # TL format — TL önce
    if tl >= 1000:
        tl_str = f"₺{tl:,.0f}"
    else:
        tl_str = f"₺{tl:.2f}"

    # Sonuç: iki satır. "₺890,000 ($19,786)\n🪙 Miktar   175,800 ITA"
    miktar_satir = f"🪙 Miktar   {coin_str} {coin_sembol}".rstrip()
    return f"{tl_str} ({usdt_str})\n{miktar_satir}"


def orderbook_ask(borsa, coin):
    """Cache'ten ask fiyatını oku. Cache'te yoksa None dön.

    RATE LIMIT KORUMASI: Eskiden cache'te yoksa direkt HTTP isteği atıyorduk.
    100+ aday olduğunda dakikada 200+ ekstra istek → Binance IP banı. Artık
    sadece `_tumfiyatlar` fonksiyonlarının doldurduğu cache'i okuyoruz;
    cache yoksa o turda o aday skip edilir (bir sonraki turda cache dolu olur).
    Binance cooldown aktifse zaten boş cache var → None döner → aday skip.
    """
    veri = None
    if borsa == "Binance":
        veri = binance_cache.get(coin)
    elif borsa == "Gate":
        veri = gate_cache.get(coin)
    elif borsa == "MEXC":
        veri = mexc_cache.get(coin)
    elif borsa == "OKX":
        veri = okx_cache.get(coin)
    elif borsa == "KuCoin":
        veri = kucoin_cache.get(coin)
    elif borsa == "Bybit":
        veri = bybit_cache.get(coin)

    if veri and veri.get("ask", 0) > 0:
        return veri["ask"]
    return None


def orderbook_bid(borsa, coin):
    """Cache'ten bid fiyatını oku. Cache'te yoksa None (orderbook_ask ile aynı mantık)."""
    veri = None
    if borsa == "Binance":
        veri = binance_cache.get(coin)
    elif borsa == "Gate":
        veri = gate_cache.get(coin)
    elif borsa == "MEXC":
        veri = mexc_cache.get(coin)
    elif borsa == "OKX":
        veri = okx_cache.get(coin)
    elif borsa == "KuCoin":
        veri = kucoin_cache.get(coin)
    elif borsa == "Bybit":
        veri = bybit_cache.get(coin)

    if veri and veri.get("bid", 0) > 0:
        return veri["bid"]
    return None


# ─── YARDIMCI ────────────────────────────────────────────────────────────────

def usdt_tl_kurlari(paribu, btcturk):
    """Her TL borsasının kendi USDT/TL kurunu döner.
    Binance→Paribu arbitrajı için Paribu kuru, Binance→BTCTürk için BTCTürk kuru
    kullanılmalı — iki borsanın USDT fiyatı farklı olabilir, ortalama almak
    yanlış fark hesabına yol açar.

    Returns:
        {"Paribu": 40.25, "BTCTürk": 40.30}  — herhangi biri yoksa genel
        ortalamayı fallback olarak o borsaya da atar.
    """
    kurlar = {}
    if "USDT" in paribu:
        kurlar["Paribu"] = paribu["USDT"]["fiyat"]
    if "USDT" in btcturk:
        kurlar["BTCTürk"] = btcturk["USDT"]["fiyat"]

    # Fallback: biri yoksa diğerinin kurunu kullan (ikisi de yoksa None)
    if not kurlar:
        return None
    if "Paribu" not in kurlar:
        kurlar["Paribu"] = kurlar["BTCTürk"]
    if "BTCTürk" not in kurlar:
        kurlar["BTCTürk"] = kurlar["Paribu"]
    return kurlar


def usdt_tl_kuru(paribu, btcturk):
    """Geriye uyumluluk için — Paribu durumu kontrol mesajında vb. kullanılır.
    Gerçek karşılaştırmalarda usdt_tl_kurlari() kullanılmalı."""
    kurlar = usdt_tl_kurlari(paribu, btcturk)
    if not kurlar:
        return None
    return sum(kurlar.values()) / len(kurlar)


def bildirim_gonder(coin, al_borsa, sat_borsa, al_fiyat_str, sat_fiyat_str, fark_yuzde, hacim_usdt, kur, arb_str=None):
    for esik, chat_id in get_gruplar():
        if fark_yuzde >= esik:
            anahtar = f"{coin}_{esik}"
            simdi   = time.time()

            if anahtar in coin_ban:
                if simdi < coin_ban[anahtar]:
                    # DÜZELTME (madde 9): Bu eşik banlı → sadece bu eşiği atla,
                    # sıradaki daha düşük eşiğe geç. Eskiden 'break' idi → tüm
                    # eşikleri kilitliyordu.
                    continue
                else:
                    del coin_ban[anahtar]
                    coin_sayac[anahtar] = []
                    # DÜZELTME (madde 12): Ban süresi dolduğunda ban_seviye'yi
                    # 1 azalt. Eski kod seviyeyi hiç sıfırlamıyordu → bir kere
                    # spam yapan coin ömür boyu yüksek seviye ban yiyordu.
                    if anahtar in ban_seviye and ban_seviye[anahtar] > 0:
                        ban_seviye[anahtar] -= 1

            son     = son_bildirim.get(anahtar, 0)
            bekleme = TEKRAR_SURE.get(esik, 600)
            if simdi - son > bekleme:
                # Mesaj gönder
                son_bildirim[anahtar] = simdi
                zaman      = datetime.now(TZ_TR).strftime("%H:%M:%S")
                grup_emoji = GRUP_EMOJI.get(esik, "📊")
                hacim_str  = f"${hacim_usdt:,.0f}" if hacim_usdt >= MIN_HACIM_USDT else "⚠️ Yetersiz"

                # al_fiyat_str ve sat_fiyat_str'i TL önce, dolar parantez formatına standartlaştır
                def tl_once(s):
                    s = s.strip()
                    if s.startswith("₺") and "(≈$" in s:
                        return s.replace("(≈$", "($")
                    if s.startswith("$") and "(≈₺" in s:
                        dolar_kismi, tl_kismi = s.split(" (≈", 1)
                        tl_kismi = tl_kismi.rstrip(")")
                        return f"{tl_kismi} ({dolar_kismi})"
                    return s

                al_tl  = tl_once(al_fiyat_str)
                sat_tl = tl_once(sat_fiyat_str)

                # Tasarım 1 — minimalist & hizalı
                # arb_str iki satırlı gelir:
                #   "₺890,000 ($19,786)
                #    🪙 Miktar   175,800 ITA"
                # İlk satırı "💎 Hacim    ..." ile birlikte kullanırız,
                # ikinci satır kendisi zaten "🪙 Miktar ..." diye başlar.
                satirlar = [
                    f"🚨 <b>{coin}</b> — %{fark_yuzde:.2f} {grup_emoji}",
                    "━━━━━━━━━━━━━━━",
                    f"🟢 Al  │ <b>{al_borsa}</b>   {al_tl}",
                    f"🔴 Sat │ <b>{sat_borsa}</b>   {sat_tl}",
                    "━━━━━━━━━━━━━━━",
                ]
                if arb_str:
                    # arb_str = "₺X (₹Y)\n🪙 Miktar   Z COIN"  → iki ayrı satır olarak yaz
                    arb_satirlar = arb_str.split("\n")
                    satirlar.append(f"💎 Hacim    {arb_satirlar[0]}")
                    for ek in arb_satirlar[1:]:
                        satirlar.append(ek)
                satirlar.append(f"📊 24h      {hacim_str}")
                satirlar.append(f"⏱️ {zaman} · ₺{kur:.2f}")
                mesaj = "\n".join(satirlar)
                print(f"[{zaman}] {grup_emoji} {coin} {al_borsa}→{sat_borsa} %{fark_yuzde:.2f}"
                      + (f" | {arb_str.split(chr(10))[0]}" if arb_str else ""))
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

    # TL orderbook seviyeleri — Paribu WS'ten gelir, BTCTürk için None
    # (karsilastir_orderbook anında HTTP ile fetch edilir)
    tl_bids = tl_veri.get("bids")  # [(fiyat, miktar), ...] yüksekten düşüğe
    tl_asks = tl_veri.get("asks")  # [(fiyat, miktar), ...] düşükten yükseğe

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
                "tl_bids": tl_bids, "tl_asks": tl_asks,
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
                "tl_bids": tl_bids, "tl_asks": tl_asks,
            })

    return sonuclar if sonuclar else None


def karsilastir_orderbook(aday):
    """Tek bir aday için orderbook çekip gerçek farkı hesaplar ve alarm gönderir.
    Ayrıca TL borsasındaki orderbook derinliğinden arbitraj hacmini hesaplar."""
    kur       = aday["kur"]
    coin      = aday["coin"]
    borsa_usdt = aday["borsa_usdt"]
    borsa_tl   = aday["borsa_tl"]
    min_hacim  = aday["min_hacim"]

    # TL orderbook — Paribu'dan geldiyse cache'te var; BTCTürk ise anında fetch
    tl_bids = aday.get("tl_bids")
    tl_asks = aday.get("tl_asks")
    if borsa_tl == "BTCTürk" and (tl_bids is None or tl_asks is None):
        tl_bids, tl_asks = btcturk_orderbook_al(coin)

    if aday["yon"] == "usdt_al":
        tl_bid  = aday["tl_bid"]
        ask = orderbook_ask(borsa_usdt, coin)
        if not ask or ask <= 0:
            print(f"[ORDERBOOK] {coin} {borsa_usdt} ask alınamadı, alarm atlandı")
            return
        ask_tl      = ask * kur
        gercek_fark = ((tl_bid - ask_tl) / ask_tl) * 100
        if gercek_fark > 0:
            # Arbitraj hacmi: TL borsasının bid tarafında usdt_ask×kur'un
            # üstündeki emirleri topla
            arb = arb_hacim_hesapla_tl_sat(ask, tl_bids, kur) if tl_bids else None
            arb_str = format_arb_hacim(arb, coin)

            bildirim_gonder(coin, borsa_usdt, borsa_tl,
                f"${fiyat_formatla(ask)} (≈₺{fiyat_formatla(ask_tl)})",
                f"₺{fiyat_formatla(tl_bid)} (≈${fiyat_formatla(tl_bid/kur)})",
                gercek_fark, min_hacim, kur, arb_str=arb_str)

    elif aday["yon"] == "tl_al":
        tl_ask      = aday["tl_ask"]
        tl_ask_usdt = aday["tl_ask_usdt"]
        bid = orderbook_bid(borsa_usdt, coin)
        if not bid or bid <= 0:
            print(f"[ORDERBOOK] {coin} {borsa_usdt} bid alınamadı, alarm atlandı")
            return
        gercek_fark = ((bid - tl_ask_usdt) / tl_ask_usdt) * 100
        if gercek_fark > 0:
            # Ters yön: TL borsasının ask tarafında usdt_bid×kur'un altındaki
            # emirleri topla (ucuza alınabilecek hacim)
            arb = arb_hacim_hesapla_tl_al(bid, tl_asks, kur) if tl_asks else None
            arb_str = format_arb_hacim(arb, coin)

            bildirim_gonder(coin, borsa_tl, borsa_usdt,
                f"₺{fiyat_formatla(tl_ask)} (≈${fiyat_formatla(tl_ask_usdt)})",
                f"${fiyat_formatla(bid)} (≈₺{fiyat_formatla(bid*kur)})",
                gercek_fark, min_hacim, kur, arb_str=arb_str)


def karsilastir_tl(coin, paribu_veri, btcturk_veri, kur_paribu, kur_btcturk):
    """Paribu ↔ BTCTürk arbitrajı — her borsa kendi USDT kuruyla hacim hesaplar.

    DÜZELTME (madde 9): Eski kod herhangi bir eşik banlıysa tüm fonksiyonu
    terk ediyordu — düşük eşikler de kilitleniyordu. Kaldırıldı. Ban kontrolü
    zaten bildirim_gonder içinde her eşik için ayrı yapılıyor."""
    if coin in MANUEL_BAN:
        return

    p_ask  = paribu_veri.get("ask",  paribu_veri["fiyat"])
    p_bid  = paribu_veri.get("bid",  paribu_veri["fiyat"])
    b_ask  = btcturk_veri.get("ask", btcturk_veri["fiyat"])
    b_bid  = btcturk_veri.get("bid", btcturk_veri["fiyat"])

    # Hacim hesabı — her borsa kendi kuruyla USDT'ye çevrilir
    p_hacim   = paribu_veri["hacim"]  / kur_paribu
    b_hacim   = btcturk_veri["hacim"] / kur_btcturk
    min_hacim = min(p_hacim, b_hacim)

    if p_ask <= 0 or b_ask <= 0:
        return

    # TL-TL arbitraj için ortak kur (her iki borsa TL) — fark hesabında
    # doğrudan TL fiyatlar kullanılıyor zaten, kur sadece hacim USDT dönüşümü için.
    # Paribu'dan al → BTCTürk'te sat
    if b_bid > p_ask:
        fark = ((b_bid - p_ask) / p_ask) * 100
        if 0 < fark <= 50:
            # Arb hacmi: BTCTürk bid tarafında p_ask'ın üstündeki emirler
            # (BTCTürk'ün kendi TL fiyatına göre)
            btcturk_bids, _ = btcturk_orderbook_al(coin)
            arb = None
            if btcturk_bids:
                # BTCTürk'te satacağız, p_ask ise TL cinsinden alış fiyatı
                # Dolaysız TL karşılaştırma: BTCTürk bid > p_ask olan tüm seviyeler
                toplam_c = 0.0
                toplam_tl = 0.0
                for fiyat, miktar in btcturk_bids:
                    if fiyat <= p_ask:
                        break
                    toplam_c += miktar
                    toplam_tl += fiyat * miktar
                if toplam_c > 0:
                    arb = {
                        "coin_miktar": toplam_c,
                        "tl_toplam": toplam_tl,
                        "usdt_toplam": toplam_tl / kur_btcturk,
                    }
            arb_str = format_arb_hacim(arb, coin)

            bildirim_gonder(coin, "Paribu", "BTCTürk",
                f"₺{fiyat_formatla(p_ask)}", f"₺{fiyat_formatla(b_bid)}",
                fark, min_hacim, kur_btcturk, arb_str=arb_str)

    # BTCTürk'ten al → Paribu'da sat
    elif p_bid > b_ask:
        fark = ((p_bid - b_ask) / b_ask) * 100
        if 0 < fark <= 50:
            # Paribu bid tarafında b_ask'ın üstündeki emirler
            paribu_bids = paribu_veri.get("bids", [])
            arb = None
            if paribu_bids:
                toplam_c = 0.0
                toplam_tl = 0.0
                for fiyat, miktar in paribu_bids:
                    if fiyat <= b_ask:
                        break
                    toplam_c += miktar
                    toplam_tl += fiyat * miktar
                if toplam_c > 0:
                    arb = {
                        "coin_miktar": toplam_c,
                        "tl_toplam": toplam_tl,
                        "usdt_toplam": toplam_tl / kur_paribu,
                    }
            arb_str = format_arb_hacim(arb, coin)

            bildirim_gonder(coin, "BTCTürk", "Paribu",
                f"₺{fiyat_formatla(b_ask)}", f"₺{fiyat_formatla(p_bid)}",
                fark, min_hacim, kur_paribu, arb_str=arb_str)


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

    # 1. Config kontrolü — eksik env var varsa hemen çık
    _config_kontrol_et()

    # 1.5 Manuel ban listesini diskten yükle (restart sonrası kaybolmasın)
    manuel_ban_yukle()

    # 2. Paribu WebSocket thread'i (arka planda sürekli çalışır)
    ws_thread = threading.Thread(target=paribu_ws_thread, daemon=True)
    ws_thread.start()

    # 3. İlk Paribu datası gelene kadar bekle (max 20sn)
    print("Paribu WebSocket ilk veriler bekleniyor...")
    bekleme_basladi = time.time()
    while time.time() - bekleme_basladi < 20:
        with paribu_ws_lock:
            if len(paribu_ws_cache) > 10:
                break
        time.sleep(0.5)
    with paribu_ws_lock:
        print(f"Paribu WS: {len(paribu_ws_cache)} coin hazır, ana döngü başlıyor")

    # 4. Telegram komut dinleyici
    komut_thread = threading.Thread(target=komut_dinleyici, daemon=True)
    komut_thread.start()

    # 5. Periyodik memory temizlik (her saatte bir)
    temizlik_thread = threading.Thread(target=periyodik_temizlik, daemon=True)
    temizlik_thread.start()

    telegram_gonder(os.getenv("CHAT_ID_06"),
        f"✅ <b>Arbitraj Alarm Botu v5.1 Başladı</b>\n"
        f"🏦 Binance, Gate, MEXC, OKX, KuCoin, Bybit\n"
        f"🇹🇷 Paribu ↔ BTCTürk\n"
        f"📊 %0.6 / 📈 %1.5 / 🚀 %4.0\n"
        f"🛡 Rate limit koruması: Binance 418/429 → {BINANCE_COOLDOWN//60}dk cooldown\n"
        f"⚡ Cache TTL: {CACHE_TTL}sn | Orderbook workers: 10\n"
        f"💱 Min hacim: ${MIN_HACIM_USDT:,}\n"
        f"ℹ️ Komut: /stat /banlist /ban /unban"
    )

    while not _shutdown.is_set():
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

        kurlar = usdt_tl_kurlari(paribu, btcturk)
        if not kurlar:
            print("USDT/TL kuru alınamadı, bekleniyor...")
            time.sleep(10)
            continue

        kur_paribu = kurlar["Paribu"]
        kur_btcturk = kurlar["BTCTürk"]
        print(f"USDT/TL Paribu:{kur_paribu:.2f} BTCTürk:{kur_btcturk:.2f} | "
              f"Paribu:{len(paribu)} BTCTürk:{len(btcturk)} Binance:{len(binance)} Bybit:{len(bybit)} coin")

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
                    # Paribu arbitrajı için Paribu'nun USDT kuru
                    sonuc = karsilastir(coin, fiyatlar_usdt[coin], paribu[coin], borsa_usdt, "Paribu", kur_paribu)
                    if sonuc:
                        adaylar.extend(sonuc)
                if coin in btcturk:
                    # BTCTürk arbitrajı için BTCTürk'ün USDT kuru
                    sonuc = karsilastir(coin, fiyatlar_usdt[coin], btcturk[coin], borsa_usdt, "BTCTürk", kur_btcturk)
                    if sonuc:
                        adaylar.extend(sonuc)
            if coin in paribu and coin in btcturk:
                # Paribu↔BTCTürk karşılaştırması — iki kuru da geçir
                karsilastir_tl(coin, paribu[coin], btcturk[coin], kur_paribu, kur_btcturk)

        # ── 2. Adaylar için orderbook'ları paralel çek ──
        if adaylar:
            print(f"[{datetime.now(TZ_TR).strftime('%H:%M:%S')}] {len(adaylar)} aday bulundu, orderbook çekiliyor...")
            # ThreadPool ile 1000+ thread yaratma maliyetini ortadan kaldır
            futures = [ORDERBOOK_POOL.submit(karsilastir_orderbook, aday) for aday in adaylar]
            # Max 10sn bekle, uzun süren thread'leri bırak (bir sonraki turda yine denenir)
            wait(futures, timeout=10)

        tur_suresi = time.time() - tur_baslangic
        print(f"[{datetime.now(TZ_TR).strftime('%H:%M:%S')}] Tur tamamlandı. ({tur_suresi:.2f}sn)")
        # Saniyede 1 tur — shutdown sinyali gelirse hemen çıkar
        if _shutdown.wait(max(0.01, 1.0 - tur_suresi)):
            break

    # ─── Temiz kapanış ─────────────────────────────────────────────────────
    print("📤 Son Telegram mesajları gönderiliyor...")
    TELEGRAM_POOL.shutdown(wait=True)  # kuyruktaki mesajlar gitsin
    print("🧹 Worker havuzları kapatılıyor...")
    ORDERBOOK_POOL.shutdown(wait=False, cancel_futures=True)
    print("✅ Bot temiz kapandı")


if __name__ == "__main__":
    bot_calistir()
