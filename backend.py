#!/usr/bin/env python3
"""
Kripto Para Futures Piyasa Analiz Sistemi
Binance Futures Testnet API ile canli veri
"""

import socket

old_getaddrinfo = socket.getaddrinfo
def new_getaddrinfo(*args, **kwargs):
    responses = old_getaddrinfo(*args, **kwargs)
    return [r for r in responses if r[0] == socket.AF_INET]
socket.getaddrinfo = new_getaddrinfo

import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests.packages.urllib3.util.connection as urllib3_cn
urllib3_cn.HAS_IPV6 = False
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import pandas as pd
import numpy as np
import pandas_ta as ta
import requests
from flask import Flask, jsonify, request, send_file
from flask_cors import CORS

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = 'crypto-trader-secret-key'
CORS(app)

# ============================================================
# BINANCE FUTURES TESTNET YAPILANDIRMA
# ============================================================

BINANCE_FUTURES_TESTNET = 'https://testnet.binancefuture.com'
BINANCE_FUTURES_LIVE = 'https://fapi.binance.com'

_MARKET_DATA_URL_CACHE = None
_MARKET_DATA_URL_TIME = 0

def _get_market_data_url():
    global _MARKET_DATA_URL_CACHE, _MARKET_DATA_URL_TIME
    now = time.time()
    if _MARKET_DATA_URL_CACHE and (now - _MARKET_DATA_URL_TIME) < 60:
        return _MARKET_DATA_URL_CACHE
    try:
        cfg_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'binance_config.json')
        with open(cfg_path, 'r') as f:
            cfg = json.load(f)
        if cfg.get('active_mode') == 'live':
            _MARKET_DATA_URL_CACHE = BINANCE_FUTURES_LIVE
        else:
            _MARKET_DATA_URL_CACHE = BINANCE_FUTURES_TESTNET
    except:
        _MARKET_DATA_URL_CACHE = BINANCE_FUTURES_TESTNET
    _MARKET_DATA_URL_TIME = now
    return _MARKET_DATA_URL_CACHE

class Config:
    MAJOR_PAIRS = [
        'BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'XRP/USDT', 'BNB/USDT',
        'DOGE/USDT', 'AVAX/USDT', 'LINK/USDT', 'ADA/USDT', 'TON/USDT',
        'SUI/USDT', 'SEI/USDT', 'INJ/USDT', 'NEAR/USDT', 'ARB/USDT',
        'OP/USDT', 'PEPE/USDT', 'WIF/USDT', 'BONK/USDT', 'FLOKI/USDT'
    ]

    TIMEFRAMES = ['1m', '3m', '5m', '15m', '1h', '4h', '1d']

    TOP_VOLUME_PAIRS = []
    TOP_VOLUME_LOADED = False

    @staticmethod
    def to_binance_symbol(pair: str) -> str:
        return pair.replace('/', '')

    @staticmethod
    def to_pair_symbol(binance_sym: str) -> str:
        if binance_sym.endswith('USDT'):
            base = binance_sym[:-4]
            return f'{base}/USDT'
        return binance_sym

    @staticmethod
    def fetch_top_volume_pairs(limit=300):
        try:
            url = f'{_get_market_data_url()}/fapi/v1/ticker/24hr'
            resp = requests.get(url, timeout=15, headers={'User-Agent': 'Mozilla/5.0'}, verify=False)
            tickers = resp.json()
            if not isinstance(tickers, list):
                return []
            usdt_pairs = []
            for t in tickers:
                symbol = t.get('symbol', '')
                if symbol.endswith('USDT'):
                    pair = Config.to_pair_symbol(symbol)
                    vol = float(t.get('quoteVolume', 0))
                    usdt_pairs.append((pair, vol))
            usdt_pairs.sort(key=lambda x: x[1], reverse=True)
            top_pairs = [p[0] for p in usdt_pairs[:limit]]
            logger.info('Binance Futures top {} hacimli coin yuklendi: {} coin'.format(limit, len(top_pairs)))
            return top_pairs
        except Exception as e:
            logger.error('Top volume coins yuklenemedi: {}'.format(e))
            return []

config = Config()

# ============================================================
# BINANCE FUTURES TESTNET CANLI VERI CEKICI
# ============================================================

class LiveDataFetcher:
    """Binance Futures Testnet API ile canli kripto verisi ceker"""

    TF_KLINE_INTERVAL = {'1m': '1m', '3m': '3m', '5m': '5m', '15m': '15m',
                         '1h': '1h', '4h': '4h', '1d': '1d'}

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0'})
        self.session.verify = False
        from requests.adapters import HTTPAdapter
        adapter = HTTPAdapter(pool_connections=30, pool_maxsize=30, max_retries=3)
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)
        self._live_prices = {}
        self._live_tickers = {}
        self._last_fetch = 0
        self._fetch_interval = 5
        self._last_kline_time = 0
        self.news_cache = []
        self.news_cache_time = None
        self.last_error = None
        if not config.TOP_VOLUME_LOADED:
            limit = PAPER_SETTINGS.get('coin_adedi', 20)
            config.TOP_VOLUME_PAIRS = Config.fetch_top_volume_pairs(limit)
            if not config.TOP_VOLUME_PAIRS:
                logger.warning('Top volume coins yuklenemedi; MAJOR_PAIRS fallback kullaniliyor.')
                config.TOP_VOLUME_PAIRS = config.MAJOR_PAIRS[:limit]
            config.TOP_VOLUME_LOADED = True
        self._fetch_news()

    def _fetch_live_prices(self) -> Dict[str, float]:
        now = time.time()
        if now - self._last_fetch < self._fetch_interval:
            return self._live_prices

        prices = {}
        tickers = {}
        try:
            r = self.session.get(f'{_get_market_data_url()}/fapi/v1/ticker/24hr', timeout=15)
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, list):
                    for t in data:
                        symbol = t.get('symbol', '')
                        if symbol.endswith('USDT'):
                            pair = Config.to_pair_symbol(symbol)
                            price = float(t.get('lastPrice', 0))
                            vol = float(t.get('quoteVolume', 0))
                            chg = float(t.get('priceChangePercent', 0))
                            high = float(t.get('highPrice', 0))
                            low = float(t.get('lowPrice', 0))
                            if price > 0:
                                prices[pair] = price
                                tickers[pair] = {
                                    'price': price,
                                    'volume_24h': vol,
                                    'change_24h': chg,
                                    'high_24h': high,
                                    'low_24h': low,
                                    'bid': float(t.get('bidPrice', 0)),
                                    'ask': float(t.get('askPrice', 0)),
                                }
                    self.last_error = None
                else:
                    self.last_error = 'Binance ticker veri formati hatali'
                    logger.warning(self.last_error)
            else:
                self.last_error = 'Binance ticker HTTP {}'.format(r.status_code)
                logger.warning(self.last_error)
        except Exception as e:
            self.last_error = 'Binance ticker baglanti hatasi: {}'.format(str(e)[:120])
            logger.warning(self.last_error)

        self._live_prices = prices
        self._live_tickers = tickers
        self._last_fetch = now
        return self._live_prices

    def _get_latest_price(self, symbol: str) -> float:
        prices = self._fetch_live_prices()
        return prices.get(symbol, 0)

    def get_ticker(self, symbol: str) -> Dict:
        self._fetch_live_prices()
        ticker_data = self._live_tickers.get(symbol, {})
        live_price = ticker_data.get('price', 0)
        if live_price == 0:
            live_price = self._live_prices.get(symbol, 0)
        if live_price == 0:
            return {'symbol': symbol, 'price': 0, 'error': 'Fiyat alinamadi'}

        vol = ticker_data.get('volume_24h', 0)
        change = ticker_data.get('change_24h', 0)
        high_24h = ticker_data.get('high_24h', live_price)
        low_24h = ticker_data.get('low_24h', live_price)
        bid = ticker_data.get('bid', live_price * 0.9999)
        ask = ticker_data.get('ask', live_price * 1.0001)

        return {
            'symbol': symbol,
            'price': live_price,
            'bid': bid,
            'ask': ask,
            'spread': (ask - bid) / bid * 100 if bid > 0 else 0,
            'high_24h': high_24h,
            'low_24h': low_24h,
            'volume_24h': vol,
            'change_24h': change,
            'timestamp': datetime.now().isoformat()
        }

    def get_ohlcv(self, symbol: str, timeframe: str = '15m', limit: int = 100) -> pd.DataFrame:
        binance_sym = Config.to_binance_symbol(symbol)
        interval = self.TF_KLINE_INTERVAL.get(timeframe, '15m')

        now = time.time()
        elapsed = now - self._last_kline_time
        if elapsed < 0.2:
            time.sleep(0.2 - elapsed)
        self._last_kline_time = time.time()

        try:
            url = f'{_get_market_data_url()}/fapi/v1/klines'
            params = {'symbol': binance_sym, 'interval': interval, 'limit': limit}
            r = self.session.get(url, params=params, timeout=10)
            if r.status_code != 200:
                self.last_error = 'Binance klines {} HTTP {}'.format(symbol, r.status_code)
                logger.warning(self.last_error)
                return pd.DataFrame()

            data = r.json()
            if not data or not isinstance(data, list):
                self.last_error = 'Binance klines {} veri yok'.format(symbol)
                logger.warning(self.last_error)
                return pd.DataFrame()

            ohlcv = []
            for row in data:
                ohlcv.append({
                    'timestamp': pd.to_datetime(int(row[0]), unit='ms'),
                    'open': float(row[1]),
                    'high': float(row[2]),
                    'low': float(row[3]),
                    'close': float(row[4]),
                    'volume': float(row[5])
                })

            df = pd.DataFrame(ohlcv)
            if not df.empty:
                df.set_index('timestamp', inplace=True)
            return df

        except Exception as e:
            self.last_error = 'Binance klines {} baglanti hatasi: {}'.format(symbol, str(e)[:120])
            logger.warning(self.last_error)
            return pd.DataFrame()

    def get_funding_rate(self, symbol: str) -> Dict:
        binance_sym = Config.to_binance_symbol(symbol)
        try:
            url = f'{_get_market_data_url()}/fapi/v1/premiumIndex'
            params = {'symbol': binance_sym}
            r = self.session.get(url, params=params, timeout=10)
            if r.status_code == 200:
                data = r.json()
                last_funding_rate = float(data.get('lastFundingRate', 0))
                next_funding_time = int(data.get('nextFundingTime', 0))
                mark_price = float(data.get('markPrice', 0))
                index_price = float(data.get('indexPrice', 0))
                return {
                    'symbol': symbol,
                    'funding_rate': last_funding_rate,
                    'funding_time': int(datetime.now().timestamp() * 1000),
                    'next_funding_time': next_funding_time,
                    'mark_price': mark_price,
                    'index_price': index_price
                }
        except Exception as e:
            self.last_error = 'Binance funding {} hatasi: {}'.format(symbol, str(e)[:120])
            logger.warning(self.last_error)

        return {
            'symbol': symbol,
            'funding_rate': 0,
            'funding_time': int(datetime.now().timestamp() * 1000),
            'next_funding_time': 0,
            'mark_price': 0,
            'index_price': 0
        }

    def get_open_interest(self, symbol: str) -> Dict:
        binance_sym = Config.to_binance_symbol(symbol)
        try:
            url = f'{_get_market_data_url()}/fapi/v1/openInterest'
            params = {'symbol': binance_sym}
            r = self.session.get(url, params=params, timeout=10)
            if r.status_code == 200:
                data = r.json()
                oi = float(data.get('openInterest', 0))
                return {
                    'symbol': symbol,
                    'open_interest': oi,
                    'timestamp': int(datetime.now().timestamp() * 1000)
                }
        except Exception as e:
            self.last_error = 'Binance OI {} hatasi: {}'.format(symbol, str(e)[:120])
            logger.warning(self.last_error)

        return {
            'symbol': symbol,
            'open_interest': 0,
            'timestamp': int(datetime.now().timestamp() * 1000)
        }

    def get_market_overview(self) -> Dict:
        btc_price = self._get_latest_price('BTC/USDT')
        eth_price = self._get_latest_price('ETH/USDT')

        total_vol = 0
        active_count = 0
        for pair in config.MAJOR_PAIRS:
            vol = self._live_tickers.get(pair, {}).get('volume_24h', 0)
            total_vol += vol
            if self._get_latest_price(pair) > 0:
                active_count += 1

        btc_chg = self._live_tickers.get('BTC/USDT', {}).get('change_24h', 0)

        return {
            'total_market_cap': 0,
            'total_volume_24h': total_vol,
            'btc_dominance': 0,
            'eth_dominance': 0,
            'market_cap_change_24h': btc_chg,
            'active_cryptocurrencies': len(config.TOP_VOLUME_PAIRS),
            'total_markets': len(config.TOP_VOLUME_PAIRS)
        }

    def _fetch_news(self):
        try:
            url = 'https://cryptopanic.com/api/free/v1/posts/?auth_token=&public=true'
            resp = self.session.get(url, timeout=8)
            if resp.status_code == 200:
                data = resp.json()
                items = data.get('results', [])[:15]
                news = []
                for item in items:
                    news.append({
                        'title': item.get('title', ''),
                        'source': item.get('source', {}).get('title', 'CryptoPanic'),
                        'url': item.get('url', ''),
                        'published': item.get('published_at', ''),
                        'kaynak': 'CryptoPanic'
                    })
                self.news_cache = news
                self.news_cache_time = datetime.now()
                logger.info('Haberler CryptoPanic API ile guncellendi: {} haber'.format(len(news)))
                return
        except Exception as e:
            logger.warning('CryptoPanic haber hatasi: {}'.format(e))

        try:
            import xml.etree.ElementTree as ET
            url = 'https://cointelegraph.com/rss'
            resp = self.session.get(url, timeout=8)
            if resp.status_code == 200:
                root = ET.fromstring(resp.content)
                items = root.findall('.//item')[:15]
                news = []
                for item in items:
                    title = item.find('title')
                    link = item.find('link')
                    pub = item.find('pubDate')
                    news.append({
                        'title': title.text if title is not None else '',
                        'source': 'CoinTelegraph',
                        'url': link.text if link is not None else '',
                        'published': pub.text if pub is not None else '',
                        'kaynak': 'CoinTelegraph'
                    })
                self.news_cache = news
                self.news_cache_time = datetime.now()
                logger.info('Haberler CoinTelegraph RSS ile guncellendi: {} haber'.format(len(news)))
                return
        except Exception as e:
            logger.warning('CoinTelegraph RSS hatasi: {}'.format(e))

        if not self.news_cache:
            self.news_cache = [
                {'title': 'Haberler yuklenemedi - Internet baglantisini kontrol edin', 'source': 'Sistem', 'url': '', 'published': datetime.now().isoformat(), 'kaynak': 'Sistem'},
            ]
            self.news_cache_time = datetime.now()

    def get_news(self) -> List[Dict]:
        if self.news_cache_time is None or (datetime.now() - self.news_cache_time).total_seconds() > 300:
            self._fetch_news()
        return self.news_cache

# ============================================================
# TEKNIK ANALIZ MOTORU
# ============================================================

class TechnicalAnalyzer:
    """pandas_ta kullanarak profesyonel teknik analiz"""

    @staticmethod
    def analyze_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """DataFrame uzerinde tum teknik gostergeleri hesapla"""
        if df.empty or len(df) < 50:
            return df

        # RSI
        df['rsi'] = ta.rsi(df['close'], length=14)

        # MACD
        macd = ta.macd(df['close'], fast=12, slow=26, signal=9)
        df = pd.concat([df, macd], axis=1)

        # EMA'lar
        for period in [9, 21, 50, 100, 200]:
            if len(df) >= period:
                df['ema_{}'.format(period)] = ta.ema(df['close'], length=period)

        # Bollinger Bands
        bb = ta.bbands(df['close'], length=20, std=2)
        df = pd.concat([df, bb], axis=1)

        # ATR
        df['atr'] = ta.atr(df['high'], df['low'], df['close'], length=14)

        # VWAP
        if 'volume' in df.columns:
            df['vwap'] = ta.vwap(df['high'], df['low'], df['close'], df['volume'])

        # ADX
        adx = ta.adx(df['high'], df['low'], df['close'], length=14)
        df = pd.concat([df, adx], axis=1)

        # Stochastic
        stoch = ta.stoch(df['high'], df['low'], df['close'], k=14, d=3, smooth_k=3)
        df = pd.concat([df, stoch], axis=1)

        # Volume SMA
        if 'volume' in df.columns:
            df['volume_sma'] = ta.sma(df['volume'], length=20)

        return df

# ============================================================
# ISLEM SINYAL URETICI
# ============================================================

class SignalGenerator:
    """Teknik gostergelere gore islem sinyali uret"""

    def __init__(self):
        self.analyzer = TechnicalAnalyzer()

    def generate_signals(self, symbol: str, df: pd.DataFrame, timeframe: str = '15m') -> Dict:
        """Coklu zaman diliminde sinyal uret"""
        if df.empty or len(df) < 50:
            return self._empty_signal(symbol, timeframe)

        df = self.analyzer.analyze_dataframe(df)
        if len(df) < 2:
            return self._empty_signal(symbol, timeframe)

        latest = df.iloc[-1]
        prev = df.iloc[-2]
        price = float(latest['close'])

        signals = []
        signal_score = 0
        bull_count = 0
        bear_count = 0

        # RSI
        rsi = latest.get('rsi')
        if rsi is not None and not pd.isna(rsi):
            rsi = float(rsi)
            if rsi < 30:
                signals.append({'tip': 'AL', 'gosterge': 'RSI', 'deger': '{:.1f}'.format(rsi), 'siddet': 3, 'aciklama': 'Asiri satim bolgesi'})
                signal_score += 3; bull_count += 1
            elif rsi < 40:
                signals.append({'tip': 'AL', 'gosterge': 'RSI', 'deger': '{:.1f}'.format(rsi), 'siddet': 1, 'aciklama': 'Hafif asiri satim'})
                signal_score += 1; bull_count += 1
            elif rsi > 70:
                signals.append({'tip': 'SAT', 'gosterge': 'RSI', 'deger': '{:.1f}'.format(rsi), 'siddet': 3, 'aciklama': 'Asiri alim bolgesi'})
                signal_score -= 3; bear_count += 1
            elif rsi > 60:
                signals.append({'tip': 'SAT', 'gosterge': 'RSI', 'deger': '{:.1f}'.format(rsi), 'siddet': 1, 'aciklama': 'Hafif asiri alim'})
                signal_score -= 1; bear_count += 1

        # MACD
        macd_col = 'MACD_12_26_9'
        signal_col = 'MACDs_12_26_9'
        macd_val = latest.get(macd_col)
        macd_signal = latest.get(signal_col)
        prev_macd = prev.get(macd_col)

        if macd_val is not None and macd_signal is not None and prev_macd is not None:
            if not pd.isna(macd_val) and not pd.isna(macd_signal) and not pd.isna(prev_macd):
                macd_val_f = float(macd_val); macd_signal_f = float(macd_signal); prev_macd_f = float(prev_macd)
                if prev_macd_f < macd_signal_f and macd_val_f > macd_signal_f:
                    signals.append({'tip': 'AL', 'gosterge': 'MACD', 'deger': '{:.2f}'.format(macd_val_f), 'siddet': 2, 'aciklama': 'MACD sinyali yukari kesti'})
                    signal_score += 2; bull_count += 1
                elif prev_macd_f > macd_signal_f and macd_val_f < macd_signal_f:
                    signals.append({'tip': 'SAT', 'gosterge': 'MACD', 'deger': '{:.2f}'.format(macd_val_f), 'siddet': 2, 'aciklama': 'MACD sinyali asagi kesti'})
                    signal_score -= 2; bear_count += 1

        # EMA
        ema_9 = latest.get('ema_9')
        ema_21 = latest.get('ema_21')
        ema_50 = latest.get('ema_50')

        if ema_9 is not None and ema_21 is not None and not pd.isna(ema_9) and not pd.isna(ema_21):
            ema_9_f = float(ema_9); ema_21_f = float(ema_21)
            prev_ema_9 = prev.get('ema_9'); prev_ema_21 = prev.get('ema_21')
            if prev_ema_9 is not None and prev_ema_21 is not None and not pd.isna(prev_ema_9) and not pd.isna(prev_ema_21):
                if float(prev_ema_9) < float(prev_ema_21) and ema_9_f > ema_21_f:
                    signals.append({'tip': 'AL', 'gosterge': 'EMA 9/21', 'deger': '{:.2f}'.format(price), 'siddet': 2, 'aciklama': 'Altin kesisme'})
                    signal_score += 2; bull_count += 1
                elif float(prev_ema_9) > float(prev_ema_21) and ema_9_f < ema_21_f:
                    signals.append({'tip': 'SAT', 'gosterge': 'EMA 9/21', 'deger': '{:.2f}'.format(price), 'siddet': 2, 'aciklama': 'Olum kesismesi'})
                    signal_score -= 2; bear_count += 1

            if ema_50 is not None and not pd.isna(ema_50):
                ema_50_f = float(ema_50)
                if price > ema_9_f > ema_21_f > ema_50_f:
                    signals.append({'tip': 'AL', 'gosterge': 'EMA Siralama', 'deger': '{:.2f}'.format(price), 'siddet': 2, 'aciklama': 'Guclu yukselis trendi'})
                    signal_score += 2; bull_count += 1
                elif price < ema_9_f < ema_21_f < ema_50_f:
                    signals.append({'tip': 'SAT', 'gosterge': 'EMA Siralama', 'deger': '{:.2f}'.format(price), 'siddet': 2, 'aciklama': 'Guclu dusus trendi'})
                    signal_score -= 2; bear_count += 1

        # Bollinger Bands
        bb_upper = latest.get('BBU_20_2.0'); bb_lower = latest.get('BBL_20_2.0')
        if bb_upper is not None and bb_lower is not None and not pd.isna(bb_upper) and not pd.isna(bb_lower):
            bb_upper_f = float(bb_upper); bb_lower_f = float(bb_lower)
            if price > bb_upper_f:
                signals.append({'tip': 'SAT', 'gosterge': 'BB Ust', 'deger': '{:.2f}'.format(price), 'siddet': 2, 'aciklama': 'Bollinger ust bandini kirdi'})
                signal_score -= 2; bear_count += 1
            elif price < bb_lower_f:
                signals.append({'tip': 'AL', 'gosterge': 'BB Alt', 'deger': '{:.2f}'.format(price), 'siddet': 2, 'aciklama': 'Bollinger alt bandina dokundu'})
                signal_score += 2; bull_count += 1

        # ADX
        adx = latest.get('ADX_14')
        if adx is not None and not pd.isna(adx):
            adx_f = float(adx)
            if adx_f > 25:
                signals.append({'tip': 'BILGI', 'gosterge': 'ADX', 'deger': '{:.1f}'.format(adx_f), 'siddet': 1, 'aciklama': 'Guclu trend'})
            elif adx_f < 20:
                signals.append({'tip': 'BILGI', 'gosterge': 'ADX', 'deger': '{:.1f}'.format(adx_f), 'siddet': 1, 'aciklama': 'Zayif / Yatay'})

        # VWAP
        vwap = latest.get('vwap')
        if vwap is not None and not pd.isna(vwap):
            vwap_f = float(vwap)
            if price > vwap_f * 1.01:
                signals.append({'tip': 'AL', 'gosterge': 'VWAP', 'deger': '{:.2f}'.format(price), 'siddet': 1, 'aciklama': 'VWAP uzerinde'})
                signal_score += 1; bull_count += 1
            elif price < vwap_f * 0.99:
                signals.append({'tip': 'SAT', 'gosterge': 'VWAP', 'deger': '{:.2f}'.format(price), 'siddet': 1, 'aciklama': 'VWAP altinda'})
                signal_score -= 1; bear_count += 1

        # Hacim Analizi
        volume = latest.get('volume')
        volume_sma = latest.get('volume_sma')
        if volume is not None and volume_sma is not None and not pd.isna(volume) and not pd.isna(volume_sma):
            vol_ratio = float(volume) / float(volume_sma) if float(volume_sma) > 0 else 1
            close = float(latest['close']); open_p = float(latest['open'])
            if vol_ratio > 1.5 and close > open_p:
                signals.append({'tip': 'AL', 'gosterge': 'HACIM', 'deger': '{:.1f}x'.format(vol_ratio), 'siddet': 2, 'aciklama': 'Yuksek hacimli yukselis'})
                signal_score += 2; bull_count += 1
            elif vol_ratio > 1.5 and close < open_p:
                signals.append({'tip': 'SAT', 'gosterge': 'HACIM', 'deger': '{:.1f}x'.format(vol_ratio), 'siddet': 2, 'aciklama': 'Yuksek hacimli dusus'})
                signal_score -= 2; bear_count += 1

        # Destek / Direnc Seviyeleri
        support_levels = self._find_key_levels(df, 'low')
        resistance_levels = self._find_key_levels(df, 'high')

        # Nihai Karar
        trend = 'NOTR'
        if bull_count > bear_count * 2:
            trend = 'YUKSELIS'
        elif bear_count > bull_count * 2:
            trend = 'DUSUS'

        # SL/TP Hesaplama
        islem_onerisi = self._calculate_sl_tp(latest, price, trend, support_levels, resistance_levels, timeframe)

        return {
            'sembol': symbol,
            'zaman_dilimi': timeframe,
            'anlik_fiyat': price,
            'trend': trend,
            'sinyal_puani': signal_score,
            'guven_puani': min(abs(signal_score) * 10, 100),
            'bull_sinyal': bull_count,
            'bear_sinyal': bear_count,
            'sinyaller': signals,
            'islem_onerisi': islem_onerisi,
            'destek_seviyeleri': support_levels,
            'direnc_seviyeleri': resistance_levels,
            'teknik_gostergeler': {
                'rsi': round(float(rsi), 1) if rsi is not None and not pd.isna(rsi) else None,
                'macd': round(float(macd_val), 2) if macd_val is not None and not pd.isna(macd_val) else None,
                'ema_9': round(float(ema_9), 2) if ema_9 is not None and not pd.isna(ema_9) else None,
                'ema_21': round(float(ema_21), 2) if ema_21 is not None and not pd.isna(ema_21) else None,
                'ema_50': round(float(ema_50), 2) if ema_50 is not None and not pd.isna(ema_50) else None,
                'adx': round(float(adx), 1) if adx is not None and not pd.isna(adx) else None,
                'atr': round(float(latest.get('atr', 0)), 2) if latest.get('atr') is not None and not pd.isna(latest['atr']) else None,
                'bb_upper': round(float(bb_upper), 2) if bb_upper is not None and not pd.isna(bb_upper) else None,
                'bb_lower': round(float(bb_lower), 2) if bb_lower is not None and not pd.isna(bb_lower) else None,
                'vwap': round(float(vwap), 2) if vwap is not None and not pd.isna(vwap) else None,
            },
            'zaman_damgasi': datetime.now().isoformat()
        }

    def _find_key_levels(self, df: pd.DataFrame, price_col: str, n_levels: int = 3) -> List[float]:
        """Onemli fiyat seviyelerini bul"""
        if df.empty or len(df) < 20:
            return []

        prices = df[price_col].values[-50:]
        current_price = float(df['close'].iloc[-1])

        # Pivot noktalari
        key_levels = []
        for i in range(5, len(prices) - 5):
            if price_col == 'low':
                if prices[i] == min(prices[i-5:i+6]) and prices[i] < current_price:
                    key_levels.append(round(float(prices[i]), 2))
            else:
                if prices[i] == max(prices[i-5:i+6]) and prices[i] > current_price:
                    key_levels.append(round(float(prices[i]), 2))

        # Benzersiz yap
        key_levels = sorted(set(key_levels))
        if price_col == 'low':
            return key_levels[-n_levels:] if len(key_levels) > n_levels else key_levels
        else:
            return key_levels[:n_levels] if len(key_levels) > n_levels else key_levels

    def _calculate_sl_tp(self, latest, price, trend, support_levels, resistance_levels, timeframe='15m'):
        atr_val = float(latest.get('atr', 0))
        if atr_val is None or (isinstance(atr_val, float) and pd.isna(atr_val)) or atr_val == 0:
            atr_val = price * 0.01

        # 4-5 saat icinde kapanacak sekilde sikinti SL/TP
        tf_mult = {'1m': 0.5, '3m': 0.6, '5m': 0.7, '15m': 0.8, '1h': 1.0, '4h': 1.2, '1d': 1.5}
        mult = tf_mult.get(timeframe, 0.8)
        sl_dist = max(atr_val * mult, price * 0.002)
        sl_dist = min(sl_dist, price * 0.03)

        if trend == 'YUKSELIS':
            entry = price
            sl_candidate = entry - sl_dist
            valid_supports = [s for s in support_levels if s < entry and s > sl_candidate * 0.98]
            if valid_supports:
                closest = max(valid_supports)
                sl_candidate = closest - atr_val * 0.3
            ema_50 = float(latest.get('ema_50', 0))
            if ema_50 and not pd.isna(ema_50) and ema_50 < entry and ema_50 > sl_candidate:
                sl_candidate = ema_50 - atr_val * 0.3
            sl = sl_candidate
            risk = entry - sl
            if risk <= 0: risk = sl_dist
            return {
                'yon': 'LONG', 'giris': entry, 'stop_loss': sl,
                'take_kar_1': entry + risk * 1.5, 'take_kar_2': entry + risk * 2.5,
                'take_kar_3': entry + risk * 4.0, 'kar_zarar_orani': '1:1.5 / 1:2.5 / 1:4',
                'sl_yuzde': round(risk / entry * 100, 2), 'tp1_yuzde': round(risk * 1.5 / entry * 100, 2)
            }
        elif trend == 'DUSUS':
            entry = price
            sl_candidate = entry + sl_dist
            valid_resist = [r for r in resistance_levels if r > entry and r < sl_candidate * 1.02]
            if valid_resist:
                closest = min(valid_resist)
                sl_candidate = closest + atr_val * 0.3
            ema_50 = float(latest.get('ema_50', 0))
            if ema_50 and not pd.isna(ema_50) and ema_50 > entry and ema_50 < sl_candidate:
                sl_candidate = ema_50 + atr_val * 0.3
            sl = sl_candidate
            risk = sl - entry
            if risk <= 0: risk = sl_dist
            return {
                'yon': 'SHORT', 'giris': entry, 'stop_loss': sl,
                'take_kar_1': entry - risk * 1.5, 'take_kar_2': entry - risk * 2.5,
                'take_kar_3': entry - risk * 4.0, 'kar_zarar_orani': '1:1.5 / 1:2.5 / 1:4',
                'sl_yuzde': round(risk / entry * 100, 2), 'tp1_yuzde': round(risk * 1.5 / entry * 100, 2)
            }
        return None

    def _empty_signal(self, symbol: str, timeframe: str) -> Dict:
        return {
            'sembol': symbol,
            'zaman_dilimi': timeframe,
            'anlik_fiyat': 0,
            'trend': 'YETERSIZ_VERI',
            'sinyal_puani': 0,
            'guven_puani': 0,
            'bull_sinyal': 0,
            'bear_sinyal': 0,
            'sinyaller': [{'tip': 'UYARI', 'gosterge': 'VERI', 'deger': '0', 'siddet': 0, 'aciklama': 'Yeterli veri yok'}],
            'islem_onerisi': None,
            'destek_seviyeleri': [],
            'direnc_seviyeleri': [],
            'teknik_gostergeler': {},
            'zaman_damgasi': datetime.now().isoformat()
        }

# ============================================================
# RISK YONETIMI
# ============================================================

class RiskManager:
    """Risk yonetimi hesaplamalari"""

    @staticmethod
    def calculate_position_size(balance: float, risk_percent: float, entry: float, stop: float, leverage: int = 1, yon: str = 'LONG') -> Dict:
        risk_amount = balance * (risk_percent / 100)
        risk_per_unit = abs(entry - stop)
        if risk_per_unit == 0:
            return {'hata': 'Stop-loss giris ile ayni olamaz'}

        # Pozisyon birim sayisi ve sozlesme degeri (kaldiracsiz)
        pos_units = risk_amount / risk_per_unit
        pos_value = pos_units * entry
        margin = pos_value / leverage if leverage else pos_value

        # Likidasyon fiyati
        if yon == 'LONG':
            liq_price = entry - (entry - stop) * (leverage / max(leverage - 1, 0.1))
        else:
            liq_price = entry + (stop - entry) * (leverage / max(leverage - 1, 0.1))

        return {
            'bakiye': balance,
            'risk_yuzdesi': risk_percent,
            'risk_miktari': round(risk_amount, 2),
            'yon': yon,
            'giris': entry,
            'stop': stop,
            'birim_risk': round(risk_per_unit, 2),
            'birim_sayisi': round(pos_units, 4),
            'sozlesme_degeri': round(pos_value, 2),
            'kullanilan_teminat': round(margin, 2),
            'kaldirac': leverage,
            'likidasyon_fiyati': round(liq_price, 2),
        }

    @staticmethod
    def calculate_rr_ratio(entry: float, stop: float, tp1: float, tp2: float, tp3: float = None) -> Dict:
        risk = abs(entry - stop)
        if risk == 0:
            return {'rr_tp1': 0, 'rr_tp2': 0}
        result = {
            'rr_tp1': round(abs(tp1 - entry) / risk, 2),
            'rr_tp2': round(abs(tp2 - entry) / risk, 2),
        }
        if tp3:
            result['rr_tp3'] = round(abs(tp3 - entry) / risk, 2)
        return result

    @staticmethod
    def calculate_leverage_suggestion(atr: float, price: float, max_risk_percent: float = 1.0) -> Dict:
        if price == 0 or atr == 0:
            return {'onerilen_kaldirac': 3, 'maks_kaldirac': 5, 'uyari': 'ATR verisi yetersiz'}
        atr_percent = (atr / price) * 100
        if atr_percent > 5:
            return {'onerilen_kaldirac': 2, 'maks_kaldirac': 3, 'atr_yuzdesi': '{:.1f}%'.format(atr_percent), 'uyari': 'Yuksek volatilite'}
        elif atr_percent > 3:
            return {'onerilen_kaldirac': 3, 'maks_kaldirac': 5, 'atr_yuzdesi': '{:.1f}%'.format(atr_percent), 'uyari': 'Orta volatilite'}
        elif atr_percent > 1:
            return {'onerilen_kaldirac': 5, 'maks_kaldirac': 8, 'atr_yuzdesi': '{:.1f}%'.format(atr_percent), 'uyari': 'Normal volatilite'}
        else:
            return {'onerilen_kaldirac': 8, 'maks_kaldirac': 10, 'atr_yuzdesi': '{:.1f}%'.format(atr_percent), 'uyari': 'Dusuk volatilite'}

# ============================================================
# PAPER TRADER (Sanal Islem Sistemi)
# ============================================================

class PaperTrader:
    """Sanal islem sistemi - bakiye = kullanilabilir nakit, teminat ayri takip edilir"""

    KOMISYON_ORANI = 0.0004  # Binance Futures taker: %0.04

    def __init__(self, initial_balance=100):
        self.initial_balance = initial_balance
        self.balance = initial_balance      # kullanilabilir nakit
        self.locked_margin = 0.0            # acik pozisyonlarda kilitli teminat
        self.positions = {}
        self.trade_history = []
        self.total_trades = 0
        self.winning_trades = 0
        self.losing_trades = 0
        self.equity_curve = [{'zaman': datetime.now().isoformat(), 'bakiye': initial_balance}]
        self.start_time = datetime.now().isoformat()
        self.durum = 'durdu'
        self.son_islem_zamani = {}

    def get_equity(self, results=None):
        """ozsermaye = bakiye + kilitli teminat + acik pozisyon kar/zarar"""
        open_pnl = 0
        for sym, pos in self.positions.items():
            current_price = pos.get('current_price', pos['entry_price'])
            open_pnl += self._position_pnl(pos, current_price)
        return round(self.balance + self.locked_margin + open_pnl, 2)

    def process(self, results):
        if self.durum == 'durdu':
            return
        for symbol, pos in list(self.positions.items()):
            r = next((r for r in results if r['sembol'] == symbol), None)
            if not r:
                continue
            price = r['fiyat']
            self._check_position(symbol, pos, price)
            pos['current_price'] = price

        if self.durum == 'baslat' and len(self.positions) < PAPER_SETTINGS['max_pozisyon']:
            for r in results:
                symbol = r['sembol']
                if symbol in self.positions:
                    continue
                if symbol in PAPER_SETTINGS.get('kara_liste', []):
                    continue
                soguma = PAPER_SETTINGS.get('soguma_dakika', 60)
                if soguma > 0 and symbol in self.son_islem_zamani:
                    gecen = (datetime.now() - self.son_islem_zamani[symbol]).total_seconds() / 60
                    if gecen < soguma:
                        continue
                price = r.get('fiyat', 0)
                if price < PAPER_SETTINGS['min_fiyat']:
                    continue
                best_signal = None
                best_guven = 0
                best_tf = None
                for stf, sig in r.get('tf_signals', {}).items():
                    trend_stf = sig.get('trend', 'NOTR')
                    if trend_stf == 'NOTR':
                        continue
                    oneri_stf = sig.get('islem_onerisi')
                    if not oneri_stf:
                        continue
                    guven_stf = sig.get('guven_puani', 0)
                    sp_stf = sig.get('sinyal_puani', 0)
                    if guven_stf < PAPER_SETTINGS['min_guven'] or abs(sp_stf) < PAPER_SETTINGS['min_sinyal_puani']:
                        continue
                    if guven_stf > best_guven:
                        best_guven = guven_stf
                        best_signal = oneri_stf
                        best_tf = stf
                if best_signal is None:
                    continue
                self._open_position(symbol, best_signal, price, best_tf)

        total_equity = self.get_equity(results)
        self.locked_margin = sum(
            pos.get('teminat', pos.get('position_value', 0) / pos.get('leverage', 1))
            for pos in self.positions.values()
        )
        self.equity_curve.append({'zaman': datetime.now().isoformat(), 'bakiye': round(total_equity, 2)})
        if len(self.equity_curve) > 500:
            self.equity_curve = self.equity_curve[-500:]

    def _position_pnl(self, pos, current_price):
        entry = pos['entry_price']
        quantity = pos.get('quantity', 0)
        if pos['direction'] == 'LONG':
            return (current_price - entry) * quantity
        else:
            return (entry - current_price) * quantity

    def _check_position(self, symbol, pos, price):
        direction = pos['direction']
        ts_pct = (PAPER_SETTINGS.get('trailing_stop_yuzde') or 3.0) / 100
        trailing = pos.get('trailing_stop', 0)
        if trailing <= 0:
            return

        entry = pos['entry_price']
        leverage = pos.get('leverage', 1)

        if not pos.get('kismi_satis'):
            kar_hedefi = (PAPER_SETTINGS.get('kismi_satis_kar_hedefi') or 1.0) / 100
            kismi_yuzde = PAPER_SETTINGS.get('kismi_satis_yuzde') or 25
            if direction == 'LONG':
                kar_orani = (price - entry) / entry
            else:
                kar_orani = (entry - price) / entry
            if kar_orani >= kar_hedefi:
                self._partial_close(symbol, kismi_yuzde, 'KISMI_SATIS', price)
                if symbol in self.positions:
                    pos = self.positions[symbol]
                    pos['kismi_satis'] = True
                    if direction == 'LONG':
                        pos['trailing_stop'] = entry
                    else:
                        pos['trailing_stop'] = entry
                    logger.info('Kismi satis yapildi: %s, stop girise cekildi: %.6f', symbol, entry)
                return

        if direction == 'LONG':
            if price <= trailing:
                self._close_position(symbol, trailing, 'TRAILING_STOP')
            else:
                new_trailing = price * (1 - ts_pct)
                if new_trailing > trailing:
                    pos['trailing_stop'] = new_trailing
        else:
            if price >= trailing:
                self._close_position(symbol, trailing, 'TRAILING_STOP')
            else:
                new_trailing = price * (1 + ts_pct)
                if new_trailing < trailing:
                    pos['trailing_stop'] = new_trailing

    def _partial_close(self, symbol, yuzde, reason, current_price=None):
        if symbol not in self.positions:
            return False
        pos = self.positions[symbol]
        satilan_miktar = pos['quantity'] * (yuzde / 100)
        leverage = pos.get('leverage', 1)
        entry = pos['entry_price']
        satilan_notional = satilan_miktar * entry
        satilan_teminat = satilan_notional / leverage
        komisyon = satilan_notional * self.KOMISYON_ORANI
        if current_price and pos['direction'] == 'LONG':
            pnl = (current_price - entry) * satilan_miktar
        elif current_price and pos['direction'] == 'SHORT':
            pnl = (entry - current_price) * satilan_miktar
        else:
            pnl = 0
        pnl -= komisyon
        pos['quantity'] -= satilan_miktar
        pos['position_value'] = round(pos['quantity'] * entry, 2)
        pos['teminat'] = round(pos['quantity'] * entry / leverage, 2)
        pos['toplam_komisyon'] = round(pos.get('toplam_komisyon', 0) + komisyon, 4)
        self.locked_margin -= satilan_teminat
        self.balance += satilan_teminat + pnl
        self.total_trades += 1
        if pnl > 0:
            self.winning_trades += 1
        else:
            self.losing_trades += 1
        pnl_percent = (pnl / satilan_teminat * 100) if satilan_teminat > 0 else 0
        self.trade_history.append({
            'symbol': symbol,
            'direction': pos['direction'],
            'entry_price': entry,
            'current_price': current_price or entry,
            'stop_loss': pos['stop_loss'],
            'trailing_stop': pos['trailing_stop'],
            'timeframe': pos['timeframe'],
            'leverage': pos['leverage'],
            'quantity': round(satilan_miktar, 6),
            'position_value': round(satilan_notional, 2),
            'open_time': pos['open_time'],
            'close_time': datetime.now().isoformat(),
            'close_price': current_price or entry,
            'pnl': round(pnl, 2),
            'pnl_percent': round(pnl_percent, 2),
            'komisyon': round(komisyon, 4),
            'reason': reason
        })
        return True

    def _open_position(self, symbol, oneri, price, timeframe='15m'):
        leverage = PAPER_SETTINGS.get('kaldirac', 1)
        base_dolar = PAPER_SETTINGS['islem_basi_dolar']
        pos_value = base_dolar * leverage
        teminat = pos_value / leverage
        komisyon = pos_value * self.KOMISYON_ORANI
        if teminat + komisyon > self.balance:
            return
        quantity = pos_value / price
        if quantity <= 0:
            return

        ts_pct = (PAPER_SETTINGS.get('trailing_stop_yuzde') or 3.0) / 100
        if oneri['yon'] == 'LONG':
            initial_trailing = price * (1 - ts_pct)
        else:
            initial_trailing = price * (1 + ts_pct)

        self.balance -= (teminat + komisyon)
        self.locked_margin += teminat
        self.positions[symbol] = {
            'symbol': symbol,
            'direction': oneri['yon'],
            'entry_price': price,
            'current_price': price,
            'trailing_stop': initial_trailing,
            'timeframe': timeframe,
            'leverage': leverage,
            'quantity': quantity,
            'position_value': round(pos_value, 2),
            'teminat': round(teminat, 2),
            'open_time': datetime.now().isoformat(),
            'close_time': None,
            'close_price': None,
            'pnl': None,
            'pnl_percent': None,
            'reason': None,
            'toplam_komisyon': round(komisyon, 4),
            'kismi_satis': False
        }

    def _close_position(self, symbol, close_price, reason):
        if symbol not in self.positions:
            return
        pos = self.positions.pop(symbol)
        pnl = self._position_pnl(pos, close_price)
        pos_value = pos.get('position_value', 0)
        leverage = pos.get('leverage', 1)
        teminat = pos.get('teminat', pos_value / leverage)
        komisyon = pos_value * self.KOMISYON_ORANI
        pnl -= komisyon
        toplam_komisyon = pos.get('toplam_komisyon', 0) + komisyon
        pnl_percent = (pnl / teminat * 100) if teminat > 0 else 0
        self.locked_margin -= teminat
        self.balance += teminat + pnl
        self.total_trades += 1
        if pnl > 0:
            self.winning_trades += 1
        else:
            self.losing_trades += 1
        pos['close_time'] = datetime.now().isoformat()
        pos['close_price'] = close_price
        pos['pnl'] = round(pnl, 2)
        pos['pnl_percent'] = round(pnl_percent, 2)
        pos['komisyon'] = round(komisyon, 4)
        pos['toplam_komisyon'] = round(toplam_komisyon, 4)
        pos['reason'] = reason
        self.trade_history.append(pos)
        self.son_islem_zamani[symbol] = datetime.now()
        if len(self.trade_history) > 200:
            self.trade_history = self.trade_history[-200:]

    def get_state(self):
        open_pnl = 0
        total_locked = 0
        for pos in self.positions.values():
            current_price = pos.get('current_price', pos['entry_price'])
            open_pnl += self._position_pnl(pos, current_price)
            total_locked += pos.get('teminat', pos.get('position_value', 0) / pos.get('leverage', 1))
        self.locked_margin = round(total_locked, 2)
        
        total_equity = round(self.balance + self.locked_margin + open_pnl, 2)
        win_rate = (self.winning_trades / self.total_trades * 100) if self.total_trades > 0 else 0
        toplam_kapali_kar = round(sum(t.get('pnl', 0) for t in self.trade_history), 2)
        
        positions_with_pnl = []
        for sym, pos in self.positions.items():
            current_price = pos.get('current_price', pos['entry_price'])
            pnl = self._position_pnl(pos, current_price)
            pos_val = pos.get('position_value', 0)
            pnl_percent = (pnl / pos_val * 100) if pos_val > 0 else 0
            trailing_stop = pos.get('trailing_stop', 0)
            entry = pos.get('entry_price', 0)
            quantity = pos.get('quantity', 0)
            direction = pos.get('direction', 'LONG')
            trailing_kz = 0
            if trailing_stop > 0 and entry > 0 and quantity > 0:
                if direction == 'LONG':
                    trailing_kz = round((trailing_stop - entry) * quantity, 4)
                else:
                    trailing_kz = round((entry - trailing_stop) * quantity, 4)
            pos_data = pos.copy()
            pos_data['current_pnl'] = round(pnl, 2)
            pos_data['current_pnl_percent'] = round(pnl_percent, 2)
            pos_data['trailing_kz'] = trailing_kz
            positions_with_pnl.append(pos_data)
        
        return {
            'durum': self.durum,
            'baslangic_bakiyesi': self.initial_balance,
            'kullanilabilir_bakiye': round(self.balance, 2),
            'kilitli_teminat': self.locked_margin,
            'acik_kar_zarar': round(open_pnl, 2),
            'toplam_kapali_kar': toplam_kapali_kar,
            'ozsermaye': total_equity,
            'acik_pozisyon_sayisi': len(self.positions),
            'toplam_islem': self.total_trades,
            'kapanan_islem_sayisi': len(self.trade_history),
            'kazanan': self.winning_trades,
            'kaybeden': self.losing_trades,
            'kazanma_orani': round(win_rate, 1),
            'baslangic_zamani': self.start_time,
            'equity_curve': self.equity_curve[-50:],
            'acik_pozisyonlar': positions_with_pnl
        }

    def reset(self, initial_balance=100):
        self.__init__(initial_balance)
        return self.get_state()

# ============================================================
# BINANCE CANLI ISLEM MODULU
# ============================================================

import hmac
import hashlib
import time as _time

BINANCE_LIVE_URL = 'https://fapi.binance.com'
BINANCE_TESTNET_URL = 'https://testnet.binancefuture.com'

class BinanceLiveTrader:
    """Binance Futures ile canli islem - test/live + otomatik islem"""

    KOMISYON_ORANI = 0.0004

    def __init__(self):
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.config_file = os.path.join(self.base_dir, 'binance_config.json')
        self.session = requests.Session()
        self.session.verify = False
        self.durum = 'durdu'
        self.total_trades = 0
        self.winning_trades = 0
        self.losing_trades = 0
        self.start_time = datetime.now().isoformat()
        self.son_islem_zamani = {}
        self.trade_history = []
        self.config = self._load_config()
        self.local_positions = {}  # trailing stop vs icin yerel takip
        self.equity_curve = [{'zaman': datetime.now().isoformat(), 'bakiye': 0}]
        self._load_trader_state()

    def _load_config(self):
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r') as f:
                    data = json.load(f)
                    if 'active_mode' not in data:
                        data['active_mode'] = 'test'
                    if 'testnet' not in data:
                        data['testnet'] = {'api_key': '', 'api_secret': ''}
                    if 'live' not in data:
                        data['live'] = {'api_key': '', 'api_secret': ''}
                    return data
        except:
            pass
        return {
            'active_mode': 'test',
            'testnet': {'api_key': '', 'api_secret': ''},
            'live': {'api_key': '', 'api_secret': ''}
        }

    def _save_config(self):
        with open(self.config_file, 'w') as f:
            json.dump(self.config, f, indent=2)

    def save_testnet_config(self, api_key, api_secret):
        self.config['testnet'] = {'api_key': api_key, 'api_secret': api_secret}
        self._save_config()

    def save_live_config(self, api_key, api_secret):
        self.config['live'] = {'api_key': api_key, 'api_secret': api_secret}
        self._save_config()

    def set_active_mode(self, mode):
        if mode not in ('test', 'live'):
            return False
        self.config['active_mode'] = mode
        self._save_config()
        return True

    def _get_state_file(self):
        mode = self.config.get('active_mode', 'test')
        suffix = 'test' if mode == 'test' else 'live'
        return os.path.join(self.base_dir, f'trader_state_{suffix}.json')

    def _save_trader_state(self):
        state_file = self._get_state_file()
        state = {
            'durum': self.durum,
            'total_trades': self.total_trades,
            'winning_trades': self.winning_trades,
            'losing_trades': self.losing_trades,
            'start_time': self.start_time,
            'son_islem_zamani': {k: v.isoformat() for k, v in self.son_islem_zamani.items()},
            'trade_history': self.trade_history[-500:],
            'local_positions': self.local_positions
        }
        with open(state_file, 'w') as f:
            json.dump(state, f, indent=2)

    def _load_trader_state(self):
        state_file = self._get_state_file()
        try:
            if os.path.exists(state_file):
                with open(state_file, 'r') as f:
                    state = json.load(f)
                self.durum = state.get('durum', 'durdu')
                self.total_trades = state.get('total_trades', 0)
                self.winning_trades = state.get('winning_trades', 0)
                self.losing_trades = state.get('losing_trades', 0)
                self.start_time = state.get('start_time', datetime.now().isoformat())
                self.son_islem_zamani = {k: datetime.fromisoformat(v) for k, v in state.get('son_islem_zamani', {}).items()}
                self.trade_history = state.get('trade_history', [])
                self.local_positions = state.get('local_positions', {})
        except:
            pass

    def get_summary(self):
        history = self.trade_history
        closed = [t for t in history if t.get('reason') != 'ACILIS']
        total = len(closed)
        kazanan = [t for t in closed if t.get('pnl', 0) > 0]
        kaybeden = [t for t in closed if t.get('pnl', 0) < 0]
        notr = [t for t in closed if t.get('pnl', 0) == 0]
        toplam_kar = sum(t.get('pnl', 0) for t in kazanan)
        toplam_zarar = sum(t.get('pnl', 0) for t in kaybeden)
        net_kar_zarar = sum(t.get('pnl', 0) for t in closed)

        long_islemler = [t for t in closed if t.get('direction') == 'LONG']
        short_islemler = [t for t in closed if t.get('direction') == 'SHORT']
        long_kazanan = len([t for t in long_islemler if t.get('pnl', 0) > 0])
        long_kaybeden = len([t for t in long_islemler if t.get('pnl', 0) < 0])
        short_kazanan = len([t for t in short_islemler if t.get('pnl', 0) > 0])
        short_kaybeden = len([t for t in short_islemler if t.get('pnl', 0) < 0])

        en_cok_kar = max(closed, key=lambda t: t.get('pnl', 0)) if closed else None
        en_cok_zarar = min(closed, key=lambda t: t.get('pnl', 0)) if closed else None

        sebep_dagilimi = {}
        for t in closed:
            r = t.get('reason', 'BILINMIYOR')
            if r not in sebep_dagilimi:
                sebep_dagilimi[r] = {'sayi': 0, 'toplam_pnl': 0}
            sebep_dagilimi[r]['sayi'] += 1
            sebep_dagilimi[r]['toplam_pnl'] = round(sebep_dagilimi[r]['toplam_pnl'] + t.get('pnl', 0), 4)

        tf_dagilimi = {}
        for t in closed:
            tf = t.get('timeframe', '?')
            if tf not in tf_dagilimi:
                tf_dagilimi[tf] = {'sayi': 0, 'kazanan': 0, 'kaybeden': 0, 'toplam_pnl': 0}
            tf_dagilimi[tf]['sayi'] += 1
            if t.get('pnl', 0) > 0:
                tf_dagilimi[tf]['kazanan'] += 1
            elif t.get('pnl', 0) < 0:
                tf_dagilimi[tf]['kaybeden'] += 1
            tf_dagilimi[tf]['toplam_pnl'] = round(tf_dagilimi[tf]['toplam_pnl'] + t.get('pnl', 0), 4)

        coin_dagilimi = {}
        for t in closed:
            s = t.get('symbol', '?')
            if s not in coin_dagilimi:
                coin_dagilimi[s] = {'sayi': 0, 'kazanan': 0, 'kaybeden': 0, 'toplam_pnl': 0}
            coin_dagilimi[s]['sayi'] += 1
            if t.get('pnl', 0) > 0:
                coin_dagilimi[s]['kazanan'] += 1
            elif t.get('pnl', 0) < 0:
                coin_dagilimi[s]['kaybeden'] += 1
            coin_dagilimi[s]['toplam_pnl'] = round(coin_dagilimi[s]['toplam_pnl'] + t.get('pnl', 0), 4)
        en_cok_islem = sorted(coin_dagilimi.items(), key=lambda x: x[1]['sayi'], reverse=True)[:10]

        return {
            'toplam_islem': total,
            'kazanan': len(kazanan),
            'kaybeden': len(kaybeden),
            'notr': len(notr),
            'kazanma_orani': round(len(kazanan) / total * 100, 1) if total > 0 else 0,
            'toplam_kar': round(toplam_kar, 4),
            'toplam_zarar': round(toplam_zarar, 4),
            'net_kar_zarar': round(net_kar_zarar, 4),
            'ortalama_kar': round(toplam_kar / len(kazanan), 4) if kazanan else 0,
            'ortalama_zarar': round(toplam_zarar / len(kaybeden), 4) if kaybeden else 0,
            'long_sayisi': len(long_islemler),
            'short_sayisi': len(short_islemler),
            'long_kazanan': long_kazanan,
            'long_kaybeden': long_kaybeden,
            'short_kazanan': short_kazanan,
            'short_kaybeden': short_kaybeden,
            'en_cok_kar': en_cok_kar,
            'en_cok_zarar': en_cok_zarar,
            'sebep_dagilimi': sebep_dagilimi,
            'tf_dagilimi': tf_dagilimi,
            'en_cok_islem_yapilanlar': [{'symbol': s, **v} for s, v in en_cok_islem],
            'trade_history': closed[-200:]
        }

    def reset_stats(self):
        self.trade_history = []
        self.total_trades = 0
        self.winning_trades = 0
        self.losing_trades = 0
        self._save_trader_state()
        return {'durum': 'sifirlandi'}

    def _get_active_config(self):
        mode = self.config.get('active_mode', 'test')
        if mode == 'test':
            return self.config.get('testnet', {'api_key': '', 'api_secret': ''})
        return self.config.get('live', {'api_key': '', 'api_secret': ''})

    def _get_base_url(self):
        mode = self.config.get('active_mode', 'test')
        if mode == 'test':
            return BINANCE_TESTNET_URL
        return BINANCE_LIVE_URL

    def _get_step_size(self, symbol):
        try:
            url = self._get_base_url() + '/fapi/v1/exchangeInfo'
            r = self.session.get(url, timeout=10)
            data = r.json()
            for s in data.get('symbols', []):
                if s['symbol'] == symbol:
                    for f in s.get('filters', []):
                        if f['filterType'] == 'LOT_SIZE':
                            return float(f['stepSize'])
            return 0.001
        except:
            return 0.001

    def _round_step(self, qty, step):
        if step <= 0:
            return round(qty, 6)
        precision = len(str(step).rstrip('0').split('.')[-1]) if '.' in str(step) else 0
        return round(int(qty / step) * step, precision)

    def _sign(self, params):
        active = self._get_active_config()
        query = '&'.join(f'{k}={v}' for k, v in params.items())
        signature = hmac.new(
            active['api_secret'].encode(),
            query.encode(),
            hashlib.sha256
        ).hexdigest()
        params['signature'] = signature
        return params

    def _api_request(self, method, endpoint, params=None, retries=3):
        active = self._get_active_config()
        if not active.get('api_key'):
            return {'error': 'API key tanimli degil'}
        url = self._get_base_url() + endpoint
        params = params or {}
        params['timestamp'] = int(_time.time() * 1000)
        params = self._sign(params)
        headers = {'X-MBX-APIKEY': active['api_key']}
        last_error = None
        for attempt in range(retries):
            try:
                if method == 'GET':
                    r = self.session.get(url, params=params, headers=headers, timeout=10)
                else:
                    r = self.session.post(url, data=params, headers=headers, timeout=10)
                data = r.json()
                if isinstance(data, dict) and 'code' in data and data['code'] != 200:
                    last_error = data.get('msg', 'Binance hatasi')
                    _time.sleep(0.5)
                    continue
                return data
            except Exception as e:
                last_error = str(e)
                _time.sleep(0.5)
        return {'error': f'{retries} denemede basarisiz: {last_error}'}

    def _api_get(self, endpoint, params=None):
        return self._api_request('GET', endpoint, params)

    def _api_post(self, endpoint, params=None):
        return self._api_request('POST', endpoint, params)

    def _get_fill_price(self, symbol, side, limit=20):
        trades = self._api_get('/fapi/v1/userTrades', {'symbol': symbol, 'limit': limit})
        if not isinstance(trades, list) or not trades:
            return 0
        close_side = 'SELL' if side == 'LONG' else 'BUY'
        entry_side = 'BUY' if side == 'LONG' else 'SELL'
        entry_trades = [t for t in trades if t.get('side') == entry_side]
        close_trades = [t for t in trades if t.get('side') == close_side]
        if not close_trades:
            return 0
        if entry_trades:
            entry_trades.sort(key=lambda t: t.get('time', 0), reverse=True)
            last_entry_ts = entry_trades[0].get('time', 0)
            close_after_entry = [t for t in close_trades if t.get('time', 0) > last_entry_ts]
            if close_after_entry:
                close_trades = close_after_entry
        close_trades.sort(key=lambda t: t.get('time', 0), reverse=True)
        return float(close_trades[0].get('price', 0))

    def _api_delete(self, endpoint, params=None):
        active = self._get_active_config()
        if not active.get('api_key'):
            return {'error': 'API key tanimli degil'}
        url = self._get_base_url() + endpoint
        params = params or {}
        params['timestamp'] = int(_time.time() * 1000)
        params = self._sign(params)
        headers = {'X-MBX-APIKEY': active['api_key']}
        try:
            r = self.session.delete(url, params=params, headers=headers, timeout=10)
            return r.json()
        except Exception as e:
            return {'error': str(e)}

    def get_balance(self):
        data = self._api_get('/fapi/v2/balance')
        if 'error' in data:
            return data
        usdt = next((b for b in data if b.get('asset') == 'USDT'), None)
        if usdt:
            return {
                'bakiye': float(usdt.get('balance', 0)),
                'kullanilabilir': float(usdt.get('availableBalance', 0)),
                'kilitli': float(usdt.get('crossWalletBalance', 0)) - float(usdt.get('availableBalance', 0)),
                'gercekizede': float(usdt.get('crossUnPnl', 0))
            }
        return {'error': 'USDT bakiyesi bulunamadi'}

    def get_positions(self):
        data = self._api_get('/fapi/v2/positionRisk')
        if 'error' in data:
            return data
        positions = []
        for p in data:
            amt = float(p.get('positionAmt', 0))
            if amt != 0:
                entry = float(p.get('entryPrice', 0))
                mark = float(p.get('markPrice', 0))
                lev = float(p.get('leverage', 1)) or 1
                notional = abs(amt) * entry
                pnl = float(p.get('unRealizedProfit', 0))
                side = 'LONG' if amt > 0 else 'SHORT'
                binance_sym = p.get('symbol', '')
                local = self.local_positions.get(binance_sym, {})
                teminat = notional / lev if lev > 0 else notional
                pnl_yuzde = (pnl / teminat * 100) if teminat > 0 else 0
                entry_time = local.get('entry_time', '')
                sure_dk = 0
                if entry_time:
                    try:
                        entry_dt = datetime.fromisoformat(entry_time)
                        sure_dk = round((datetime.now() - entry_dt).total_seconds() / 60, 1)
                    except:
                        pass
                trailing_stop = local.get('trailing_stop', 0)
                trailing_kz = 0
                if trailing_stop > 0 and entry > 0 and amt != 0:
                    if side == 'LONG':
                        trailing_kz = round((trailing_stop - entry) * abs(amt), 4)
                    else:
                        trailing_kz = round((entry - trailing_stop) * abs(amt), 4)
                positions.append({
                    'symbol': binance_sym,
                    'display_symbol': binance_sym.replace('USDT', '/USDT'),
                    'yon': side,
                    'miktar': amt,
                    'giris_fiyati': entry,
                    'mark_fiyati': mark,
                    'kaldirac': lev,
                    'notional': round(notional, 2),
                    'teminat': round(notional / lev, 2),
                    'acik_kz': round(pnl, 4),
                    'acik_kz_yuzde': round(pnl_yuzde, 2),
                    'trailing_stop': trailing_stop,
                    'trailing_kz': trailing_kz,
                    'timeframe': local.get('timeframe', '?'),
                    'entry_time': entry_time,
                    'sure_dk': sure_dk,
                    'guven': local.get('guven', 0),
                    'base_dolar': local.get('base_dolar', 0),
                })
        return positions

    def open_position(self, symbol, yon, leverage=5, base_dolar=4):
        balance = self.get_balance()
        if 'error' in balance:
            return balance
        if balance['kullanilabilir'] < base_dolar * 0.95:
            return {'error': f'Yetersiz bakiye: ${balance["kullanilabilir"]:.2f} (gerekli: ${base_dolar:.2f})'}

        binance_sym = symbol.replace('/', '')

        try:
            lev_result = self._api_post('/fapi/v1/leverage', {'symbol': binance_sym, 'leverage': leverage})
            if 'error' in lev_result:
                logger.warning('[LIVE] Kaldirac ayarlanamadi %s: %s', binance_sym, lev_result)
        except Exception as e:
            logger.warning('[LIVE] Kaldirac hatasi %s: %s', binance_sym, str(e))

        try:
            margin_result = self._api_post('/fapi/v1/marginType', {'symbol': binance_sym, 'marginType': 'ISOLATED'})
            if 'error' in margin_result:
                logger.warning('[LIVE] Margin ayarlanamadi %s: %s', binance_sym, margin_result)
        except Exception as e:
            logger.warning('[LIVE] Margin hatasi %s: %s', binance_sym, str(e))

        ticker = self._api_get('/fapi/v1/ticker/price', {'symbol': binance_sym})
        if 'error' in ticker:
            return ticker
        price = float(ticker.get('price', 0))
        if price <= 0:
            return {'error': 'Fiyat alinamadi'}

        step_size = self._get_step_size(binance_sym)

        notional = base_dolar * leverage
        raw_qty = notional / price
        qty = self._round_step(raw_qty, step_size)
        if qty <= 0:
            return {'error': f'Miktar cok kucuk: {raw_qty}'}

        actual_notional = qty * price
        if actual_notional < 5:
            return {'error': f'Notional cok kucuk: ${actual_notional:.2f} (min $5)'}

        side = 'BUY' if yon == 'LONG' else 'SELL'

        result = self._api_post('/fapi/v1/order', {
            'symbol': binance_sym,
            'side': side,
            'type': 'MARKET',
            'quantity': qty
        })

        if 'error' in result:
            return result

        self.total_trades += 1
        return {
            'durum': 'acildi',
            'symbol': symbol,
            'yon': yon,
            'fiyat': price,
            'miktar': qty,
            'notional': notional,
            'kaldirac': leverage,
            'emir_id': result.get('orderId'),
            'sonuc': result
        }

    def close_position(self, symbol, yon, reason='MANUEL'):
        binance_sym = symbol.replace('/', '')
        self._cancel_binance_sl(binance_sym)

        positions = self.get_positions()
        if 'error' in positions:
            return positions
        pos = next((p for p in positions if p['symbol'] == binance_sym), None)
        if not pos:
            return {'error': 'Pozisyon bulunamadi - zaten kapali olabilir'}

        yon = pos['yon']
        side = 'SELL' if yon == 'LONG' else 'BUY'

        qty = abs(pos['miktar'])

        step_size = self._get_step_size(binance_sym)
        qty = self._round_step(qty, step_size)

        result = self._api_post('/fapi/v1/order', {
            'symbol': binance_sym,
            'side': side,
            'type': 'MARKET',
            'quantity': qty,
            'reduceOnly': True
        })

        if 'error' in result:
            logger.error('KAPATMA BASARISIZ: %s result=%s', symbol, result)
            return {'error': 'Kapatma basarisiz', 'symbol': symbol, 'result': result}

        order_id = result.get('orderId')
        fill_price = float(result.get('avgPrice', 0))
        order_status = result.get('status', 'UNKNOWN')

        if fill_price <= 0 and order_status != 'FILLED':
            logger.info('MARKET emir dolmadi, bekleniyor: %s orderId=%s status=%s', symbol, order_id, order_status)
            for i in range(15):
                _time.sleep(1)
                status_result = self._api_get('/fapi/v1/order', {
                    'symbol': binance_sym,
                    'orderId': order_id
                })
                if 'error' not in status_result:
                    order_status = status_result.get('status', 'UNKNOWN')
                    fill_price = float(status_result.get('avgPrice', 0))
                    if fill_price > 0 or order_status in ('FILLED', 'CANCELED', 'EXPIRED'):
                        break
            logger.info('Emir sonucu: %s status=%s fill_price=%s', symbol, order_status, fill_price)

        if fill_price <= 0:
            logger.error('KAPATMA BASARISIZ: Fill price alinamadi: %s status=%s', symbol, order_status)
            return {'error': 'Kapatma basarisiz - fill price alinamadi', 'symbol': symbol, 'order_status': order_status}

        entry = pos['giris_fiyati']
        if yon == 'LONG':
            realized_pnl = (fill_price - entry) * qty
        else:
            realized_pnl = (entry - fill_price) * qty

        if realized_pnl > 0:
            self.winning_trades += 1
        elif realized_pnl < 0:
            self.losing_trades += 1

        local = self.local_positions.get(binance_sym, {})
        entry_time = local.get('entry_time', '')
        close_time = datetime.now().isoformat()
        sure_dk = 0
        if entry_time:
            try:
                sure_dk = round((datetime.now() - datetime.fromisoformat(entry_time)).total_seconds() / 60, 1)
            except:
                pass

        self.trade_history.append({
            'symbol': symbol, 'direction': yon,
            'entry_price': entry,
            'close_price': fill_price,
            'pnl': round(realized_pnl, 4),
            'pnl_yuzde': round(realized_pnl / (entry * qty) * 100, 2) if entry > 0 and qty > 0 else 0,
            'timeframe': local.get('timeframe', '?'),
            'leverage': local.get('leverage', 1),
            'reason': reason,
            'entry_time': entry_time,
            'close_time': close_time,
            'sure_dk': sure_dk,
        })

        self.local_positions.pop(binance_sym, None)
        self._save_trader_state()

        return {
            'durum': 'kapandı',
            'symbol': symbol,
            'pnl': realized_pnl
        }

    def set_stop_loss(self, symbol, yon, stop_price):
        binance_sym = symbol.replace('/', '')
        side = 'SELL' if yon == 'LONG' else 'BUY'
        price_tick = self._get_price_tick(binance_sym)
        trigger_price = self._round_price(stop_price, price_tick)
        return self._api_post('/fapi/v1/algoOrder', {
            'symbol': binance_sym,
            'side': side,
            'type': 'STOP_MARKET',
            'algoType': 'CONDITIONAL',
            'triggerPrice': str(trigger_price),
            'reduceOnly': 'true',
            'workingType': 'MARK_PRICE'
        })

    def get_state(self):
        active = self._get_active_config()
        active_mode = self.config.get('active_mode', 'test')
        api_set = bool(active.get('api_key'))
        balance = self.get_balance() if api_set else {'error': 'API key tanimli degil'}
        positions = self.get_positions() if api_set else []
        total_pnl = sum(p['acik_kz'] for p in positions) if isinstance(positions, list) else 0
        total_margin = sum(p['teminat'] for p in positions) if isinstance(positions, list) else 0
        win_rate = (self.winning_trades / (self.winning_trades + self.losing_trades) * 100) if (self.winning_trades + self.losing_trades) > 0 else 0
        toplam_kapali_kar = round(sum(t.get('pnl', 0) for t in self.trade_history), 4)

        return {
            'durum': self.durum,
            'active_mode': active_mode,
            'testnet': active_mode == 'test',
            'api_key_set': api_set,
            'bakiye': balance.get('kullanilabilir', 0) if isinstance(balance, dict) and 'error' not in balance else 0,
            'toplam_bakiye': balance.get('bakiye', 0) if isinstance(balance, dict) and 'error' not in balance else 0,
            'acik_kar_zarar': round(total_pnl, 4),
            'toplam_kapali_kar': toplam_kapali_kar,
            'kilitli_teminat': round(total_margin, 2),
            'acik_pozisyon_sayisi': len(positions) if isinstance(positions, list) else 0,
            'toplam_islem': self.total_trades,
            'kapanan_islem_sayisi': len(self.trade_history),
            'kazanan': self.winning_trades,
            'kaybeden': self.losing_trades,
            'kazanma_orani': round(win_rate, 1),
            'baslangic_zamani': self.start_time,
            'acik_pozisyonlar': positions if isinstance(positions, list) else [],
            'hata': balance.get('error') if isinstance(balance, dict) and 'error' in balance else None,
            'trade_history': self.trade_history[-50:],
            'equity_curve': self.equity_curve[-50:]
        }

    def process(self, results):
        api_active = self._get_active_config()
        if not api_active.get('api_key'):
            return

        try:
            binance_positions = self.get_positions()
        except Exception as e:
            logger.error('LiveTrader process: get_positions hatasi: %s', str(e))
            return
        if isinstance(binance_positions, dict) and 'error' in binance_positions:
            logger.warning('LiveTrader process: pozisyon alinamadi: %s', binance_positions['error'])
            return

        binance_syms = set(p['symbol'] for p in binance_positions)
        orphans = [sym for sym in list(self.local_positions.keys()) if sym not in binance_syms]
        if orphans:
            logger.info('Yetim pozisyon tespit edildi: %d adet - %s', len(orphans), ', '.join(orphans[:10]))
        for sym in orphans[:10]:
            try:
                local = self.local_positions[sym]
                direction = local.get('direction', 'LONG')
                entry = local.get('entry_price', 0)
                entry_time = local.get('entry_time', '')
                leverage = local.get('leverage', 1)
                timeframe = local.get('timeframe', '?')
                base_dolar = local.get('base_dolar', 4)
                fill_price = self._get_fill_price(sym, direction, limit=20)
                if fill_price <= 0:
                    fill_price = entry
                if direction == 'LONG':
                    pnl_pct = (fill_price - entry) / entry if entry > 0 else 0
                else:
                    pnl_pct = (entry - fill_price) / entry if entry > 0 else 0
                realized_pnl = pnl_pct * base_dolar
                sure_dk = 0
                if entry_time:
                    try:
                        sure_dk = round((datetime.now() - datetime.fromisoformat(entry_time)).total_seconds() / 60, 1)
                    except:
                        pass
                self.trade_history.append({
                    'symbol': sym, 'direction': direction,
                    'entry_price': entry,
                    'close_price': fill_price,
                    'pnl': round(realized_pnl, 4),
                    'pnl_yuzde': round(pnl_pct * 100, 2),
                    'timeframe': timeframe,
                    'leverage': leverage,
                    'reason': 'BINANCE_SL',
                    'entry_time': entry_time,
                    'close_time': datetime.now().isoformat(),
                    'sure_dk': sure_dk,
                })
                if realized_pnl > 0:
                    self.winning_trades += 1
                elif realized_pnl < 0:
                    self.losing_trades += 1
                logger.info('Yetim pozisyon kaydedildi: %s %s entry=%.6f close=%.6f pnl=%.4f reason=BINANCE_SL', sym, direction, entry, fill_price, realized_pnl)
                self.local_positions.pop(sym, None)
            except Exception as e:
                logger.error('Yetim pozisyon islenemedi: %s - %s', sym, str(e))
                self.local_positions.pop(sym, None)

        if orphans:
            self._save_trader_state()

        if self.durum == 'durdu':
            return

        for pos in binance_positions:
            binance_sym = pos['symbol']
            entry = pos['giris_fiyati']
            if entry <= 0 or pos['mark_fiyati'] <= 0:
                logger.warning('Gecersiz veri: %s entry=%s mark=%s - atlandi', binance_sym, entry, pos['mark_fiyati'])
                continue
            if binance_sym not in self.local_positions:
                entry = pos['giris_fiyati']
                lev = pos.get('kaldirac', 1)
                ts_pct = (PAPER_SETTINGS.get('trailing_stop_yuzde') or 3.0) / 100
                if pos['yon'] == 'LONG':
                    init_trailing = entry * (1 - ts_pct)
                else:
                    init_trailing = entry * (1 + ts_pct)
                self.local_positions[binance_sym] = {
                    'trailing_stop': init_trailing,
                    'timeframe': '?',
                    'entry_time': datetime.now().isoformat(),
                    'entry_price': entry,
                    'direction': pos['yon'],
                    'leverage': lev,
                    'guven': 0,
                    'base_dolar': PAPER_SETTINGS.get('islem_basi_dolar', 4),
                    'v2': True,
                }

        for pos in binance_positions:
            sym = pos['symbol']
            r = next((r for r in results if r['sembol'] == sym or r['sembol'].replace('/', '') == sym), None)
            if r:
                price = r['fiyat']
            else:
                price = pos['mark_fiyati']
            self._check_live_position(sym, pos, price)

        if self.durum == 'bekle':
            pass
        elif len(binance_positions) < PAPER_SETTINGS.get('max_pozisyon', 5):
            for r in results:
                symbol = r['sembol']
                binance_sym = symbol.replace('/', '')
                if any(p['symbol'] == binance_sym for p in binance_positions):
                    continue
                if symbol in PAPER_SETTINGS.get('kara_liste', []):
                    continue
                soguma = PAPER_SETTINGS.get('soguma_dakika', 60)
                if soguma > 0 and symbol in self.son_islem_zamani:
                    gecen = (datetime.now() - self.son_islem_zamani[symbol]).total_seconds() / 60
                    if gecen < soguma:
                        continue
                price = r.get('fiyat', 0)
                if price < PAPER_SETTINGS.get('min_fiyat', 0.01):
                    continue
                best_signal = None
                best_guven = 0
                best_tf = None
                for stf, sig in r.get('tf_signals', {}).items():
                    if sig.get('trend', 'NOTR') == 'NOTR':
                        continue
                    oneri_stf = sig.get('islem_onerisi')
                    if not oneri_stf:
                        continue
                    guven_stf = sig.get('guven_puani', 0)
                    sp_stf = sig.get('sinyal_puani', 0)
                    if guven_stf < PAPER_SETTINGS.get('min_guven', 60) or abs(sp_stf) < PAPER_SETTINGS.get('min_sinyal_puani', 2):
                        continue
                    if guven_stf > best_guven:
                        best_guven = guven_stf
                        best_signal = oneri_stf
                        best_tf = stf
                if best_signal is None:
                    continue
                logger.info('[LIVE] Islem sinyali: %s %s guven=%s tf=%s', symbol, best_signal.get('yon','?'), best_guven, best_tf)
                self._open_live_position(symbol, best_signal, price, best_tf)


        try:
            balance = self.get_balance()
            bakiye = balance.get('bakiye', 0) if isinstance(balance, dict) and 'error' not in balance else 0
            self.equity_curve.append({'zaman': datetime.now().isoformat(), 'bakiye': round(bakiye, 2)})
            if len(self.equity_curve) > 500:
                self.equity_curve = self.equity_curve[-500:]
        except:
            pass
        self._save_trader_state()

    def _check_live_position(self, symbol, pos, current_price):
        direction = pos['yon']
        local = self.local_positions.get(symbol, {})
        trailing = local.get('trailing_stop', 0)
        leverage = pos.get('kaldirac', 1)
        ts_pct = (PAPER_SETTINGS.get('trailing_stop_yuzde') or 3.0) / 100

        binance_mark = pos['mark_fiyati']
        if binance_mark <= 0:
            return

        if trailing <= 0:
            entry = pos['giris_fiyati']
            if direction == 'LONG':
                trailing = entry * (1 - ts_pct)
            else:
                trailing = entry * (1 + ts_pct)
            self.local_positions[symbol] = {
                **local,
                'trailing_stop': trailing,
                'direction': direction,
                'leverage': leverage,
                'v2': True,
            }
            sl_result = self._place_binance_sl(symbol, direction, trailing, leverage)
            if sl_result and 'error' not in sl_result:
                self.local_positions[symbol]['sl_order_id'] = sl_result.get('algoId') or sl_result.get('orderId')
            return

        if not local.get('kismi_satis'):
            kar_hedefi = (PAPER_SETTINGS.get('kismi_satis_kar_hedefi') or 1.0) / 100
            kismi_yuzde = PAPER_SETTINGS.get('kismi_satis_yuzde') or 25
            entry = pos['giris_fiyati']
            if direction == 'LONG':
                kar_orani = (binance_mark - entry) / entry
            else:
                kar_orani = (entry - binance_mark) / entry
            if kar_orani >= kar_hedefi:
                self._cancel_binance_sl(symbol)
                result = self._partial_close_live(symbol, kismi_yuzde, direction, binance_mark)
                if result and 'error' not in result:
                    self.local_positions[symbol]['kismi_satis'] = True
                    self.local_positions[symbol]['trailing_stop'] = entry
                    sl_result = self._place_binance_sl(symbol, direction, entry, leverage)
                    if sl_result and 'error' not in sl_result:
                        self.local_positions[symbol]['sl_order_id'] = sl_result.get('algoId') or sl_result.get('orderId')
                    logger.info('Kismi satis yapildi: %s, stop girise cekildi: %.6f', symbol, entry)
                else:
                    logger.warning('Kismi satis basarisiz: %s %s', symbol, result)
                return

        if direction == 'LONG':
            if binance_mark <= trailing:
                self._cancel_binance_sl(symbol)
                result = self.close_position(symbol, direction, reason='TRAILING_STOP')
                if result and 'error' not in result:
                    self.local_positions.pop(symbol, None)
                else:
                    logger.warning('Trailing stop kapatma basarisiz (race condition?): %s %s', symbol, result)
            else:
                new_trailing = binance_mark * (1 - ts_pct)
                if new_trailing > trailing:
                    self.local_positions[symbol]['trailing_stop'] = new_trailing
                    sl_result = self._place_binance_sl(symbol, direction, new_trailing, leverage)
                    if sl_result and 'error' not in sl_result:
                        self.local_positions[symbol]['sl_order_id'] = sl_result.get('algoId') or sl_result.get('orderId')
                        logger.info('Trailing stop Binance SL guncellendi: %s @ %s', symbol, new_trailing)
        else:
            if binance_mark >= trailing:
                self._cancel_binance_sl(symbol)
                result = self.close_position(symbol, direction, reason='TRAILING_STOP')
                if result and 'error' not in result:
                    self.local_positions.pop(symbol, None)
                else:
                    logger.warning('Trailing stop kapatma basarisiz (race condition?): %s %s', symbol, result)
            else:
                new_trailing = binance_mark * (1 + ts_pct)
                if new_trailing < trailing:
                    self.local_positions[symbol]['trailing_stop'] = new_trailing
                    sl_result = self._place_binance_sl(symbol, direction, new_trailing, leverage)
                    if sl_result and 'error' not in sl_result:
                        self.local_positions[symbol]['sl_order_id'] = sl_result.get('algoId') or sl_result.get('orderId')
                        logger.info('Trailing stop Binance SL guncellendi: %s @ %s', symbol, new_trailing)

    def _partial_close_live(self, symbol, yuzde, direction, current_price=None):
        binance_sym = symbol.replace('/', '')
        positions = self.get_positions()
        if not isinstance(positions, list):
            return {'error': 'Pozisyon alinamadi'}
        pos = next((p for p in positions if p['symbol'] == binance_sym), None)
        if not pos:
            return {'error': 'Pozisyon bulunamadi'}
        direction = pos['yon']
        side = 'SELL' if direction == 'LONG' else 'BUY'
        qty = abs(pos['miktar']) * (yuzde / 100)
        step_size = self._get_step_size(binance_sym)
        qty = self._round_step(qty, step_size)
        if qty <= 0:
            return {'error': 'Miktar cok kucuk'}
        result = self._api_post('/fapi/v1/order', {
            'symbol': binance_sym,
            'side': side,
            'type': 'MARKET',
            'quantity': qty,
            'reduceOnly': True
        })
        if 'error' in result:
            logger.error('KISMI_SATIS BASARISIZ: %s result=%s', symbol, result)
            return {'error': 'Kismi satis basarisiz', 'symbol': symbol, 'result': result}

        order_id = result.get('orderId')
        fill_price = float(result.get('avgPrice', 0))
        order_status = result.get('status', 'UNKNOWN')

        if fill_price <= 0 and order_status != 'FILLED':
            logger.info('KISMI_SATIS: Emir dolmadi, bekleniyor: %s orderId=%s', symbol, order_id)
            for i in range(15):
                _time.sleep(1)
                status_result = self._api_get('/fapi/v1/order', {
                    'symbol': binance_sym,
                    'orderId': order_id
                })
                if 'error' not in status_result:
                    order_status = status_result.get('status', 'UNKNOWN')
                    fill_price = float(status_result.get('avgPrice', 0))
                    if fill_price > 0 or order_status in ('FILLED', 'CANCELED', 'EXPIRED'):
                        break
            logger.info('KISMI_SATIS emir sonucu: %s status=%s fill_price=%s', symbol, order_status, fill_price)

        if fill_price <= 0:
            logger.error('KISMI_SATIS BASARISIZ: Fill price alinamadi: %s status=%s', symbol, order_status)
            return {'error': 'Kismi satis basarisiz - fill price alinamadi', 'symbol': symbol, 'order_status': order_status}

        entry = pos['giris_fiyati']
        if direction == 'LONG':
            realized_pnl = (fill_price - entry) * qty
        else:
            realized_pnl = (entry - fill_price) * qty
        self.total_trades += 1
        local = self.local_positions.get(symbol, {})
        entry_time = local.get('entry_time', '')
        close_time = datetime.now().isoformat()
        sure_dk = 0
        if entry_time:
            try:
                sure_dk = round((datetime.now() - datetime.fromisoformat(entry_time)).total_seconds() / 60, 1)
            except:
                pass
        self.trade_history.append({
            'symbol': symbol, 'direction': direction,
            'entry_price': entry, 'close_price': fill_price,
            'pnl': round(realized_pnl, 4),
            'pnl_yuzde': round(realized_pnl / (entry * qty) * 100, 2) if entry > 0 and qty > 0 else 0,
            'timeframe': local.get('timeframe', '?'),
            'leverage': local.get('leverage', 1),
            'reason': 'KISMI_SATIS',
            'entry_time': entry_time,
            'close_time': close_time,
            'sure_dk': sure_dk,
        })
        sl_result = self._place_binance_sl(binance_sym, direction, entry, pos.get('kaldirac', 1))
        if sl_result and 'error' not in sl_result:
            logger.info('Kismi satis sonrasi SL guncellendi (entry): %s @ %.6f', symbol, entry)
            if binance_sym in self.local_positions:
                self.local_positions[binance_sym]['sl_order_id'] = sl_result.get('algoId') or sl_result.get('orderId')
        else:
            logger.warning('Kismi satis sonrasi SL guncellenemedi: %s -> %s', symbol, sl_result)
        return result

    def _cancel_binance_sl(self, binance_sym):
        try:
            open_orders = self._api_get('/fapi/v1/openAlgoOrders', {'symbol': binance_sym})
            if isinstance(open_orders, dict):
                orders = open_orders.get('orders', [])
            elif isinstance(open_orders, list):
                orders = open_orders
            else:
                orders = []
            for o in orders:
                algo_id = o.get('algoId') or o.get('orderId') or o.get('order_id')
                if algo_id:
                    self._api_delete('/fapi/v1/algoOrder', {'symbol': binance_sym, 'algoId': algo_id})
                    logger.info('Eski algo SL emri iptal edildi: %s algoId=%s', binance_sym, algo_id)
        except Exception as e:
            logger.warning('Algo SL emir iptali hatasi %s: %s', binance_sym, str(e))

    def _get_price_tick(self, symbol):
        try:
            url = self._get_base_url() + '/fapi/v1/exchangeInfo'
            r = self.session.get(url, timeout=10)
            data = r.json()
            for s in data.get('symbols', []):
                if s['symbol'] == symbol:
                    for f in s.get('filters', []):
                        if f['filterType'] == 'PRICE_FILTER':
                            return float(f['tickSize'])
        except:
            pass
        return 0.00001

    def _round_price(self, price, tick):
        if tick <= 0:
            return price
        from decimal import Decimal
        d_tick = Decimal(str(tick))
        d_price = Decimal(str(price))
        return float((d_price // d_tick) * d_tick)

    def _place_binance_sl(self, binance_sym, yon, sl_price, leverage):
        step_size = self._get_step_size(binance_sym)
        price_tick = self._get_price_tick(binance_sym)
        try:
            pos_data = self._api_get('/fapi/v2/positionRisk', {'symbol': binance_sym})
            if isinstance(pos_data, list):
                for p in pos_data:
                    amt = float(p.get('positionAmt', 0))
                    if amt != 0:
                        qty = abs(amt)
                        qty = self._round_step(qty, step_size)
                        if qty > 0:
                            self._cancel_binance_sl(binance_sym)
                            side = 'SELL' if yon == 'LONG' else 'BUY'
                            trigger_price = self._round_price(sl_price, price_tick)
                            if trigger_price <= 0:
                                logger.warning('SL atlandi (fiyat cok kucuk): %s trigger=%s', binance_sym, trigger_price)
                                return None
                            trigger_price_str = f'{trigger_price}'
                            result = self._api_post('/fapi/v1/algoOrder', {
                                'symbol': binance_sym,
                                'side': side,
                                'type': 'STOP_MARKET',
                                'algoType': 'CONDITIONAL',
                                'quantity': qty,
                                'triggerPrice': trigger_price_str,
                                'reduceOnly': 'true',
                                'workingType': 'MARK_PRICE'
                            })
                            if 'error' not in result:
                                algo_id = result.get('algoId') or result.get('orderId')
                                logger.info('Algo STOP_MARKET basarili: %s %s @ %s (algoId: %s)', yon, binance_sym, trigger_price_str, algo_id)
                            else:
                                logger.warning('Algo STOP_MARKET hatasi: %s -> %s', binance_sym, result)
                            return result
        except Exception as e:
            logger.warning('SL emir hatasi %s: %s', binance_sym, str(e))
        return None

    def _open_live_position(self, symbol, oneri, price, timeframe='15m'):
        leverage = PAPER_SETTINGS.get('kaldirac', 5)
        balance = self.get_balance()
        bakiye = balance.get('kullanilabilir', 0) if isinstance(balance, dict) and 'error' not in balance else 0
        max_poz = PAPER_SETTINGS.get('max_pozisyon', 5)
        base_dolar = round(bakiye / max_poz, 2) if max_poz > 0 and bakiye > 0 else PAPER_SETTINGS.get('islem_basi_dolar', 4)
        yon = oneri['yon']
        result = self.open_position(symbol, yon, leverage, base_dolar)
        if 'error' in result:
            logger.warning('[LIVE] Acilamadi: %s - %s', symbol, result.get('error'))
            return

        ts_pct = (PAPER_SETTINGS.get('trailing_stop_yuzde') or 3.0) / 100
        if yon == 'LONG':
            initial_trailing = price * (1 - ts_pct)
        else:
            initial_trailing = price * (1 + ts_pct)

        binance_sym = symbol.replace('/', '')
        self.local_positions[binance_sym] = {
            'trailing_stop': initial_trailing,
            'timeframe': timeframe,
            'entry_time': datetime.now().isoformat(),
            'entry_price': price,
            'direction': yon,
            'leverage': leverage,
            'guven': oneri.get('guven_puani', 0),
            'base_dolar': base_dolar,
            'v2': True,
            'sl_order_id': None,
            'kismi_satis': False,
        }
        sl_result = self._place_binance_sl(binance_sym, yon, initial_trailing, leverage)
        if sl_result and 'error' not in sl_result:
            self.local_positions[binance_sym]['sl_order_id'] = sl_result.get('algoId') or sl_result.get('orderId')
            logger.info('Trailing STOP gonderildi: %s %s @ %.6f (%.1f%%)', yon, symbol, initial_trailing, ts_pct * 100)
        else:
            logger.warning('Trailing STOP gonderilemedi: %s -> %s', symbol, sl_result)
        self.son_islem_zamani[symbol] = datetime.now()

# Paper Trader Ayarlari (runtime'da degistirilebilir, dosyaya kaydedilir)
SETTINGS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'paper_settings.json')

DEFAULT_PAPER_SETTINGS = {
    'islem_basi_dolar': 4,
    'kaldirac': 5,
    'min_fiyat': 0.01,
    'trailing_stop_yuzde': 3.0,
    'kismi_satis_kar_hedefi': 1.0,
    'kismi_satis_yuzde': 25,
    'max_pozisyon': 5,
    'min_guven': 40,
    'min_sinyal_puani': 1,
    'tarama_tfleri': ['1m', '3m', '5m', '15m'],
    'max_islem_suresi_saat': 0,
    'kara_liste': [],
    'soguma_dakika': 60,
    'coin_adedi': 100,
    'gercek_veri_modu': True,
}

def load_paper_settings():
    try:
        if os.path.exists(SETTINGS_FILE):
            with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
                saved = json.load(f)
            merged = dict(DEFAULT_PAPER_SETTINGS)
            merged.update(saved)
            return merged
    except Exception as e:
        logger.warning('Ayarlar yuklenemedi: {}'.format(e))
    return dict(DEFAULT_PAPER_SETTINGS)

def save_paper_settings(settings):
    try:
        with open(SETTINGS_FILE, 'w', encoding='utf-8') as f:
            json.dump(settings, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.warning('Ayarlar kaydedilemedi: {}'.format(e))

PAPER_SETTINGS = load_paper_settings()

data_fetcher = LiveDataFetcher()
signal_generator = SignalGenerator()
risk_manager = RiskManager()
paper_trader = PaperTrader(initial_balance=100)
live_trader = BinanceLiveTrader()

# ============================================================
# FLASK API ENDPOINTS
# ============================================================
# TARAMA THROTTLE
# ============================================================

import threading
_last_scan_time = 0
_scan_lock = threading.Lock()
SCAN_MIN_INTERVAL = 90

@app.route('/')
def index():
    return send_file(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'index.html'))

@app.route('/api/health')
def health():
    return jsonify({
        'durum': 'saglikli',
        'zaman': datetime.now().isoformat(),
        'servisler': {
            'veri_kaynagi': 'Binance Futures Testnet canli',
            'teknik_analiz': 'aktif',
            'sinyal_uretici': 'aktif',
            'risk_yoneticisi': 'aktif'
        }
    })

@app.route('/api/market/overview')
def market_overview():
    overview = data_fetcher.get_market_overview()
    btc = data_fetcher.get_ticker('BTC/USDT')
    eth = data_fetcher.get_ticker('ETH/USDT')
    return jsonify({
        'piyasa': overview,
        'btc': btc,
        'eth': eth,
        'zaman': datetime.now().isoformat()
    })

@app.route('/api/market/ticker/<sembol>')
def get_ticker(sembol):
    sym = sembol.replace('_', '/').upper()
    if not sym.endswith('/USDT'):
        sym = sym + '/USDT'
    ticker = data_fetcher.get_ticker(sym)
    if ticker.get('price', 0) == 0:
        return jsonify({'error': 'Canli fiyat alinamadi.', 'reason': ticker.get('error', 'Canli veri kaynagi kesildi.')}, 503)
    return jsonify(ticker)

_last_scan_result = None
_last_scan_lock = threading.Lock()

@app.route('/api/market/all')
def get_all_analysis():
    """Tum coinler - her coin tamamen paralel islenir"""
    global _last_scan_time, _last_scan_result
    tf = request.args.get('timeframe', '15m')
    now = time.time()

    with _scan_lock:
        if now - _last_scan_time < SCAN_MIN_INTERVAL and _last_scan_result is not None:
            return jsonify(_last_scan_result)

    SCAN_TFS = PAPER_SETTINGS.get('tarama_tfleri', ['1m', '3m', '5m', '15m'])
    kara_liste = PAPER_SETTINGS.get('kara_liste', [])

    all_pairs = [p for p in config.TOP_VOLUME_PAIRS if p not in kara_liste]
    if not all_pairs:
        all_pairs = [p for p in config.MAJOR_PAIRS if p not in kara_liste]
        logger.warning('TOP_VOLUME_PAIRS bos; MAJOR_PAIRS kullaniliyor.')

    # Fiyatlari tek seferde cek
    data_fetcher._fetch_live_prices()
    if not data_fetcher._live_prices:
        reason = data_fetcher.last_error or 'Binance fiyat verisi alinamadi.'
        return jsonify({'error': 'Canli veri alinamadi.', 'reason': reason}), 503

    def _process_coin(pair):
        ticker = data_fetcher.get_ticker(pair)
        if ticker.get('price', 0) == 0:
            return None
        tf_signals = {}
        best_tf_signal = None
        best_tf_guven = 0
        best_trend = 'NOTR'
        for stf in SCAN_TFS:
            try:
                df_stf = data_fetcher.get_ohlcv(pair, stf, 50)
                if df_stf.empty:
                    continue
                sig_stf = signal_generator.generate_signals(pair, df_stf, stf)
                tf_signals[stf] = sig_stf
                if sig_stf.get('trend', 'NOTR') != 'NOTR' and sig_stf.get('guven_puani', 0) > best_tf_guven:
                    best_tf_guven = sig_stf.get('guven_puani', 0)
                    best_tf_signal = sig_stf
                    best_tf_signal['_tf'] = stf
                if best_trend == 'NOTR' and sig_stf.get('trend', 'NOTR') != 'NOTR':
                    best_trend = sig_stf.get('trend', 'NOTR')
            except:
                pass
        return {
            'sembol': pair,
            'fiyat': ticker.get('price', 0),
            'degisim_24h': ticker.get('change_24h', 0),
            'hacim_24h': ticker.get('volume_24h', 0),
            'sinyal_primary': tf_signals.get(tf, {}),
            'sinyal_1h': tf_signals.get('15m', {}),
            'trend': best_trend,
            'trend_1h': tf_signals.get('15m', {}).get('trend', 'NOTR'),
            'guven_puani': best_tf_guven,
            'tf_signals': tf_signals,
            'best_tf_signal': best_tf_signal
        }

    results = []
    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = {executor.submit(_process_coin, pair): pair for pair in all_pairs}
        for future in as_completed(futures):
            try:
                r = future.result()
                if r:
                    results.append(r)
            except:
                pass

    if not results:
        reason = data_fetcher.last_error or 'Canli OHLCV verisi alinamadi.'
        return jsonify({'error': 'Canli veri alinamadi.', 'reason': reason}), 503

    results.sort(key=lambda x: x.get('guven_puani', 0), reverse=True)
    
    try:
        paper_trader.process(results)
    except Exception as e:
        logger.error('PaperTrader process hatasi: %s', str(e))
    try:
        live_trader.process(results)
    except Exception as e:
        logger.error('LiveTrader process hatasi: %s', str(e))

    response_data = {
        'sonuclar': results,
        'toplam': len(results),
        'zaman': datetime.now().isoformat(),
        'zaman_dilimi': tf
    }
    with _scan_lock:
        _last_scan_time = time.time()
        _last_scan_result = response_data

    return jsonify(response_data)

@app.route('/api/market/news')
def get_news():
    return jsonify({'haberler': data_fetcher.get_news(), 'zaman': datetime.now().isoformat()})

@app.route('/api/market/funding/<sembol>')
def get_funding(sembol):
    sym = sembol.replace('_', '/').upper()
    if not sym.endswith('/USDT'):
        sym = sym + '/USDT'
    return jsonify(data_fetcher.get_funding_rate(sym))

@app.route('/api/market/oi/<sembol>')
def get_open_interest(sembol):
    sym = sembol.replace('_', '/').upper()
    if not sym.endswith('/USDT'):
        sym = sym + '/USDT'
    return jsonify(data_fetcher.get_open_interest(sym))

@app.route('/api/risk/calculate', methods=['POST'])
def calculate_risk():
    data = request.json
    balance = data.get('balance', 10000)
    risk_percent = data.get('risk_percent', 1.0)
    entry = data.get('entry', 0)
    stop = data.get('stop', 0)
    tp1 = data.get('tp1', 0)
    tp2 = data.get('tp2', 0)
    leverage = data.get('leverage', 1)
    yon = data.get('yon', 'LONG')

    pos = risk_manager.calculate_position_size(balance, risk_percent, entry, stop, leverage, yon)
    rr = risk_manager.calculate_rr_ratio(entry, stop, tp1, tp2)

    return jsonify({
        'pozisyon_buyuklugu': pos,
        'risk_odul': rr,
        'zaman': datetime.now().isoformat()
    })

@app.route('/api/risk/leverage')
def get_leverage_suggestion():
    sembol = request.args.get('sembol', 'BTC/USDT')
    sym = sembol.replace('_', '/').upper()
    if not sym.endswith('/USDT'):
        sym = sym + '/USDT'

    df = data_fetcher.get_ohlcv(sym, '15m', 100)
    df = TechnicalAnalyzer.analyze_dataframe(df)
    if not df.empty:
        atr = float(df['atr'].iloc[-1]) if 'atr' in df.columns and not pd.isna(df['atr'].iloc[-1]) else 0
        price = float(df['close'].iloc[-1])
    else:
        atr = 0; price = 0

    return jsonify(risk_manager.calculate_leverage_suggestion(atr, price))

@app.route('/api/paper/state')
def get_paper_state():
    return jsonify(paper_trader.get_state())

@app.route('/api/paper/history')
def get_paper_history():
    return jsonify({
        'gecmis': paper_trader.trade_history[-50:],
        'toplam': len(paper_trader.trade_history)
    })

@app.route('/api/paper/reset', methods=['POST'])
def reset_paper():
    return jsonify(paper_trader.reset())

@app.route('/api/paper/settings', methods=['GET'])
def get_paper_settings():
    return jsonify(PAPER_SETTINGS)

@app.route('/api/paper/settings', methods=['POST'])
def set_paper_settings():
    global PAPER_SETTINGS
    data = request.json
    for key in PAPER_SETTINGS:
        if key in data:
            PAPER_SETTINGS[key] = data[key]
    save_paper_settings(PAPER_SETTINGS)
    return jsonify({'durum': 'kaydedildi', 'ayarlar': PAPER_SETTINGS})

@app.route('/api/paper/control', methods=['POST'])
def paper_control():
    data = request.json
    cmd = data.get('komut', '')
    kapatilan = None
    kapatilamayan = None
    if cmd == 'baslat':
        paper_trader.durum = 'baslat'
    elif cmd == 'durdur':
        paper_trader.durum = 'durdu'
        kapatilan = []
        kapatilamayan = []
        for sym, pos in list(paper_trader.positions.items()):
            live_price = data_fetcher._get_latest_price(sym)
            if live_price > 0:
                paper_trader._close_position(sym, live_price, 'MANUEL_DURDUR')
                kapatilan.append(sym)
            else:
                kapatilamayan.append(sym)
    elif cmd == 'bekle':
        paper_trader.durum = 'bekle'
    elif cmd == 'hepsini_kapat':
        paper_trader.durum = 'durdu'
        kapatilan = []
        kapatilamayan = []
        for sym, pos in list(paper_trader.positions.items()):
            live_price = data_fetcher._get_latest_price(sym)
            if live_price > 0:
                paper_trader._close_position(sym, live_price, 'HEPSINI_KAPAT')
                kapatilan.append(sym)
            else:
                kapatilamayan.append(sym)
    result = {'durum': paper_trader.durum}
    if kapatilan is not None:
        result['kapatilan'] = kapatilan
        result['kapatilamayan'] = kapatilamayan
    return jsonify(result)

@app.route('/api/paper/close/<sembol>', methods=['POST'])
def close_position(sembol):
    sym = sembol.replace('_', '/').upper()
    if not sym.endswith('/USDT'):
        sym = sym + '/USDT'
    if sym in paper_trader.positions:
        pos = paper_trader.positions[sym]
        live_price = data_fetcher._get_latest_price(sym)
        if live_price <= 0:
            return jsonify({'hata': 'Canli fiyat alinamadi, pozisyon kapatilmadi'}), 503
        paper_trader._close_position(sym, live_price, 'MANUEL_KAPAT')
        return jsonify({'durum': 'kapandı', 'sembol': sym, 'fiyat': live_price})
    return jsonify({'durum': 'pozisyon bulunamadi'}), 404

# ============================================================
# CANLI ISLEM ENDPOINTLERI
# ============================================================

@app.route('/api/live/state')
def live_state():
    return jsonify(live_trader.get_state())

@app.route('/api/live/config', methods=['GET', 'POST'])
def live_config():
    if request.method == 'GET':
        return jsonify({
            'active_mode': live_trader.config.get('active_mode', 'test'),
            'testnet': {
                'api_key_set': bool(live_trader.config.get('testnet', {}).get('api_key')),
            },
            'live': {
                'api_key_set': bool(live_trader.config.get('live', {}).get('api_key')),
            }
        })
    data = request.json
    mode = data.get('mode', 'test')
    api_key = data.get('api_key', '')
    api_secret = data.get('api_secret', '')
    if not api_key or not api_secret:
        return jsonify({'error': 'API key ve secret gerekli'}), 400
    if mode == 'testnet':
        live_trader.save_testnet_config(api_key, api_secret)
    else:
        live_trader.save_live_config(api_key, api_secret)
    return jsonify({'mesaj': f'{mode} API bilgileri kaydedildi'})

@app.route('/api/live/mode', methods=['POST'])
def live_mode():
    data = request.json
    mode = data.get('mode', 'test')
    ok = live_trader.set_active_mode(mode)
    if not ok:
        return jsonify({'error': 'Gecersiz mod: test veya live olmali'}), 400
    live_trader.durum = 'durdu'
    live_trader.total_trades = 0
    live_trader.winning_trades = 0
    live_trader.losing_trades = 0
    live_trader.trade_history = []
    live_trader.local_positions = {}
    live_trader.son_islem_zamani = {}
    live_trader.start_time = datetime.now().isoformat()
    live_trader._load_trader_state()
    return jsonify({'mesaj': f'{mode} moduna gecildi', 'active_mode': mode, 'durum': live_trader.durum})

@app.route('/api/live/balance')
def live_balance():
    return jsonify(live_trader.get_balance())

@app.route('/api/live/positions')
def live_positions():
    return jsonify(live_trader.get_positions())

@app.route('/api/live/start', methods=['POST'])
def live_start():
    active = live_trader._get_active_config()
    if not active.get('api_key'):
        return jsonify({'hata': f'Once {live_trader.config.get("active_mode","test")} icin API key tanimlayin'})
    live_trader.durum = 'aktif'
    live_trader._save_trader_state()
    return jsonify({'mesaj': f'{live_trader.config["active_mode"]} modunda canli islem baslatildi'})

@app.route('/api/live/wait', methods=['POST'])
def live_wait():
    live_trader.durum = 'bekle'
    live_trader._save_trader_state()
    return jsonify({'mesaj': 'Bekleme moduna gecildi - yeni islem acilmayacak, mevcut islemler devam edecek'})

@app.route('/api/live/stop', methods=['POST'])
def live_stop():
    live_trader.durum = 'durdu'
    live_trader._save_trader_state()
    return jsonify({'mesaj': 'Canli islem durduruldu'})

@app.route('/api/live/close-all', methods=['POST'])
def live_close_all():
    positions = live_trader.get_positions()
    if not isinstance(positions, list):
        return jsonify({'hata': 'Pozisyonlar alinamadi'})
    closed = []
    for pos in positions:
        result = live_trader.close_position(pos['display_symbol'], pos['yon'], reason='HEPSINI_KAPAT')
        closed.append({'symbol': pos['symbol'], 'sonuc': result})
    return jsonify({'mesaj': f'{len(closed)} pozisyon kapatildi', 'detay': closed})

@app.route('/api/live/clear-history', methods=['POST'])
def live_clear_history():
    live_trader.trade_history = []
    live_trader.total_trades = 0
    live_trader.winning_trades = 0
    live_trader.losing_trades = 0
    live_trader._save_trader_state()
    return jsonify({'mesaj': 'Islem gecmisi temizlendi'})

@app.route('/api/live/open', methods=['POST'])
def live_open():
    data = request.json
    symbol = data.get('symbol', '')
    yon = data.get('yon', 'LONG')
    leverage = data.get('leverage', 5)
    base_dolar = data.get('base_dolar', 4)
    result = live_trader.open_position(symbol, yon, leverage, base_dolar)
    return jsonify(result)

@app.route('/api/live/close', methods=['POST'])
def live_close():
    data = request.json
    symbol = data.get('symbol', '')
    yon = data.get('yon', 'LONG')
    reason = data.get('reason', 'MANUEL')
    result = live_trader.close_position(symbol, yon, reason=reason)
    return jsonify(result)

@app.route('/api/live/stoploss', methods=['POST'])
def live_stoploss():
    data = request.json
    symbol = data.get('symbol', '')
    yon = data.get('yon', 'LONG')
    stop_price = data.get('stop_price', 0)
    result = live_trader.set_stop_loss(symbol, yon, stop_price)
    return jsonify(result)

@app.route('/api/mode', methods=['GET', 'POST'])
def trading_mode():
    if request.method == 'GET':
        return jsonify({
            'trading_mode': live_trader.config.get('active_mode', 'test'),
            'testnet': live_trader.config.get('active_mode', 'test') == 'test'
        })
    data = request.json
    mode = data.get('mode', 'test')
    live_trader.set_active_mode(mode)
    live_trader.durum = 'durdu'
    live_trader.total_trades = 0
    live_trader.winning_trades = 0
    live_trader.losing_trades = 0
    live_trader.trade_history = []
    live_trader.local_positions = {}
    live_trader.son_islem_zamani = {}
    live_trader.start_time = datetime.now().isoformat()
    live_trader._load_trader_state()
    return jsonify({'durum': 'degistirildi', 'trading_mode': mode, 'live_durum': live_trader.durum})

@app.route('/api/live/summary')
def live_summary():
    return jsonify(live_trader.get_summary())

@app.route('/api/live/reset-stats', methods=['POST'])
def live_reset_stats():
    return jsonify(live_trader.reset_stats())

# ============================================================
# ANA BASLANGIC
# ============================================================

if __name__ == '__main__':
    logger.info('=' * 60)
    logger.info('KRIPTO PARA FUTURES PIYASA ANALIZ SISTEMI')
    logger.info('Python {}'.format(os.sys.version))
    logger.info('Coin sayisi: {}'.format(len(config.MAJOR_PAIRS)))
    market_url = _get_market_data_url()
    logger.info('Veri kaynagi: {}'.format(market_url))
    logger.info('=' * 60)
    logger.info('Sunucu baslatiliyor: http://localhost:5000')
    app.run(host='0.0.0.0', port=5000, debug=False)
