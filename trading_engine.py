import os
import csv
import asyncio
import pandas as pd
import numpy as np
import logging
import time
import json
from datetime import datetime
from binance.client import Client
from binance.websockets import BinanceSocketManager
from twisted.internet import reactor

# Delete the signals.csv file if it exists
if os.path.exists("signals.csv"):
    os.remove("signals.csv")

# ---------------------------
# Configuration and Logging
# ---------------------------
SIMULATION_MODE = True  # Set to False when ready for live order execution
ACCOUNT_BALANCE = 10000  # Example USDT balance
SYMBOLS_TO_MONITOR = []  # Will be populated with USDT pairs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ---------------------------
# Load Binance API Keys
# ---------------------------
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")
client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)

# ---------------------------
# CSV Logging Function
# ---------------------------
def write_signal_to_csv(symbol, bias, signal_type, explanation, entry, stop_loss, take_profit, position_size):
    filename = "signals.csv"
    header = ["Timestamp", "Symbol", "Bias", "Signal Type", "Explanation", "Entry", "Stop Loss", "Take Profit", "Position Size"]
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row = [timestamp, symbol, bias, signal_type, explanation, entry, stop_loss, take_profit, position_size]
    
    file_exists = os.path.isfile(filename)
    with open(filename, "a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        if not file_exists:
            writer.writerow(header)
        writer.writerow(row)

# ---------------------------
# Data Management
# ---------------------------
class DataManager:
    def __init__(self, client):
        self.client = client
        self.live_data = {}  # Will store real-time klines for each symbol
        self.historical_data = {}  # Will store historical data for each symbol
    
    def get_historical_klines(self, symbol, interval, lookback):
        """Fetch historical klines from Binance"""
        klines = self.client.get_historical_klines(symbol, interval, lookback)
        if not klines:
            raise Exception(f"No historical data returned for {symbol}")
        df = pd.DataFrame(klines, columns=[
            "Open Time", "Open", "High", "Low", "Close", "Volume",
            "Close Time", "Quote Asset Volume", "Number of Trades",
            "Taker Buy Base Asset Volume", "Taker Buy Quote Asset Volume", "Ignore"
        ])
        df["Open Time"] = pd.to_datetime(df["Open Time"], unit="ms")
        df["Close Time"] = pd.to_datetime(df["Close Time"], unit="ms")
        for col in ["Open", "High", "Low", "Close", "Volume"]:
            df[col] = pd.to_numeric(df[col])
        return df
    
    def initialize_symbol_data(self, symbol):
        """Initialize both historical and live data structures for a symbol"""
        logging.info(f"Initializing data for {symbol}")
        # Get 4H data for trend analysis
        df_4h = self.get_historical_klines(symbol, Client.KLINE_INTERVAL_4HOUR, "16 day ago UTC")
        # Get 15M data for order block detection
        df_15m = self.get_historical_klines(symbol, Client.KLINE_INTERVAL_15MINUTE, "2 day ago UTC")
        
        self.historical_data[symbol] = {
            "4h": df_4h,
            "15m": df_15m,
            "last_update": datetime.now()
        }
        self.live_data[symbol] = {
            "current_4h": None,  # Will store current unclosed 4h candle
            "current_15m": None,  # Will store current unclosed 15m candle
            "last_price": None,
            "last_update": None
        }
        return len(df_4h) > 0 and len(df_15m) > 0
    
    def update_with_kline(self, symbol, interval, kline_data):
        """Update data with a new kline from WebSocket"""
        if symbol not in self.historical_data:
            return False
        
        # Parse kline data from WebSocket
        open_time = pd.to_datetime(kline_data['t'], unit='ms')
        close_time = pd.to_datetime(kline_data['T'], unit='ms')
        open_price = float(kline_data['o'])
        high_price = float(kline_data['h'])
        low_price = float(kline_data['l'])
        close_price = float(kline_data['c'])
        volume = float(kline_data['v'])
        is_candle_closed = kline_data['x']
        
        # Store current price and update time
        self.live_data[symbol]["last_price"] = close_price
        self.live_data[symbol]["last_update"] = datetime.now()
        
        # If the interval is 4h
        if interval == "4h":
            self.live_data[symbol]["current_4h"] = {
                "Open Time": open_time,
                "Close Time": close_time,
                "Open": open_price,
                "High": high_price,
                "Low": low_price,
                "Close": close_price,
                "Volume": volume
            }
            # If candle closed, append to historical data
            if is_candle_closed:
                new_row = pd.DataFrame([self.live_data[symbol]["current_4h"]])
                self.historical_data[symbol]["4h"] = pd.concat([self.historical_data[symbol]["4h"], new_row]).reset_index(drop=True)
                logging.info(f"Added new 4h candle for {symbol}")
        
        # If the interval is 15m
        elif interval == "15m":
            self.live_data[symbol]["current_15m"] = {
                "Open Time": open_time,
                "Close Time": close_time,
                "Open": open_price,
                "High": high_price,
                "Low": low_price,
                "Close": close_price,
                "Volume": volume
            }
            # If candle closed, append to historical data
            if is_candle_closed:
                new_row = pd.DataFrame([self.live_data[symbol]["current_15m"]])
                self.historical_data[symbol]["15m"] = pd.concat([self.historical_data[symbol]["15m"], new_row]).reset_index(drop=True)
                logging.info(f"Added new 15m candle for {symbol}")
        
        return is_candle_closed
    
    def get_combined_data(self, symbol, interval):
        """Get combined historical and current candle data"""
        if symbol not in self.historical_data:
            return None
        
        df = self.historical_data[symbol][interval].copy()
        
        # Add current unclosed candle if available
        if interval == "4h" and self.live_data[symbol]["current_4h"] is not None:
            current_candle = pd.DataFrame([self.live_data[symbol]["current_4h"]])
            df = pd.concat([df, current_candle]).reset_index(drop=True)
        elif interval == "15m" and self.live_data[symbol]["current_15m"] is not None:
            current_candle = pd.DataFrame([self.live_data[symbol]["current_15m"]])
            df = pd.concat([df, current_candle]).reset_index(drop=True)
        
        return df

# ---------------------------
# Indicator Functions (Same as before)
# ---------------------------
def calculate_atr(df, period=14):
    df['prev_close'] = df['Close'].shift(1)
    df['high_low'] = df['High'] - df['Low']
    df['high_prev_close'] = abs(df['High'] - df['prev_close'])
    df['low_prev_close'] = abs(df['Low'] - df['prev_close'])
    df['TR'] = df[['high_low', 'high_prev_close', 'low_prev_close']].max(axis=1)
    atr = df['TR'].rolling(window=period).mean()
    return atr

def calculate_rsi(df, period=14):
    delta = df['Close'].diff()
    gain = delta.clip(lower=0)
    loss = -1 * delta.clip(upper=0)
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def calculate_macd(df, fast=12, slow=26, signal_period=9):
    df["EMA_fast"] = df["Close"].ewm(span=fast, adjust=False).mean()
    df["EMA_slow"] = df["Close"].ewm(span=slow, adjust=False).mean()
    df["MACD"] = df["EMA_fast"] - df["EMA_slow"]
    df["MACD_Signal"] = df["MACD"].ewm(span=signal_period, adjust=False).mean()
    df["MACD_Hist"] = df["MACD"] - df["MACD_Signal"]
    return df

def calculate_bollinger_bands(df, period=20, num_std=2):
    df["MA20"] = df["Close"].rolling(window=period).mean()
    df["STD20"] = df["Close"].rolling(window=period).std()
    df["UpperBB"] = df["MA20"] + num_std * df["STD20"]
    df["LowerBB"] = df["MA20"] - num_std * df["STD20"]
    return df

# ---------------------------
# Trading Strategy Module (Same as before with minor adjustments)
# ---------------------------
class TradingStrategy:
    def __init__(self, volume_threshold=2.0, wick_threshold=0.7):
        self.volume_threshold = volume_threshold  
        self.wick_threshold = wick_threshold      
    
    def compute_trend_bias(self, df_4h):
        df_4h["SMA50"] = df_4h["Close"].rolling(window=50).mean()
        latest_close = df_4h["Close"].iloc[-1]
        latest_sma = df_4h["SMA50"].iloc[-1]
        if latest_close > latest_sma:
            bias = "bullish"
            explanation = (f"Bullish bias: 4H close {latest_close:.2f} is above SMA50 ({latest_sma:.2f}).")
        else:
            bias = "bearish"
            explanation = (f"Bearish bias: 4H close {latest_close:.2f} is below SMA50 ({latest_sma:.2f}).")
        return bias, explanation

    def detect_order_block(self, df_15m, bias):
        signal = None
        explanation = None

        df_15m['ATR'] = calculate_atr(df_15m)
        df_15m['RSI'] = calculate_rsi(df_15m)
        df_15m = calculate_macd(df_15m)
        df_15m = calculate_bollinger_bands(df_15m)

        baseline_volume = df_15m['Volume'].rolling(window=20).mean().iloc[-1]
        avg_atr = df_15m['ATR'].rolling(window=20).mean().iloc[-1]
        
        # Focus on last 30 candles for real-time analysis
        lookback_range = min(30, len(df_15m) - 1)
        for i in range(len(df_15m) - lookback_range, len(df_15m)):
            row = df_15m.iloc[i]
            body = abs(row["Close"] - row["Open"])
            candle_range = row["High"] - row["Low"]
            if candle_range == 0:
                continue
            wick_ratio = (candle_range - body) / candle_range

            if row["Volume"] < baseline_volume * self.volume_threshold:
                continue
            if row["ATR"] < avg_atr:
                continue

            if bias == "bullish":
                if row["RSI"] > 30:
                    continue
                if row["MACD_Hist"] <= 0:
                    continue
                if (row["Close"] - row["LowerBB"]) / row["Close"] > 0.02:
                    continue
            else:
                if row["RSI"] < 70:
                    continue
                if row["MACD_Hist"] >= 0:
                    continue
                if (row["UpperBB"] - row["Close"]) / row["Close"] > 0.02:
                    continue

            if bias == "bullish" and row["Close"] > row["Open"] and wick_ratio > self.wick_threshold:
                signal = {"type": "buy", "order_block": row}
                explanation = (
                    f"Bullish order block at {row['Open Time']}:\n"
                    f"  Wick ratio: {wick_ratio:.2f} (>{self.wick_threshold}),\n"
                    f"  Volume: {row['Volume']:.2f} vs baseline {baseline_volume:.2f},\n"
                    f"  RSI: {row['RSI']:.2f} (<30),\n"
                    f"  MACD Hist: {row['MACD_Hist']:.2f} (>0),\n"
                    f"  Close: {row['Close']:.2f} near LowerBB: {row['LowerBB']:.2f},\n"
                    f"  ATR: {row['ATR']:.2f} (>= avg {avg_atr:.2f})."
                )
                break
            elif bias == "bearish" and row["Close"] < row["Open"] and wick_ratio > self.wick_threshold:
                signal = {"type": "sell", "order_block": row}
                explanation = (
                    f"Bearish order block at {row['Open Time']}:\n"
                    f"  Wick ratio: {wick_ratio:.2f} (>{self.wick_threshold}),\n"
                    f"  Volume: {row['Volume']:.2f} vs baseline {baseline_volume:.2f},\n"
                    f"  RSI: {row['RSI']:.2f} (>70),\n"
                    f"  MACD Hist: {row['MACD_Hist']:.2f} (<0),\n"
                    f"  Close: {row['Close']:.2f} near UpperBB: {row['UpperBB']:.2f},\n"
                    f"  ATR: {row['ATR']:.2f} (>= avg {avg_atr:.2f})."
                )
                break
        if signal:
            signal["explanation"] = explanation
        return signal

# ---------------------------
# Risk Management Module (Same as before)
# ---------------------------
class RiskManager:
    def __init__(self, risk_percent=1.0):
        self.risk_percent = risk_percent  # 1% risk per trade
    
    def calculate_trade_levels(self, signal):
        if signal is None:
            return None
        ob = signal["order_block"]
        candle_range = ob["High"] - ob["Low"]
        if signal["type"] == "buy":
            entry = ob["High"] + 0.1 * candle_range
            stop_loss = ob["Low"] - 0.1 * candle_range
            risk = entry - stop_loss
            take_profit = entry + 2 * risk
            explanation = (
                f"Bullish levels: Entry = {ob['High']:.2f} + 10% range ({0.1*candle_range:.2f}), "
                f"Stop Loss = {ob['Low']:.2f} - 10% range, Risk = {risk:.2f}, "
                f"Take Profit = 2x risk above entry ({take_profit:.2f})."
            )
        else:
            entry = ob["Low"] - 0.1 * candle_range
            stop_loss = ob["High"] + 0.1 * candle_range
            risk = stop_loss - entry
            take_profit = entry - 2 * risk
            explanation = (
                f"Bearish levels: Entry = {ob['Low']:.2f} - 10% range ({0.1*candle_range:.2f}), "
                f"Stop Loss = {ob['High']:.2f} + 10% range, Risk = {risk:.2f}, "
                f"Take Profit = 2x risk below entry ({take_profit:.2f})."
            )
        return {"entry": entry, "stop_loss": stop_loss, "take_profit": take_profit, "risk": risk, "explanation": explanation}

    def calculate_position_size(self, account_balance, stop_loss_distance, current_price):
        risk_amount = account_balance * (self.risk_percent / 100)
        position_size_usd = risk_amount / stop_loss_distance
        position_units = position_size_usd / current_price
        return position_units

    def calculate_trailing_stop(self, current_price, entry, risk, trailing_percent=0.5):
        if current_price > entry:
            return entry + (current_price - entry) * (1 - trailing_percent/100)
        else:
            return entry - (entry - current_price) * (1 - trailing_percent/100)

# ---------------------------
# WebSocket Manager
# ---------------------------
class RealTimeTrader:
    def __init__(self, client):
        self.client = client
        self.bsm = BinanceSocketManager(client)
        self.data_manager = DataManager(client)
        self.strategy = TradingStrategy(volume_threshold=2.0, wick_threshold=0.7)
        self.risk_manager = RiskManager(risk_percent=1.0)
        self.active_symbols = set()
        self.symbol_connections = {}
    
    def initialize_symbols(self):
        """Initialize the list of symbols to monitor and their historical data"""
        tickers = self.client.get_all_tickers()
        symbols = [t["symbol"] for t in tickers if t["symbol"].endswith("USDT")]
        logging.info(f"Found {len(symbols)} USDT pairs.")
        
        # Initialize 100 symbols to begin with for performance reasons
        initial_symbols = symbols[:100]
        global SYMBOLS_TO_MONITOR
        SYMBOLS_TO_MONITOR = initial_symbols
        
        for symbol in initial_symbols:
            try:
                if self.data_manager.initialize_symbol_data(symbol):
                    self.active_symbols.add(symbol)
            except Exception as e:
                logging.error(f"Error initializing {symbol}: {e}")
        
        logging.info(f"Successfully initialized {len(self.active_symbols)} symbols.")
    
    def process_kline_message(self, msg):
        """Process incoming kline message from WebSocket"""
        if msg['e'] == 'error':
            logging.error(f"WebSocket error: {msg}")
            return
        
        if msg['e'] != 'kline':
            return
        
        symbol = msg['s']
        interval = msg['k']['i']
        
        # Map Binance intervals to our internal format
        interval_map = {
            '4h': '4h',
            '15m': '15m'
        }
        
        if interval in ['4h', '15m']:
            mapped_interval = interval_map[interval]
            candle_closed = self.data_manager.update_with_kline(symbol, mapped_interval, msg['k'])
            
            # If a 15m candle closed, analyze for potential signals
            if interval == '15m' and candle_closed:
                self.analyze_symbol(symbol)
    
    def analyze_symbol(self, symbol):
        """Analyze a symbol for potential trading signals using real-time data"""
        try:
            df_4h = self.data_manager.get_combined_data(symbol, "4h")
            df_15m = self.data_manager.get_combined_data(symbol, "15m")
            
            if len(df_4h) < 50 or len(df_15m) < 20:
                return
            
            # Compute trend bias using the latest 4h data
            bias, bias_explanation = self.strategy.compute_trend_bias(df_4h)
            
            # Detect order blocks using the latest 15m data
            signal = self.strategy.detect_order_block(df_15m, bias)
            
            if signal:
                trade_levels = self.risk_manager.calculate_trade_levels(signal)
                current_price = self.data_manager.live_data[symbol]["last_price"]
                position_size = self.risk_manager.calculate_position_size(ACCOUNT_BALANCE, trade_levels["risk"], current_price)
                
                # In real-time trading, we need to check if the entry point is close to current price
                price_distance = abs(trade_levels["entry"] - current_price) / current_price * 100
                
                if price_distance < 2.0:  # Only process signals within 2% of current price
                    message = (
                        f"REAL-TIME Trade Opportunity on {symbol} ({bias.upper()}):\n"
                        f"  Signal: {signal['type'].upper()}\n"
                        f"  {trade_levels['explanation']}\n"
                        f"  Current Price: {current_price:.2f}\n"
                        f"  Calculated Position Size: {position_size:.4f} units\n"
                        f"  Signal Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                    logging.info(message)
                    send_notification(message)
                    
                    # Write the signal to CSV for dashboard display
                    write_signal_to_csv(symbol, bias, signal['type'].upper(), trade_levels['explanation'],
                                        trade_levels['entry'], trade_levels['stop_loss'], trade_levels['take_profit'],
                                        position_size)
                    
                    if not SIMULATION_MODE:
                        # Order execution logic would go here
                        pass
        except Exception as e:
            logging.error(f"Error analyzing {symbol}: {e}")
    
    def start_websocket_streams(self):
        """Start WebSocket streams for active symbols"""
        for symbol in self.active_symbols:
            # Subscribe to 15-minute and 4-hour klines
            conn_key_15m = self.bsm.start_kline_socket(symbol, self.process_kline_message, interval='15m')
            conn_key_4h = self.bsm.start_kline_socket(symbol, self.process_kline_message, interval='4h')
            self.symbol_connections[symbol] = {'15m': conn_key_15m, '4h': conn_key_4h}
        
        # Start the WebSocket manager
        self.bsm.start()
        logging.info(f"Started WebSocket streams for {len(self.active_symbols)} symbols.")
    
    def stop_websocket_streams(self):
        """Stop all WebSocket streams"""
        self.bsm.stop_socket(list(self.symbol_connections.values()))
        self.bsm.close()
        logging.info("Stopped all WebSocket streams.")

# ---------------------------
# Notification Function
# ---------------------------
def send_notification(message):
    logging.info("NOTIFICATION: " + message)
    # Expand this function to send emails, SMS, or Telegram alerts

# ---------------------------
# Main Execution
# ---------------------------
def main():
    try:
        # Initialize the real-time trading system
        trader = RealTimeTrader(client)
        trader.initialize_symbols()
        
        # Start WebSocket streams
        trader.start_websocket_streams()
        
        # Keep the program running
        logging.info("Real-time trading system is running. Press CTRL+C to stop.")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Interrupted by user. Shutting down.")
        trader.stop_websocket_streams()
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        if 'trader' in locals():
            trader.stop_websocket_streams()

if __name__ == "__main__":
    main()