
import pandas as pd
import numpy as np
import yfinance as yf
import sys
import os
from datetime import datetime, timedelta

# Append backend to path to import modules
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from backend.market_analysis_logic import get_market_analysis_data
from backend.calculate_atr_trailing_stop import calculate_strategies as calc_atr_ts, resample_to_weekly as resample_atr
from backend.calculate_rti import calculate_rti as calc_rti
from backend.calculate_zone_rs import calculate_zone_rs as calc_zone, resample_to_weekly as resample_zone
from backend.calculate_rs_percentile_histogram import calculate_rs_percentile as calc_rs_perc

# Map US100 to ^NDX
# Added SLV and ^SOX
TICKERS = ["GLD", "QQQ", "^NDX", "SLV", "^SOX"]
BENCHMARK = "^GSPC"

def fetch_data(tickers, period="2y"):
    """Fetch daily data for tickers and benchmark."""
    print(f"Fetching data for {tickers} + {BENCHMARK}...")

    all_tickers = tickers + [BENCHMARK]
    # Use threads=False to avoid some yfinance issues in containers if any
    data = yf.download(all_tickers, period=period, progress=False, group_by='ticker', auto_adjust=True)
    return data

def analyze_market_analysis(ticker):
    """Get Market Analysis (BB, RSI, StochRSI) for a ticker."""
    print(f"Analyzing Market Analysis for {ticker}...")
    results, df = get_market_analysis_data(ticker, period="1y")

    if df.empty:
        return None

    latest = df.iloc[-1]

    return {
        "Close": latest['Close'],
        "RSI": latest['RSI'],
        "StochRSI_K": latest['Fast_K'],
        "StochRSI_D": latest['Slow_D'],
        "BB_Upper": latest['BB_Upper'],
        "BB_Lower": latest['BB_Lower'],
        "BB_RSI_Buy": latest['BB_RSI_Buy'],
        "BB_RSI_Sell": latest['BB_RSI_Sell'],
        "Bullish_Phase": latest['Bullish_Phase'],
        "Bearish_Phase": latest['Bearish_Phase']
    }

def analyze_strong_stocks_metrics(tickers_data, benchmark_data):
    results = {}

    for ticker in TICKERS:
        print(f"Analyzing Strong Stocks metrics for {ticker}...")
        try:
            if isinstance(tickers_data.columns, pd.MultiIndex):
                # Check if ticker exists in columns
                if ticker not in tickers_data.columns.levels[0]:
                    print(f"  Warning: {ticker} not found in downloaded data.")
                    results[ticker] = {"Error": "Data not found"}
                    continue
                df = tickers_data[ticker].copy()
            else:
                df = tickers_data.copy()

            # Remove rows with all NaNs (often happens if tickers have different holidays)
            df = df.dropna(how='all')

            if df.empty:
                 results[ticker] = {"Error": "Empty data"}
                 continue

            # 1. ATR Trailing Stop (Weekly)
            w_op, w_hi, w_lo, w_cl = resample_atr(df)
            w_cl_df = w_cl.to_frame(ticker)
            w_hi_df = w_hi.to_frame(ticker)
            w_lo_df = w_lo.to_frame(ticker)

            t1, t2, states, sigs = calc_atr_ts(w_cl_df, w_hi_df, w_lo_df, 5, 0.5, 10, 3.0)
            latest_atr_state = states.iloc[-1].item()

            # 2. RTI (Weekly)
            rti_vals, rti_sigs = calc_rti(w_hi_df, w_lo_df, 5)
            latest_rti = rti_vals.iloc[-1].item()
            latest_rti_sig = rti_sigs.iloc[-1].item()

            # 3. Zone RS (Weekly)
            bench_df = benchmark_data.copy()
            w_bench_cl = resample_zone(bench_df)
            if isinstance(w_bench_cl, pd.DataFrame):
                 w_bench_cl = w_bench_cl['Close'] if 'Close' in w_bench_cl.columns else w_bench_cl.iloc[:, 0]

            # Calculate
            rs_ratio, rs_mom, zones = calc_zone(w_cl_df, w_bench_cl, 50, 20)

            latest_zone = zones.iloc[-1].item()
            latest_ratio = rs_ratio.iloc[-1].item()
            latest_mom = rs_mom.iloc[-1].item()

            # 4. RS Percentile (Weekly)
            perc_1m, _ = calc_rs_perc(w_cl_df, w_bench_cl, mode="1M", lookback_1m=26)
            latest_perc = perc_1m.iloc[-1].item()

            results[ticker] = {
                "ATR_State": latest_atr_state,
                "RTI": latest_rti,
                "RTI_Signal": latest_rti_sig,
                "Zone": latest_zone,
                "RS_Ratio": latest_ratio,
                "RS_Momentum": latest_mom,
                "RS_Percentile": latest_perc
            }

        except Exception as e:
            print(f"Error calculating Strong Stocks metrics for {ticker}: {e}")
            results[ticker] = {"Error": str(e)}

    return results

def main():
    ma_results = {}
    for t in TICKERS:
        ma_data = analyze_market_analysis(t)
        if ma_data:
            ma_results[t] = ma_data

    raw_data = fetch_data(TICKERS, period="2y")

    if BENCHMARK in raw_data.columns.levels[0]:
        bench_data = raw_data[BENCHMARK]
    else:
        print("Benchmark not found in grouped data, fetching separately...")
        bench_data = yf.download(BENCHMARK, period="2y", progress=False, auto_adjust=True)

    ss_results = analyze_strong_stocks_metrics(raw_data, bench_data)

    print("\n" + "="*50)
    print("CONSOLIDATED ANALYSIS REPORT")
    print("="*50)

    for t in TICKERS:
        print(f"\n--- {t} ---")

        if t in ma_results:
            ma = ma_results[t]
            print(f"[Market Analysis (Daily)]")
            print(f"  Price: {ma['Close']:.2f}")
            print(f"  RSI(14): {ma['RSI']:.2f}")
            print(f"  StochRSI: K={ma['StochRSI_K']:.2f}, D={ma['StochRSI_D']:.2f}")
            print(f"  BB Position: {'Above Upper' if ma['Close'] > ma['BB_Upper'] else 'Below Lower' if ma['Close'] < ma['BB_Lower'] else 'Inside'}")

            signal = "NONE"
            if ma['BB_RSI_Buy']: signal = "BUY (Green Triangle)"
            if ma['BB_RSI_Sell']: signal = "SELL (Magenta Triangle)"
            print(f"  Signal: {signal}")

            phase = "Bullish" if ma['Bullish_Phase'] else ("Bearish" if ma['Bearish_Phase'] else "Neutral")
            print(f"  Cycle Phase: {phase}")

        if t in ss_results:
            ss = ss_results[t]
            print(f"[Strong Stocks (Weekly)]")
            if "Error" in ss:
                print(f"  Error: {ss['Error']}")
            else:
                atr_map = {0: "Red (Bear)", 1: "Yellow (Bear Rally)", 2: "Blue (Bull Dip)", 3: "Green (Bull)"}
                zone_map = {0: "Dead", 1: "Lift", 2: "Drift", 3: "Power"}

                print(f"  ATR Trailing Stop: {atr_map.get(ss['ATR_State'], 'Unknown')}")
                print(f"  RTI: {ss['RTI']:.2f} (Sig: {ss['RTI_Signal']})")
                print(f"  Zone RS: {zone_map.get(ss['Zone'], 'Unknown')} (Ratio={ss['RS_Ratio']:.2f}, Mom={ss['RS_Momentum']:.2f})")
                print(f"  RS Percentile (1M): {ss['RS_Percentile']:.2f}")

if __name__ == "__main__":
    main()
