import QuantLib as ql
import yfinance as yf
from curl_cffi import requests as cffi_requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import math
from datetime import timedelta

# Setup Plotting Style
plt.style.use('ggplot')

class TimeSeriesQuantLibAnalyzer:
    def __init__(self, ticker):
        self.ticker = ticker
        self.risk_free_rate = 0.045
        self.day_count = ql.Actual365Fixed()
        self.calendar = ql.UnitedStates(ql.UnitedStates.NYSE)

    def fetch_history(self):
        """Fetch 1 year of daily history."""
        try:
            self.stock = yf.Ticker(self.ticker)
            self.hist = self.stock.history(period="1y")
            if self.hist.empty:
                return False
            return True
        except Exception as e:
            print(f"Error: {e}")
            return False

    def calculate_metrics(self):
        """Calculate HV and Theoretical Probabilities for each day."""
        # 1. Log Returns & HV
        self.hist['LogReturn'] = np.log(self.hist['Close'] / self.hist['Close'].shift(1))
        # 20-day HV
        self.hist['HV_20d'] = self.hist['LogReturn'].rolling(window=20).std() * np.sqrt(252)

        # 2. Theoretical Probability Width (Proxy for IV/Risk)
        # We calculate the theoretical price of a "1-month Straddle" (ATM Call + ATM Put)
        # using HV as the volatility input. This represents the "Expected Move" cost.

        expected_moves = []
        downside_probs = []
        upside_probs = []

        for date_idx, row in self.hist.iterrows():
            price = row['Close']
            vol = row['HV_20d']

            if pd.isna(vol) or vol == 0:
                expected_moves.append(np.nan)
                downside_probs.append(np.nan)
                upside_probs.append(np.nan)
                continue

            # QuantLib Setup for that specific day
            ql_date = ql.Date(date_idx.day, date_idx.month, date_idx.year)
            ql.Settings.instance().evaluationDate = ql_date

            # Expiry 30 days out
            expiry_date = ql_date + 30

            spot_handle = ql.QuoteHandle(ql.SimpleQuote(price))
            rate_handle = ql.YieldTermStructureHandle(ql.FlatForward(ql_date, self.risk_free_rate, self.day_count))
            div_handle = ql.YieldTermStructureHandle(ql.FlatForward(ql_date, 0.0, self.day_count))
            vol_handle = ql.BlackVolTermStructureHandle(ql.BlackConstantVol(ql_date, self.calendar, vol, self.day_count))

            bs_process = ql.BlackScholesMertonProcess(spot_handle, div_handle, rate_handle, vol_handle)
            engine = ql.AnalyticEuropeanEngine(bs_process)
            exercise = ql.EuropeanExercise(expiry_date)

            # 1. Expected Move (ATM Straddle Price)
            # Call
            c_payoff = ql.PlainVanillaPayoff(ql.Option.Call, price)
            c_option = ql.VanillaOption(c_payoff, exercise)
            c_option.setPricingEngine(engine)
            # Put
            p_payoff = ql.PlainVanillaPayoff(ql.Option.Put, price)
            p_option = ql.VanillaOption(p_payoff, exercise)
            p_option.setPricingEngine(engine)

            straddle_price = c_option.NPV() + p_option.NPV()
            expected_move_pct = (straddle_price / price) * 100
            expected_moves.append(expected_move_pct)

            # 2. Probability Skew Proxy (Theoretical)
            # Calculate Prob of -10% vs +10% using the HV
            # Down (-10%)
            target_down = price * 0.90
            bin_payoff_down = ql.CashOrNothingPayoff(ql.Option.Put, target_down, 1.0)
            bin_opt_down = ql.VanillaOption(bin_payoff_down, exercise)
            bin_opt_down.setPricingEngine(engine)

            # Up (+10%)
            target_up = price * 1.10
            bin_payoff_up = ql.CashOrNothingPayoff(ql.Option.Call, target_up, 1.0)
            bin_opt_up = ql.VanillaOption(bin_payoff_up, exercise)
            bin_opt_up.setPricingEngine(engine)

            # Discount back to get probability roughly (or just use NPV as risk-neutral prob * discount)
            # We want raw probability N(d2) approx.
            # NPV = Prob * exp(-rT) -> Prob = NPV * exp(rT)
            time_to_expiry = self.day_count.yearFraction(ql_date, expiry_date)
            disc = math.exp(self.risk_free_rate * time_to_expiry)

            downside_probs.append(bin_opt_down.NPV() * disc * 100)
            upside_probs.append(bin_opt_up.NPV() * disc * 100)

        self.hist['Expected_Move_30d_Pct'] = expected_moves
        self.hist['Prob_Down_10pct'] = downside_probs
        self.hist['Prob_Up_10pct'] = upside_probs
        self.hist['Skew_Bias'] = self.hist['Prob_Down_10pct'] - self.hist['Prob_Up_10pct']

    def plot_analysis(self, output_dir='.'):
        """Generate comprehensive plot."""
        fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 18), sharex=True)

        # Plot 1: Price and Volatility Regime
        ax1.plot(self.hist.index, self.hist['Close'], label='Stock Price', color='black', linewidth=1.5)
        ax1.set_title(f"{self.ticker} Price & Volatility Regime (1 Year)")
        ax1.set_ylabel("Price ($)")

        # Color background based on HV Level
        # Low Vol (<40%), Med (40-80%), High (>80%) - customized for CDE
        # CDE is high vol, so let's adjust: Low < 50, High > 90
        for i in range(len(self.hist) - 1):
            hv = self.hist['HV_20d'].iloc[i] * 100
            if pd.isna(hv): continue

            color = 'green' # Low Vol (Base?)
            if hv > 60: color = 'yellow' # Normal
            if hv > 90: color = 'red' # High Vol (Panic/Climax)

            ax1.axvspan(self.hist.index[i], self.hist.index[i+1], color=color, alpha=0.1)

        ax1_twin = ax1.twinx()
        ax1_twin.plot(self.hist.index, self.hist['HV_20d']*100, color='blue', linestyle='--', alpha=0.6, label='HV (20d)')
        ax1_twin.set_ylabel("HV (%)")
        ax1.legend(loc='upper left')
        ax1_twin.legend(loc='upper right')

        # Plot 2: Theoretical Expected Move (Risk Width)
        ax2.plot(self.hist.index, self.hist['Expected_Move_30d_Pct'], color='purple', label='30d Expected Move (%)')
        ax2.set_title("Market Risk Perception (Theoretical 30d Move)")
        ax2.set_ylabel("Move %")
        ax2.fill_between(self.hist.index, 0, self.hist['Expected_Move_30d_Pct'], color='purple', alpha=0.2)
        ax2.legend()

        # Plot 3: Probability Skew (Downside vs Upside Risk)
        ax3.plot(self.hist.index, self.hist['Prob_Down_10pct'], label='Prob Drop >10%', color='red')
        ax3.plot(self.hist.index, self.hist['Prob_Up_10pct'], label='Prob Rise >10%', color='green')
        ax3.set_title("Probability Skew (Tail Risk)")
        ax3.set_ylabel("Probability (%)")

        # Plot 'Bias' bar
        # ax3_twin = ax3.twinx()
        # ax3_twin.bar(self.hist.index, self.hist['Skew_Bias'], color='gray', alpha=0.3, label='Downside Bias')
        # ax3_twin.set_ylabel("Bias (Pts)")

        ax3.legend()

        plt.tight_layout()
        import os
        filename = os.path.join(output_dir, f"{self.ticker.lower()}_timeseries_analysis.png")
        plt.savefig(filename)
        # print(f"Chart saved to {filename}")

        # 3ヶ月拡大版の保存
        try:
            # 軸の範囲を直近3ヶ月（約90日）に設定
            end_date = self.hist.index[-1]
            start_date = end_date - timedelta(days=90)

            # 各サブプロットのX軸範囲を更新
            ax1.set_xlim(left=start_date, right=end_date)
            # ax2, ax3はsharex=Trueなので自動的に連動するはずだが、念のため

            filename_3m = os.path.join(output_dir, f"{self.ticker.lower()}_timeseries_analysis_3m.png")
            plt.savefig(filename_3m)
        except Exception as e:
            print(f"Error saving 3m chart: {e}")

        return filename

    def generate_report(self):
        current = self.hist.iloc[-1]
        hv_now = current['HV_20d'] * 100
        hv_max = self.hist['HV_20d'].max() * 100
        hv_min = self.hist['HV_20d'].min() * 100

        bias = current['Skew_Bias']

        # Cycle Analysis
        if hv_now < hv_min + (hv_max - hv_min) * 0.2:
            cycle = "contraction" # Lowercase for consistency
        elif hv_now > hv_max * 0.8:
            cycle = "expansion"
        else:
            cycle = "transition"

        return {
            'hv_current': hv_now,
            'skew_bias': bias,
            'cycle_phase': cycle,
            'expected_move_30d': current['Expected_Move_30d_Pct']
        }
