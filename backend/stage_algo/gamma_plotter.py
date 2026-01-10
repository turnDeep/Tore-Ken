import QuantLib as ql
import yfinance as yf
from curl_cffi import requests as cffi_requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import math
from datetime import datetime, timedelta
import argparse

# Setup Plotting Style
plt.style.use('ggplot')

class GammaPlotter:
    def __init__(self, ticker):
        self.ticker = ticker
        self.risk_free_rate = 0.045
        self.day_count = ql.Actual365Fixed()
        self.calendar = ql.UnitedStates(ql.UnitedStates.NYSE)

        # Placeholders for Gamma Levels
        self.gamma_flip = None
        self.gamma_magnet = None
        self.gamma_accel = None

    def fetch_data(self):
        """Fetch stock data and option chain."""
        try:
            self.stock = yf.Ticker(self.ticker)
            self.hist = self.stock.history(period="1y")
            if self.hist.empty:
                return False

            self.current_price = self.hist['Close'].iloc[-1]
            self.current_date = self.hist.index[-1].date()
            self.evaluation_date = ql.Date(self.current_date.day, self.current_date.month, self.current_date.year)
            ql.Settings.instance().evaluationDate = self.evaluation_date

            # Dividend Yield
            self.dividend_yield = 0.0
            try:
                info = self.stock.info
                if 'dividendYield' in info and info['dividendYield'] is not None:
                    self.dividend_yield = info['dividendYield']
            except:
                pass

            return True
        except Exception as e:
            print(f"Error fetching data: {e}")
            return False

    def calculate_current_gamma_levels(self):
        """Calculate the Key Gamma Levels for the current date."""
        options_dates = self.stock.options
        if not options_dates:
            print("No options data.")
            return

        target_expiry_str = options_dates[1] if len(options_dates) > 1 else options_dates[0]
        self.expiry_date = datetime.strptime(target_expiry_str, "%Y-%m-%d").date()

        chain = self.stock.option_chain(target_expiry_str)
        calls = chain.calls
        puts = chain.puts

        if calls.empty or puts.empty: return

        # IV Calc
        strikes = calls['strike'].values
        atm_strike = min(strikes, key=lambda x: abs(x - self.current_price))
        atm_call = calls[calls['strike'] == atm_strike].iloc[0]
        iv_current = atm_call['impliedVolatility']
        if iv_current < 0.01: iv_current = 0.5 # Fallback

        # GEX Calc
        ql_expiry = ql.Date(self.expiry_date.day, self.expiry_date.month, self.expiry_date.year)
        spot_handle = ql.QuoteHandle(ql.SimpleQuote(self.current_price))
        rate_handle = ql.YieldTermStructureHandle(ql.FlatForward(self.evaluation_date, self.risk_free_rate, self.day_count))
        div_handle = ql.YieldTermStructureHandle(ql.FlatForward(self.evaluation_date, self.dividend_yield, self.day_count))
        vol_handle = ql.BlackVolTermStructureHandle(ql.BlackConstantVol(self.evaluation_date, self.calendar, iv_current, self.day_count))

        bs_process = ql.BlackScholesMertonProcess(spot_handle, div_handle, rate_handle, vol_handle)
        engine = ql.AnalyticEuropeanEngine(bs_process)
        exercise = ql.EuropeanExercise(ql_expiry)

        gex_by_strike = {}
        all_strikes = sorted(list(set(calls['strike']).union(set(puts['strike']))))

        for k in all_strikes:
            call_oi = calls[calls['strike'] == k]['openInterest'].sum()
            put_oi = puts[puts['strike'] == k]['openInterest'].sum()

            payoff = ql.PlainVanillaPayoff(ql.Option.Call, k)
            option = ql.VanillaOption(payoff, exercise)
            option.setPricingEngine(engine)
            try:
                gamma = option.gamma()
            except:
                gamma = 0

            net_gex = (put_oi * gamma) - (call_oi * gamma)
            gex_by_strike[k] = net_gex

        strikes = sorted(gex_by_strike.keys())
        gex_vals = [gex_by_strike[k] for k in strikes]

        if not gex_vals: return

        self.gamma_magnet = strikes[np.argmax(gex_vals)]
        self.gamma_accel = strikes[np.argmin(gex_vals)]

        # Zero Gamma Flip
        flip_price = None
        current_idx = np.searchsorted(strikes, self.current_price)

        # Scan for flip
        for i in range(len(strikes)-1):
             if (gex_vals[i] > 0 and gex_vals[i+1] < 0) or (gex_vals[i] < 0 and gex_vals[i+1] > 0):
                 if abs(strikes[i] - self.current_price) < self.current_price * 0.2: # Only near money
                     flip_price = (strikes[i] + strikes[i+1]) / 2
                     break

        self.gamma_flip = flip_price
        # print(f"Gamma Levels: Flip={self.gamma_flip}, Magnet={self.gamma_magnet}, Accel={self.gamma_accel}")

    def calculate_historical_metrics(self):
        """Calculate historical HV and Probability Bands (Percentage)."""
        self.hist['LogReturn'] = np.log(self.hist['Close'] / self.hist['Close'].shift(1))
        # CORRECTED: Keep HV as decimal (0.30), NOT percentage (30.0)
        self.hist['HV_20d'] = self.hist['LogReturn'].rolling(window=20).std() * np.sqrt(252)

        expected_moves_pct = []
        downside_probs = []
        upside_probs = []

        for date_idx, row in self.hist.iterrows():
            price = row['Close']
            vol = row['HV_20d']

            if pd.isna(vol) or vol == 0:
                expected_moves_pct.append(np.nan)
                downside_probs.append(np.nan)
                upside_probs.append(np.nan)
                continue

            ql_date = ql.Date(date_idx.day, date_idx.month, date_idx.year)
            ql.Settings.instance().evaluationDate = ql_date
            expiry_date = ql_date + 30

            spot_handle = ql.QuoteHandle(ql.SimpleQuote(price))
            rate_handle = ql.YieldTermStructureHandle(ql.FlatForward(ql_date, self.risk_free_rate, self.day_count))
            div_handle = ql.YieldTermStructureHandle(ql.FlatForward(ql_date, self.dividend_yield, self.day_count))
            # Pass decimal vol (e.g., 0.30)
            vol_handle = ql.BlackVolTermStructureHandle(ql.BlackConstantVol(ql_date, self.calendar, vol, self.day_count))
            bs_process = ql.BlackScholesMertonProcess(spot_handle, div_handle, rate_handle, vol_handle)
            engine = ql.AnalyticEuropeanEngine(bs_process)
            exercise = ql.EuropeanExercise(expiry_date)

            # Straddle for Expected Move %
            c_payoff = ql.PlainVanillaPayoff(ql.Option.Call, price)
            c_opt = ql.VanillaOption(c_payoff, exercise)
            c_opt.setPricingEngine(engine)
            p_payoff = ql.PlainVanillaPayoff(ql.Option.Put, price)
            p_opt = ql.VanillaOption(p_payoff, exercise)
            p_opt.setPricingEngine(engine)

            straddle_val = c_opt.NPV() + p_opt.NPV()
            expected_moves_pct.append((straddle_val / price) * 100)

            # Skew Probs
            target_down = price * 0.90
            bin_payoff_down = ql.CashOrNothingPayoff(ql.Option.Put, target_down, 1.0)
            bin_opt_down = ql.VanillaOption(bin_payoff_down, exercise)
            bin_opt_down.setPricingEngine(engine)

            target_up = price * 1.10
            bin_payoff_up = ql.CashOrNothingPayoff(ql.Option.Call, target_up, 1.0)
            bin_opt_up = ql.VanillaOption(bin_payoff_up, exercise)
            bin_opt_up.setPricingEngine(engine)

            time_to_expiry = self.day_count.yearFraction(ql_date, expiry_date)
            disc = math.exp(self.risk_free_rate * time_to_expiry)

            downside_probs.append(bin_opt_down.NPV() * disc * 100)
            upside_probs.append(bin_opt_up.NPV() * disc * 100)

        self.hist['Expected_Move_30d_Pct'] = expected_moves_pct
        self.hist['Prob_Down_10pct'] = downside_probs
        self.hist['Prob_Up_10pct'] = upside_probs

    def plot_analysis(self, output_dir='.'):
        plt.close('all')
        fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 18), sharex=True)

        # Plot 1: Price and Gamma Levels
        ax1.plot(self.hist.index, self.hist['Close'], label='Stock Price', color='black', linewidth=1.5)
        ax1.set_title(f"{self.ticker} Gamma Levels & Risk Profile")
        ax1.set_ylabel("Price ($)")

        if self.gamma_flip:
            ax1.axhline(self.gamma_flip, color='orange', linestyle='--', linewidth=2, label=f'Zero Gamma Flip (${self.gamma_flip:.2f})')
            ax1.text(self.hist.index[0], self.gamma_flip, ' Zero Gamma Flip', color='orange', va='bottom', fontsize=10, weight='bold')
        if self.gamma_magnet:
            ax1.axhline(self.gamma_magnet, color='green', linestyle='-', linewidth=2, alpha=0.7, label=f'Magnet/Wall (${self.gamma_magnet:.2f})')
        if self.gamma_accel:
            ax1.axhline(self.gamma_accel, color='red', linestyle='-', linewidth=2, alpha=0.7, label=f'Accel Zone (${self.gamma_accel:.2f})')

        for i in range(len(self.hist) - 1):
            hv = self.hist['HV_20d'].iloc[i] * 100 # Display check
            if pd.isna(hv): continue
            color = 'green' if hv < 50 else 'yellow' if hv < 90 else 'red'
            ax1.axvspan(self.hist.index[i], self.hist.index[i+1], color=color, alpha=0.05)

        ax1_twin = ax1.twinx()
        # Plot HV multiplied by 100 for percentage display
        ax1_twin.plot(self.hist.index, self.hist['HV_20d']*100, color='blue', linestyle='--', alpha=0.6, label='HV (20d)')
        ax1_twin.set_ylabel("HV (Vol Points)")

        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax1_twin.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')

        # Plot 2: Probability Band Width (%)
        ax2.plot(self.hist.index, self.hist['Expected_Move_30d_Pct'], color='purple', label='30d Expected Move (%)')
        ax2.fill_between(self.hist.index, 0, self.hist['Expected_Move_30d_Pct'], color='purple', alpha=0.2)
        ax2.set_title("Market Risk Perception (Theoretical 30d Move %)")
        ax2.set_ylabel("Move (%)")
        ax2.legend()

        # Plot 3: Probability Skew
        ax3.plot(self.hist.index, self.hist['Prob_Down_10pct'], label='Prob Drop >10%', color='red')
        ax3.plot(self.hist.index, self.hist['Prob_Up_10pct'], label='Prob Rise >10%', color='green')
        ax3.set_title("Probability Skew (Tail Risk)")
        ax3.set_ylabel("Probability (%)")
        ax3.legend()

        plt.tight_layout()
        import os
        filename = os.path.join(output_dir, f"{self.ticker}_gamma_analysis.png")
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

            filename_3m = os.path.join(output_dir, f"{self.ticker}_gamma_analysis_3m.png")
            plt.savefig(filename_3m)
        except Exception as e:
            print(f"Error saving 3m chart: {e}")

        return filename
