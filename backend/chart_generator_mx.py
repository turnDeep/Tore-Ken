import pandas as pd
import mplfinance as mpf
import matplotlib.pyplot as plt
from backend.rdt_data_fetcher import RDTDataFetcher
import os
import numpy as np

class RDTChartGenerator:
    def __init__(self):
        self.fetcher = RDTDataFetcher()
        self.data_folder = "data"

    def load_pickle_data(self, filename):
        path = os.path.join(self.data_folder, filename)
        if os.path.exists(path):
            return pd.read_pickle(path)
        return None

    def generate_chart(self, ticker, output_filename=None):
        print(f"Generating chart for {ticker}...")

        # 1. Load Data
        # Load Weekly Indicators
        zone_rs_data = self.load_pickle_data("zone_rs_weekly.pkl")
        rs_perc_data = self.load_pickle_data("rs_percentile_histogram_weekly.pkl")
        rs_vol_data = self.load_pickle_data("rs_volatility_adjusted_weekly.pkl")
        rti_data = self.load_pickle_data("rti_weekly.pkl")
        atr_ts_data = self.load_pickle_data("atr_trailing_stop_weekly.pkl")

        # Load Raw Price (Daily) and Resample to Weekly for Main Chart
        price_pkl = self.load_pickle_data("price_data_ohlcv.pkl")

        if price_pkl is None:
            print("Error: price_data_ohlcv.pkl not found.")
            return

        # Extract Ticker Data
        if isinstance(price_pkl.columns, pd.MultiIndex):
            try:
                # Handle MultiIndex safely
                df = pd.DataFrame({
                    'Open': price_pkl['Open'][ticker],
                    'High': price_pkl['High'][ticker],
                    'Low': price_pkl['Low'][ticker],
                    'Close': price_pkl['Close'][ticker],
                    'Volume': price_pkl['Volume'][ticker]
                })
            except KeyError:
                print(f"Error: {ticker} not found in price data.")
                return
        else:
            # Single ticker case
            df = price_pkl.copy()

        # Resample to Weekly (W-FRI)
        df.index = pd.to_datetime(df.index)
        weekly_agg = {
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum'
        }
        df_weekly = df.resample('W-FRI').agg(weekly_agg).dropna()

        # Slicing: Last 2 Years approx (104 weeks)
        if len(df_weekly) > 104:
            df_weekly = df_weekly.iloc[-104:]

        plot_df = df_weekly.copy()
        valid_idx = plot_df.index

        # --- Prepare Additional Plots (apds) ---
        apds = []

        def add_plot_safe(data, **kwargs):
            if isinstance(data, pd.Series):
                if data.dropna().empty:
                    return
            elif isinstance(data, pd.DataFrame):
                if data.dropna(how='all').empty:
                    return
            apds.append(mpf.make_addplot(data, **kwargs))

        # 1. ATR Trailing Stop (Main Chart Overlay)
        buy_indices = []
        sell_indices = []

        if atr_ts_data:
            try:
                fast_trail = atr_ts_data["Fast_Trail"][ticker].reindex(valid_idx)
                slow_trail = atr_ts_data["Slow_Trail"][ticker].reindex(valid_idx)
                signals = atr_ts_data["Signals"][ticker].reindex(valid_idx)

                # Determine Bull/Bear state based on Trails (for Slow Trail Color)
                # Green if Trail1 > Trail2, Red otherwise
                bull_cond = fast_trail > slow_trail

                slow_green = slow_trail.copy()
                slow_red = slow_trail.copy()

                slow_green[~bull_cond] = np.nan
                slow_red[bull_cond] = np.nan

                # Plot Split Slow Trail
                add_plot_safe(slow_green, panel=0, color='green', width=1.5)
                add_plot_safe(slow_red, panel=0, color='red', width=1.5)

                # Note: Fast Trail (Blue Line) is removed as requested.

                # Signals (Arrows)
                # Buy = 1 (Green Arrow Up), Sell = -1 (Red Arrow Down)
                # To place markers, we need arrays of same length as plot_df
                buy_markers = signals.apply(lambda x: x if x == 1 else np.nan)
                sell_markers = signals.apply(lambda x: x if x == -1 else np.nan)

                # We need to set the value for the marker position.
                # Usually slightly below Low for Buy, slightly above High for Sell.
                buy_points = plot_df['Low'] * 0.98
                sell_points = plot_df['High'] * 1.02

                buy_markers_plot = buy_points.copy()
                buy_markers_plot[buy_markers.isna()] = np.nan

                sell_markers_plot = sell_points.copy()
                sell_markers_plot[sell_markers.isna()] = np.nan

                if not buy_markers_plot.isna().all():
                    add_plot_safe(buy_markers_plot, panel=0, type='scatter', markersize=100, marker='^', color='green')

                if not sell_markers_plot.isna().all():
                    add_plot_safe(sell_markers_plot, panel=0, type='scatter', markersize=100, marker='v', color='red')

                # Collect indices for text annotation
                # We need the DATE index to find integer location later
                buy_indices = signals[signals == 1].index
                sell_indices = signals[signals == -1].index

            except KeyError:
                print(f"Warning: {ticker} not found in ATR TS data.")

        # 2. Zone RS (Panel 2)
        if zone_rs_data:
            try:
                ratio = zone_rs_data["Ratio"][ticker].reindex(valid_idx)
                momentum = zone_rs_data["Momentum"][ticker].reindex(valid_idx)
                zones = zone_rs_data["Zone"][ticker].reindex(valid_idx)

                # Check if all NaNs
                if ratio.isna().all() or momentum.isna().all():
                    print(f"DEBUG: {ticker} Zone RS data all NaN")
                    raise ValueError("No valid Zone RS data")

                y_min = min(ratio.min(), momentum.min())
                y_max = max(ratio.max(), momentum.max())

                # Handle NaN if min/max returns NaN (though all() check should prevent this)
                if pd.isna(y_min) or pd.isna(y_max):
                     y_min = 0
                     y_max = 1

                y_min -= abs(y_min) * 0.1
                y_max += abs(y_max) * 0.1

                height = y_max - y_min

                def create_bar_series(mask):
                    s = pd.Series(np.nan, index=valid_idx)
                    s[mask] = height
                    return s

                mask_dead = zones == 0
                mask_lift = zones == 1
                mask_drift = zones == 2
                mask_power = zones == 3

                bar_dead = create_bar_series(mask_dead)
                bar_lift = create_bar_series(mask_lift)
                bar_drift = create_bar_series(mask_drift)
                bar_power = create_bar_series(mask_power)

                if not bar_dead.isna().all():
                    add_plot_safe(bar_dead, type='bar', panel=2, color='red', alpha=0.15, width=1.0, bottom=y_min, secondary_y=False)
                if not bar_lift.isna().all():
                    add_plot_safe(bar_lift, type='bar', panel=2, color='blue', alpha=0.15, width=1.0, bottom=y_min, secondary_y=False)
                if not bar_drift.isna().all():
                    add_plot_safe(bar_drift, type='bar', panel=2, color='yellow', alpha=0.15, width=1.0, bottom=y_min, secondary_y=False)
                if not bar_power.isna().all():
                    add_plot_safe(bar_power, type='bar', panel=2, color='green', alpha=0.15, width=1.0, bottom=y_min, secondary_y=False)

                add_plot_safe(ratio, panel=2, color='blue', ylabel='Zone RS')
                add_plot_safe(momentum, panel=2, color='orange', secondary_y=False)

            except KeyError:
                pass

        # 3. Historical Percentile (Panel 3)
        if rs_perc_data:
            try:
                perc = rs_perc_data["Percentile_1M"][ticker].reindex(valid_idx)
                colors = []
                for v in perc:
                    if pd.isna(v): colors.append('white')
                    elif v < 10: colors.append('#d86eef')
                    elif v < 30: colors.append('#d3eeff')
                    elif v < 50: colors.append('#4e7eff')
                    elif v < 70: colors.append('#96d7ff')
                    elif v < 85: colors.append('#80cfff')
                    elif v < 95: colors.append('#1eaaff')
                    else: colors.append('#30b0ff')

                add_plot_safe(perc, type='bar', panel=3, color=colors, ylabel='Hist %')

            except KeyError:
                pass

        # 4. Volatility Adjusted RS (Panel 4)
        if rs_vol_data:
            try:
                rs_val = rs_vol_data["RS_Values"][ticker].reindex(valid_idx)
                rs_ma = rs_vol_data["RS_MA"][ticker].reindex(valid_idx)

                rs_pos = rs_val.apply(lambda x: x if x >= 0 else np.nan)
                rs_neg = rs_val.apply(lambda x: x if x <= 0 else np.nan)

                add_plot_safe(rs_pos, panel=4, color='blue', width=1.5, ylabel='Vol Adj RS')
                add_plot_safe(rs_neg, panel=4, color='fuchsia', width=1.5)

                if not rs_ma.isna().all():
                    ma_diff = rs_ma.diff()
                    ma_rising = rs_ma.copy()
                    ma_falling = rs_ma.copy()
                    ma_rising_mask = ma_diff >= 0
                    ma_falling_mask = ma_diff < 0
                    ma_rising[~ma_rising_mask] = np.nan
                    ma_falling[~ma_falling_mask] = np.nan

                    add_plot_safe(ma_rising, panel=4, color='blue', width=1.5)
                    add_plot_safe(ma_falling, panel=4, color='fuchsia', width=1.5)

                v_rs = rs_val.values
                v_ma = rs_ma.values
                v_zero = np.zeros_like(v_rs)
                mask_valid_rs = ~np.isnan(v_rs)
                mask_valid_ma = ~np.isnan(v_ma)

                where_rs_pos = np.zeros_like(v_rs, dtype=bool)
                where_rs_pos[mask_valid_rs] = v_rs[mask_valid_rs] > 0
                where_rs_neg = np.zeros_like(v_rs, dtype=bool)
                where_rs_neg[mask_valid_rs] = v_rs[mask_valid_rs] <= 0

                # Note: fill_between logic uses the whole series (rs_val) which is not empty if we are here
                # But the fill might be empty if condition is never met.
                # make_addplot with fill_between usually safe if series is valid.
                add_plot_safe(
                    rs_val, panel=4, color='blue', alpha=0,
                    fill_between=dict(y1=v_rs, y2=v_zero, where=where_rs_pos, color='#0084ff', alpha=0.2),
                    secondary_y=False
                )
                add_plot_safe(
                    rs_val, panel=4, color='pink', alpha=0,
                    fill_between=dict(y1=v_rs, y2=v_zero, where=where_rs_neg, color='#ff52c8', alpha=0.2),
                    secondary_y=False
                )

                mask_both = mask_valid_rs & mask_valid_ma
                where_rs_gt_ma = np.zeros_like(v_rs, dtype=bool)
                where_rs_gt_ma[mask_both] = v_rs[mask_both] > v_ma[mask_both]
                where_rs_lt_ma = np.zeros_like(v_rs, dtype=bool)
                where_rs_lt_ma[mask_both] = v_rs[mask_both] <= v_ma[mask_both]

                add_plot_safe(
                    rs_val, panel=4, color='blue', alpha=0,
                    fill_between=dict(y1=v_rs, y2=v_ma, where=where_rs_gt_ma, color='#0084ff', alpha=0.2),
                    secondary_y=False
                )
                add_plot_safe(
                    rs_val, panel=4, color='pink', alpha=0,
                    fill_between=dict(y1=v_rs, y2=v_ma, where=where_rs_lt_ma, color='#ff52c8', alpha=0.2),
                    secondary_y=False
                )

            except KeyError:
                pass

        # 5. RTI (Panel 5)
        if rti_data:
            try:
                rti_val = rti_data["RTI_Values"][ticker].reindex(valid_idx)
                rti_sig = rti_data["RTI_Signals"][ticker].reindex(valid_idx)

                # --- Zones & Background ---
                # Zone 1 (0-5): Red, alpha=0.1
                add_plot_safe(rti_val, panel=5, color='red', alpha=0, secondary_y=False,
                    fill_between=dict(y1=5, y2=0, color='red', alpha=0.1))

                # Zone 2 (5-20): Green, alpha=0.15 (PineScript 80 transparent -> 0.2 alpha approx)
                add_plot_safe(rti_val, panel=5, color='green', alpha=0, secondary_y=False,
                    fill_between=dict(y1=20, y2=5, color='green', alpha=0.15))

                # --- Reference Lines (Gray) ---
                # 0, 5, 20, 100
                ref_lines = [0, 5, 20, 100]
                for line_val in ref_lines:
                    line_series = pd.Series(line_val, index=valid_idx)
                    add_plot_safe(line_series, panel=5, color='gray', width=0.8)

                # --- RTI Line (Blue Base) ---
                add_plot_safe(rti_val, panel=5, color='blue', ylabel='RTI', width=1.5)

                # --- RTI Line (Green Segments for Expansion) ---
                # Logic: If Expansion (Signal 3) at T, segment T-1 -> T is Green.
                # If consecutive expansions, segments connect.
                # We need points where (Sig[i] == 3) OR (Sig[i+1] == 3).
                # Note: valid_idx ensures alignment.
                mask_exp = (rti_sig == 3)
                # Create boolean mask for points to include in green line
                # Use shift(-1) to look ahead (if next is exp, current is start of segment)
                mask_green = mask_exp | mask_exp.shift(-1).fillna(False)

                rti_green = rti_val.copy()
                rti_green[~mask_green] = np.nan

                if not rti_green.isna().all():
                    add_plot_safe(rti_green, panel=5, color='green', width=2.0)

                # --- Orange Dot (Signal 2: Consecutive < 20) ---
                mask_dot = (rti_sig == 2)
                dots = rti_val.copy()
                dots[:] = np.nan
                dots[mask_dot] = rti_val[mask_dot]

                if not dots.isna().all():
                    add_plot_safe(dots, panel=5, type='scatter', markersize=25, color='orange', marker='o')

            except KeyError:
                pass

        # 6. Plotting
        if output_filename is None:
            output_filename = f"{ticker}_weekly_chart.png"

        # Font size adjustment (approx 1.5x)
        # Default often ~10, so we target ~15.
        rc_params = {
            'axes.grid': True,
            'grid.linestyle': ':',
            'font.size': 15,
            'axes.titlesize': 18,
            'axes.labelsize': 15,
            'xtick.labelsize': 14,
            'ytick.labelsize': 14,
            'legend.fontsize': 14
        }
        s = mpf.make_mpf_style(base_mpf_style='yahoo', rc=rc_params)

        fig, axes = mpf.plot(
            plot_df,
            type='candle',
            style=s,
            addplot=apds,
            volume=True,
            volume_panel=1,
            panel_ratios=(4, 1, 1, 1, 1, 1),
            returnfig=True,
            figsize=(12, 16),
            tight_layout=True,
            title=f"{ticker} Weekly Analysis",
            datetime_format='%Y-%-m-%-d',
            xrotation=0
        )

        # Add Text Annotations for Buy/Sell
        # axes[0] is the main chart
        if len(buy_indices) > 0 or len(sell_indices) > 0:
            # We need to map dates to integer indices
            # plot_df index matches valid_idx

            # Helper to find integer index
            def get_x_loc(date_idx):
                try:
                    return plot_df.index.get_loc(date_idx)
                except:
                    return None

            for date in buy_indices:
                if date in plot_df.index:
                    loc = get_x_loc(date)
                    if loc is not None:
                        # Position text below the marker
                        y_pos = plot_df.loc[date, 'Low'] * 0.85
                        axes[0].text(loc, y_pos, 'Buy', ha='center', va='top', fontsize=14, color='black', fontweight='bold')

            for date in sell_indices:
                if date in plot_df.index:
                    loc = get_x_loc(date)
                    if loc is not None:
                        # Position text above the marker
                        y_pos = plot_df.loc[date, 'High'] * 1.15
                        axes[0].text(loc, y_pos, 'Sell', ha='center', va='bottom', fontsize=14, color='black', fontweight='bold')

        # Save
        fig.savefig(output_filename, bbox_inches='tight')
        print(f"Chart saved to {output_filename}")
        plt.close(fig)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--ticker", type=str, required=True, help="Ticker symbol")
    parser.add_argument("-o", "--output", type=str, help="Output filename")
    args = parser.parse_args()

    generator = RDTChartGenerator()
    generator.generate_chart(args.ticker, args.output)
