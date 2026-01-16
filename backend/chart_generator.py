
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import mplfinance as mpf
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import logging
import trendln

logger = logging.getLogger(__name__)

def generate_market_chart(df, output_path):
    """
    Generates a static image of the market chart using mplfinance.
    Matches the style of one_op_viz.py.
    """
    if df.empty:
        logger.error("No data to plot")
        return

    # Ensure index is DatetimeIndex
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df['date'])
        df = df.set_index('date') if 'date' in df.columns else df

    # Setup Fill Data for Background
    y_max = df['High'].max() * 1.05
    y_min = df['Low'].min() * 0.95

    # Check for phase columns
    if 'market_status' in df.columns:
        bull_mask = df['market_status'] == 'Green'
        bear_mask = df['market_status'] == 'Red'
    elif 'Bullish_Phase' in df.columns and 'Bearish_Phase' in df.columns:
        bull_mask = df['Bullish_Phase'].astype(bool)
        bear_mask = df['Bearish_Phase'].astype(bool)
    else:
        # Fallback if not calculated
        bull_mask = np.zeros(len(df), dtype=bool)
        bear_mask = np.zeros(len(df), dtype=bool)

    # Prepare AddPlots
    apds = []

    # TSV (Panel 1)
    if 'TSV' in df.columns:
        apds.append(mpf.make_addplot(df['TSV'], panel=1, color='teal', width=1.5, ylabel='TSV'))

    # StochRSI (Panel 2)
    if 'Fast_K' in df.columns and 'Slow_D' in df.columns:
        apds.append(mpf.make_addplot(df['Fast_K'], panel=2, color='cyan', width=1.5, ylabel='StochRSI'))
        apds.append(mpf.make_addplot(df['Slow_D'], panel=2, color='orange', width=1.5))

    # Background Colors (Panel 0)
    y1_series = pd.Series(y_max, index=df.index)

    # Bullish Zone
    apds.append(mpf.make_addplot(y1_series, panel=0, color='g', alpha=0.0, secondary_y=False,
                     fill_between=dict(y1=y_max, y2=y_min, where=bull_mask, color='skyblue', alpha=0.15)))

    # Bearish Zone
    apds.append(mpf.make_addplot(y1_series, panel=0, color='r', alpha=0.0, secondary_y=False,
                     fill_between=dict(y1=y_max, y2=y_min, where=bear_mask, color='lightcoral', alpha=0.15)))

    # --- Algo Line Analysis (Resistance & Support) ---
    try:
        # Use existing High/Low columns
        h_high = df['High']
        h_low = df['Low']
        values_high = h_high.values
        values_low = h_low.values

        # Series for plotting (filled with NaNs initially)
        high_line_series = pd.Series(np.nan, index=df.index)
        low_line_series = pd.Series(np.nan, index=df.index)

        # Config
        left, right, count = 5, 5, 5
        length = 150

        # 1. High Pivots (Resistance)
        high_pivots = []
        for i in range(left, len(values_high) - right):
            window = values_high[i-left : i+right+1]
            if values_high[i] == np.max(window):
                 high_pivots.append((i, values_high[i]))

        recent_highs = high_pivots[-count:] if len(high_pivots) > count else high_pivots

        if len(recent_highs) >= 2:
            far_idx, far_val = recent_highs[0]
            near_idx, near_val = recent_highs[-1]
            diff = near_idx - far_idx
            if diff != 0:
                slope = (near_val - far_val) / diff
                intercept = far_val - slope * far_idx

                # Calculate coordinates for plotting
                # We need to fill the series from x1 to x2
                x2 = len(values_high) - 1
                x1 = max(0, x2 - (length - 1))

                # Fill the series
                for x in range(x1, x2 + 1):
                    y = slope * x + intercept
                    if 0 <= x < len(df):
                         high_line_series.iloc[x] = y

        # 2. Low Pivots (Support)
        low_pivots = []
        for i in range(left, len(values_low) - right):
            window = values_low[i-left : i+right+1]
            if values_low[i] == np.min(window):
                 low_pivots.append((i, values_low[i]))

        recent_lows = low_pivots[-count:] if len(low_pivots) > count else low_pivots

        if len(recent_lows) >= 2:
            far_idx, far_val = recent_lows[0]
            near_idx, near_val = recent_lows[-1]
            diff = near_idx - far_idx
            if diff != 0:
                slope = (near_val - far_val) / diff
                intercept = far_val - slope * far_idx

                x2 = len(values_low) - 1
                x1 = max(0, x2 - (length - 1))

                for x in range(x1, x2 + 1):
                    y = slope * x + intercept
                    if 0 <= x < len(df):
                         low_line_series.iloc[x] = y

        # Add to plots
        # Resistance
        apds.append(mpf.make_addplot(high_line_series, panel=0, color='#ff7b00', linestyle='--', width=2))

        # Support (with fill to Resistance)
        apds.append(mpf.make_addplot(low_line_series, panel=0, color='#ff7b00', linestyle='--', width=2,
                                     fill_between=dict(y1=high_line_series.values, y2=low_line_series.values, color='#ff7b00', alpha=0.1)))

    except Exception as e:
        logger.error(f"Error calculating algo lines: {e}")

    # Style
    mc = mpf.make_marketcolors(up='green', down='red', inherit=True)
    s = mpf.make_mpf_style(marketcolors=mc, gridstyle=':', y_on_right=True)

    try:
        fig, axlist = mpf.plot(
            df,
            type='candle',
            style=s,
            addplot=apds,
            volume=False,
            panel_ratios=(6, 1, 1),
            title="",
            returnfig=True,
            figsize=(10, 13),
            tight_layout=False,
        )

        # Enforce fixed margins for frontend alignment
        left_margin = 0.05
        right_boundary = 0.88
        plot_width = right_boundary - left_margin

        for ax in axlist:
            pos = ax.get_position()
            ax.set_position([left_margin, pos.y0, plot_width, pos.height])

        if len(axlist) >= 5:
            ax_tsv = axlist[2]
            ax_tsv.axhline(0, color='gray', linestyle='--', linewidth=0.8)

            ax_stoch = axlist[4]
            ax_stoch.axhline(80, color='red', linestyle='--', linewidth=0.8)
            ax_stoch.axhline(20, color='green', linestyle='--', linewidth=0.8)

        fig.savefig(output_path, bbox_inches='tight', dpi=100)
        plt.close(fig)
        logger.info(f"Market chart generated at {output_path}")
        return True
    except Exception as e:
        logger.error(f"Error generating market chart: {e}")
        return False

def generate_stock_chart(df, output_path, ticker, vcp_data=None):
    """
    Generates a static image of a stock chart (Strong Stock) using mplfinance.
    Adapts logic from RDT-system/chart_generator.py.
    """
    if df.empty:
        logger.error(f"No data to plot for {ticker}")
        return False

    # Ensure index is DatetimeIndex
    if not isinstance(df.index, pd.DatetimeIndex):
        if 'Date' in df.columns:
            df.index = pd.to_datetime(df['Date'])
        else:
            # Try to convert existing index
            df.index = pd.to_datetime(df.index)

    # Slice Data (Last 6 Months)
    try:
        end_date = df.index[-1]
        start_date = end_date - pd.DateOffset(months=6)
        plot_df = df.loc[start_date:].copy()
    except Exception as e:
        logger.error(f"Error slicing data for {ticker}: {e}")
        return False

    if plot_df.empty:
        logger.error(f"Plot DataFrame is empty after slicing for {ticker}")
        return False

    # Prepare AddPlots
    apds = []

    # --- Panel 0: VCP Overlays (AVWAP) ---
    if vcp_data and 'avwap' in vcp_data:
        avwap_series = vcp_data['avwap']
        # Align with plot_df
        if isinstance(avwap_series, pd.Series):
             avwap_plot = avwap_series.reindex(plot_df.index)
             apds.append(mpf.make_addplot(avwap_plot, panel=0, color='purple', width=1.5))

    # --- Panel 1: RRS (Real Relative Strength) ---
    if 'RRS' in plot_df.columns:
        # Calculate symmetric limits to center 0
        rrs_vals = plot_df['RRS'].dropna()
        if not rrs_vals.empty:
            rrs_max = rrs_vals.abs().max()
            if rrs_max == 0: rrs_max = 1.0
            rrs_ylim = (-rrs_max * 1.1, rrs_max * 1.1)

            zero_line = pd.Series(0, index=plot_df.index)
            apds.append(mpf.make_addplot(plot_df['RRS'], panel=1, color='orange', width=1.5, ylabel='RRS', ylim=rrs_ylim))
            apds.append(mpf.make_addplot(zero_line, panel=1, color='gray', linestyle='--', width=0.8))

    # --- Panel 2: Volume + RVol Overlay ---
    if 'Volume' in plot_df.columns and 'RVol' in plot_df.columns:
        rvol_line = pd.Series(1.5, index=plot_df.index)
        # RVol on secondary Y of panel 2
        apds.append(mpf.make_addplot(plot_df['RVol'], panel=2, color='blue', width=1.2, secondary_y=True, ylabel='RVol'))
        apds.append(mpf.make_addplot(rvol_line, panel=2, color='gray', linestyle='--', width=0.8, secondary_y=True))

    # --- Algo Line Analysis (Resistance & Support) ---
    try:
        # Use plot_df High/Low columns
        h_high = plot_df['High']
        h_low = plot_df['Low']
        values_high = h_high.values
        values_low = h_low.values

        # Series for plotting (filled with NaNs initially)
        high_line_series = pd.Series(np.nan, index=plot_df.index)
        low_line_series = pd.Series(np.nan, index=plot_df.index)

        # Config
        left, right, count = 5, 5, 5
        length = 150

        # --- 1. Resistance (Using trendln) ---
        try:
            # calc_support_resistance(h, accuracy=8) returns (support, resistance) tuple of tuples
            # resistance tuple = (maximaIdxs, pmax, maxtrend, maxwindows)
            # maxtrend is list of trendlines
            result = trendln.calc_support_resistance(values_high, accuracy=8)
            res_data = result[1]
            maxtrend = res_data[2]

            if len(maxtrend) > 0:
                best_line = maxtrend[0] # Best trendline sorted by default
                slope_intercept = best_line[1]
                slope = slope_intercept[0]
                intercept = slope_intercept[1]

                # Plot for the range we want (e.g. last 'length' days or full visible)
                # Let's plot for visible range
                for x in range(len(plot_df)):
                    y = slope * x + intercept
                    if 0 <= x < len(plot_df):
                         high_line_series.iloc[x] = y
        except Exception as e_trend:
            logger.warning(f"trendln failed for {ticker}: {e_trend}. Falling back to manual method is not implemented.")

        # --- 2. Support (Manual Low Pivots - As requested to keep) ---
        low_pivots = []
        for i in range(left, len(values_low) - right):
            window = values_low[i-left : i+right+1]
            if values_low[i] == np.min(window):
                 low_pivots.append((i, values_low[i]))

        recent_lows = low_pivots[-count:] if len(low_pivots) > count else low_pivots

        if len(recent_lows) >= 2:
            far_idx, far_val = recent_lows[0]
            near_idx, near_val = recent_lows[-1]
            diff = near_idx - far_idx
            if diff != 0:
                slope = (near_val - far_val) / diff
                intercept = far_val - slope * far_idx

                x2 = len(values_low) - 1
                x1 = max(0, x2 - (length - 1))

                for x in range(x1, x2 + 1):
                    y = slope * x + intercept
                    if 0 <= x < len(plot_df):
                         low_line_series.iloc[x] = y

        # Add to plots
        apds.append(mpf.make_addplot(high_line_series, panel=0, color='#ff7b00', linestyle='--', width=2))
        apds.append(mpf.make_addplot(low_line_series, panel=0, color='#ff7b00', linestyle='--', width=2,
                                     fill_between=dict(y1=high_line_series.values, y2=low_line_series.values, color='#ff7b00', alpha=0.1)))

    except Exception as e:
        logger.error(f"Error calculating algo lines for {ticker}: {e}")

    # Styling
    mc = mpf.make_marketcolors(up='green', down='red', edge='inherit', wick='inherit', volume='inherit')
    s = mpf.make_mpf_style(marketcolors=mc, gridstyle=':', y_on_right=True, facecolor='white')

    try:
        # Plot
        fig, axes = mpf.plot(
            plot_df,
            type='candle',
            style=s,
            addplot=apds,
            volume=True,
            volume_panel=2,
            panel_ratios=(3, 1, 1),
            returnfig=True,
            figsize=(10, 8),
            scale_padding={'left': 0.1, 'top': 0.1, 'right': 1.0, 'bottom': 0.1},
            tight_layout=True
        )

        # --- Plot ZigZag Manually ---
        if vcp_data and 'pivots' in vcp_data:
            pivots = vcp_data['pivots']
            date_to_idx = {date: i for i, date in enumerate(plot_df.index)}

            zigzag_x = []
            zigzag_y = []

            display_pivots = [p for p in pivots if p['date'] >= plot_df.index[0]]
            prior_pivots = [p for p in pivots if p['date'] < plot_df.index[0]]
            if prior_pivots:
                display_pivots.insert(0, prior_pivots[-1])

            for i in range(len(display_pivots) - 1):
                p1 = display_pivots[i]
                p2 = display_pivots[i+1]

                idx1 = date_to_idx.get(p1['date'], 0 if p1['date'] < plot_df.index[0] else None)
                idx2 = date_to_idx.get(p2['date'])

                if idx1 is not None and idx2 is not None:
                    if not zigzag_x or zigzag_x[-1] != idx1:
                        zigzag_x.append(idx1)
                        zigzag_y.append(p1['price'])
                    zigzag_x.append(idx2)
                    zigzag_y.append(p2['price'])

            if zigzag_x:
                axes[0].plot(zigzag_x, zigzag_y, color='blue', marker='o', linestyle='-', linewidth=1.0, markersize=3, alpha=0.7)

        # Set Title
        axes[0].set_title(f'{ticker} - D1', loc='left', fontsize=12)

        # Enable left-side ticks for Main Panel (axes[0])
        axes[0].tick_params(axis='y', labelleft=True)

        # RRS Panel: Disable left ticks and labels strictly (axes[2] is usually RRS panel primary)
        # Note: mplfinance axes indices depend on addplot config.
        # Typically: 0=Main, 1=SecondaryMain(if any), 2=Panel1, 3=Panel1Secondary...
        # With RRS (panel 1) and Volume (panel 2):
        # axes[0]: Main
        # axes[2]: Panel 1 (RRS)
        # axes[4]: Panel 2 (Volume)
        # But we added RVol as secondary on panel 2.

        # Let's try to target by panel index if possible, or just iterate.
        # RDT logic specifically disables left labels for RRS.
        if len(axes) > 2:
            axes[2].tick_params(axis='y', which='both', left=False, labelleft=False)

        # If there's a secondary axis for RRS (we didn't add one, but let's be safe)
        if len(axes) > 3:
            axes[3].tick_params(axis='y', which='both', left=False, labelleft=False)

        fig.savefig(output_path, bbox_inches='tight')
        plt.close(fig)
        logger.info(f"Stock chart generated for {ticker} at {output_path}")
        return True

    except Exception as e:
        logger.error(f"Error generating stock chart for {ticker}: {e}")
        return False
