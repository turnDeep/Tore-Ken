
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import mplfinance as mpf
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import logging

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
            panel_ratios=(3, 1, 1),
            title="",
            returnfig=True,
            figsize=(10, 8),
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

def generate_stock_chart(df, output_path, ticker):
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
