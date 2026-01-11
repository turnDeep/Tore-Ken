
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

    # Prepare logic data (assuming it's already calculated in df)
    # Required columns: Open, High, Low, Close, TSV, Fast_K, Slow_D, Bullish_Phase, Bearish_Phase

    # Calculate Phase Masks if not present (or trust passed df)
    # The detect_cycle_phases logic is already in rdt_logic.py and merged into df there?
    # Actually, rdt_logic.py returns a list of dicts. We need to convert to DataFrame first.
    # Let's assume the caller passes a DataFrame with all indicators.

    # Setup Fill Data for Background
    # Note: mplfinance fill_between needs Series with same index
    # We define y_max/min for filling the background of the Price panel (panel 0)
    y_max = df['High'].max() * 1.05
    y_min = df['Low'].min() * 0.95

    # Check for phase columns
    if 'market_status' in df.columns:
        bull_mask = df['market_status'] == 'Green'
        bear_mask = df['market_status'] == 'Red'
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
    # We use fill_between logic.
    # We need a Series for y1 and y2.
    y1_series = pd.Series(y_max, index=df.index)
    y2_series = pd.Series(y_min, index=df.index)

    # Bullish Zone
    apds.append(mpf.make_addplot(y1_series, panel=0, color='g', alpha=0.0, secondary_y=False,
                     fill_between=dict(y1=y_max, y2=y_min, where=bull_mask, color='skyblue', alpha=0.15)))

    # Bearish Zone
    apds.append(mpf.make_addplot(y1_series, panel=0, color='r', alpha=0.0, secondary_y=False,
                     fill_between=dict(y1=y_max, y2=y_min, where=bear_mask, color='lightcoral', alpha=0.15)))

    # Style
    mc = mpf.make_marketcolors(up='green', down='red', inherit=True)
    # Remove margins to make X-axis mapping easier?
    # Actually, keeping margins is better for labels, but we need to know where the plot area is.
    # mpf.plot doesn't easily return the pixel bbox of the axes.
    # But if we use 'tight_layout', it might vary.
    # We will use a fixed figure size.

    s = mpf.make_mpf_style(marketcolors=mc, gridstyle=':', y_on_right=True)

    # Plot
    # We use returnfig=True to save manually if needed, or just savefig directly.
    # We set tight_layout=True to maximize space.

    try:
        fig, axlist = mpf.plot(
            df,
            type='candle',
            style=s,
            addplot=apds,
            volume=False,
            panel_ratios=(3, 1, 1),
            title="", # No title inside the image to save space, handled by HTML
            returnfig=True,
            figsize=(10, 8), # Fixed size for web
            tight_layout=True,
            scale_padding={'left': 0.1, 'top': 0.1, 'right': 1.0, 'bottom': 1.0} # Attempt to control padding? No, scale_padding is internal.
        )

        # Add reference lines manually
        if len(axlist) > 4: # 0=Main, 2=TSV, 4=Stoch (indices depend on mpf internals, usually odd are legends/secondary)
            # Actually axlist structure:
            # Main, TSV, StochRSI
            # Let's iterate.
            # Usually axlist[0] is main, axlist[2] is panel 1, axlist[4] is panel 2 if we use addplot with panels?
            # Let's just try to identify by geometry or assume order.
            # With panel_ratios=(3,1,1), we have 3 panels.
            pass

        # Reference lines
        # TSV is panel 1
        # Stoch is panel 2

        # Identify axes (mpf returns list of axes)
        # Typically: Body, BodySecondary(if any), Panel1, Panel1Secondary...
        # Let's inspect axes in a safe way if possible, or just skip manual ax lines if mpf doesn't support easy access.
        # But wait, one_op_viz.py did: ax_tsv = axlist[2], ax_stoch = axlist[4]

        if len(axlist) >= 5:
            ax_tsv = axlist[2]
            ax_tsv.axhline(0, color='gray', linestyle='--', linewidth=0.8)

            ax_stoch = axlist[4]
            ax_stoch.axhline(80, color='red', linestyle='--', linewidth=0.8)
            ax_stoch.axhline(20, color='green', linestyle='--', linewidth=0.8)

        # Save
        fig.savefig(output_path, bbox_inches='tight', dpi=100)
        plt.close(fig)
        logger.info(f"Chart generated at {output_path}")
        return True
    except Exception as e:
        logger.error(f"Error generating chart: {e}")
        return False
