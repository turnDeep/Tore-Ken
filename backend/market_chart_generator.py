import pandas as pd
import numpy as np
import mplfinance as mpf
import matplotlib.pyplot as plt
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def generate_market_chart(df, output_path):
    """
    Generates the Market Analysis chart (SPY) with trend background colors.
    """
    if df.empty:
        logger.error("No market data to plot")
        return False

    # Ensure index is DatetimeIndex
    if not isinstance(df.index, pd.DatetimeIndex):
        if 'Date' in df.columns:
            df.index = pd.to_datetime(df['Date'])
        else:
            df.index = pd.to_datetime(df.index)

    # Prepare AddPlots
    apds = []

    # --- Panel 0: BB & Markers ---
    if 'BB_Upper' in df.columns and 'BB_Lower' in df.columns:
        apds.append(mpf.make_addplot(df['BB_Upper'], panel=0, color='gray', alpha=0.5, width=0.8))
        apds.append(mpf.make_addplot(df['BB_Lower'], panel=0, color='gray', alpha=0.5, width=0.8))

    if 'BB_RSI_Buy' in df.columns:
        # Buy Markers (Low * 0.99 for spacing)
        # Use np.where to filter NaN for non-signals
        buy_signals = np.where(df['BB_RSI_Buy'], df['Low'] * 0.99, np.nan)
        apds.append(mpf.make_addplot(buy_signals, panel=0, type='scatter', markersize=100, marker='^', color='lime'))

    if 'BB_RSI_Sell' in df.columns:
        # Sell Markers (High * 1.01)
        sell_signals = np.where(df['BB_RSI_Sell'], df['High'] * 1.01, np.nan)
        apds.append(mpf.make_addplot(sell_signals, panel=0, type='scatter', markersize=100, marker='v', color='magenta'))

    # --- Panel 1: TSV (Approx) ---
    if 'TSV' in df.columns:
        # TSV line color: Teal (similar to image)
        apds.append(mpf.make_addplot(df['TSV'], panel=1, color='teal', width=1.5, ylabel='TSV'))
        if 'TSV_MA' in df.columns:
            apds.append(mpf.make_addplot(df['TSV_MA'], panel=1, color='orange', width=1.0))

    # --- Panel 2: StochRSI ---
    # Fast_K: Cyan, Slow_D: Orange
    # Add horizontal lines for Overbought (80) and Oversold (20)

    # 80 line (Red dotted)
    apds.append(mpf.make_addplot(np.full(len(df), 80), panel=2, color='red', linestyle=':', width=1.0, secondary_y=False))
    # 20 line (Green dotted)
    apds.append(mpf.make_addplot(np.full(len(df), 20), panel=2, color='green', linestyle=':', width=1.0, secondary_y=False))

    # Legacy logic uses 'Fast_K' and 'Slow_D'
    if 'Fast_K' in df.columns and 'Slow_D' in df.columns:
        apds.append(mpf.make_addplot(df['Fast_K'], panel=2, color='cyan', width=1.2, ylabel='StochRSI'))
        apds.append(mpf.make_addplot(df['Slow_D'], panel=2, color='orange', width=1.2))
    # Fallback to StochRSI_K/D if provided
    elif 'StochRSI_K' in df.columns and 'StochRSI_D' in df.columns:
        apds.append(mpf.make_addplot(df['StochRSI_K'], panel=2, color='cyan', width=1.2, ylabel='StochRSI'))
        apds.append(mpf.make_addplot(df['StochRSI_D'], panel=2, color='orange', width=1.2))

    # --- Panel 3: RSI ---
    if 'RSI' in df.columns:
        # RSI 50 line
        apds.append(mpf.make_addplot(np.full(len(df), 50), panel=3, color='white', linestyle='--', width=0.8, secondary_y=False))
        # RSI line
        apds.append(mpf.make_addplot(df['RSI'], panel=3, color='yellow', width=1.5, ylabel='RSI', ylim=(0, 100)))

    # --- Trend Background Logic ---
    if 'Trend_Signal' in df.columns:
        signal = df['Trend_Signal']
        y_high = df['High'].max() * 1.05
        y_low = df['Low'].min() * 0.95

        # Bullish (Green Signal) -> Cyan/SkyBlue background
        apds.append(mpf.make_addplot(
            np.full(len(df), y_high),
            panel=0,
            color='g',
            alpha=0.0,
            secondary_y=False,
            fill_between=dict(y1=y_high, y2=y_low, where=signal.values==1, color='skyblue', alpha=0.15)
        ))

        # Bearish (Red Signal) -> Red background
        apds.append(mpf.make_addplot(
            np.full(len(df), y_high),
            panel=0,
            color='r',
            alpha=0.0,
            secondary_y=False,
            fill_between=dict(y1=y_high, y2=y_low, where=signal.values==-1, color='red', alpha=0.15)
        ))

    # Style
    mc = mpf.make_marketcolors(up='green', down='red', inherit=True)
    s = mpf.make_mpf_style(marketcolors=mc, gridstyle=':', y_on_right=True)

    try:
        fig, axlist = mpf.plot(
            df,
            type='candle',
            style=s,
            addplot=apds,
            volume=False, # Volume usually not on SPY analysis chart or simplified
            panel_ratios=(6, 1, 1, 1), # Added ratio for Panel 3
            title="",
            returnfig=True,
            figsize=(10, 15), # Increased height
            tight_layout=False,
        )

        # Enforce fixed margins for frontend alignment (retained from legacy)
        left_margin = 0.05
        right_boundary = 0.88
        plot_width = right_boundary - left_margin

        for ax in axlist:
            pos = ax.get_position()
            ax.set_position([left_margin, pos.y0, plot_width, pos.height])

        # Add horizontal lines for indicators
        if len(axlist) >= 5: # 0:Main, 2:Panel1, 4:Panel2 (indices jump due to secondary axes?)
            # Usually: axlist[0]=Main, axlist[2]=Panel1, axlist[4]=Panel2 if no secondary y on main
            # With fill_between on Main, indices might shift? No, addplot doesn't add axes unless new panel.

            # Find axes by panel index assuming standard order
            # The indices in axlist returned by mpf.plot depend on the panels created.
            # 3 panels -> at least 3 axes.
            pass

        fig.savefig(output_path, bbox_inches='tight', dpi=100)
        plt.close(fig)
        logger.info(f"Market chart generated at {output_path}")
        return True
    except Exception as e:
        logger.error(f"Error generating market chart: {e}")
        return False
