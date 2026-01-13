
import matplotlib
matplotlib.use('Agg')
import mplfinance as mpf
import pandas as pd
import logging
import os

logger = logging.getLogger(__name__)

def generate_stock_chart(df, ticker, output_path):
    """
    Generates a static image of a stock chart using mplfinance.
    Includes SMAs (50, 100, 200) and Volume.
    """
    if df.empty:
        logger.error(f"No data to plot for {ticker}")
        return False

    # Ensure index is DatetimeIndex
    if not isinstance(df.index, pd.DatetimeIndex):
        if 'Date' in df.columns:
            df = df.set_index('Date')
        else:
            df.index = pd.to_datetime(df.index)

    # Use last 6 months (approx 126 trading days) for visibility, similar to Market Analysis
    # Or maybe 1 year? Market chart is 6mo. Let's do 6mo to keep it clear.
    # But we need SMAs calculated on full history.
    # We assume SMAs are already in the DF or we calculate them on the full DF, then slice.

    # Calculate SMAs if missing
    if 'SMA_50' not in df.columns:
        df['SMA_50'] = df['Close'].rolling(window=50).mean()
    if 'SMA_100' not in df.columns:
        df['SMA_100'] = df['Close'].rolling(window=100).mean()
    if 'SMA_200' not in df.columns:
        df['SMA_200'] = df['Close'].rolling(window=200).mean()

    # Slice for plotting (last 6 months)
    plot_df = df.tail(130).copy() # A bit more than 6mo

    if plot_df.empty:
        return False

    # Prepare AddPlots (SMAs)
    apds = []

    # SMA 50 - Blue
    if not plot_df['SMA_50'].isnull().all():
        apds.append(mpf.make_addplot(plot_df['SMA_50'], color='blue', width=1.5))

    # SMA 100 - Orange
    if not plot_df['SMA_100'].isnull().all():
        apds.append(mpf.make_addplot(plot_df['SMA_100'], color='orange', width=1.5))

    # SMA 200 - Red
    if not plot_df['SMA_200'].isnull().all():
        apds.append(mpf.make_addplot(plot_df['SMA_200'], color='red', width=1.5))

    # Style
    # Use 'yahoo' style or similar clean style
    # We want white background.
    mc = mpf.make_marketcolors(up='green', down='red', edge='inherit', wick='inherit', volume='in')
    s = mpf.make_mpf_style(marketcolors=mc, gridstyle=':', y_on_right=True, facecolor='white', figcolor='white')

    try:
        fig, axlist = mpf.plot(
            plot_df,
            type='candle',
            style=s,
            addplot=apds,
            volume=True, # Show volume in separate panel
            panel_ratios=(4, 1), # Main chart bigger
            title=f"{ticker} (Daily)",
            returnfig=True,
            figsize=(10, 6),
            tight_layout=True
        )

        # Save
        fig.savefig(output_path, bbox_inches='tight', dpi=100)
        plt.close(fig)
        logger.info(f"Stock chart generated for {ticker} at {output_path}")
        return True
    except Exception as e:
        logger.error(f"Error generating stock chart for {ticker}: {e}")
        return False
