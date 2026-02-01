"""
Yahoo Financeから株価データ(OHLCV)を取得して保存するスクリプト
既存のprice_data_ohlcv.pklがある場合は差分のみをダウンロードして更新
"""
import os
import glob
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import time
import logging
import argparse

# ============================================================
# データ取得期間設定
# ============================================================
# 取得開始日 (YYYY-MM-DD形式、Noneの場合は自動計算)
START_DATE = None  # 例: "2024-01-01"

# 取得終了日 (YYYY-MM-DD形式、Noneの場合は今日)
END_DATE = None    # 例: "2025-01-13"

# ============================================================

DATA_FOLDER = "data"
if not os.path.exists(DATA_FOLDER):
    os.makedirs(DATA_FOLDER)
PRICE_DATA_PATH = os.path.join(DATA_FOLDER, "price_data_ohlcv.pkl")
BACKUP_PATH = os.path.join(DATA_FOLDER, "price_data_ohlcv_backup.pkl")

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fetch_price_data.log'),
        logging.StreamHandler()
    ]
)

def load_existing_price_data():
    """
    既存の価格データを読み込む

    Returns:
        tuple: (price_data, last_date) or (None, None) if file doesn't exist
    """
    if not os.path.exists(PRICE_DATA_PATH):
        logging.info("No existing price data file found. Will perform full download.")
        return None, None

    try:
        price_data = pd.read_pickle(PRICE_DATA_PATH)
        last_date = price_data.index.max()

        logging.info(f"\n{'='*60}")
        logging.info("EXISTING PRICE DATA FOUND")
        logging.info(f"{'='*60}")
        logging.info(f"File: {PRICE_DATA_PATH}")
        logging.info(f"Shape: {price_data.shape}")
        logging.info(f"Date range: {price_data.index.min().date()} to {last_date.date()}")
        logging.info(f"Symbols: {len(price_data.columns.get_level_values(1).unique())}")
        logging.info(f"Days of data: {len(price_data)}")

        # バックアップを作成
        price_data.to_pickle(BACKUP_PATH)
        logging.info(f"✓ Backup created: {BACKUP_PATH}")
        logging.info(f"{'='*60}\n")

        return price_data, last_date

    except Exception as e:
        logging.error(f"Error loading existing price data: {e}")
        logging.info("Will perform full download instead.")
        return None, None


def get_unique_symbols(symbol_limit=None, override_start_date=None):
    """
    Reads target_stocks CSV file from the data folder and returns a list of stock symbols.
    Falls back to Excel files if CSV is not found.

    Args:
        symbol_limit: シンボル数の制限
        override_start_date: 開始日の上書き（グローバル設定より優先）
    """
    # Check for stock.csv in the current directory
    stock_csv_path = "stock.csv"
    if os.path.exists(stock_csv_path):
        try:
            logging.info(f"Reading symbols from: {stock_csv_path}")
            df = pd.read_csv(stock_csv_path)

            # stock.csv has 'Ticker', map to 'Symbol'
            if 'Ticker' in df.columns and 'Symbol' not in df.columns:
                df = df.rename(columns={'Ticker': 'Symbol'})

            if 'Symbol' in df.columns:
                all_symbols = df['Symbol'].dropna().unique().tolist()
                unique_symbols = sorted(list(set(all_symbols)))
                logging.info(f"Found {len(unique_symbols)} unique symbols from stock.csv.")

                # Determine start date logic
                if override_start_date:
                    start_date = override_start_date
                    logging.info(f"Base start date: {start_date} (from override)")
                elif START_DATE:
                    start_date = START_DATE
                    logging.info(f"Base start date: {start_date} (from config)")
                else:
                    start_date = (datetime.now() - timedelta(days=365*10)).strftime('%Y-%m-%d')
                    logging.info(f"Base start date: {start_date} (10 years before today, default)")

                if symbol_limit is not None and symbol_limit > 0:
                    logging.info(f"Limiting to {symbol_limit} symbols for this run.")
                    unique_symbols = unique_symbols[:symbol_limit]

                return unique_symbols, start_date
            else:
                logging.warning("'Symbol' (or 'Ticker') column not found in stock.csv. Falling back to target_stocks CSV.")
        except Exception as e:
            logging.error(f"Error reading {stock_csv_path}: {e}. Falling back to target_stocks CSV.")

    # Try to find target_stocks CSV file
    csv_files = glob.glob(os.path.join(DATA_FOLDER, "target_stocks*.csv"))

    if csv_files:
        # Use the most recent target_stocks CSV file
        csv_files.sort(reverse=True)  # Sort in descending order to get the latest
        target_file = csv_files[0]

        try:
            logging.info(f"Reading symbols from: {os.path.basename(target_file)}")
            df = pd.read_csv(target_file)

            # Extract symbols from the CSV
            if 'Symbol' in df.columns:
                all_symbols = df['Symbol'].dropna().unique().tolist()
                unique_symbols = sorted(list(set(all_symbols)))
                logging.info(f"Found {len(unique_symbols)} unique symbols from target_stocks CSV.")

                # 開始日の決定優先順位: override_start_date > START_DATE > デフォルト（6ヶ月前）
                if override_start_date:
                    start_date = override_start_date
                    logging.info(f"Base start date: {start_date} (from override)")
                elif START_DATE:
                    start_date = START_DATE
                    logging.info(f"Base start date: {start_date} (from config)")
                else:
                    start_date = (datetime.now() - timedelta(days=365*10)).strftime('%Y-%m-%d')
                    logging.info(f"Base start date: {start_date} (10 years before today, default)")

                if symbol_limit is not None and symbol_limit > 0:
                    logging.info(f"Limiting to {symbol_limit} symbols for this run.")
                    unique_symbols = unique_symbols[:symbol_limit]

                return unique_symbols, start_date
            else:
                logging.warning("'Symbol' column not found in target_stocks CSV. Falling back to Excel files.")
        except Exception as e:
            logging.error(f"Error reading {target_file}: {e}. Falling back to Excel files.")
    else:
        logging.info("No target_stocks CSV found. Falling back to Excel files.")

    # Fallback to original Excel-based logic
    all_symbols = []
    excel_files = glob.glob(os.path.join(DATA_FOLDER, "integrated_screening_*.xlsx"))
    if not excel_files:
        excel_files = glob.glob(os.path.join(DATA_FOLDER, "stock_screening_*.xlsx"))

    excel_files.sort()

    if not excel_files:
        logging.warning("No Excel files found in the data directory.")
        return [], None

    oldest_file = excel_files[0]
    try:
        basename = os.path.basename(oldest_file)
        if basename.startswith('integrated_screening_'):
            date_str = basename.split('_')[2]
        else:
            date_str = basename.split('_')[2].split('.')[0]

        screening_date = datetime.strptime(date_str, '%Y%m%d')
        start_date = (screening_date - timedelta(days=180)).strftime('%Y-%m-%d')
        logging.info(f"First screening file: {os.path.basename(oldest_file)} (date: {screening_date.strftime('%Y-%m-%d')})")
        logging.info(f"Base start date: {start_date} (6 months before first screening)")
    except (IndexError, ValueError) as e:
        logging.error(f"Could not parse date from filename '{os.path.basename(oldest_file)}'. Error: {e}")
        if override_start_date:
            start_date = override_start_date
            logging.info(f"Using override start date: {start_date}")
        elif START_DATE:
            start_date = START_DATE
            logging.info(f"Using config start date: {start_date}")
        else:
            start_date = (datetime.now() - timedelta(days=365*10)).strftime('%Y-%m-%d')
            logging.info(f"Defaulting to start date: {start_date}")

    logging.info(f"Found {len(excel_files)} Excel files. Reading symbols...")
    for file_path in excel_files:
        try:
            df = pd.read_excel(file_path, sheet_name='Screening_Results', usecols=['Symbol'])
            all_symbols.extend(df['Symbol'].dropna().unique())
        except Exception as e:
            logging.error(f"Error reading {file_path}: {e}")

    unique_symbols = sorted(list(set(all_symbols)))
    logging.info(f"Found a total of {len(unique_symbols)} unique symbols.")

    if symbol_limit is not None and symbol_limit > 0:
        logging.info(f"Limiting to {symbol_limit} symbols for this run.")
        unique_symbols = unique_symbols[:symbol_limit]

    return unique_symbols, start_date


def download_price_data(symbols, start_date, end_date=None, chunk_size=50, delay=1, max_retries=3):
    """
    Downloads historical daily price data for a list of symbols from Yahoo Finance in chunks.
    """
    if not symbols or not start_date:
        logging.error("Symbol list or start date is empty. Cannot download data.")
        return None

    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')

    logging.info(f"\n{'='*60}")
    logging.info("DOWNLOADING PRICE DATA")
    logging.info(f"{'='*60}")
    logging.info(f"Symbols: {len(symbols)}")
    logging.info(f"Period: {start_date} to {end_date}")
    logging.info(f"Chunk size: {chunk_size}, Delay: {delay}s")
    logging.info(f"{'='*60}\n")

    all_data = []
    failed_symbols = []
    total_chunks = (len(symbols) + chunk_size - 1) // chunk_size

    for i in range(0, len(symbols), chunk_size):
        chunk = symbols[i:i + chunk_size]
        chunk_num = i // chunk_size + 1

        success = False
        for retry in range(max_retries):
            try:
                logging.info(f"Chunk {chunk_num}/{total_chunks} ({len(chunk)} symbols, retry {retry + 1})...")

                data = yf.download(
                    chunk,
                    start=start_date,
                    end=end_date,
                    threads=False,
                    progress=False,
                    auto_adjust=True
                )

                if not data.empty:
                    # Check for Volume
                    if 'Volume' not in data.columns.get_level_values(0):
                        logging.warning(f"⚠ Chunk {chunk_num}: Volume data missing!")

                    all_data.append(data)
                    successful = data.columns.get_level_values(1).unique().tolist()
                    logging.info(f"✓ Chunk {chunk_num}: {len(successful)}/{len(chunk)} symbols, {len(data)} rows")
                    success = True
                    break
                else:
                    logging.warning(f"⚠ Chunk {chunk_num} returned empty data")

            except Exception as e:
                logging.error(f"✗ Chunk {chunk_num} error (retry {retry + 1}): {e}")
                if retry < max_retries - 1:
                    time.sleep(delay * 2)

        if not success:
            failed_symbols.extend(chunk)

        if chunk_num % 20 == 0 and all_data:
            temp_df = pd.concat(all_data, axis=1)
            temp_path = os.path.join(DATA_FOLDER, f"temp_price_data_chunk_{chunk_num}.pkl")
            temp_df.to_pickle(temp_path)
            logging.info(f"💾 Progress saved: {temp_path}")

        time.sleep(delay)

    if failed_symbols:
        logging.warning(f"\n⚠ {len(failed_symbols)} symbols failed")
        with open(os.path.join(DATA_FOLDER, 'failed_symbols.txt'), 'w') as f:
            f.write('\n'.join(failed_symbols))

    if not all_data:
        return None

    logging.info("\n✓ Merging chunks...")
    return pd.concat(all_data, axis=1)


def merge_price_data(existing_data, new_data):
    """
    既存データと新規データを正しくマージ

    修正ポイント:
    1. 列（銘柄）方向でも結合が必要
    2. 日付の重複処理
    3. 既存銘柄の更新 + 新規銘柄の追加
    """
    logging.info(f"\n{'='*60}")
    logging.info("MERGING DATA")
    logging.info(f"{'='*60}")
    logging.info(f"Existing: {existing_data.shape}")
    logging.info(f"New: {new_data.shape}")

    # 既存銘柄と新規銘柄を分離
    existing_symbols = set(existing_data.columns.get_level_values(1).unique())
    new_symbols_all = set(new_data.columns.get_level_values(1).unique())

    # 既存銘柄（更新が必要）
    common_symbols = existing_symbols & new_symbols_all
    # 新規追加銘柄
    added_symbols = new_symbols_all - existing_symbols

    logging.info(f"Common symbols (to update): {len(common_symbols)}")
    logging.info(f"New symbols (to add): {len(added_symbols)}")

    # ステップ1: 既存銘柄のデータを更新（行方向で結合）
    if common_symbols:
        # 既存銘柄のみのデータを抽出
        common_cols_existing = [col for col in existing_data.columns
                               if col[1] in common_symbols]
        common_cols_new = [col for col in new_data.columns
                          if col[1] in common_symbols]

        existing_common = existing_data[common_cols_existing]
        new_common = new_data[common_cols_new]

        # 行方向で結合（日付軸）
        updated_common = pd.concat([existing_common, new_common], axis=0)
        # 重複する日付は新しいデータを優先
        updated_common = updated_common[~updated_common.index.duplicated(keep='last')]
        updated_common = updated_common.sort_index()

        logging.info(f"✓ Updated common symbols: {updated_common.shape}")
    else:
        updated_common = None

    # ステップ2: 削除された銘柄のデータを保持
    removed_symbols = existing_symbols - new_symbols_all
    if removed_symbols:
        logging.info(f"⚠ Symbols no longer in screening: {len(removed_symbols)}")
        removed_cols = [col for col in existing_data.columns
                       if col[1] in removed_symbols]
        kept_removed = existing_data[removed_cols]
    else:
        kept_removed = None

    # ステップ3: 新規銘柄を追加
    if added_symbols:
        added_cols = [col for col in new_data.columns
                     if col[1] in added_symbols]
        added_data = new_data[added_cols]
        logging.info(f"✓ Added new symbols: {added_data.shape}")
    else:
        added_data = None

    # ステップ4: 全てを列方向で結合
    parts_to_merge = []
    if updated_common is not None:
        parts_to_merge.append(updated_common)
    if kept_removed is not None:
        parts_to_merge.append(kept_removed)
    if added_data is not None:
        parts_to_merge.append(added_data)

    if not parts_to_merge:
        logging.error("No data to merge!")
        return existing_data

    # 列方向で結合
    merged = pd.concat(parts_to_merge, axis=1)
    # 欠損値を前方埋め（新規銘柄の過去データは存在しないため）
    # merged = merged.fillna(method='ffill')  # 不要：NaNのままでOK
    merged = merged.sort_index()

    logging.info(f"✓ Final merged: {merged.shape}")
    logging.info(f"  Date range: {merged.index.min().date()} to {merged.index.max().date()}")
    logging.info(f"  Symbols: {len(merged.columns.get_level_values(1).unique())}")
    logging.info(f"{'='*60}\n")

    return merged



def save_price_data(price_data):
    """Save price data to pickle file"""
    try:
        price_data.to_pickle(PRICE_DATA_PATH)
        file_size_mb = os.path.getsize(PRICE_DATA_PATH) / 1024 / 1024
        logging.info(f"\n{'='*60}")
        logging.info("✓ PRICE DATA SAVED")
        logging.info(f"{'='*60}")
        logging.info(f"Path: {PRICE_DATA_PATH}")
        logging.info(f"Shape: {price_data.shape}")
        logging.info(f"Range: {price_data.index[0].date()} to {price_data.index[-1].date()}")
        logging.info(f"Symbols: {len(price_data.columns.get_level_values(1).unique())}")
        logging.info(f"Size: {file_size_mb:.2f} MB")
        logging.info(f"{'='*60}\n")
        return True
    except Exception as e:
        logging.error(f"Error saving: {e}")
        return False

class RDTDataFetcher:
    """
    Wrapper class for data fetching operations.
    Maintains compatibility with chart_generator.py.
    """
    def __init__(self):
        pass

    def fetch_single(self, ticker, period="2y"):
        """Fetch single ticker data (Daily)."""
        # Logic similar to main but for single ticker and returning DF
        end_date = datetime.now()
        # Parse period string roughly
        days = 365 * 2
        if period == "1y": days = 365
        elif period == "6mo": days = 180

        start_date = end_date - timedelta(days=days)
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')

        try:
            df = yf.download(ticker, start=start_str, end=end_str, progress=False, auto_adjust=True)
            return df
        except Exception as e:
            logging.error(f"Error fetching {ticker}: {e}")
            return None

    def fetch_spy(self, period="2y"):
        """Fetch SPY benchmark."""
        return self.fetch_single("SPY", period)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Fetch stock price data (incremental)')
    parser.add_argument('--symbol_limit', type=int, help='Limit symbols (testing)')
    parser.add_argument('--chunk_size', type=int, default=50)
    parser.add_argument('--delay', type=float, default=1.0)
    parser.add_argument('--full', action='store_true', help='Force full download')
    parser.add_argument('--start_date', type=str, help='Override start date (YYYY-MM-DD)')
    parser.add_argument('--end_date', type=str, help='Override end date (YYYY-MM-DD)')
    args = parser.parse_args()

    logging.info("="*60)
    logging.info("STOCK PRICE DATA FETCHER (INCREMENTAL)")
    logging.info("="*60)

    # グローバル設定を表示
    if START_DATE or END_DATE:
        logging.info(f"\nGlobal Config:")
        if START_DATE:
            logging.info(f"  START_DATE: {START_DATE}")
        if END_DATE:
            logging.info(f"  END_DATE: {END_DATE}")

    # コマンドライン引数による上書き
    if args.start_date:
        logging.info(f"  Overriding with --start_date: {args.start_date}")
    if args.end_date:
        logging.info(f"  Overriding with --end_date: {args.end_date}")

    # 既存データ読み込み
    existing_data, last_date = None, None
    if not args.full:
        existing_data, last_date = load_existing_price_data()

    # シンボル取得
    symbols, base_start_date = get_unique_symbols(args.symbol_limit, args.start_date)
    if not symbols:
        logging.error("No symbols found.")
        exit(1)

    logging.info(f"\nSymbols: {len(symbols)}")
    logging.info(f"First 10: {symbols[:10]}")

    # ダウンロード期間決定
    if existing_data is not None and last_date is not None:
        # 増分更新モード
        start_date = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
        # 終了日の決定: コマンドライン > グローバル設定 > デフォルト（今日）
        if args.end_date:
            end_date = args.end_date
        elif END_DATE:
            end_date = END_DATE
        else:
            end_date = datetime.now().strftime('%Y-%m-%d')

        logging.info(f"\n{'='*60}")
        logging.info("INCREMENTAL MODE")
        logging.info(f"Last date: {last_date.date()}")
        logging.info(f"Download: {start_date} to {end_date}")
        logging.info(f"{'='*60}")

        if last_date.date() >= datetime.now().date():
            logging.info("\n✓ Already up to date!")
            exit(0)

        # 新規シンボルチェック
        existing_symbols = set(existing_data.columns.get_level_values(1).unique())
        new_symbols_set = set(symbols) - existing_symbols

        if new_symbols_set:
            logging.info(f"\n⚠ {len(new_symbols_set)} new symbols detected")
            # 新規シンボルは全期間、既存は差分
            new_full = download_price_data(list(new_symbols_set), base_start_date, end_date,
                                          args.chunk_size, args.delay)
            incremental = download_price_data(list(existing_symbols), start_date, end_date,
                                             args.chunk_size, args.delay)

            if new_full is not None and incremental is not None:
                new_data = pd.concat([new_full, incremental], axis=1)
            else:
                new_data = new_full or incremental
        else:
            new_data = download_price_data(symbols, start_date, end_date,
                                          args.chunk_size, args.delay)

        final_data = merge_price_data(existing_data, new_data) if new_data is not None else existing_data
    else:
        logging.info(f"\n{'='*60}")
        logging.info("FULL DOWNLOAD MODE")
        logging.info(f"{'='*60}\n")
        # 終了日の決定: コマンドライン > グローバル設定 > None（今日）
        if args.end_date:
            full_end_date = args.end_date
        elif END_DATE:
            full_end_date = END_DATE
        else:
            full_end_date = None

        final_data = download_price_data(symbols, base_start_date, full_end_date,
                                        args.chunk_size, args.delay)

    if final_data is not None and save_price_data(final_data):
        logging.info("🎉 Success!")
        if existing_data is not None:
            added = len(final_data) - len(existing_data)
            logging.info(f"📊 Added {added} days of data")
    else:
        logging.error("Failed")
        if os.path.exists(BACKUP_PATH):
            import shutil
            shutil.copy(BACKUP_PATH, PRICE_DATA_PATH)
            logging.info("✓ Restored from backup")
        exit(1)
