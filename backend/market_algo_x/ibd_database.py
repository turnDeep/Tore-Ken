"""
IBD Database Manager

SQLiteデータベースを使用して、株価、EPS、その他の財務データを集約・管理します。
"""

import os
import sqlite3
from typing import List, Dict, Optional

import pandas as pd


class IBDDatabase:
    """IBD スクリーナー用のSQLiteデータベース管理クラス"""

    def __init__(self, db_path='data/ibd_data.db', silent=False):
        """
        Args:
            db_path: データベースファイルのパス
        """
        self.db_path = db_path
        self.conn = None
        self.initialize_database(silent)

    def initialize_database(self, silent=False):
        """データベースの初期化とテーブル作成"""
        # データベースファイルのディレクトリが存在することを確認
        db_dir = os.path.dirname(self.db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        cursor = self.conn.cursor()

        # 1. 銘柄マスターテーブル
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tickers (
                ticker TEXT PRIMARY KEY,
                exchange TEXT,
                name TEXT,
                sector TEXT,
                industry TEXT,
                market_cap REAL,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # 2. 株価履歴テーブル
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS price_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                date DATE NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                UNIQUE(ticker, date),
                FOREIGN KEY (ticker) REFERENCES tickers(ticker)
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_price_ticker_date ON price_history(ticker, date)')

        # 3. 四半期損益計算書テーブル
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS income_statements_quarterly (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                date DATE NOT NULL,
                fiscal_year INTEGER,
                fiscal_quarter INTEGER,
                revenue REAL,
                net_income REAL,
                eps REAL,
                eps_diluted REAL,
                UNIQUE(ticker, date),
                FOREIGN KEY (ticker) REFERENCES tickers(ticker)
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_income_q_ticker_date ON income_statements_quarterly(ticker, date)')

        # 4. 年次損益計算書テーブル
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS income_statements_annual (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                date DATE NOT NULL,
                fiscal_year INTEGER,
                revenue REAL,
                net_income REAL,
                eps REAL,
                eps_diluted REAL,
                UNIQUE(ticker, date),
                FOREIGN KEY (ticker) REFERENCES tickers(ticker)
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_income_a_ticker_date ON income_statements_annual(ticker, date)')

        # 4b. 年次貸借対照表テーブル
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS balance_sheet_annual (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                date DATE NOT NULL,
                fiscal_year INTEGER,
                total_assets REAL,
                total_liabilities REAL,
                total_stockholders_equity REAL,
                total_equity REAL,
                UNIQUE(ticker, date),
                FOREIGN KEY (ticker) REFERENCES tickers(ticker)
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_balance_ticker_date ON balance_sheet_annual(ticker, date)')

        # 5. 企業プロファイルテーブル
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS company_profiles (
                ticker TEXT PRIMARY KEY,
                company_name TEXT,
                sector TEXT,
                industry TEXT,
                market_cap REAL,
                description TEXT,
                ceo TEXT,
                website TEXT,
                country TEXT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (ticker) REFERENCES tickers(ticker)
            )
        ''')

        # 6. 計算済みRS値テーブル
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS calculated_rs (
                ticker TEXT PRIMARY KEY,
                rs_value REAL,
                roc_63d REAL,
                roc_126d REAL,
                roc_189d REAL,
                roc_252d REAL,
                calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (ticker) REFERENCES tickers(ticker)
            )
        ''')

        # 7. 計算済みEPS要素テーブル
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS calculated_eps (
                ticker TEXT PRIMARY KEY,
                eps_growth_last_qtr REAL,
                eps_growth_prev_qtr REAL,
                annual_growth_rate REAL,
                stability_score REAL,
                calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (ticker) REFERENCES tickers(ticker)
            )
        ''')

        # 8. 計算済みSMR要素テーブル
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS calculated_smr (
                ticker TEXT PRIMARY KEY,
                sales_growth_q1 REAL,
                sales_growth_q2 REAL,
                sales_growth_q3 REAL,
                avg_sales_growth_3q REAL,
                pretax_margin_annual REAL,
                aftertax_margin_quarterly REAL,
                roe_annual REAL,
                calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (ticker) REFERENCES tickers(ticker)
            )
        ''')

        # 9. 最終レーティングテーブル
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS calculated_ratings (
                ticker TEXT PRIMARY KEY,
                rs_rating REAL,
                eps_rating REAL,
                ad_rating TEXT,
                smr_rating TEXT,
                comp_rating REAL,
                price_vs_52w_high REAL,
                industry_group_rs REAL,
                calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (ticker) REFERENCES tickers(ticker)
            )
        ''')

        # 10. セクターパフォーマンステーブル
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sector_performance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sector TEXT NOT NULL,
                date DATE NOT NULL,
                change_percentage REAL,
                UNIQUE(sector, date)
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_sector_perf_date ON sector_performance(sector, date)')

        # 11. Industry Group RSテーブル
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS calculated_industry_group_rs (
                ticker TEXT PRIMARY KEY,
                sector TEXT,
                industry TEXT,
                stock_rs_value REAL,
                sector_rs_value REAL,
                industry_group_rs_value REAL,
                calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (ticker) REFERENCES tickers(ticker)
            )
        ''')

        self.conn.commit()
        if not silent:
            print(f"データベースを初期化しました: {self.db_path}")

    def close(self):
        """データベース接続を閉じる"""
        if self.conn:
            self.conn.close()

    # ==================== ティッカーマスター ====================

    def insert_ticker(self, ticker: str, exchange: str = None, name: str = None):
        """ティッカーをマスターテーブルに追加"""
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO tickers (ticker, exchange, name)
            VALUES (?, ?, ?)
        ''', (ticker, exchange, name))
        self.conn.commit()

    def insert_tickers_bulk(self, tickers_data: List[Dict]):
        """ティッカーを一括追加"""
        cursor = self.conn.cursor()
        cursor.executemany('''
            INSERT OR REPLACE INTO tickers (ticker, exchange, name)
            VALUES (:ticker, :exchange, :name)
        ''', tickers_data)
        self.conn.commit()

    def get_all_tickers(self) -> List[str]:
        """全ティッカーを取得"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT ticker FROM tickers ORDER BY ticker')
        return [row[0] for row in cursor.fetchall()]

    # ==================== 株価履歴 ====================

    def insert_price_history(self, ticker: str, prices_df: pd.DataFrame):
        """株価履歴を挿入（DataFrameから）"""
        if prices_df is None or len(prices_df) == 0:
            return

        # DataFrameをSQLiteに挿入
        records = []
        for _, row in prices_df.iterrows():
            records.append({
                'ticker': ticker,
                'date': row['date'].strftime('%Y-%m-%d') if isinstance(row['date'], pd.Timestamp) else row['date'],
                'open': row.get('open'),
                'high': row.get('high'),
                'low': row.get('low'),
                'close': row.get('close'),
                'volume': row.get('volume')
            })

        cursor = self.conn.cursor()
        cursor.executemany('''
            INSERT OR REPLACE INTO price_history (ticker, date, open, high, low, close, volume)
            VALUES (:ticker, :date, :open, :high, :low, :close, :volume)
        ''', records)
        self.conn.commit()

    def get_price_history(self, ticker: str, days: int = 300) -> Optional[pd.DataFrame]:
        """株価履歴を取得"""
        query = '''
            SELECT date, open, high, low, close, volume
            FROM price_history
            WHERE ticker = ?
            ORDER BY date DESC
            LIMIT ?
        '''
        df = pd.read_sql_query(query, self.conn, params=(ticker, days))

        if len(df) > 0:
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date').reset_index(drop=True)
            return df
        return None

    def get_latest_price_date(self) -> Optional[str]:
        """最新の価格データの日付を取得"""
        query = '''
            SELECT MAX(date) as latest_date
            FROM price_history
        '''
        cursor = self.conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()

        if result and result['latest_date']:
            return result['latest_date']
        return None

    def has_price_data(self, ticker: str, min_days: int = 252) -> bool:
        """指定された日数以上の株価データが存在するかチェック"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT COUNT(*) FROM price_history WHERE ticker = ?
        ''', (ticker,))
        count = cursor.fetchone()[0]
        return count >= min_days

    # ==================== 損益計算書 ====================

    def insert_income_statements_quarterly(self, ticker: str, statements: List[Dict]):
        """四半期損益計算書を挿入"""
        if not statements:
            return

        records = []
        for stmt in statements:
            records.append({
                'ticker': ticker,
                'date': stmt.get('date'),
                'fiscal_year': stmt.get('calendarYear'),
                'fiscal_quarter': stmt.get('period', '').replace('Q', '') if 'Q' in str(stmt.get('period', '')) else None,
                'revenue': stmt.get('revenue'),
                'net_income': stmt.get('netIncome'),
                'eps': stmt.get('eps'),
                'eps_diluted': stmt.get('epsdiluted')
            })

        cursor = self.conn.cursor()
        cursor.executemany('''
            INSERT OR REPLACE INTO income_statements_quarterly
            (ticker, date, fiscal_year, fiscal_quarter, revenue, net_income, eps, eps_diluted)
            VALUES (:ticker, :date, :fiscal_year, :fiscal_quarter, :revenue, :net_income, :eps, :eps_diluted)
        ''', records)
        self.conn.commit()

    def insert_income_statements_annual(self, ticker: str, statements: List[Dict]):
        """年次損益計算書を挿入"""
        if not statements:
            return

        records = []
        for stmt in statements:
            records.append({
                'ticker': ticker,
                'date': stmt.get('date'),
                'fiscal_year': stmt.get('calendarYear'),
                'revenue': stmt.get('revenue'),
                'net_income': stmt.get('netIncome'),
                'eps': stmt.get('eps'),
                'eps_diluted': stmt.get('epsdiluted')
            })

        cursor = self.conn.cursor()
        cursor.executemany('''
            INSERT OR REPLACE INTO income_statements_annual
            (ticker, date, fiscal_year, revenue, net_income, eps, eps_diluted)
            VALUES (:ticker, :date, :fiscal_year, :revenue, :net_income, :eps, :eps_diluted)
        ''', records)
        self.conn.commit()

    def get_income_statements_quarterly(self, ticker: str, limit: int = 8) -> List[Dict]:
        """四半期損益計算書を取得"""
        query = '''
            SELECT date, fiscal_year, fiscal_quarter, revenue, net_income, eps, eps_diluted
            FROM income_statements_quarterly
            WHERE ticker = ?
            ORDER BY date DESC
            LIMIT ?
        '''
        cursor = self.conn.cursor()
        cursor.execute(query, (ticker, limit))
        rows = cursor.fetchall()

        return [dict(row) for row in rows]

    def get_income_statements_annual(self, ticker: str, limit: int = 5) -> List[Dict]:
        """年次損益計算書を取得"""
        query = '''
            SELECT date, fiscal_year, revenue, net_income, eps, eps_diluted
            FROM income_statements_annual
            WHERE ticker = ?
            ORDER BY date DESC
            LIMIT ?
        '''
        cursor = self.conn.cursor()
        cursor.execute(query, (ticker, limit))
        rows = cursor.fetchall()

        return [dict(row) for row in rows]

    def has_income_data(self, ticker: str, min_quarters: int = 5) -> bool:
        """指定された四半期数以上の損益計算書データが存在するかチェック"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT COUNT(*) FROM income_statements_quarterly WHERE ticker = ?
        ''', (ticker,))
        count = cursor.fetchone()[0]
        return count >= min_quarters

    # ==================== 貸借対照表 ====================

    def insert_balance_sheet_annual(self, ticker: str, statements: List[Dict]):
        """年次貸借対照表を挿入"""
        if not statements:
            return

        records = []
        for stmt in statements:
            records.append({
                'ticker': ticker,
                'date': stmt.get('date'),
                'fiscal_year': stmt.get('calendarYear'),
                'total_assets': stmt.get('totalAssets'),
                'total_liabilities': stmt.get('totalLiabilities'),
                'total_stockholders_equity': stmt.get('totalStockholdersEquity'),
                'total_equity': stmt.get('totalEquity')
            })

        cursor = self.conn.cursor()
        cursor.executemany('''
            INSERT OR REPLACE INTO balance_sheet_annual
            (ticker, date, fiscal_year, total_assets, total_liabilities, total_stockholders_equity, total_equity)
            VALUES (:ticker, :date, :fiscal_year, :total_assets, :total_liabilities, :total_stockholders_equity, :total_equity)
        ''', records)
        self.conn.commit()

    def get_balance_sheet_annual(self, ticker: str, limit: int = 5) -> List[Dict]:
        """年次貸借対照表を取得"""
        query = '''
            SELECT date, fiscal_year, total_assets, total_liabilities, total_stockholders_equity, total_equity
            FROM balance_sheet_annual
            WHERE ticker = ?
            ORDER BY date DESC
            LIMIT ?
        '''
        cursor = self.conn.cursor()
        cursor.execute(query, (ticker, limit))
        rows = cursor.fetchall()

        return [dict(row) for row in rows]

    def has_balance_sheet_data(self, ticker: str, min_years: int = 1) -> bool:
        """指定された年数以上の貸借対照表データが存在するかチェック"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT COUNT(*) FROM balance_sheet_annual WHERE ticker = ?
        ''', (ticker,))
        count = cursor.fetchone()[0]
        return count >= min_years

    # ==================== 企業プロファイル ====================

    def insert_company_profile(self, ticker: str, profile: Dict):
        """企業プロファイルを挿入"""
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO company_profiles
            (ticker, company_name, sector, industry, market_cap, description, ceo, website, country, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (
            ticker,
            profile.get('companyName'),
            profile.get('sector'),
            profile.get('industry'),
            profile.get('mktCap'),
            profile.get('description'),
            profile.get('ceo'),
            profile.get('website'),
            profile.get('country')
        ))
        self.conn.commit()

    def get_company_profile(self, ticker: str) -> Optional[Dict]:
        """企業プロファイルを取得"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM company_profiles WHERE ticker = ?', (ticker,))
        row = cursor.fetchone()
        return dict(row) if row else None

    # ==================== 計算済みRS値 ====================

    def insert_calculated_rs(self, ticker: str, rs_value: float, roc_63d: float, roc_126d: float, roc_189d: float, roc_252d: float):
        """計算済みRS値を挿入"""
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO calculated_rs
            (ticker, rs_value, roc_63d, roc_126d, roc_189d, roc_252d, calculated_at)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (ticker, rs_value, roc_63d, roc_126d, roc_189d, roc_252d))
        self.conn.commit()

    def get_all_rs_values(self) -> Dict[str, float]:
        """全銘柄のRS値を取得"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT ticker, rs_value FROM calculated_rs WHERE rs_value IS NOT NULL')
        return {row[0]: row[1] for row in cursor.fetchall()}

    # ==================== 計算済みEPS要素 ====================

    def insert_calculated_eps(self, ticker: str, eps_growth_last_qtr: float, eps_growth_prev_qtr: float,
                             annual_growth_rate: float, stability_score: float):
        """計算済みEPS要素を挿入"""
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO calculated_eps
            (ticker, eps_growth_last_qtr, eps_growth_prev_qtr, annual_growth_rate, stability_score, calculated_at)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (ticker, eps_growth_last_qtr, eps_growth_prev_qtr, annual_growth_rate, stability_score))
        self.conn.commit()

    def get_all_eps_components(self) -> Dict[str, Dict]:
        """全銘柄のEPS要素を取得"""
        query = '''
            SELECT ticker, eps_growth_last_qtr, eps_growth_prev_qtr, annual_growth_rate, stability_score
            FROM calculated_eps
        '''
        cursor = self.conn.cursor()
        cursor.execute(query)

        result = {}
        for row in cursor.fetchall():
            result[row[0]] = {
                'eps_growth_last_qtr': row[1],
                'eps_growth_prev_qtr': row[2],
                'annual_growth_rate': row[3],
                'stability_score': row[4]
            }
        return result

    # ==================== 計算済みSMR要素 ====================

    def insert_calculated_smr(self, ticker: str, sales_growth_q1: float, sales_growth_q2: float,
                             sales_growth_q3: float, avg_sales_growth_3q: float,
                             pretax_margin_annual: float, aftertax_margin_quarterly: float,
                             roe_annual: float):
        """計算済みSMR要素を挿入"""
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO calculated_smr
            (ticker, sales_growth_q1, sales_growth_q2, sales_growth_q3, avg_sales_growth_3q,
             pretax_margin_annual, aftertax_margin_quarterly, roe_annual, calculated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (ticker, sales_growth_q1, sales_growth_q2, sales_growth_q3, avg_sales_growth_3q,
              pretax_margin_annual, aftertax_margin_quarterly, roe_annual))
        self.conn.commit()

    def get_all_smr_components(self) -> Dict[str, Dict]:
        """全銘柄のSMR要素を取得"""
        query = '''
            SELECT ticker, sales_growth_q1, sales_growth_q2, sales_growth_q3, avg_sales_growth_3q,
                   pretax_margin_annual, aftertax_margin_quarterly, roe_annual
            FROM calculated_smr
        '''
        cursor = self.conn.cursor()
        cursor.execute(query)

        result = {}
        for row in cursor.fetchall():
            result[row[0]] = {
                'sales_growth_q1': row[1],
                'sales_growth_q2': row[2],
                'sales_growth_q3': row[3],
                'avg_sales_growth_3q': row[4],
                'pretax_margin_annual': row[5],
                'aftertax_margin_quarterly': row[6],
                'roe_annual': row[7]
            }
        return result

    # ==================== 最終レーティング ====================

    def insert_calculated_rating(self, ticker: str, rs_rating: float, eps_rating: float,
                                 ad_rating: str, comp_rating: float, price_vs_52w_high: float,
                                 smr_rating: str = None, industry_group_rs: float = None):
        """最終レーティングを挿入"""
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO calculated_ratings
            (ticker, rs_rating, eps_rating, ad_rating, smr_rating, comp_rating, price_vs_52w_high, industry_group_rs, calculated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (ticker, rs_rating, eps_rating, ad_rating, smr_rating, comp_rating, price_vs_52w_high, industry_group_rs))
        self.conn.commit()

    def get_all_ratings(self) -> Dict[str, Dict]:
        """全銘柄のレーティングを取得"""
        query = '''
            SELECT ticker, rs_rating, eps_rating, ad_rating, smr_rating, comp_rating, price_vs_52w_high
            FROM calculated_ratings
        '''
        cursor = self.conn.cursor()
        cursor.execute(query)

        result = {}
        for row in cursor.fetchall():
            result[row[0]] = {
                'rs_rating': row[1],
                'eps_rating': row[2],
                'ad_rating': row[3],
                'smr_rating': row[4],
                'comp_rating': row[5],
                'price_vs_52w_high': row[6]
            }
        return result

    def get_rating(self, ticker: str) -> Optional[Dict]:
        """特定銘柄のレーティングを取得"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM calculated_ratings WHERE ticker = ?', (ticker,))
        row = cursor.fetchone()
        return dict(row) if row else None

    # ==================== セクターパフォーマンス ====================

    def insert_sector_performance(self, sector: str, date: str, change_percentage: float):
        """セクターパフォーマンスデータを挿入"""
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO sector_performance (sector, date, change_percentage)
            VALUES (?, ?, ?)
        ''', (sector, date, change_percentage))
        self.conn.commit()

    def insert_sector_performance_bulk(self, data: List[Dict]):
        """セクターパフォーマンスデータを一括挿入"""
        cursor = self.conn.cursor()
        cursor.executemany('''
            INSERT OR REPLACE INTO sector_performance (sector, date, change_percentage)
            VALUES (:sector, :date, :change_percentage)
        ''', data)
        self.conn.commit()

    def get_sector_performance_history(self, sector: str, days: int = 300) -> Optional[pd.DataFrame]:
        """特定セクターのパフォーマンス履歴を取得"""
        query = '''
            SELECT date, change_percentage
            FROM sector_performance
            WHERE sector = ?
            ORDER BY date DESC
            LIMIT ?
        '''
        df = pd.read_sql_query(query, self.conn, params=(sector, days))

        if len(df) > 0:
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date').reset_index(drop=True)
            return df
        return None

    def get_all_sectors(self) -> List[str]:
        """データベース内の全セクターを取得"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT DISTINCT sector FROM sector_performance ORDER BY sector')
        return [row[0] for row in cursor.fetchall() if row[0]]

    # ==================== Industry Group RS ====================

    def insert_industry_group_rs(self, ticker: str, sector: str, industry: str,
                                 stock_rs_value: float, sector_rs_value: float,
                                 industry_group_rs_value: float):
        """Industry Group RSを挿入"""
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO calculated_industry_group_rs
            (ticker, sector, industry, stock_rs_value, sector_rs_value, industry_group_rs_value, calculated_at)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (ticker, sector, industry, stock_rs_value, sector_rs_value, industry_group_rs_value))
        self.conn.commit()

    def get_all_industry_group_rs(self) -> Dict[str, float]:
        """全銘柄のIndustry Group RS値を取得"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT ticker, industry_group_rs_value FROM calculated_industry_group_rs WHERE industry_group_rs_value IS NOT NULL')
        return {row[0]: row[1] for row in cursor.fetchall()}

    def get_industry_group_rs(self, ticker: str) -> Optional[Dict]:
        """特定銘柄のIndustry Group RSを取得"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM calculated_industry_group_rs WHERE ticker = ?', (ticker,))
        row = cursor.fetchone()
        return dict(row) if row else None

    # ==================== ユーティリティ ====================

    def clear_all_data(self):
        """全データをクリア（テスト用）"""
        cursor = self.conn.cursor()
        tables = [
            'calculated_ratings', 'calculated_eps', 'calculated_rs',
            'company_profiles', 'income_statements_annual', 'income_statements_quarterly',
            'price_history', 'tickers'
        ]
        for table in tables:
            cursor.execute(f'DELETE FROM {table}')
        self.conn.commit()
        print("全データをクリアしました")

    def get_database_stats(self):
        """データベースの統計情報を表示"""
        cursor = self.conn.cursor()

        stats = {}
        tables = [
            'tickers', 'price_history', 'income_statements_quarterly',
            'income_statements_annual', 'company_profiles', 'calculated_rs',
            'calculated_eps', 'calculated_smr', 'calculated_ratings',
            'sector_performance', 'calculated_industry_group_rs'
        ]

        for table in tables:
            try:
                cursor.execute(f'SELECT COUNT(*) FROM {table}')
                count = cursor.fetchone()[0]
                stats[table] = count
            except:
                stats[table] = 0

        print("\n=== データベース統計 ===")
        for table, count in stats.items():
            print(f"  {table}: {count:,} レコード")

        return stats
