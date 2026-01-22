import pandas as pd
import yfinance as yf
from backend.screener_logic import RDTIndicators
import datetime

def test_form():
    target_date_str = "2026-01-02"
    target_date = pd.Timestamp(target_date_str)

    print(f"Testing FORM for date: {target_date_str}")

    start_date = target_date - pd.DateOffset(years=1, months=2)

    try:
        spy = yf.download("SPY", start=start_date, end=target_date + pd.DateOffset(days=5), progress=False, ignore_tz=True)
        form = yf.download("FORM", start=start_date, end=target_date + pd.DateOffset(days=5), progress=False, ignore_tz=True)

        if isinstance(spy.columns, pd.MultiIndex):
            spy.columns = spy.columns.droplevel(1)
        if isinstance(form.columns, pd.MultiIndex):
            form.columns = form.columns.droplevel(1)

        spy = spy[spy.index <= target_date]
        form = form[form.index <= target_date]

        if form.empty:
            print("FORM data empty.")
            return

        df_calc = RDTIndicators.calculate_all(form, spy)
        last_row = df_calc.iloc[-1]
        results = RDTIndicators.check_filters(last_row)

        print("\n--- Results ---")
        for k, v in results.items():
            print(f"{k}: {v}")

        print("\n--- Detailed Values ---")
        print(f"RRS: {last_row['RRS']} (> 0.0)")
        print(f"ADR%: {last_row['ADR_Percent']} (> 3.0)")
        print(f"Liq (Vol SMA 20): {last_row['Vol_SMA_20']} (> 1M)")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_form()
