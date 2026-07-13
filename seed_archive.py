import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os
import time

# 1. Setup folders
os.makedirs('data/archive', exist_ok=True)

# 2. Ticker Discovery Logic
tickers = []
if os.path.exists('data/tickers.csv'):
    tickers = pd.read_csv('data/tickers.csv')['Code'].unique().tolist()
    print(f"Found {len(tickers)} stocks in master tickers.csv")
else:
    print("Master tickers.csv not found. Searching archive for all known codes...")
    archive_path = 'data/archive'
    all_files = [os.path.join(archive_path, f) for f in os.listdir(archive_path) if f.endswith('.csv')]
    
    if all_files:
        # Instead of picking one file, we combine the unique codes from ALL files
        temp_list = []
        for f in all_files:
            try:
                temp_list.extend(pd.read_csv(f)['Code'].unique().tolist())
            except:
                continue
        tickers = list(set(temp_list)) # This removes duplicates
        print(f"Discovered {len(tickers)} unique stocks across your existing archive.")
    else:
        print("No data found at all. Please create data/tickers.csv with your stock codes.")
        exit()

def bulk_seed():
    end_date = datetime.now()
    start_date = end_date - timedelta(days=120) 
    print(f"Starting download for {len(tickers)} stocks...")

    for code in tickers:
        symbol = f"{code}.T"
        try:
            df = yf.download(symbol, start=start_date, end=end_date, progress=False, auto_adjust=True)
            if not df.empty:
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
                df = df.reset_index()
                
                for _, row in df.iterrows():
                    raw_date = row['Date']
                    if isinstance(raw_date, pd.Series): raw_date = raw_date.iloc[0]
                    date_str = raw_date.strftime('%Y-%m-%d')
                    
                    path = f"data/archive/prices_{date_str}.csv"
                    new_row = pd.DataFrame([{'Code': code, 'Date': date_str, 'Close': round(float(row['Close']), 2), 'MarketCapB': 0}])

                    if os.path.exists(path):
                        existing = pd.read_csv(path)
                        if str(code) not in existing['Code'].astype(str).values:
                            pd.concat([existing, new_row]).to_csv(path, index=False)
                    else:
                        new_row.to_csv(path, index=False)
                print(f"  Done: {symbol}")
            time.sleep(1.0)
        except Exception as e:
            print(f"  Error for {symbol}: {e}")

if __name__ == "__main__":
    bulk_seed()