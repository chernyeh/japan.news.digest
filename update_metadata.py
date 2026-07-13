import yfinance as yf
import pandas as pd
import os
import time

def fetch_metadata():
    if not os.path.exists('data/tickers.csv'):
        print("Error: data/tickers.csv not found.")
        return

    codes = pd.read_csv('data/tickers.csv')['Code'].unique().tolist()
    metadata = []
    
    print(f"Fetching metadata for {len(codes)} stocks. Please wait...")

    for i, code in enumerate(codes):
        ticker_str = f"{code}.T"
        try:
            t = yf.Ticker(ticker_str)
            # We use .info to get the 'sharesOutstanding'
            info = t.info
            
            metadata.append({
                'Code': code,
                'Name': info.get('longName', 'N/A'),
                'Shares': info.get('sharesOutstanding', 0),
                'Sector': info.get('sector', 'N/A')
            })
            print(f"[{i+1}/{len(codes)}] Processed {ticker_str}")
            
        except Exception as e:
            print(f"Error on {ticker_str}: {e}")
            metadata.append({'Code': code, 'Name': 'N/A', 'Shares': 0, 'Sector': 'N/A'})

        # Mandatory breather to prevent Yahoo 429 errors
        time.sleep(1.0)

        # Save progress every 50 stocks in case of a crash
        if (i + 1) % 50 == 0:
            pd.DataFrame(metadata).to_csv('data/metadata.csv', index=False)

    pd.DataFrame(metadata).to_csv('data/metadata.csv', index=False)
    print("Success: data/metadata.csv created.")

if __name__ == "__main__":
    fetch_metadata()