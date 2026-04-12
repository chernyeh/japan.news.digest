import yfinance as yf
import pandas as pd
import glob
import os

def update_market_caps():
    # 1. Find the latest price file
    files = glob.glob('data/archive/prices_*.csv')
    if not files:
        print("No price files found.")
        return
    
    latest_file = max(files, key=os.path.getctime)
    print(f"Targeting: {latest_file}")
    
    df = pd.read_csv(latest_file)
    
    # 2. Find rows missing Market Cap
    mask = df['MarketCapB'].isna() | (df['MarketCapB'] == 0)
    to_update = df[mask]['Code'].tolist()
    
    print(f"Updating {len(to_update)} stocks via Yahoo Finance...")

    for code in to_update:
        try:
            ticker = yf.Ticker(f"{code}.T")
            cap = ticker.info.get('marketCap', 0) / 1_000_000_000
            if cap > 0:
                df.loc[df['Code'] == code, 'MarketCapB'] = round(cap, 2)
                print(f"Success: {code} -> {round(cap, 2)}B")
        except:
            continue

    # 3. Save it back
    df.to_csv(latest_file, index=False)
    print("Market caps updated successfully.")

if __name__ == "__main__":
    update_market_caps()