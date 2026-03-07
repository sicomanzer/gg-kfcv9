import yfinance as yf
import pandas as pd
import requests
import numpy as np
from consts import SET100_TICKERS, LONG_TERM_GROWTH, RISK_FREE_RATE, MARKET_RETURN
import concurrent.futures
import time
import random

def load_tickers():
    import json
    import os
    try:
        with open('tickers.json', 'r') as f:
            return json.load(f)
    except:
        return []

def get_vix_data():
    """
    Fetches the CBOE Volatility Index (^VIX) data.
    Returns: {
        'current': float,
        'change': float, 
        'previous_close': float,
        'history': pd.DataFrame (1 Year)
    }
    """
    try:
        # Ticker for VIX
        vix = yf.Ticker("^VIX")
        
        # Get History (5 Years for chart)
        hist = vix.history(period="5y")
        
        if hist.empty:
            return None
            
        # Get Current Data
        # Use last row of history as 'current' if market is closed, 
        # or use fast_info/info if available/reliable. 
        # For indices, history last row is usually safest for "closing/current" price.
        current_data = hist.iloc[-1]
        previous_data = hist.iloc[-2] if len(hist) > 1 else current_data
        
        current_price = current_data['Close']
        previous_close = previous_data['Close']
        change = current_price - previous_close
        pct_change = (change / previous_close) * 100
        
        return {
            'current': current_price,
            'change': change,
            'pct_change': pct_change,
            'history': hist
        }
    except Exception as e:
        print(f"Error fetching VIX: {e}")
        return None

def get_fear_and_greed_index():
    """
    Fetches the CNN Fear and Greed Index.
    Returns: {
        'score': float,
        'rating': str,
        'timestamp': str,
        'previous_close': float,
        'previous_1_week': float,
        'previous_1_month': float,
        'previous_1_year': float
    }
    """
    try:
        url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Referer": "https://www.cnn.com/",
            "Origin": "https://www.cnn.com"
        }
        
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        fng = data.get('fear_and_greed', {})
        
        if not fng:
            return None
            
        return {
            'score': fng.get('score'),
            'rating': fng.get('rating'),
            'timestamp': fng.get('timestamp'),
            'previous_close': fng.get('previous_close'),
            'previous_1_week': fng.get('previous_1_week'),
            'previous_1_month': fng.get('previous_1_month'),
            'previous_1_year': fng.get('previous_1_year')
        }
    except Exception as e:
        print(f"Error fetching Fear and Greed Index: {e}")
        return None

def get_set_index_data():
    """
    Fetches the SET Index (^SET.BK) data.
    Returns: {
        'current': float,
        'change': float, 
        'pct_change': float,
        'high': float,
        'low': float,
        'volume': int,
        'value': float,
        'history': pd.DataFrame (1 Day, 5m interval)
    }
    """
    try:
        ticker = yf.Ticker("^SET.BK")
        
        # 1. Get Intraday History for Chart
        hist = ticker.history(period="1d", interval="5m")
        
        if hist.empty:
            return None
            
        # 2. Get Previous Close to calculate change
        # Try info first
        info = ticker.info
        prev_close = info.get('previousClose') or info.get('regularMarketPreviousClose')
        
        # If info fails, fetch daily history
        if prev_close is None:
            hist_daily = ticker.history(period="5d")
            if len(hist_daily) >= 2:
                prev_close = hist_daily['Close'].iloc[-2]
            else:
                prev_close = hist['Open'].iloc[0] # Fallback
        
        current_price = hist['Close'].iloc[-1]
        change = current_price - prev_close
        pct_change = (change / prev_close) * 100
        
        high = hist['High'].max()
        low = hist['Low'].min()
        volume = hist['Volume'].sum()
        
        # Value (Turnover) is hard to get exactly from yfinance for index
        # We can approximate or just leave it as None/0 if unavailable
        value = 0 
        
        return {
            'current': current_price,
            'change': change,
            'pct_change': pct_change,
            'high': high,
            'low': low,
            'volume': volume,
            'value': value,
            'history': hist,
            'prev_close': prev_close
        }
    except Exception as e:
        print(f"Error fetching SET data: {e}")
        return None

def save_tickers(tickers):
    import json
    with open('tickers.json', 'w') as f:
        json.dump(tickers, f)

def _calculate_ibd_ratio(bs_series):
    """
    Helper to calculate IBD/E Ratio from a Balance Sheet Series (latest year).
    Returns float or None.
    """
    try:
        # 1. Calculate Interest Bearing Debt (IBD)
        # Try to find Short Term part
        short_term_debt = 0
        st_keys = ['Short Long Term Debt', 'Current Long Term Debt', 'Current Debt', 'Short Term Debt'] 
        
        found_st = False
        for k in st_keys:
            if k in bs_series.index and not pd.isna(bs_series[k]):
                short_term_debt = bs_series[k]
                found_st = True
                break
        
        # Try to find Long Term part
        long_term_debt = 0
        lt_keys = ['Long Term Debt', 'Long Term Debt And Capital Lease Obligation', 'Long Term Debt Excluding Current Portion']
        
        found_lt = False
        for k in lt_keys:
            if k in bs_series.index and not pd.isna(bs_series[k]):
                long_term_debt = bs_series[k]
                found_lt = True
                break
                
        # If neither found, check Total Debt as fallback if available
        ibd = 0
        if not found_st and not found_lt:
             if 'Total Debt' in bs_series.index and not pd.isna(bs_series['Total Debt']):
                 ibd = bs_series['Total Debt']
             else:
                 # If absolutely no debt info found, return None
                 return None
        else:
             ibd = short_term_debt + long_term_debt

        # 2. Calculate Equity
        # User: 'Stockholders Equity' or 'Total Stockholder Equity' or 'Common Stock Equity' (Choose max non-negative)
        equity_keys = ['Stockholders Equity', 'Total Stockholder Equity', 'Common Stock Equity', 'Total Equity Gross Minority Interest']
        
        equity_values = []
        for k in equity_keys:
            if k in bs_series.index and not pd.isna(bs_series[k]):
                val = bs_series[k]
                if val > 0: # User said "non-negative"
                    equity_values.append(val)
        
        if not equity_values:
            return None
            
        equity = max(equity_values)
        
        # 3. Ratio
        if equity == 0: return None
        
        return round(ibd / equity, 2)
        
    except Exception:
        return None

def calculate_ibd_to_equity(ticker_symbol: str) -> float | None:
    """
    Calculates IBD/E Ratio (Interest Bearing Debt to Equity)
    IBD = Short Long Term Debt + Long Term Debt
    Equity = Max(Stockholders Equity)
    """
    try:
        # Handle suffix
        if not ticker_symbol.endswith('.BK'):
            full_ticker = f"{ticker_symbol}.BK"
        else:
            full_ticker = ticker_symbol
            
        stock = yf.Ticker(full_ticker)
        bs = stock.balance_sheet
        
        if bs.empty:
            return None
            
        # Use latest annual
        latest_bs = bs.iloc[:, 0]
        
        return _calculate_ibd_ratio(latest_bs)
        
    except Exception as e:
        print(f"Error calculating IBD/E for {ticker_symbol}: {e}")
        return None

def get_stock_data(ticker_symbol):
    """
    Fetches raw financial data for a single stock from yfinance.
    Includes delay and retry logic to avoid rate limits.
    """
    # Add random delay to prevent rate limiting (429)
    # Adjusted: Reduced delay to speed up while keeping some safety
    time.sleep(random.uniform(0.01, 0.1))
    
    max_retries = 3
    retry_delay = 1 # Reduced retry delay
    
    for attempt in range(max_retries):
        try:
            if not ticker_symbol.endswith('.BK'):
                full_ticker = f"{ticker_symbol}.BK"
            else:
                full_ticker = ticker_symbol
                
            stock = yf.Ticker(full_ticker)
            info = stock.info
            
            # Helper to safely get float or np.nan
            def get_float(key):
                val = info.get(key)
                if val is None:
                    return np.nan
                try:
                    return float(val)
                except:
                    return np.nan


            # --- MANUAL CALCULATION OVERRIDE (For Accuracy) ---
            ibd_e = None
            
            # 1. Fetch Basic Financials (One Call)
            # Try to patch 0/null values by calculating from raw statements if available
            try:
                # We use 'fast_info' for price/market_cap which is faster and often more up-to-date
                current_price = stock.fast_info.last_price
                market_cap = stock.fast_info.market_cap
                
                # Use 'income_stmt' and 'balance_sheet' (new yf)
                fin = stock.income_stmt
                bs = stock.balance_sheet
                
                if not fin.empty and not bs.empty:
                    # Latest Annual
                    latest_fin = fin.iloc[:, 0]
                    latest_bs = bs.iloc[:, 0]
                    
                    # EPS
                    # 'Basic EPS', 'Diluted EPS'
                    eps_calc = latest_fin.get('Basic EPS', 0)
                    if pd.isna(eps_calc) or eps_calc == 0: eps_calc = latest_fin.get('Diluted EPS', 0)
                    
                    # Equity
                    equity_calc = latest_bs.get('Stockholders Equity', latest_bs.get('Total Equity Gross Minority Interest', 0))
                    
                    # Net Income
                    net_income_calc = latest_fin.get('Net Income', latest_fin.get('Net Income Common Stockholders', 0))
                    if pd.isna(net_income_calc): net_income_calc = 0
                    
                    # Shares
                    shares_calc = latest_bs.get('Ordinary Shares Number', latest_bs.get('Share Issued', 0))
                    
                    # BVPS
                    bvps_calc = equity_calc / shares_calc if shares_calc > 0 else 0
                    
                    # ROE
                    roe_calc = net_income_calc / equity_calc if equity_calc > 0 else 0
                    
                    # NPM
                    total_rev = latest_fin.get('Total Revenue', 0)
                    npm_calc = net_income_calc / total_rev if total_rev > 0 else 0
                    
                    # D/E
                    total_debt = latest_bs.get('Total Debt', latest_bs.get('Total Liabilities Net Minority Interest', 0)) # Fallback to TL
                    de_calc = total_debt / equity_calc if equity_calc > 0 else 0
                    
                    # Apply Overrides if API is weak
                    get_float_safe = lambda k, default: float(info.get(k)) if info.get(k) is not None else default
                    
                    eps = get_float_safe('trailingEps', eps_calc)
                    if eps == 0 and eps_calc != 0: eps = eps_calc # Priority Correction
                    
                    bvps = get_float_safe('bookValue', bvps_calc)
                    if bvps == 0 and bvps_calc != 0: bvps = bvps_calc
                    
                    roe = get_float_safe('returnOnEquity', roe_calc)
                    if (roe == 0 or pd.isna(roe)) and roe_calc != 0: roe = roe_calc
                    
                    npm = get_float_safe('profitMargins', npm_calc)
                    if (npm == 0 or pd.isna(npm)) and npm_calc != 0: npm = npm_calc
                    
                    de = get_float_safe('debtToEquity', de_calc * 100) # API uses %, we use ratio in calc
                    
                    # IBD/E Ratio
                    ibd_e = _calculate_ibd_ratio(latest_bs)
                    
                else:
                    # Fallback to direct info
                    eps = get_float('trailingEps')
                    bvps = get_float('bookValue')
                    roe = get_float('returnOnEquity')
                    npm = get_float('profitMargins')
                    de = get_float('debtToEquity')

            except Exception as e_calc:
                # print(f"Calc error {ticker_symbol}: {e_calc}")
                # Valid fallback
                current_price = get_float('currentPrice')
                market_cap = get_float('marketCap')
                eps = get_float('trailingEps')
                bvps = get_float('bookValue')
                roe = get_float('returnOnEquity')
                npm = get_float('profitMargins')
                de = get_float('debtToEquity')

            data = {
                'symbol': ticker_symbol, # keep original without .BK for display
                'price': current_price,
                'beta': get_float('beta'),
                'dividendRate': get_float('dividendRate'),
                'dividendYield': get_float('dividendYield'),
                'payoutRatio': get_float('payoutRatio'),
                'trailingEps': eps,
                'bookValue': bvps,
                'returnOnEquity': roe,
                'longName': info.get('longName', ticker_symbol),
                'sector': info.get('sector', 'Unknown'),
                'summary': info.get('longBusinessSummary', 'No description available.'),
                # Pro Fields
                'targetPrice': get_float('targetMeanPrice'),
                'recommendation': get_float('recommendationMean'),
                'pegRatio': get_float('pegRatio'),
                'debtToEquity': de,
                'ibdToEquity': ibd_e,
                'profitMargins': npm,
                'revenueGrowth': get_float('revenueGrowth'),
                'earningsGrowth': get_float('earningsGrowth'),
                'ebitda': get_float('ebitda'),
                'returnOnAssets': get_float('returnOnAssets'),
                'currentRatio': get_float('currentRatio'),
                'forwardEps': get_float('forwardEps'),
                'marketCap': market_cap,
                'grossMargins': get_float('grossMargins'),
                'operatingMargins': get_float('operatingMargins'),
                'enterpriseToEbitda': get_float('enterpriseToEbitda'),
                'quickRatio': get_float('quickRatio'),
                # VI Score 2.0 Additions
                'freeCashflow': get_float('freeCashflow'),
                'operatingCashflow': get_float('operatingCashflow'),
                'totalRevenue': get_float('totalRevenue'),
                # Metadata for Verification
                'last_price_time': info.get('regularMarketTime', 0), # Unix Timestamp
                'currency': info.get('currency', 'THB'),
                'exchange': info.get('exchange', 'SET'),
            }
            return data

        except Exception as e:
            if "429" in str(e) or "Too Many Requests" in str(e):
                print(f"Rate limit hit for {ticker_symbol}, retrying in {retry_delay}s... (Attempt {attempt+1}/{max_retries})")
                time.sleep(retry_delay * (attempt + 1)) # Exponential backoff
                continue
            print(f"Error fetching {ticker_symbol}: {e}")
            return None
            
    return None

def get_financial_history(ticker_symbol):
    """
    Fetches historical financial statements for plotting.
    """
    try:
        if not ticker_symbol.endswith('.BK'):
            full_ticker = f"{ticker_symbol}.BK"
        else:
            full_ticker = ticker_symbol
            
        stock = yf.Ticker(full_ticker)
        
        # Get financials (Income Statement) and Balance Sheet
        fin = stock.financials.T
        
        # Create a simple df for plotting
        # Sort by date ascending
        fin = fin.sort_index(ascending=True)
        
        data = pd.DataFrame()
        
        # Revenue
        if 'Total Revenue' in fin.columns:
            data['Revenue'] = fin['Total Revenue']
        
        # Net Income
        if 'Net Income' in fin.columns:
            data['Net Profit'] = fin['Net Income']
        elif 'Net Income Common Stockholders' in fin.columns:
             data['Net Profit'] = fin['Net Income Common Stockholders']
            
        if 'Basic EPS' in fin.columns:
            data['EPS'] = fin['Basic EPS']
        
        # --- Ratio Calculations (Available Years) ---
        # Need Balance Sheet for granular equity/assets
        bs = stock.balance_sheet.T
        bs = bs.sort_index(ascending=True)
        
        # Prepare for merge
        fin['Year'] = fin.index.strftime('%Y')
        bs['Year'] = bs.index.strftime('%Y')
        
        # Merge
        # Inner merge to ensure we have both numerator and denominator
        merged = pd.merge(fin, bs, on='Year', how='inner', suffixes=('', '_bs'))
        merged.index = merged['Year']
        
        final_data = pd.DataFrame(index=merged.index)
        
        # 1. EPS & Revenue
        if 'Basic EPS' in merged.columns:
             final_data['EPS'] = merged['Basic EPS']
        if 'Total Revenue' in merged.columns:
             final_data['Revenue'] = merged['Total Revenue']
        if 'Net Income' in merged.columns:
             final_data['Net Profit'] = merged['Net Income']

        # 2. Profitability
        # NPM = Net Income / Revenue
        if 'Net Income' in merged.columns and 'Total Revenue' in merged.columns:
            final_data['NPM (%)'] = (merged['Net Income'] / merged['Total Revenue']) * 100
            
        # ROE = Net Income / Equity
        # Equity keys vary: 'Stockholders Equity', 'Total Equity Gross Minority Interest'
        equity_col = 'Stockholders Equity' if 'Stockholders Equity' in merged.columns else 'Total Equity Gross Minority Interest'
        if 'Net Income' in merged.columns and equity_col in merged.columns:
            final_data['ROE (%)'] = (merged['Net Income'] / merged[equity_col]) * 100
            
        # ROA = Net Income / Total Assets
        if 'Net Income' in merged.columns and 'Total Assets' in merged.columns:
            final_data['ROA (%)'] = (merged['Net Income'] / merged['Total Assets']) * 100
            
        # 3. Health
        # D/E = Total Debt / Equity
        # Debt keys: 'Total Debt'
        if 'Total Debt' in merged.columns and equity_col in merged.columns:
            final_data['D/E (x)'] = merged['Total Debt'] / merged[equity_col]
        elif 'Net Debt' in merged.columns and equity_col in merged.columns: # fallback
             final_data['D/E (x)'] = merged['Net Debt'] / merged[equity_col]

        return final_data
            
    except Exception as e:
        print(f"Error fetching history for {ticker_symbol}: {e}")
        return pd.DataFrame()

def get_dividend_history(ticker_symbol, years=10):
    """
    Fetches dividend history for plotting.
    Returns: DataFrame with Year, Dividend
    """
    try:
        if not ticker_symbol.endswith('.BK'):
            full_ticker = f"{ticker_symbol}.BK"
        else:
            full_ticker = ticker_symbol
            
        stock = yf.Ticker(full_ticker)
        
        # Get Dividends
        divs = stock.dividends
        
        if divs.empty:
            return pd.DataFrame()
            
        # Resample to Annual Sum
        # Ensure index is datetime
        divs.index = pd.to_datetime(divs.index).tz_localize(None)
        
        # Filter last N years
        current_year = pd.Timestamp.now().year
        start_year = current_year - years
        divs = divs[divs.index.year >= start_year]
        
        # Group by Year
        annual_divs = divs.groupby(divs.index.year).sum()
        
        # Create DF
        df = pd.DataFrame({'Year': annual_divs.index, 'Dividend': annual_divs.values})
        df['Year'] = df['Year'].astype(str)
        
        return df
        
    except Exception as e:
        print(f"Error fetching dividends for {ticker_symbol}: {e}")
        return pd.DataFrame()



def get_dividends_batch(tickers, years=5):
    """
    Fetches dividend history for multiple tickers efficiently.
    Returns: dict {symbol: {year: dividend_amount}}
    """
    results = {}
    
    # Use ThreadPool to fetch concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        future_to_ticker = {executor.submit(get_dividend_history, t, years): t for t in tickers}
        
        for future in concurrent.futures.as_completed(future_to_ticker):
            t = future_to_ticker[future]
            try:
                df = future.result()
                if not df.empty:
                    # Convert to dict {year: div}
                    # Year is string in df
                    results[t] = dict(zip(df['Year'], df['Dividend']))
            except Exception as e:
                print(f"Error batch fetching dividends for {t}: {e}")
                
    return results

def calculate_magic_formula_and_f_score(ticker_symbol):
    """
    Fetches detailed financials to calculate:
    1. Magic Formula (ROC, Earnings Yield)
    2. Piotroski F-Score (0-9)
    """
    try:
        if not ticker_symbol.endswith('.BK'):
            full_ticker = f"{ticker_symbol}.BK"
        else:
            full_ticker = ticker_symbol
            
        stock = yf.Ticker(full_ticker)
        
        # Fetch Data (this triggers multiple requests)
        info = stock.info
        fin = stock.financials.T.sort_index(ascending=False) # Recent first
        bs = stock.balance_sheet.T.sort_index(ascending=False)
        cf = stock.cashflow.T.sort_index(ascending=False)
        
        if fin.empty or bs.empty:
            return None
            
        # Get TTM or Most Recent Year
        # For simplicity in screening, we often use Most Recent Year (MRY) if TTM not fully available in tables
        # yfinance financials are usually annual.
        
        # --- 1. MAGIC FORMULA ---
        # ROC = EBIT / (Net Working Capital + Net Fixed Assets)
        # Earnings Yield = EBIT / Enterprise Value
        
        ebit = fin['EBIT'].iloc[0] if 'EBIT' in fin.columns else (fin['Net Income'].iloc[0] + fin['Interest Expense'].iloc[0] + fin['Tax Provision'].iloc[0] if 'Interest Expense' in fin.columns else fin['Net Income'].iloc[0])
        
        # Working Capital = Total Current Assets - Total Current Liabilities
        # Keys: 'Current Assets', 'Current Liabilities'
        curr_assets = bs['Current Assets'].iloc[0] if 'Current Assets' in bs.columns else (bs['Total Current Assets'].iloc[0] if 'Total Current Assets' in bs.columns else 0)
        curr_liab = bs['Current Liabilities'].iloc[0] if 'Current Liabilities' in bs.columns else (bs['Total Current Liabilities'].iloc[0] if 'Total Current Liabilities' in bs.columns else 0)
        
        working_capital = bs['Working Capital'].iloc[0] if 'Working Capital' in bs.columns else (curr_assets - curr_liab)
        
        # Net Fixed Assets = Total Assets - Total Current Assets (Rough proxy for Net PPE + Intangibles?)
        # Better: Net PPE
        total_assets = bs['Total Assets'].iloc[0]
        net_fixed_assets = bs['Net Tangible Assets'].iloc[0] if 'Net Tangible Assets' in bs.columns else (total_assets - curr_assets)
        
        invested_capital = working_capital + net_fixed_assets
        if invested_capital <= 0: invested_capital = 1 # Avoid div by zero
        
        roc = ebit / invested_capital
        
        # EV
        enterprise_value = info.get('enterpriseValue', 0)
        if enterprise_value is None or enterprise_value == 0:
            # Approx: Market Cap + Total Debt - Cash
            market_cap = info.get('marketCap', 0)
            total_debt = bs['Total Debt'].iloc[0] if 'Total Debt' in bs.columns else 0
            cash = bs['Cash And Cash Equivalents'].iloc[0] if 'Cash And Cash Equivalents' in bs.columns else 0
            enterprise_value = market_cap + total_debt - cash
            
        earnings_yield = ebit / enterprise_value if enterprise_value > 0 else 0
        
        # --- 2. PIOTROSKI F-SCORE ---
        # Needs Current Year (0) vs Previous Year (1)
        f_score = 0
        
        if len(fin) >= 2 and len(bs) >= 2:
            # 1. ROA > 0
            net_income = fin['Net Income'].iloc[0] if 'Net Income' in fin.columns else fin['Net Income Common Stockholders'].iloc[0]
            avg_assets = (bs['Total Assets'].iloc[0] + bs['Total Assets'].iloc[1]) / 2
            roa = net_income / avg_assets
            if roa > 0: f_score += 1
            
            # 2. CFO > 0
            cfo = cf['Operating Cash Flow'].iloc[0] if 'Operating Cash Flow' in cf.columns else 0
            if cfo > 0: f_score += 1
            
            # 3. Delta ROA > 0
            net_income_prev = fin['Net Income'].iloc[1] if 'Net Income' in fin.columns else fin['Net Income Common Stockholders'].iloc[1]
            avg_assets_prev = bs['Total Assets'].iloc[1] # Simplify
            roa_prev = net_income_prev / avg_assets_prev
            if roa > roa_prev: f_score += 1
            
            # 4. Accrual (CFO > Net Income)
            if cfo > net_income: f_score += 1
            
            # 5. Delta Leverage < 0 (Long Term Debt / Assets)
            lt_debt = bs['Long Term Debt'].iloc[0] if 'Long Term Debt' in bs.columns else 0
            lt_debt_prev = bs['Long Term Debt'].iloc[1] if 'Long Term Debt' in bs.columns else 0
            lev = lt_debt / avg_assets
            lev_prev = lt_debt_prev / avg_assets_prev
            if lev < lev_prev: f_score += 1
            
            # 6. Delta Current Ratio > 0
            curr_assets = bs['Current Assets'].iloc[0] if 'Current Assets' in bs.columns else (bs['Total Current Assets'].iloc[0] if 'Total Current Assets' in bs.columns else 0)
            curr_liab = bs['Current Liabilities'].iloc[0] if 'Current Liabilities' in bs.columns else (bs['Total Current Liabilities'].iloc[0] if 'Total Current Liabilities' in bs.columns else 0)
            curr_ratio = curr_assets / curr_liab if curr_liab > 0 else 0
            
            curr_assets_prev = bs['Current Assets'].iloc[1] if 'Current Assets' in bs.columns else (bs['Total Current Assets'].iloc[1] if 'Total Current Assets' in bs.columns else 0)
            curr_liab_prev = bs['Current Liabilities'].iloc[1] if 'Current Liabilities' in bs.columns else (bs['Total Current Liabilities'].iloc[1] if 'Total Current Liabilities' in bs.columns else 0)
            curr_ratio_prev = curr_assets_prev / curr_liab_prev if curr_liab_prev > 0 else 0
            
            if curr_ratio > curr_ratio_prev: f_score += 1
            
            # 7. Delta Shares Outstanding <= 0 (No Dilution)
            shares = bs['Ordinary Shares Number'].iloc[0] if 'Ordinary Shares Number' in bs.columns else bs['Share Issued'].iloc[0]
            shares_prev = bs['Ordinary Shares Number'].iloc[1] if 'Ordinary Shares Number' in bs.columns else bs['Share Issued'].iloc[1]
            if shares <= shares_prev: f_score += 1
            
            # 8. Delta Gross Margin > 0
            gp = fin['Gross Profit'].iloc[0]
            rev = fin['Total Revenue'].iloc[0]
            gm = gp / rev if rev > 0 else 0
            
            gp_prev = fin['Gross Profit'].iloc[1]
            rev_prev = fin['Total Revenue'].iloc[1]
            gm_prev = gp_prev / rev_prev if rev_prev > 0 else 0
            
            if gm > gm_prev: f_score += 1
            
            # 9. Delta Asset Turnover > 0 (Revenue / Assets)
            at = rev / avg_assets
            at_prev = rev_prev / avg_assets_prev
            if at > at_prev: f_score += 1
            
        else:
            # Fallback if history not enough (New IPO?)
            f_score = -1 
            
        # --- 3. GRAHAM NUMBER & FCF ---
        # Graham Number = Sqrt(22.5 * EPS * BVPS)
        # Use recent annual
        eps = fin['Basic EPS'].iloc[0] if 'Basic EPS' in fin.columns else 0
        
        # Book Value Per Share = Equity / Shares
        equity = bs['Stockholders Equity'].iloc[0] if 'Stockholders Equity' in bs.columns else bs['Total Equity Gross Minority Interest'].iloc[0]
        shares_outstanding = bs['Ordinary Shares Number'].iloc[0] if 'Ordinary Shares Number' in bs.columns else bs['Share Issued'].iloc[0]
        
        bvps = equity / shares_outstanding if shares_outstanding > 0 else 0
        
        graham_number = 0
        if eps > 0 and bvps > 0:
            graham_number = (22.5 * eps * bvps) ** 0.5
            
        # Free Cash Flow (FCF)
        # FCF = Operating Cash Flow - Capital Expenditure
        # yfinance Cashflow table usually has 'Free Cash Flow' calculated or we do it manually
        
        fcf = 0
        if 'Free Cash Flow' in cf.columns:
            fcf = cf['Free Cash Flow'].iloc[0]
        else:
            # Manual
            cfo = cf['Operating Cash Flow'].iloc[0] if 'Operating Cash Flow' in cf.columns else 0
            capex = cf['Capital Expenditure'].iloc[0] if 'Capital Expenditure' in cf.columns else 0
            fcf = cfo + capex # Capex is usually negative in cashflow statement
            
        # FCF Yield = FCF / Market Cap
        market_cap = info.get('marketCap', 0)
        if market_cap == 0 and shares_outstanding > 0:
             # Estimate MC
             current_price = info.get('currentPrice', 0)
             market_cap = current_price * shares_outstanding
             
        fcf_yield = fcf / market_cap if market_cap > 0 else 0
        
        # --- 4. ALTMAN Z-SCORE (Bankruptcy Risk) ---
        # Z = 1.2A + 1.4B + 3.3C + 0.6D + 1.0E (Original Manufacturer)
        # Z = 6.56A + 3.26B + 6.72C + 1.05D (Emerging Market / Non-Manufacturer Model) - Often better for general use
        # Let's use the standard one but handle missing data carefully
        # A = Working Capital / Total Assets
        # B = Retained Earnings / Total Assets
        # C = EBIT / Total Assets
        # D = Market Value of Equity / Total Liabilities
        # E = Sales / Total Assets
        
        retained_earnings = bs['Retained Earnings'].iloc[0] if 'Retained Earnings' in bs.columns else 0
        total_liabilities = bs['Total Liabilities Net Minority Interest'].iloc[0] if 'Total Liabilities Net Minority Interest' in bs.columns else (bs['Total Liabilities'].iloc[0] if 'Total Liabilities' in bs.columns else 0)
        total_revenue = fin['Total Revenue'].iloc[0]
        
        A = working_capital / total_assets if total_assets > 0 else 0
        B = retained_earnings / total_assets if total_assets > 0 else 0
        C = ebit / total_assets if total_assets > 0 else 0
        D = market_cap / total_liabilities if total_liabilities > 0 else 0
        E = total_revenue / total_assets if total_assets > 0 else 0
        
        z_score = (1.2 * A) + (1.4 * B) + (3.3 * C) + (0.6 * D) + (1.0 * E)
        
        # --- 5. SUSTAINABLE GROWTH RATE (SGR) ---
        # SGR = ROE * (1 - Payout Ratio)
        # Use ROE from fin history or calculated above
        # Payout Ratio from info
        
        roe_calc = net_income / equity if equity > 0 else 0
        payout_ratio = info.get('payoutRatio', 0)
        if payout_ratio is None: payout_ratio = 0
        
        sgr = roe_calc * (1 - payout_ratio)

        return {
            'symbol': ticker_symbol,
            'magic_roc': roc,
            'magic_ey': earnings_yield,
            'f_score': f_score,
            'graham_num': graham_number,
            'fcf_yield': fcf_yield,
            'z_score': z_score,
            'sgr': sgr
        }

    except Exception as e:
        print(f"Error calculating advanced metrics for {ticker_symbol}: {e}")
        return None



def calculate_valuations(data, risk_free_rate=RISK_FREE_RATE, market_return=MARKET_RETURN, long_term_growth=LONG_TERM_GROWTH, manual_k=0):
    """
    Calculates intrinsic value based on the 3 methods.
    Allows dynamic parameters for sensitivity analysis.
    """
    if data is None or pd.isna(data['price']):
        return None

    # Unpack necessary variables
    # If any critical metric is missing, we might have to skip that specific valuation method
    
    # 1. Calculate Required Return (k)
    # k = Rf + beta * (Rm - Rf)
    
    # If manual_k is provided (>0), override CAPM
    if manual_k > 0:
        k = manual_k
    else:
        # Checking for critical beta
        # If beta is unreasonably low (e.g. < 0.4), it distorts CAPM, making k too low.
        # We apply a 'Conservative Beta Floor' of 0.6 for valuation purposes.
        if pd.isna(data['beta']) or data['beta'] < 0.6:
            beta = 0.6
        else:
            beta = data['beta']
            
        k = risk_free_rate + beta * (market_return - risk_free_rate)
    
    # 2. Safety Margin for Growth/Discount Rate
    
    g = long_term_growth
    
    # If k is too close to g, valuation explodes.
    # Enforce a minimum spread (k - g) of at least 1.0% (Lowered to allow specific VI cases like 5% K - 3% G).
    min_spread = 0.01
    if (k - g) < min_spread:
        k = g + min_spread

    denominator = k - g
    
    # Method 1: DDM (2-Stage Model)
    # Matches user request for explicit forecast + terminal value
    # Default: Assumes short-term growth = long-term growth (Standard DDM) unless specified
    # Formula: Sum(PV_Div_1..N) + PV(Terminal_Value_N)
    if not pd.isna(data['dividendRate']) and data['dividendRate'] > 0:
        d0 = data['dividendRate']
        
        # Parameters
        n_years = 5
        g_short = g # Currently using same g, but structure allows split
        g_term = g
        
        # 1. Explicit Period (1-5 Years)
        sum_pv_div = 0
        d_curr = d0
        for i in range(1, n_years + 1):
            d_curr *= (1 + g_short)
            sum_pv_div += d_curr / ((1 + k) ** i)
            
        # 2. Terminal Value (at end of Year 5)
        # Value of dividends from Year 6 onwards
        d_next = d_curr * (1 + g_term) # D6
        tv = d_next / (k - g_term) # Value at Year 5
        pv_tv = tv / ((1 + k) ** n_years) # Discount back 5 years
        
        val_ddm = sum_pv_div + pv_tv
    else:
        val_ddm = np.nan

    # Method 2: Target P/E
    # Target P/E = Payout / (k - g)
    # Fair Price = Target P/E * EPS
    if not pd.isna(data['payoutRatio']) and not pd.isna(data['trailingEps']):
        target_pe = data['payoutRatio'] / denominator
        val_pe = target_pe * data['trailingEps']
    else:
        val_pe = np.nan

    # Method 3: Target P/BV
    # Target P/BV = (ROE - g) / (k - g)
    # Fair Price = Target P/BV * BVPS
    if not pd.isna(data['returnOnEquity']) and not pd.isna(data['bookValue']):
        target_pbv = (data['returnOnEquity'] - g) / denominator
        val_pbv = target_pbv * data['bookValue']
    else:
        val_pbv = np.nan
        
    # Final Fair Value
    valid_methods = [v for v in [val_ddm, val_pe, val_pbv] if not pd.isna(v) and v > 0]
    if valid_methods:
        fair_value = sum(valid_methods) / len(valid_methods)
        mos = ((fair_value - data['price']) / fair_value) * 100
        
        if mos > 0:
            status = "Undervalued"
        else:
            status = "Overvalued"
    else:
        fair_value = np.nan
        mos = np.nan
        status = "Data Unavailable"

        # Pass through VI Score 2.0 fields
    peg = data.get('pegRatio')
    if pd.isna(peg):
        # Fallback Calculation: P/E / (Earnings Growth * 100)
        pe = data.get('price', 0) / data.get('trailingEps', 1) if data.get('trailingEps', 0) > 0 else 0
        g = data.get('earningsGrowth', 0)
        if pe > 0 and g > 0:
            peg = pe / (g * 100)
        else:
            peg = 999 # Invalid or no growth

    new_fields = {
        'freeCashflow': data.get('freeCashflow', 0),
        'operatingCashflow': data.get('operatingCashflow', 0),
        'totalRevenue': data.get('totalRevenue', 0),
        'pegRatio': peg,
        'currentRatio': data.get('currentRatio', 0),
        'grossMargins': data.get('grossMargins', 0),
    }

    return {
        **data,
        **new_fields,
        'k_percent': k * 100,
        'terminal_growth_percent': g * 100,
        'valuation_ddm': val_ddm,
        'valuation_pe': val_pe,
        'valuation_pbv': val_pbv,
        'fair_value': fair_value,
        'margin_of_safety': mos,
        'status': status
    }

def fetch_history(ticker_symbol):
    if not ticker_symbol.endswith('.BK'):
        ticker_symbol = f"{ticker_symbol}.BK"
    stock = yf.Ticker(ticker_symbol)
    hist = stock.history(period="2y")
    return hist

def calculate_portfolio(capital, allocation):
    """
    allocation: dict of sector -> percent
    Returns amount per sector
    """
    return {k: capital * v for k, v in allocation.items()}

# --- NEW FEATURES: PORTFOLIO & SIMULATION ---
import json
import os
from datetime import datetime

PORTFOLIO_FILE = "portfolio.json"

def load_portfolio():
    if not os.path.exists(PORTFOLIO_FILE):
        return []
    try:
        with open(PORTFOLIO_FILE, 'r') as f:
            return json.load(f)
    except:
        return []

def save_transaction(symbol, date_str, price, qty, type='Buy'):
    portfolio = load_portfolio()
    record = {
        "id": int(datetime.now().timestamp()), # Simple ID
        "symbol": symbol.upper(),
        "date": str(date_str), # Ensure string
        "price": float(price),
        "qty": int(qty),
        "transaction_type": type # Avoid keyword 'type'
    }
    portfolio.append(record)
    with open(PORTFOLIO_FILE, 'w') as f:
        json.dump(portfolio, f)
    return True

def delete_transaction(tid):
    portfolio = load_portfolio()
    new_port = [p for p in portfolio if p['id'] != tid]
    with open(PORTFOLIO_FILE, 'w') as f:
        json.dump(new_port, f)
    return True

def get_portfolio_summary(current_prices):
    """
    Calculates weighted average price, total qty, total value, unrealized P/L
    """
    transactions = load_portfolio()
    holdings = {}
    
    for t in transactions:
        sym = t['symbol']
        if sym not in holdings:
            holdings[sym] = {'qty': 0, 'total_cost': 0}
        
        # Safe extraction
        txn_type = t.get('transaction_type', 'Buy')
        
        if txn_type == 'Buy':
            holdings[sym]['qty'] += t['qty']
            holdings[sym]['total_cost'] += t['price'] * t['qty']
        elif txn_type == 'Sell':
            if holdings[sym]['qty'] > 0:
                avg_cost = holdings[sym]['total_cost'] / holdings[sym]['qty']
                holdings[sym]['qty'] -= t['qty']
                holdings[sym]['total_cost'] -= avg_cost * t['qty']
                
    summary = []
    total_port_value = 0
    total_cost_value = 0
    
    for sym, data in holdings.items():
        if data['qty'] > 0.0001: 
            avg_price = data['total_cost'] / data['qty']
            curr_price = current_prices.get(sym, avg_price) # Fallback if no price
            mkt_value = data['qty'] * curr_price
            gain_loss = mkt_value - data['total_cost']
            gain_loss_pct = (gain_loss / data['total_cost']) * 100 if data['total_cost'] != 0 else 0
            
            summary.append({
                'Symbol': sym,
                'Qty': data['qty'],
                'Avg Price': avg_price,
                'Market Price': curr_price,
                'Cost Value': data['total_cost'],
                'Market Value': mkt_value,
                'P/L': gain_loss,
                'P/L %': gain_loss_pct
            })
            total_port_value += mkt_value
            total_cost_value += data['total_cost']
            
    return pd.DataFrame(summary), total_port_value, total_cost_value

def get_historical_pe_bands(ticker, years=5):
    """
    Constructs historical Price, EPS, and PE Bands
    """
    try:
        # Ticker suffix handling
        t = ticker + ".BK" if not ticker.endswith(".BK") else ticker
        stock = yf.Ticker(t)
        
        # 1. Price History
        hist = stock.history(period=f"{years}y")
        if hist.empty:
            return None
            
        hist = hist[['Close']].reset_index()
        hist['Date'] = pd.to_datetime(hist['Date']).dt.tz_localize(None)
        
        # 2. Financials (EPS)
        financials = stock.income_stmt.T # Use income_stmt (new yfinance) or financials
        if financials.empty:
            financials = stock.financials.T
            
        if financials.empty:
            return None
            
        # Extract EPS
        eps_col = [c for c in financials.columns if 'Basic EPS' in str(c) or 'Diluted EPS' in str(c)]
        if not eps_col:
            # Try to infer from 'Net Income' / 'Basic Average Shares' if EPS is missing? Too complex.
            return None
            
        eps_data = financials[eps_col[0]].sort_index()
        eps_data.index = pd.to_datetime(eps_data.index).tz_localize(None)
        
        # 3. Merge Pricing and EPS
        # Create EPS dataframe
        eps_df = pd.DataFrame({'Date': eps_data.index, 'EPS': eps_data.values})
        # Remove NaNs
        eps_df = eps_df.dropna()
        if eps_df.empty: return None
        
        # Sort
        hist = hist.sort_values('Date')
        eps_df = eps_df.sort_values('Date')
        
        # Use merge_asof to backward fill EPS (use latest reported EPS for current price)
        # Note: Financial statements usually reported AFTER period end, but date in yfinance is Period End.
        # This is an approximation. Ideally we add lag. But for simple band it's okay.
        merged = pd.merge_asof(hist, eps_df, on='Date', direction='backward')
        
        # Drop rows where EPS is missing (before first report)
        merged = merged.dropna(subset=['EPS'])
        
        # Calculate PE
        merged['PE'] = merged['Close'] / merged['EPS']
        
        # Filter valid PEs for stats (exclude negative PE or crazy outliers for the band calculation)
        valid_pe = merged[(merged['PE'] > 0) & (merged['PE'] < 100)]['PE']
        
        if valid_pe.empty:
            avg_pe = 15
            std_pe = 5
        else:
            avg_pe = valid_pe.mean()
            std_pe = valid_pe.std()
            
        # Construct Bands
        # Re-calculate implied prices based on constant PE lines
        merged['Mean PE'] = merged['EPS'] * avg_pe
        merged['+1 SD'] = merged['EPS'] * (avg_pe + std_pe)
        merged['+2 SD'] = merged['EPS'] * (avg_pe + (2 * std_pe))
        merged['-1 SD'] = merged['EPS'] * (avg_pe - std_pe)
        merged['-2 SD'] = merged['EPS'] * (avg_pe - (2 * std_pe))
        
        # Ensure non-negative prices
        for col in ['Mean PE', '+1 SD', '+2 SD', '-1 SD', '-2 SD']:
            merged[col] = merged[col].clip(lower=0)
        
        return {
            'data': merged,
            'avg_pe': avg_pe,
            'std_pe': std_pe,
            'current_pe': merged.iloc[-1]['PE'] if not merged.empty else 0
        }
    except Exception as e:
        print(f"Error calculating PE Bands: {e}")
        return None

def calculate_dca_simulation(ticker, monthly_amount, years=5, invest_day=25):
    try:
        t = ticker + ".BK" if not ticker.endswith(".BK") else ticker
        stock = yf.Ticker(t)
        
        hist = stock.history(period=f"{years}y")
        if hist.empty:
            return pd.DataFrame(), 0, 0, 0
            
        hist = hist.reset_index()
        hist['Date'] = pd.to_datetime(hist['Date']).dt.tz_localize(None)
        
        # Group by Month
        hist['Month'] = hist['Date'].dt.to_period('M')
        
        ledger = []
        total_shares = 0
        total_invested = 0
        
        for month, group in hist.groupby('Month'):
            # Find close day
            group['DayDiff'] = abs(group['Date'].dt.day - invest_day)
            match = group.loc[group['DayDiff'].idxmin()]
            
            price = match['Close']
            shares = monthly_amount / price
            total_shares += shares
            total_invested += monthly_amount
            
            ledger.append({
                'Date': match['Date'],
                'Invested': total_invested,
                'Value': total_shares * price,
                'Cost': total_invested
            })
            
        df = pd.DataFrame(ledger)
        if df.empty: return df, 0, 0, 0
        
        final_val = df.iloc[-1]['Value']
        profit_pct = ((final_val - total_invested) / total_invested) * 100
        
        return df, total_invested, final_val, profit_pct
    except Exception as e:
        print(f"DCA Error: {e}")
        return pd.DataFrame(), 0, 0, 0

def send_line_notify(token, message):
    url = 'https://notify-api.line.me/api/notify'
    headers = {'Authorization': f'Bearer {token}'}
    data = {'message': message}
    try:
        response = requests.post(url, headers=headers, data=data)
        return response.status_code == 200, response.text
    except Exception as e:
        return False, str(e)

def send_telegram_message(token, chat_id, message):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {'chat_id': chat_id, 'text': message, 'parse_mode': 'Markdown'}
    try:
        response = requests.post(url, data=data)
        return response.status_code == 200, response.text
    except Exception as e:
        return False, str(e)

CONFIG_FILE = "config.json"

def load_config():
    import json
    import os
    if not os.path.exists(CONFIG_FILE):
        return {}
    try:
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    except:
        return {}

def save_config(config):
    import json
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f)

ALERT_LOG_FILE = "alert_log.json"

def load_alert_log():
    import json
    import os
    if not os.path.exists(ALERT_LOG_FILE):
        return {}
    try:
        with open(ALERT_LOG_FILE, 'r') as f:
            return json.load(f)
    except:
        return {}

def save_alert_log(log):
    import json
    with open(ALERT_LOG_FILE, 'w') as f:
        json.dump(log, f)

def check_and_send_alerts(buy_list, sell_list, config):
    """
    Checks for new stocks that haven't been alerted today and sends Telegram notifications.
    Returns: list of sent messages or status strings
    """
    import datetime
    
    # 1. Check Configuration
    channel = config.get('notify_channel', 'หน้าเว็บ (Web Only)')
    if "Telegram" not in channel and "Both" not in channel:
        return []
        
    tg_token = config.get('telegram_token', '')
    tg_chat_id = config.get('telegram_chat_id', '')
    
    if not tg_token or not tg_chat_id:
        return []
        
    # 2. Load Log
    alert_log = load_alert_log()
    today_str = datetime.datetime.now().strftime("%Y-%m-%d")
    
    if today_str not in alert_log:
        alert_log[today_str] = {"buy": [], "sell": []}
        
    # 3. Identify New Alerts
    new_buys = [s for s in buy_list if s not in alert_log[today_str]["buy"]]
    new_sells = [s for s in sell_list if s not in alert_log[today_str]["sell"]]
    
    sent_logs = []
    
    # 4. Send Messages
    if new_buys:
        msg = f"🟢 *New Strong Buy Alert!*\nหุ้นเข้าเกณฑ์สะสมใหม่: {', '.join(new_buys)}"
        success, res = send_telegram_message(tg_token, tg_chat_id, msg)
        if success:
            alert_log[today_str]["buy"].extend(new_buys)
            sent_logs.append(f"Sent Buy Alert for {len(new_buys)} stocks")
            
    if new_sells:
        msg = f"🔴 *New Sell Signal Alert!*\nหุ้นสัญญาณขายใหม่: {', '.join(new_sells)}"
        success, res = send_telegram_message(tg_token, tg_chat_id, msg)
        if success:
            alert_log[today_str]["sell"].extend(new_sells)
            sent_logs.append(f"Sent Sell Alert for {len(new_sells)} stocks")
            
    # 5. Save Log
    if sent_logs:
        save_alert_log(alert_log)
        
    return sent_logs

def fetch_stock_news(ticker_symbol):
    """
    Fetches latest news for a stock using yfinance.
    Returns list of dicts: {title, publisher, link, type, thumbnail}
    """
    try:
        t = ticker_symbol + ".BK" if not ticker_symbol.endswith(".BK") else ticker_symbol
        stock = yf.Ticker(t)
        raw_news = stock.news
        
        formatted_news = []
        if raw_news:
            for item in raw_news:
                # New yfinance structure puts data inside 'content'
                data = item.get('content', item)
                
                title = data.get('title', 'No Title')
                
                # Publisher
                publisher = 'Unknown'
                if 'provider' in data and isinstance(data['provider'], dict):
                    publisher = data['provider'].get('displayName', 'Unknown')
                
                # Link
                link = '#'
                if 'clickThroughUrl' in data and isinstance(data['clickThroughUrl'], dict):
                    link = data['clickThroughUrl'].get('url', '#')
                elif 'canonicalUrl' in data and isinstance(data['canonicalUrl'], dict):
                    link = data['canonicalUrl'].get('url', '#')
                elif 'link' in data:
                    link = data['link']
                
                # Time (Convert ISO 'pubDate' to timestamp for app.py compatibility)
                timestamp = 0
                if 'pubDate' in data:
                    try:
                        # Use pandas for easy ISO parsing
                        dt = pd.to_datetime(data['pubDate'])
                        timestamp = dt.timestamp()
                    except:
                        pass
                elif 'providerPublishTime' in data:
                    timestamp = data['providerPublishTime']

                # Thumbnail
                thumbnail = data.get('thumbnail', None)
                
                formatted_news.append({
                    'title': title,
                    'publisher': publisher,
                    'link': link,
                    'providerPublishTime': timestamp,
                    'thumbnail': thumbnail
                })
                
        return formatted_news
    except Exception as e:
        print(f"Error fetching news for {ticker_symbol}: {e}")
        return []

def get_eps_10_years(ticker_list, years=10):
    """
    Fetches historical EPS for multiple tickers.
    Attempts to get up to 'years' of data.
    """
    results = {}
    
    # We use ThreadPool but be careful with rate limits
    import time
    
    for ticker in ticker_list:
        try:
            t_main = ticker
            if not t_main.endswith('.BK'): t_main += '.BK'
            stock = yf.Ticker(t_main)
            
            # --- Fetch Financial Ratios (Stats) ---
            # Try to get info safely
            stats = {}
            try:
                info = stock.info
                stats = {
                    'Price': info.get('currentPrice'),
                    'P/E': info.get('trailingPE'),
                    'P/BV': info.get('priceToBook'),
                    'D/E': info.get('debtToEquity'),
                    'ROA': info.get('returnOnAssets'),
                    'ROE': info.get('returnOnEquity'),
                    'DPS': info.get('dividendRate'),
                    'DivYield': info.get('dividendYield') # usually float 0.0x
                }
            except Exception as e_info:
                print(f"Stats error for {ticker}: {e_info}")
                stats = {}

            # --- Fetch Historical EPS ---
            # 1. Try 'income_stmt' (New yfinance) - usually returns ~4-5 years
            stmt = stock.income_stmt
            if stmt.empty:
                 stmt = stock.financials # Fallback
            
            # Extract 'Basic EPS' or 'Diluted EPS'
            eps_row = None
            if 'Basic EPS' in stmt.index:
                eps_row = stmt.loc['Basic EPS']
            elif 'Diluted EPS' in stmt.index:
                eps_row = stmt.loc['Diluted EPS']
            elif 'Net Income' in stmt.index and 'Basic Average Shares' in stmt.index:
                 # Calculate manually if needed
                 eps_row = stmt.loc['Net Income'] / stmt.loc['Basic Average Shares']
            
            history_data = {}
            if eps_row is not None:
                # Format: Index is Date
                # Convert to Year: Value
                for date, val in eps_row.items():
                    year = date.year
                    history_data[year] = val
            
            results[ticker] = {
                'history': history_data,
                'stats': stats
            }
                
        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}")
            results[ticker] = {'history': {}, 'stats': {}}
            
    return results
