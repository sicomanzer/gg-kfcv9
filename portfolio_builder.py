import yfinance as yf
import pandas as pd
import numpy as np
import concurrent.futures
import streamlit as st
from datetime import datetime

# High Priority Universe (Bonus +10)
HIGH_PRIORITY_UNIVERSE = [
    "TISCO", "SCB", "KTB", "BBL", "KBANK",
    "ADVANC", "TRUE",
    "DIF", "EGCO", "BGRIM", "GPSC", "RATCH",
    "AP", "CPN",
    "PTT", "OR"
]

def get_stock_data(ticker):
    """
    Fetches comprehensive data for a single stock.
    """
    symbol = f"{ticker}.BK" if not ticker.endswith(".BK") else ticker
    stock = yf.Ticker(symbol)
    
    try:
        # 1. Info (Fastest, contains most ratios)
        info = stock.info
        if not info: return None
        
        # 2. Dividends (History)
        dividends = stock.dividends
        
        # 3. Financials (For Growth & Cashflow)
        financials = stock.financials
        cashflow = stock.cashflow
        
        return {
            "symbol": ticker,
            "info": info,
            "dividends": dividends,
            "financials": financials,
            "cashflow": cashflow
        }
    except Exception as e:
        return None

def calculate_score(data):
    """
    Calculates the score (0-100) based on user criteria.
    """
    score = 0
    details = {}
    
    info = data.get('info', {})
    dividends = data.get('dividends', pd.Series(dtype=float))
    financials = data.get('financials', pd.DataFrame())
    cashflow = data.get('cashflow', pd.DataFrame())
    symbol = data.get('symbol')

    # --- 1. DPS Growth (30 pts) ---
    # Criterion: DPS Increasing every year OR Trend up (5 years)
    dps_score = 0
    is_growing = False
    try:
        if not dividends.empty:
            # Group by year and sum
            div_yearly = dividends.resample('Y').sum()
            # Get last 5 full years (exclude current YTD if possible, or just take last 5 points)
            current_year = datetime.now().year
            last_5 = div_yearly[div_yearly.index.year < current_year].tail(5)
            
            if len(last_5) >= 4:
                # Check for strict increase
                is_strictly_increasing = True
                prev = -1
                for d in last_5:
                    if d < prev: is_strictly_increasing = False
                    prev = d
                
                # Check for CAGR > 0
                start = last_5.iloc[0]
                end = last_5.iloc[-1]
                cagr = ((end/start)**(1/(len(last_5)-1)) - 1) if start > 0 else 0
                
                if is_strictly_increasing:
                    dps_score = 30
                    is_growing = True
                elif cagr > 0.02: # Trend up
                    dps_score = 20
                    is_growing = True
                elif cagr > 0:
                    dps_score = 10
                    
                details['DPS_Growth_Rate'] = cagr
            else:
                details['DPS_Growth_Rate'] = 0
    except:
        pass
    score += dps_score
    details['Score_DPS'] = dps_score

    # --- 2. Payout Ratio (20 pts) ---
    # Criterion: 40-80% (Sustainable)
    payout_score = 0
    payout = info.get('payoutRatio', 0)
    if payout is None: payout = 0
    
    # Strict Value Investing Rule: Avoid Payout > 80% unless it's a REIT/Infra (which we can't easily detect here without sector info)
    # However, for general stocks, > 80% is risky.
    if 0.4 <= payout <= 0.8:
        payout_score = 20
    elif 0.2 <= payout < 0.4: # Low payout is okay, might be growth
        payout_score = 10
    elif 0.8 < payout <= 0.9: # High payout, risky
        payout_score = 5 # Reduced from 10 to 5
    else:
        payout_score = 0 # > 90% or < 20% gets 0
        
    score += payout_score
    details['Score_Payout'] = payout_score
    details['Payout_Ratio'] = payout

    # --- 3. Forward Dividend Yield (15 pts) ---
    # Criterion: 5.5 - 11%
    yield_score = 0
    div_yield = info.get('dividendYield', 0) # This is usually TTM
    if div_yield is None: div_yield = 0
    
    # Normalize: If yield > 1 (e.g. 5.5 means 5.5%), convert to decimal 0.055
    # Also handle string percentages if any
    try:
        div_yield = float(div_yield)
    except:
        div_yield = 0
        
    if div_yield > 1:
        div_yield = div_yield / 100.0
    
    if 0.055 <= div_yield <= 0.11:
        yield_score = 15
    elif 0.03 <= div_yield <= 0.12: # Relaxed
        yield_score = 8
        
    score += yield_score
    details['Score_Yield'] = yield_score
    details['Yield'] = div_yield

    # --- 4. EPS Growth (15 pts) ---
    # Criterion: >= 4% (Consensus or History)
    growth_score = 0
    eps_growth = info.get('earningsGrowth', 0) # YoY
    if eps_growth is None: eps_growth = 0
    
    # Try 5Y History first
    try:
        if not financials.empty and 'Basic EPS' in financials.index:
            eps = financials.loc['Basic EPS'].sort_index().dropna()
            if len(eps) >= 4:
                start = eps.iloc[0]
                end = eps.iloc[-1]
                cagr = ((end/start)**(1/(len(eps)-1)) - 1) if start > 0 else 0
                if cagr >= 0.04:
                    growth_score = 15
                elif cagr > 0:
                    growth_score = 8
    except:
        # Fallback to analyst estimate if available (earningsGrowth)
        if eps_growth >= 0.04:
            growth_score = 15
            
    score += growth_score
    details['Score_Growth'] = growth_score

    # --- 5. ROE >= 12% & D/E <= 1.0 (10 pts) ---
    quality_score = 0
    roe = info.get('returnOnEquity', 0)
    de = info.get('debtToEquity', 0)
    
    if roe is None: roe = 0
    if de is None: de = 999
    
    # D/E from yfinance is often in percentage (e.g. 80.5)
    de_ratio = de / 100 if de > 10 else de
    
    if roe >= 0.12 and de_ratio <= 1.0:
        quality_score = 10
    elif roe >= 0.10 and de_ratio <= 1.5:
        quality_score = 5
        
    score += quality_score
    details['Score_Quality'] = quality_score
    details['ROE'] = roe
    details['DE_Ratio'] = de_ratio

    # --- 6. Cash Flow Coverage (10 pts) ---
    # Criterion: Cash Payout <= 60%
    cf_score = 0
    cash_payout = 0
    try:
        if not cashflow.empty:
            # Locate items
            # Note: yfinance format varies. Usually 'Operating Cash Flow', 'Cash Dividends Paid'
            ocf = None
            div_paid = None
            
            # Search loosely
            for idx in cashflow.index:
                if 'Operating' in idx and 'Cash' in idx:
                    ocf = cashflow.loc[idx].iloc[0]
                if 'Dividend' in idx and 'Paid' in idx:
                    div_paid = cashflow.loc[idx].iloc[0]
            
            if ocf and div_paid and ocf > 0:
                cash_payout = abs(div_paid) / ocf
                if cash_payout <= 0.6:
                    cf_score = 10
                elif cash_payout <= 0.8:
                    cf_score = 5
    except:
        pass
        
    score += cf_score
    details['Score_CF'] = cf_score
    details['Cash_Payout'] = cash_payout

    # --- Bonus: High Priority Universe (+10 pts) ---
    bonus = 0
    if symbol in HIGH_PRIORITY_UNIVERSE:
        bonus = 10
    
    score += bonus
    details['Score_Bonus'] = bonus
    
    details['Total_Score'] = score
    
    return score, details

@st.cache_data(ttl=3600)
def build_dividend_portfolio(universe, capital, monthly_target, risk_level, version=1):
    """
    Main function to build the portfolio.
    version: Cache buster
    """
    scored_stocks = []
    
    # 1. Expand Universe
    scan_list = list(set(universe + HIGH_PRIORITY_UNIVERSE))
    
    # 2. Fetch & Score (Parallel)
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        future_to_ticker = {executor.submit(get_stock_data, t): t for t in scan_list}
        
        for future in concurrent.futures.as_completed(future_to_ticker):
            try:
                data = future.result()
                if data:
                    score, details = calculate_score(data)
                    
                    # Basic Filter: Must have some yield and positive score
                    if details['Yield'] > 0.02 and score >= 40: 
                        scored_stocks.append({
                            "Ticker": data['symbol'],
                            "Price": data['info'].get('currentPrice') or data['info'].get('regularMarketPreviousClose'),
                            "Score": score,
                            "Yield": details['Yield'],
                            "DPS_Growth": details.get('DPS_Growth_Rate', 0),
                            "Details": details
                        })
            except Exception as e:
                pass
    
    # 3. Create DataFrame & Sort
    df = pd.DataFrame(scored_stocks)
    if df.empty: return pd.DataFrame(), pd.DataFrame(), 0.0, []
    
    df = df.sort_values(by='Score', ascending=False)
    
    # 4. Select Top Stocks (Max 8)
    portfolio = df.head(8).copy()
    
    # 5. Allocate
    # Strict 20% max allocation per stock
    num_stocks = len(portfolio)
    if num_stocks == 0: return pd.DataFrame(), pd.DataFrame(), 0.0, []
    
    # Calculate allocation
    max_allocation = capital * 0.20
    ideal_allocation = capital / num_stocks
    
    # Use the smaller of (Capital / N) or (Capital * 20%)
    allocation_per_stock = min(ideal_allocation, max_allocation)
    
    portfolio['Shares'] = (allocation_per_stock / portfolio['Price']).astype(int)
    portfolio['Investment'] = portfolio['Shares'] * portfolio['Price']
    portfolio['Annual_Dividend'] = portfolio['Investment'] * portfolio['Yield']
    portfolio['Monthly_Income_Gross'] = portfolio['Annual_Dividend'] / 12
    portfolio['Monthly_Income_Net'] = portfolio['Monthly_Income_Gross'] * 0.9 # Tax 10%
    
    total_investment = portfolio['Investment'].sum()
    cash_remaining = capital - total_investment
    
    # 6. Projection
    avg_yield = (portfolio['Annual_Dividend'].sum() / total_investment) if total_investment > 0 else 0
    
    # Calculate weighted average DPS growth of the portfolio
    # Use conservative growth estimates (capped at 8% per user requirement)
    portfolio['Growth_Used'] = portfolio['DPS_Growth'].clip(0.02, 0.08) # Min 2%, Max 8%
    avg_growth = (portfolio['Growth_Used'] * portfolio['Investment']).sum() / total_investment if total_investment > 0 else 0.04
    
    # Warnings
    warnings = []
    if avg_yield < 0.075:
        warnings.append(f"⚠️ Yield เฉลี่ยของพอร์ต ({avg_yield:.2%}) ต่ำกว่าเป้าหมาย 7.5% (อาจต้องเพิ่มความเสี่ยงหรือปรับเกณฑ์)")
    elif avg_yield > 0.09:
        warnings.append(f"⚠️ Yield เฉลี่ยของพอร์ต ({avg_yield:.2%}) สูงกว่า 9% (ระวังกับดักปันผล ตรวจสอบ Payout Ratio อีกครั้ง)")
        
    if num_stocks < 5:
        warnings.append(f"⚠️ พบหุ้นที่ผ่านเกณฑ์เพียง {num_stocks} ตัว (กระจายความเสี่ยงไม่เพียงพอ)")
        
    if cash_remaining > capital * 0.1: # If > 10% cash left
        warnings.append(f"💰 มีเงินสดคงเหลือ {cash_remaining:,.2f} บาท (เนื่องจากติดเพดานลงทุน 20% ต่อตัว หรือหุ้นไม่พอ)")

    projection_data = []
    current_income = portfolio['Monthly_Income_Net'].sum()
    
    for i in range(6): # 0 to 5
        year_income = current_income * ((1 + avg_growth) ** i)
        projection_data.append({
            "Year": datetime.now().year + i,
            "Monthly_Income": year_income,
            "Yield_On_Cost": (year_income * 12) / (capital * 0.9) # Yield on invested capital (approx)
        })
        
    return portfolio, pd.DataFrame(projection_data), avg_yield, warnings
