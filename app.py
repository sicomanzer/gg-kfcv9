import streamlit as st
import pandas as pd
import utils
import portfolio_builder
import os
import yfinance as yf

# Fix for "disk I/O error" / "unable to open database file"
# Redirect yfinance cache to a local folder in the workspace
cache_dir = os.path.join(os.getcwd(), "yf_cache")
if not os.path.exists(cache_dir):
    os.makedirs(cache_dir)
yf.set_tz_cache_location(cache_dir)
import datetime
import pytz
from consts import SET100_TICKERS, LONG_TERM_GROWTH, RISK_FREE_RATE, MARKET_RETURN
import concurrent.futures
import plotly.express as px
import plotly.graph_objects as go
import yfinance as yf
import base64
import io
from PIL import Image, ImageDraw

# Set Page Configuration
st.set_page_config(
    page_title="โปรแกรมคัดกรองหุ้น VI (Thai Value Investor)",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Load Tickers
SET100_TICKERS = utils.load_tickers()

# --- SIDEBAR: VALUATION MODEL ---
st.sidebar.title("🇹🇭 Thai Value Investor")
st.sidebar.markdown("### 🎛️ โมเดลประเมินมูลค่า")
with st.sidebar.expander("ตั้งค่าสมมติฐาน (Assumption)", expanded=False):
    # Economic Scenario Selector
    scenario = st.selectbox("สภาวะเศรษฐกิจ (Economic Scenario)", 
                           ["Normal (ปกติ)", "Recession (ถดถอย)", "Crisis (วิกฤต)"],
                           help="ปรับเปลี่ยนสมมติฐานความเสี่ยงและการเติบโตอัตโนมัติ")
    
    # Base Values
    base_rf = RISK_FREE_RATE
    base_rm = MARKET_RETURN
    base_g = LONG_TERM_GROWTH
    
    # Adjust based on scenario
    if "Recession" in scenario:
        base_rf = 0.02 # Bond yields drop
        base_rm = 0.06 # Market return drops
        base_g = 0.01  # Growth slows
        st.caption("⚠️ โหมดถดถอย: ลดคาดการณ์ผลตอบแทนและการเติบโต")
    elif "Crisis" in scenario:
        base_rf = 0.01
        base_rm = 0.04
        base_g = 0.00
        st.caption("🚨 โหมดวิกฤต: สมมติฐานเลวร้ายที่สุด (No Growth)")
        
    st_rf = st.number_input("อัตราผลตอบแทนพันธบัตร (Risk Free %)", value=base_rf*100, step=0.1, format="%.2f") / 100
    st_rm = st.number_input("ผลตอบแทนตลาด (Market Return %)", value=base_rm*100, step=0.1, format="%.2f") / 100
    st_g = st.number_input("การเติบโตระยะยาว (Terminal Growth %)", value=base_g*100, step=0.1, format="%.2f") / 100
    
    st.markdown("---")
    st.markdown("**กำหนดค่า K เอง (Override CAPM)**")
    st_k_manual = st.number_input("ผลตอบแทนที่คาดหวัง (Required Return / K %)", value=0.0, step=0.1, format="%.2f", help="ใส่ 0 หากต้องการใช้ค่า K จากสูตร CAPM ตามปกติ") / 100
    
    if st.button("รีเซ็ตค่าเริ่มต้น"):
        st.cache_data.clear() # Optional but good
        st.rerun()

st.sidebar.markdown("### 🔄 อัปเดตข้อมูล")
if st.sidebar.button("อัปเดตข้อมูลราคาและงบการเงิน"):
    with st.spinner("กำลังล้าง Cache และดึงข้อมูลใหม่..."):
        # Clear Streamlit Cache
        st.cache_data.clear()
        
        # Clear yfinance Cache (optional, but ensures fresh data from API)
        # Note: We already redirected cache to local folder, so we can clean it if needed
        # but st.cache_data.clear() is usually enough for the app logic.
        # If we want to force yfinance to re-download, we might need to rely on its internal expiration or clear the folder.
        # For now, clearing app cache is sufficient to trigger fetch_raw_market_data() again.
        
    st.success("อัปเดตข้อมูลเรียบร้อยแล้ว!")
    st.rerun()

# --- DATA FETCHING (Separated) ---
@st.cache_data
def fetch_raw_market_data():
    """
    Fetches raw data for all tickers. Cached for performance.
    Returns: (results, fetch_timestamp)
    """
    results = []
    # Use Thailand Time (UTC+7)
    tz = pytz.timezone('Asia/Bangkok')
    fetch_timestamp = datetime.datetime.now(tz)
    
    # Progress bar setup
    progress_text = "กำลังดึงข้อมูลหุ้น... โปรดรอสักครู่"
    my_bar = st.progress(0, text=progress_text)
    
    # Adjusted: Increased max_workers to 20 for better speed (balanced with delay)
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        # Create a dictionary to map futures to tickers
        future_to_ticker = {executor.submit(utils.get_stock_data, ticker): ticker for ticker in SET100_TICKERS}
        
        completed_count = 0
        total_count = len(SET100_TICKERS)
        
        for future in concurrent.futures.as_completed(future_to_ticker):
            data = future.result()
            if data:
                results.append(data)
            
            completed_count += 1
            if total_count > 0:
                my_bar.progress(completed_count / total_count, text=f"กำลังโหลด {future_to_ticker[future]} ({completed_count}/{total_count})")
            
    my_bar.empty()
    return results, fetch_timestamp

def process_valuations(raw_data, rf, rm, g, manual_k=0):
    """
    Calculates valuation on raw data with specific parameters.
    """
    results = []
    for item in raw_data:
        # Clone item to avoid modifying cached dict in place across reruns (shallow copy often enough but dict copy is safer)
        data_copy = item.copy()
        evaluated_data = utils.calculate_valuations(data_copy, risk_free_rate=rf, market_return=rm, long_term_growth=g, manual_k=manual_k)
        if evaluated_data:
            results.append(evaluated_data)
    return pd.DataFrame(results)

# Load Pipeline
raw_data_list, last_fetch_time = fetch_raw_market_data()
if not raw_data_list:
    st.error("Failed to fetch data.")
    st.stop()

df = process_valuations(raw_data_list, st_rf, st_rm, st_g, st_k_manual)

if not df.empty:
    # --- GLOBAL DATA ENRICHMENT ---
    # Handle NaNs for scoring
    df['debtToEquity'] = df['debtToEquity'].fillna(999) 
    df['returnOnEquity'] = df['returnOnEquity'].fillna(0)
    df['profitMargins'] = df['profitMargins'].fillna(0)
    df['margin_of_safety'] = df['margin_of_safety'].fillna(-100)
    df['marketCap'] = df['marketCap'].fillna(0)
    df['revenueGrowth'] = df['revenueGrowth'].fillna(0)
    df['pegRatio'] = df['pegRatio'].fillna(999)
    df['currentRatio'] = df['currentRatio'].fillna(0)
    df['grossMargins'] = df['grossMargins'].fillna(0)
    df['freeCashflow'] = df['freeCashflow'].fillna(0)
    
    # NOTE: yfinance 'debtToEquity' is usually returned as a percentage (e.g., 150 means 1.5x).
    # We need to divide by 100 for display if we want 'x', but for scoring logic check raw value.
    # Let's fix the dataframe column for display purposes to be 'x' (ratio).
    df['debtToEquityRatio'] = df['debtToEquity'] / 100

    # 1. Base Score (6 Points)
    df['score_debt'] = df['debtToEquity'].apply(lambda x: 1 if x < 200 else 0) # < 200% = < 2.0x
    df['score_roe'] = df['returnOnEquity'].apply(lambda x: 1 if x > 0.15 else 0)
    df['score_npm'] = df['profitMargins'].apply(lambda x: 1 if x > 0.10 else 0)
    df['score_mos'] = df['margin_of_safety'].apply(lambda x: 1 if x > 0 else 0)
    df['score_size'] = df['marketCap'].apply(lambda x: 1 if x > 50_000_000_000 else 0) # > 50B THB
    df['score_growth'] = df['revenueGrowth'].apply(lambda x: 1 if x > 0.05 else 0) # > 5% Growth
    
    # 2. VI Score 2.0 (New 4 Points)
    # 7. Cash Flow Strength: Free Cash Flow > 0 (Real Cash Generation)
    df['score_fcf'] = df['freeCashflow'].apply(lambda x: 1 if x > 0 else 0)
    
    # 8. Valuation Growth (GARP): PEG < 1.5 (Not overpaying for growth)
    df['score_peg'] = df['pegRatio'].apply(lambda x: 1 if x > 0 and x < 1.5 else 0)
    
    # 9. Liquidity: Current Ratio > 1.5 (Can pay short-term debts)
    df['score_liquidity'] = df['currentRatio'].apply(lambda x: 1 if x > 1.5 else 0)
    
    # 10. Competitive Advantage: Gross Margin > 20% (Pricing Power)
    df['score_gm'] = df['grossMargins'].apply(lambda x: 1 if x > 0.20 else 0)

    # Total Scores
    df['Quality Score'] = (df['score_debt'] + df['score_roe'] + df['score_npm'] + 
                           df['score_mos'] + df['score_size'] + df['score_growth'])
                           
    df['VI Score'] = (df['Quality Score'] + 
                      df['score_fcf'] + df['score_peg'] + df['score_liquidity'] + df['score_gm'])

    # --- BUG FIX: Fill NaN for 'Action' Logic to prevent errors ---
    df['valuation_ddm'] = df['valuation_ddm'].fillna(0)
    df['price'] = df['price'].fillna(0)

# --- SIDEBAR NAVIGATION ---
st.sidebar.title("เมนูหลัก")
page = st.sidebar.radio("ไปยังหน้า", [
    "📊 แดชบอร์ดภาพรวม", 
    "🔍 วิเคราะห์หุ้นรายตัว", 
    "📊 เจาะลึกกำไร 5 ปี (EPS Trends)",
    "⚖️ เปรียบเทียบคู่แข่ง", 
    "💡 แนะนำพอร์ตการลงทุน", 
    "💰 พอร์ตปันผล Value Growth",
    "🎒 พอร์ตของฉัน (My Portfolio)", 
    "⏳ จำลองการออมหุ้น (DCA Backtester)",
    "⚙️ ตั้งค่า"
])

# --- EPS TRENDS PAGE ---
# --- EPS TRENDS PAGE ---
if page == "📊 เจาะลึกกำไร 5 ปี (EPS Trends)":
    st.title("📊 เจาะลึกแนวโน้มกำไรต่อหุ้น (EPS) & อัตราส่วนทางการเงิน")
    st.markdown("วิเคราะห์ทิศทางกำไรและสุขภาพทางการเงิน (ย้อนหลัง 5 ปี)")
    st.caption("ℹ️ หมายเหตุ: ข้อมูลย้อนหลังฟรีมักมีเพียง 4-5 ปีล่าสุด (ปีที่ไม่มีข้อมูลจะแสดงเป็นช่องว่าง)")
    
    col_filter, col_act = st.columns([2, 1])
    with col_filter:
         mode_eps = st.radio("เลือกกลุ่มหุ้น:", ["Top 20 หุ้นแกร่ง (VI Score)", "ระบุชื่อเอง", "SET50 (ช้า)"], horizontal=True)
    
    target_tickers = []
    if mode_eps == "Top 20 หุ้นแกร่ง (VI Score)":
        if not df.empty:
            target_tickers = df.sort_values(by='VI Score', ascending=False).head(20)['symbol'].tolist()
            st.info(f"Top 20 หุ้น VI: {', '.join(target_tickers[:5])}...")
    elif mode_eps == "ระบุชื่อเอง":
        target_tickers = st.multiselect("เลือกหุ้น:", SET100_TICKERS, default=["ADVANC", "KBANK", "PTT"])
    elif mode_eps == "SET50 (ช้า)":
        target_tickers = SET100_TICKERS[:50]
        st.warning("⚠️ โหมดนี้ใช้เวลาดึงข้อมูลนาน (2-3 นาที)")
        
    if "eps_trend_df" not in st.session_state:
        st.session_state["eps_trend_df"] = None
        
    if st.button("🚀 ดึงข้อมูล (Analyze)", type="primary", disabled=not target_tickers):
        with st.spinner("กำลังดึงข้อมูลและอัตราส่วนทางการเงิน..."):
            import datetime
            import numpy as np
            # Fetch data (History + Stats)
            eps_data = utils.get_eps_10_years(target_tickers, years=6)
            
            if eps_data:
                current_year = datetime.datetime.now().year
                target_years = list(range(current_year - 5, current_year))
                
                rows = []
                for sym, info in eps_data.items():
                    # Handle new structure if implemented in utils, or fallback
                    # In utils step, we changed result format to {'history': {}, 'stats': {}}
                    history = info.get('history', {})
                    stats = info.get('stats', {})
                    
                    if not history and not stats: continue
                    
                    row = {'Symbol': sym}
                    
                    # --- Financial Stats ---
                    row['Price'] = stats.get('Price')
                    row['P/E'] = stats.get('P/E')
                    row['P/BV'] = stats.get('P/BV')
                    row['D/E'] = stats.get('D/E')
                    
                    # Convert to % (multiply by 100)
                    roa = stats.get('ROA')
                    row['ROA %'] = roa * 100 if roa is not None else None
                    
                    roe = stats.get('ROE')
                    row['ROE %'] = roe * 100 if roe is not None else None
                    
                    row['DPS'] = stats.get('DPS')
                    
                    yld = stats.get('DivYield')
                    row['Yield %'] = yld * 100 if yld is not None else None
                    
                    # --- History ---
                    trend = []
                    for y in target_years:
                        val = history.get(y, None)
                        row[str(y)] = val
                        
                        # Sanitize value for Sparkline
                        if val is None or pd.isna(val):
                            trend_val = 0.0
                        else:
                            trend_val = float(val)
                        trend.append(trend_val)
                    
                    row['Trend'] = trend
                    rows.append(row)
                    
                if rows:
                    df_res = pd.DataFrame(rows)
                    # Convert columns to numeric
                    cols_to_numeric = [str(y) for y in target_years] + ['Price', 'P/E', 'P/BV', 'D/E', 'ROA %', 'ROE %', 'DPS', 'Yield %']
                    for col in cols_to_numeric:
                        if col in df_res.columns:
                            df_res[col] = pd.to_numeric(df_res[col], errors='coerce')
                            
                    st.session_state["eps_trend_df"] = df_res
                    st.rerun()
                else:
                    st.error("ไม่พบข้อมูล")
            else:
                st.error("เกิดข้อผิดพลาดในการดึงข้อมูล")
                
    if st.session_state["eps_trend_df"] is not None:
        st.write("---")
        df_show = st.session_state["eps_trend_df"]
        
        # Columns Config
        import datetime
        current_year = datetime.datetime.now().year
        target_years = list(range(current_year - 5, current_year))
        
        cfg = {
            "Symbol": st.column_config.TextColumn("หุ้น", width="small", pinned=True),
            "Price": st.column_config.NumberColumn("Price", format="%.2f", width="small"),
            "P/E": st.column_config.NumberColumn("P/E", format="%.2f", width="small"),
            "P/BV": st.column_config.NumberColumn("P/BV", format="%.2f", width="small"),
            "D/E": st.column_config.NumberColumn("D/E", format="%.2f", width="small"),
            "ROA %": st.column_config.NumberColumn("ROA %", format="%.2f%%", width="small"),
            "ROE %": st.column_config.NumberColumn("ROE %", format="%.2f%%", width="small"),
            "DPS": st.column_config.NumberColumn("DPS (ปันผล)", format="%.2f", width="small"),
            "Yield %": st.column_config.NumberColumn("Yield %", format="%.2f%%", width="small"),
            "Trend": st.column_config.BarChartColumn("Trend (5Y)", width="small"),
        }
        for y in target_years:
            cfg[str(y)] = st.column_config.NumberColumn(str(y), format="%.2f", width="small")
            
        # Define column order explicitly: Symbol | Trend | Stats | History
        cols = ["Symbol", "Trend", "Price", "P/E", "P/BV", "Yield %", "D/E", "ROE %", "ROA %", "DPS"] + [str(y) for y in target_years]
        
        # Filter existing columns only
        valid_cols = [c for c in cols if c in df_show.columns]
        
        st.dataframe(df_show, column_order=valid_cols, column_config=cfg, hide_index=True, height=600, use_container_width=True)

if page == "📊 แดชบอร์ดภาพรวม":
    st.title("📊 โปรแกรมคัดกรองหุ้นคุณค่า (VI)")
    st.markdown("พัฒนาตามหลักการลงทุนของ คุณกวี ชูกิจเกษม")

    st.markdown("---")
    
    # --- MARKET OVERVIEW (VIX & FEAR/GREED) ---
    vix_data = utils.get_vix_data()
    fng_data = utils.get_fear_and_greed_index()
    
    c_market_1, c_market_2 = st.columns(2)
    
    with c_market_1:
        # --- VIX INDEX DISPLAY ---
        if vix_data:
            vix_price = vix_data['current']
            vix_change = vix_data['change']
            vix_pct = vix_data['pct_change']
            vix_hist = vix_data['history']
            
            # Determine Status & Color
            if vix_price < 20:
                vix_status = "ตลาดค่อนข้างนิ่ง (Calm)"
                vix_color = "#10b981" # Green
                vix_bg = "rgba(16, 185, 129, 0.1)"
                vix_icon = "😊"
                vix_desc = "VIX ต่ำกว่า 20: ความผันผวนต่ำ นักลงทุนคลายความกังวล"
            elif 20 <= vix_price < 30:
                vix_status = "เริ่มผันผวน (Caution)"
                vix_color = "#f59e0b" # Orange
                vix_bg = "rgba(245, 158, 11, 0.1)"
                vix_icon = "⚠️"
                vix_desc = "VIX 20-30: ตลาดเริ่มมีความเสี่ยงและความผันผวนเพิ่มขึ้น"
            elif 30 <= vix_price <= 40:
                vix_status = "ตลาดเริ่มกลัว (Fear)"
                vix_color = "#ef4444" # Red
                vix_bg = "rgba(239, 68, 68, 0.1)"
                vix_icon = "😨"
                vix_desc = "VIX สูงกว่า 30: ความกลัวปกคลุมตลาด มีโอกาส panic sell"
            else: # > 40
                vix_status = "Panic! (วิกฤต)"
                vix_color = "#b91c1c" # Dark Red
                vix_bg = "rgba(185, 28, 28, 0.1)"
                vix_icon = "🔥"
                vix_desc = "VIX สูงกว่า 40: ตลาดแตกตื่นรุนแรง (Panic Mode)"
    
            # Layout: Current Value (Left) + Chart (Right)
            with st.container():
                st.markdown(f"""
                <div style="background-color: {vix_bg}; padding: 15px; border-radius: 10px; border: 1px solid {vix_color}; margin-bottom: 20px;">
                    <h3 style="margin: 0; color: {vix_color};">⚡ ดัชนีความกลัว (VIX Index)</h3>
                </div>
                """, unsafe_allow_html=True)
                
                c_vix1, c_vix2 = st.columns([1, 1.5]) # Adjusted ratio for smaller column
                
                with c_vix1:
                    st.markdown(f"""
                    <div style="text-align: center; padding: 5px;">
                        <div style="font-size: 40px; font-weight: bold; color: {vix_color}; line-height: 1;">
                            {vix_price:.2f}
                        </div>
                        <div style="font-size: 16px; font-weight: bold; color: {vix_color}; margin-top: 5px;">
                            {vix_icon} {vix_status}
                        </div>
                        <div style="font-size: 14px; color: {'green' if vix_change < 0 else 'red'}; margin-top: 5px;">
                            {vix_change:+.2f} ({vix_pct:+.2f}%)
                        </div>
                        <div style="margin-top: 10px; font-size: 12px; background-color: rgba(255,255,255,0.1); padding: 5px; border-radius: 5px; border: 1px dashed {vix_color};">
                            {vix_desc}<br>
                            <span style="font-size: 10px; color: gray;">Avg ≈ 19-21</span>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    
                with c_vix2:
                    # Plotly Chart
                    fig_vix = go.Figure()
                    
                    # Main Line
                    fig_vix.add_trace(go.Scatter(
                        x=vix_hist.index, 
                        y=vix_hist['Close'],
                        mode='lines',
                        name='VIX',
                        line=dict(color=vix_color, width=2)
                    ))
                    
                    # Threshold Lines layout
                    fig_vix.update_layout(
                        title=dict(text="VIX History (5Y)", font=dict(size=14)),
                        height=220,
                        margin=dict(l=10, r=10, t=30, b=10),
                        showlegend=False,
                        xaxis=dict(showgrid=False, showticklabels=False), # Compact
                        yaxis=dict(showgrid=True, gridcolor='#eee'),
                        shapes=[
                            dict(type="line", xref="paper", x0=0, x1=1, yref="y", y0=20, y1=20, line=dict(color="green", width=1, dash="dash"), layer="below"),
                            dict(type="line", xref="paper", x0=0, x1=1, yref="y", y0=30, y1=30, line=dict(color="red", width=1, dash="dash"), layer="below"),
                        ]
                    )
                    st.plotly_chart(fig_vix, use_container_width=True)
        else:
            st.warning("⚠️ VIX Index Unavailable")

    with c_market_2:
        # --- FEAR & GREED INDEX DISPLAY ---
        if fng_data:
            fng_score = fng_data['score']
            fng_rating = fng_data['rating']
            fng_timestamp = fng_data['timestamp']
            
            # Normalize rating string
            rating_lower = fng_rating.lower()
            
            # Determine Color & Icon
            if "extreme fear" in rating_lower:
                fng_color = "#b91c1c" # Dark Red
                fng_bg = "rgba(185, 28, 28, 0.1)"
                fng_icon = "😱"
                fng_th_rating = "กลัวสุดขีด (Extreme Fear)"
            elif "fear" in rating_lower and "extreme" not in rating_lower:
                fng_color = "#ef4444" # Red
                fng_bg = "rgba(239, 68, 68, 0.1)"
                fng_icon = "😨"
                fng_th_rating = "กลัว (Fear)"
            elif "neutral" in rating_lower:
                fng_color = "#6b7280" # Gray
                fng_bg = "rgba(107, 114, 128, 0.1)"
                fng_icon = "😐"
                fng_th_rating = "เป็นกลาง (Neutral)"
            elif "extreme greed" in rating_lower:
                fng_color = "#047857" # Dark Green
                fng_bg = "rgba(4, 120, 87, 0.1)"
                fng_icon = "🤑"
                fng_th_rating = "โลภสุดขีด (Extreme Greed)"
            else: # greed
                fng_color = "#10b981" # Green
                fng_bg = "rgba(16, 185, 129, 0.1)"
                fng_icon = "🙂"
                fng_th_rating = "โลภ (Greed)"
                
            with st.container():
                st.markdown(f"""
                <div style="background-color: {fng_bg}; padding: 15px; border-radius: 10px; border: 1px solid {fng_color}; margin-bottom: 20px;">
                    <h3 style="margin: 0; color: {fng_color};">🌡️ Fear & Greed Index</h3>
                </div>
                """, unsafe_allow_html=True)
                
                c_fng1, c_fng2 = st.columns([1, 1.5]) # Adjusted ratio
                
                with c_fng1:
                    st.markdown(f"""
                    <div style="text-align: center; padding: 5px;">
                        <div style="font-size: 40px; font-weight: bold; color: {fng_color}; line-height: 1;">
                            {fng_score:.0f}
                        </div>
                        <div style="font-size: 16px; font-weight: bold; color: {fng_color}; margin-top: 5px;">
                            {fng_icon} {fng_th_rating}
                        </div>
                        <div style="font-size: 12px; color: gray; margin-top: 5px;">
                            Updated: {fng_timestamp[:10]}
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    
                with c_fng2:
                    # Gauge Chart
                    fig_fng = go.Figure(go.Indicator(
                        mode = "gauge+number",
                        value = fng_score,
                        domain = {'x': [0, 1], 'y': [0, 1]},
                        title = {'text': "Sentiment (0-100)", 'font': {'size': 14}},
                        gauge = {
                            'axis': {'range': [0, 100], 'tickwidth': 1, 'tickcolor': "darkblue"},
                            'bar': {'color': fng_color},
                            'bgcolor': "white",
                            'borderwidth': 2,
                            'bordercolor': "gray",
                            'steps': [
                                {'range': [0, 25], 'color': '#fee2e2'}, # Light Red
                                {'range': [25, 45], 'color': '#ffedd5'}, # Light Orange
                                {'range': [45, 55], 'color': '#f3f4f6'}, # Light Gray
                                {'range': [55, 75], 'color': '#dcfce7'}, # Light Green
                                {'range': [75, 100], 'color': '#d1fae5'} # Light Emerald
                            ],
                            'threshold': {
                                'line': {'color': "black", 'width': 4},
                                'thickness': 0.75,
                                'value': fng_score
                            }
                        }
                    ))
                    fig_fng.update_layout(height=220, margin=dict(l=20, r=20, t=30, b=20))
                    st.plotly_chart(fig_fng, use_container_width=True)
        else:
             st.warning("⚠️ F&G Index Unavailable")

    # --- MARKET SENTIMENT ANALYSIS (Combined VIX + F&G) ---
    if vix_data and fng_data:
        st.markdown("---")
        st.subheader("💡 วิเคราะห์จุดซื้อขายจากอารมณ์ตลาด (Market Timing)")
        
        # Logic Variables
        # Check if variables exist in scope, otherwise re-extract
        v_price = vix_data['current']
        f_score = fng_data['score']
        
        sentiment_status = "Neutral (ปกติ)"
        sentiment_color = "#6b7280" # Gray
        sentiment_action = "Wait & See (รอดูสถานการณ์ / ลงทุนตามแผนปกติ)"
        sentiment_desc = "ตลาดอยู่ในภาวะปกติ ไม่มีความกลัวหรือความโลภที่รุนแรงมากนัก เน้นคัดเลือกหุ้นรายตัว (Stock Selection)"
        
        # 1. Super Buy (Panic + Extreme Fear) -> Buy when others are fearful
        if v_price >= 30 and f_score <= 25:
            sentiment_status = "🔥 โอกาสทอง (Super Buy Opportunity)"
            sentiment_color = "#047857" # Dark Green
            sentiment_action = "Aggressive Buy (กล้าซื้อสวนตลาด)"
            sentiment_desc = "ทุกคนกลัวสุดขีด (Extreme Fear) + ตลาดเทขายรุนแรง (High VIX) = **เวลาที่ดีที่สุดในการซื้อหุ้นดีราคาถูก** (Warren Buffett: 'Be greedy when others are fearful')"
            
        # 2. Buy/Accumulate (Moderate Fear)
        elif (v_price >= 20) and (f_score < 45):
            sentiment_status = "✅ น่าสะสม (Accumulate)"
            sentiment_color = "#10b981" # Green
            sentiment_action = "Buy on Dip (ซื้อเมื่อย่อตัว)"
            sentiment_desc = "ตลาดมีความกังวล ราคาหุ้นเริ่มถูกลง เป็นจังหวะดีในการทยอยสะสมหุ้นพื้นฐานดี"
            
        # 3. Warning/Sell (Complacency + Extreme Greed) -> Sell when others are greedy
        elif v_price < 20 and f_score >= 75:
            sentiment_status = "⚠️ ระวังแรงขาย (Overbought/Caution)"
            sentiment_color = "#ef4444" # Red
            sentiment_action = "Take Profit / Wait (ขายทำกำไร / ชะลอการซื้อ)"
            sentiment_desc = "ตลาดนิ่งนอนใจ (Low VIX) + มีความโลภสูง (Extreme Greed) = **ความเสี่ยงในการปรับฐานสูง** (Warren Buffett: 'Be fearful when others are greedy')"
            
        # 4. Danger Zone (Extreme Complacency / Bubble)
        elif v_price < 15 and f_score >= 85:
             sentiment_status = "⛔ ฟองสบู่/ความเสี่ยงสูง (Bubble Risk)"
             sentiment_color = "#b91c1c" # Dark Red
             sentiment_action = "Defensive / Hold Cash (ถือเงินสด / ระวังดอย)"
             sentiment_desc = "ตลาดมั่นใจเกินเหตุ (Very Low VIX) + โลภสุดขีด = **ระวังการปรับฐานรุนแรง**"

        # 5. Mixed Signals (High Volatility but High Greed? Rare)
        elif v_price > 30 and f_score > 60:
            sentiment_status = "😵 ตลาดสับสน/ผันผวน (Mixed Signals)"
            sentiment_color = "#f59e0b" # Orange
            sentiment_action = "Wait & Watch (จับตาดูใกล้ชิด)"
            sentiment_desc = "ความผันผวนสูงแต่ตลาดยังโลภ อาจเกิดจากข่าวดี/ร้ายที่รุนแรงเฉพาะกลุ่ม"

        # Display Logic
        with st.container():
            st.markdown(f"""
            <div style="padding: 20px; border-radius: 10px; border: 2px solid {sentiment_color}; background-color: rgba(255,255,255,0.05);">
                <h3 style="margin-top: 0; color: {sentiment_color};">สรุปคำแนะนำ: {sentiment_status}</h3>
                <p style="font-size: 18px; font-weight: bold;">🎯 กลยุทธ์: {sentiment_action}</p>
                <p style="font-style: italic;">"{sentiment_desc}"</p>
                <hr style="margin: 10px 0; border-top: 1px dashed #ccc;">
                <small style="color: gray;">
                    <b>เกณฑ์การวิเคราะห์:</b><br>
                    • <b>Buy:</b> VIX สูง (กลัว) + F&G ต่ำ (กลัวสุดขีด)<br>
                    • <b>Sell:</b> VIX ต่ำ (นิ่งนอนใจ) + F&G สูง (โลภสุดขีด)
                </small>
            </div>
            """, unsafe_allow_html=True)
    
    # Dashboard uses 'df' loaded globally
    
    if not df.empty:
        # --- LOGIC: ACTION STATUS (Traffic Lights) ---
        def get_action_status(row):
            # 1. Buy Signal (Green)
            # VI Score >= 7 AND Undervalued (MOS > 0) AND Price < DDM
            # Strong Buy if MOS > 15%
            score = row.get('VI Score', 0)
            mos = row.get('margin_of_safety', -100) # Use base MOS (vs Fair) or DDM? Let's use DDM if available
            ddm = row.get('valuation_ddm', 0)
            price = row.get('price', 0)
            
            # Recalculate MOS based on DDM for consistency with user preference
            mos_ddm = ((ddm - price) / ddm * 100) if ddm > 0 else -100
            
            if score >= 7 and mos_ddm > 0:
                if mos_ddm > 15:
                    return "Strong Buy"
                return "Buy"
            
            # 2. Sell Signal (Red)
            # Overvalued significantly (MOS < -20%) OR Fundamentals Drop (Score < 5)
            if mos_ddm < -20 or score < 5:
                return "Sell"
                
            # 3. Hold Signal (Yellow)
            return "Hold"

        df['Action'] = df.apply(get_action_status, axis=1)

        # --- DAILY ACTION SUMMARY ---
        st.markdown(f"### 📢 สรุปโอกาสลงทุนวันนี้ (Daily Action Summary)")
        st.caption(f"ข้อมูลล่าสุดเมื่อ: {last_fetch_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        with st.expander("ℹ️ อ่านคำแนะนำสัญญาณการลงทุน (Action Guide)"):
            st.markdown("""
            **ความหมายของสัญญาณ (Signal Definition):**
            *   🟢 **Strong Buy (ซื้อสะสม):** หุ้นคุณภาพดี (VI Score ≥ 7) และราคาถูกมาก (MOS > 15%)
            *   🟢 **Buy (ซื้อ):** หุ้นคุณภาพดี (VI Score ≥ 7) และราคาถูกกว่ามูลค่าจริง (MOS > 0%)
            *   🟡 **Hold (ถือ/ชะลอการซื้อ):** หุ้นที่ราคาเริ่มเต็มมูลค่า หรือคุณภาพปานกลาง
            *   🔴 **Sell (ขาย/หลีกเลี่ยง):** หุ้นที่ราคาแพงเกินไป (MOS < -20%) หรือพื้นฐานแย่ลง (VI Score < 5)
            
            *หมายเหตุ: เป็นเพียงการคัดกรองเบื้องต้น ควรศึกษาข้อมูลรายตัวเพิ่มเติมก่อนตัดสินใจ*
            """)

        col_act1, col_act2, col_act3 = st.columns(3)
        
        # Filter Lists
        buy_list = df[df['Action'] == 'Strong Buy']
        hold_list = df[df['Action'] == 'Hold']
        sell_list = df[df['Action'] == 'Sell']
        

        with col_act1:
            st.success(f"🟢 **หุ้นน่าสะสม (Strong Buy): {len(buy_list)} ตัว**")
            if not buy_list.empty:
                st.dataframe(
                    buy_list[['symbol', 'price', 'valuation_ddm', 'VI Score']].style.format({'price': '{:.2f}', 'valuation_ddm': '{:.2f}'}), 
                    hide_index=True,
                    height=250
                )
            else:
                st.caption("วันนี้ยังไม่มีหุ้นเข้าเกณฑ์ Strong Buy")
                
        with col_act2:
            st.warning(f"🟡 **หุ้นถือรอ/พักเงิน (Hold): {len(hold_list)} ตัว**")
            if not hold_list.empty:
                st.dataframe(
                    hold_list[['symbol', 'price', 'valuation_ddm', 'VI Score']].style.format({'price': '{:.2f}', 'valuation_ddm': '{:.2f}'}), 
                    hide_index=True,
                    height=250
                )
            else:
                st.caption(f"หุ้นพื้นฐานดีแต่ราคาเริ่มเต็มมูลค่า")

        with col_act3:
            st.error(f"🔴 **หุ้นควรระวัง/ขาย (Sell/Avoid): {len(sell_list)} ตัว**")
            if not sell_list.empty:
                st.dataframe(
                    sell_list[['symbol', 'price', 'valuation_ddm', 'VI Score']].style.format({'price': '{:.2f}', 'valuation_ddm': '{:.2f}'}), 
                    hide_index=True,
                    height=250
                )
            else:
                st.caption("ไม่มีหุ้นที่ต้องระวังเป็นพิเศษ")
        


        # --- Styling Functions ---
        def highlight_price_ddm(x):
            df_st = pd.DataFrame('', index=x.index, columns=x.columns)
            # Use selected base (val_base) for comparison
            target_col = val_base # 'DDM', 'Fair', 'Graham', 'VI Price'
            
            if 'ราคา' in x.columns and target_col in x.columns:
                 # Target > Price -> Green (Undervalued)
                 # Target < Price -> Red (Overvalued)
                 # Only if Target > 0
                 # Ensure numeric
                 tgt = pd.to_numeric(x[target_col], errors='coerce').fillna(0)
                 prc = pd.to_numeric(x['ราคา'], errors='coerce').fillna(0)
                 
                 mask_valid = (tgt > 0)
                 mask_green = mask_valid & (tgt > prc)
                 mask_red = mask_valid & (tgt < prc)
                 
                 df_st.loc[mask_green, 'ราคา'] = 'background-color: #d4edda; color: black' # Light Green
                 df_st.loc[mask_red, 'ราคา'] = 'background-color: #f8d7da; color: black' # Light Red
            return df_st

        # Key Metrics
        col1, col2, col3 = st.columns(3)
        undervalued_count = df[df['status'] == 'Undervalued'].shape[0]
        avg_mos = df['margin_of_safety'].mean()
        
        col1.metric("หุ้นที่วิเคราะห์", f"{len(df)}")
        col2.metric("หุ้นราคาถูกกว่ามูลค่า", f"{undervalued_count}")
        col3.metric("ส่วนเผื่อความปลอดภัยเฉลี่ย (MOS)", f"{avg_mos:.2f}%")
        
        # --- QUALITY SCORING (Enhanced Auto 10 Points) ---
        # 1. Low Debt (D/E < 200%)
        # 2. Strong ROE (> 15%)
        # 3. High NPM (> 10%)
        # 4. Undervalued (MOS > 0)
        # 5. Market Leader Proxy (Market Cap > 50 Billion THB)
        # 6. Growth Proxy (Revenue Growth > 0%)
        # 7. Cash Flow Strength (FCF > 0)
        # 8. Valuation Growth (PEG < 1.5)
        # 9. Liquidity (Current Ratio > 1.5)
        # 10. Competitive Advantage (Gross Margin > 20%)
        
        # Sidebar Filter
        st.sidebar.markdown("---")
        st.sidebar.subheader("🔍 ตัวกรองหุ้น (Screener)")
        st.sidebar.info("ℹ️ **ระบบคะแนนใหม่ (VI Score):** เต็ม **10 คะแนน** เพิ่มเกณฑ์ FCF, PEG, สภาพคล่อง, และ Gross Margin")
        
        # Two-step slider or separate? Let's use one slider for VI Score
        min_score = st.sidebar.slider("คะแนนคุณภาพขั้นต่ำ (เต็ม 10)", 0, 10, 6, help="กรองจาก 10 ปัจจัยคุณภาพ (เดิม 6 + ใหม่ 4)")
        
        # Add checkbox for "Cash Flow Positive Only"
        filter_fcf = st.sidebar.checkbox("เฉพาะที่มีกระแสเงินสดอิสระบวก (FCF > 0)", value=False)
        
        # Valuation Base Selection
        val_base = st.sidebar.selectbox(
            "เลือกเกณฑ์เทียบราคา (Valuation Base)", 
            ['DDM', 'Fair', 'Graham', 'VI Price'],
            index=0, 
            help="ใช้สำหรับคำนวณ MOS% และสีของราคา (Price Color)"
        )
        
        filtered_df = df[df['VI Score'] >= min_score].copy()
        
        if filter_fcf:
            filtered_df = filtered_df[filtered_df['freeCashflow'] > 0]

        # --- ADVANCED SCANNING (Magic Formula & F-Score) ---
        st.sidebar.markdown("---")
        st.sidebar.subheader("🚀 วิเคราะห์เชิงลึก")
        
        # Initialize session state for advanced results if not exists
        if 'advanced_results' not in st.session_state:
            st.session_state['advanced_results'] = {}

        if st.sidebar.button("วิเคราะห์ Magic Formula & F-Score"):

            
            # Filter stocks to analyze (only from the filtered list to save time)
            targets = filtered_df['symbol'].tolist()
            
            progress_bar = st.sidebar.progress(0)
            status_text = st.sidebar.empty()
            
            results_adv = []
            
            # Use ThreadPool but limit workers to avoid rate limit/database lock
            # Since we are fetching deep financials, 20 workers is safe enough with our cache patch
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                future_to_ticker = {executor.submit(utils.calculate_magic_formula_and_f_score, ticker): ticker for ticker in targets}
                
                completed = 0
                total = len(targets)
                
                for future in concurrent.futures.as_completed(future_to_ticker):
                    res = future.result()
                    if res:
                        results_adv.append(res)
                    
                    completed += 1
                    progress = completed / total
                    progress_bar.progress(progress)
                    status_text.text(f"วิเคราะห์ {completed}/{total}")
            
            progress_bar.empty()
            status_text.empty()
            
            # Save to session state
            st.session_state['advanced_results'] = {r['symbol']: r for r in results_adv}
            st.success(f"วิเคราะห์เสร็จสิ้น! พบข้อมูล {len(results_adv)} หุ้น")
            st.rerun()

        # Merge Advanced Results if available
        if st.session_state['advanced_results']:
            # Create DataFrame from session state
            adv_df = pd.DataFrame(st.session_state['advanced_results'].values())
            
            # Merge with filtered_df
            if not adv_df.empty:
                # Use left merge to keep filtered_df rows
                filtered_df = filtered_df.merge(adv_df, on='symbol', how='left')
                
                # Fill NaNs for display
                filtered_df['magic_roc'] = filtered_df['magic_roc'].fillna(0)
                filtered_df['magic_ey'] = filtered_df['magic_ey'].fillna(0)
                filtered_df['f_score'] = filtered_df['f_score'].fillna(-1) # -1 means N/A

        
        # --- TOP 10 SUPER STOCKS (Integrated) ---
        st.markdown("---")
        st.subheader("🏆 10 สุดยอดหุ้นแกร่ง (The Super Stocks)")
        st.markdown(f"""
        คัดเลือกจาก **ราคาถูก (MOS > 0)**, **คุณภาพดีเยี่ยม (VI Score > {min_score})**, **กระแสเงินสดแกร่ง**, และ **ความเสี่ยงต่ำ**
        """)
        
        # Calculate yield first (for filtering)
        df['dividendYield_calc'] = df['dividendRate'] / df['price']
        
        # 1. Base Filter (Using VI Score)
        # We relax dividend rule slightly for Growth/Quality focus if Score is high
        super_candidates = df[
            (df['status'] == 'Undervalued') & 
            (df['VI Score'] >= min_score)
        ].copy()
        
        # 2. Advanced Scoring (if available)
        if 'magic_roc' in filtered_df.columns:
            # Join advanced data to candidates if not already joined
            # Note: We added 'graham_num', 'fcf_yield', 'z_score', 'sgr' to utils.py
            
            adv_cols = ['symbol', 'magic_roc', 'magic_ey', 'f_score', 'graham_num', 'fcf_yield', 'z_score', 'sgr']
            # Check if columns exist in filtered_df (in case user hasn't re-run analysis yet)
            adv_cols = [c for c in adv_cols if c in filtered_df.columns or c == 'symbol']
            
            if 'magic_roc' not in super_candidates.columns:
                 super_candidates = super_candidates.merge(filtered_df[adv_cols], on='symbol', how='left')
            
            # Fill N/A for those without deep scan
            super_candidates['magic_roc'] = super_candidates['magic_roc'].fillna(0)
            super_candidates['magic_ey'] = super_candidates['magic_ey'].fillna(0)
            super_candidates['f_score'] = super_candidates['f_score'].fillna(0)
            super_candidates['graham_num'] = super_candidates['graham_num'].fillna(0)
            super_candidates['fcf_yield'] = super_candidates['fcf_yield'].fillna(0)
            super_candidates['z_score'] = super_candidates['z_score'].fillna(0)
            super_candidates['sgr'] = super_candidates['sgr'].fillna(0)

            # Calculate Composite Score (Max 100)
            # MOS (30%) + Dividend (15%) + ROE (15%) + F-Score (20%) + Magic Rank (20%)
            
            # Rank Magic (Lower is better) -> Invert for scoring
            super_candidates['rank_roc'] = super_candidates['magic_roc'].rank(ascending=False)
            super_candidates['rank_ey'] = super_candidates['magic_ey'].rank(ascending=False)
            super_candidates['magic_rank_score'] = 100 - (super_candidates['rank_roc'] + super_candidates['rank_ey']) # Rough inversion
            
            # Normalize scores to 0-1 range for weighting
            def normalize(series):
                return (series - series.min()) / (series.max() - series.min()) if (series.max() - series.min()) > 0 else 0

            norm_mos = normalize(super_candidates['margin_of_safety'])
            norm_div = normalize(super_candidates['dividendYield_calc'])
            norm_roe = normalize(super_candidates['returnOnEquity'])
            norm_f = super_candidates['f_score'] / 9.0 # F-score is 0-9
            norm_magic = normalize(super_candidates['magic_rank_score'])
            norm_fcf = normalize(super_candidates['fcf_yield'])
            norm_z = normalize(super_candidates['z_score'])
            norm_sgr = normalize(super_candidates['sgr'])
            norm_viscore = normalize(super_candidates['VI Score']) # Add VI Score

            # Adjusted weighting for FCF, Z-Score, SGR
            super_candidates['Super_Score'] = (
                (norm_mos * 0.15) + 
                (norm_div * 0.05) + 
                (norm_roe * 0.10) + 
                (norm_f * 0.10) + 
                (norm_magic * 0.10) +
                (norm_fcf * 0.15) +
                (norm_z * 0.10) +
                (norm_sgr * 0.05) +
                (norm_viscore * 0.20) # High weight on VI Score
            ) * 100
            
            # Sort by Super Score
            top_picks = super_candidates.sort_values(by='Super_Score', ascending=False).head(10)
        
        else:
            # Fallback to original sorting if no advanced data yet
            st.info("💡 **Tips:** กดปุ่ม 'วิเคราะห์ Magic Formula & F-Score' ด้านซ้าย เพื่อเพิ่มความแม่นยำในการจัดอันดับ")
            # Sort by VI Score then MOS
            top_picks = super_candidates.sort_values(by=['VI Score', 'margin_of_safety'], ascending=[False, False]).head(10)
        
        
        if not top_picks.empty:
            # Calculate additional ratios for Super Stocks if missing
            top_picks['P/E'] = top_picks.apply(lambda row: row['price'] / row['trailingEps'] if row.get('trailingEps', 0) > 0 else 0, axis=1)
            top_picks['P/BV'] = top_picks.apply(lambda row: row['price'] / row['bookValue'] if row.get('bookValue', 0) > 0 else 0, axis=1)
            
            # Display Top 10 nicely
            cols_to_show = [
                'symbol', 'VI Score', 'price', 'fair_value', 'valuation_ddm'
            ]
            col_names = [
                'หุ้น', 'VI Score', 'ราคา', 'Fair', 'DDM'
            ]
            
            # If advanced analysis is done, insert Graham next to Fair Value
            if 'Super_Score' in top_picks.columns:
                 # Calculate VI Price (Average of Fair and Graham)
                 # Handle cases where Graham is 0 or NaN
                 def calc_vi_price(row):
                     vals = []
                     if row['fair_value'] > 0: vals.append(row['fair_value'])
                     if row['graham_num'] > 0: vals.append(row['graham_num'])
                     return sum(vals) / len(vals) if vals else 0
                 
                 top_picks['vi_price'] = top_picks.apply(calc_vi_price, axis=1)
                 top_picks['vi_mos'] = top_picks.apply(lambda row: ((row['vi_price'] - row['price']) / row['vi_price'] * 100) if row['vi_price'] > 0 else 0, axis=1)
                 
                 cols_to_show.extend(['graham_num', 'vi_price', 'vi_mos'])
                 col_names.extend(['Graham', 'VI Price', 'VI MOS%'])
            else:
                 # Standard MOS if no Graham (Override to DDM MOS per user request)
                 top_picks['mos_ddm'] = top_picks.apply(
                    lambda row: ((row['valuation_ddm'] - row['price']) / row['valuation_ddm'] * 100) 
                    if (pd.notna(row['valuation_ddm']) and row['valuation_ddm'] > 0) else -999,
                    axis=1
                 )
                 cols_to_show.append('mos_ddm')
                 col_names.append('MOS%')

            # Add remaining base columns
            cols_to_show.extend([
                'P/E', 'P/BV', 'trailingEps', 'returnOnAssets',
                'returnOnEquity', 'debtToEquityRatio', 'currentRatio', 'profitMargins',
                'dividendRate', 'dividendYield_calc', 'VI Score',
                'terminal_growth_percent', 'k_percent'
            ])
            col_names.extend([
                'P/E', 'P/BV', 'EPS', 'ROA%',
                'ROE%', 'D/E', 'Liquidity', 'NPM%',
                'ปันผล(฿)', 'ปันผล(%)', 'VI Score',
                'G%', 'K%'
            ])
            
            # Add remaining advanced columns
            if 'Super_Score' in top_picks.columns:
                cols_to_show.extend(['fcf_yield', 'z_score', 'sgr', 'f_score', 'magic_roc', 'magic_ey', 'Super_Score'])
                col_names.extend(['FCF%', 'Z-Score', 'SGR%', 'F-Score', 'ROC%', 'EY%', 'Score'])
            
            top_display = top_picks[cols_to_show].copy()
            top_display.columns = col_names
            
            # Remove duplicate columns if any (e.g. VI Score if added multiple times)
            top_display = top_display.loc[:, ~top_display.columns.duplicated()]

            # Dynamic formatting dict
            fmt_dict = {
                'ราคา': '{:.2f}',
                'Fair': '{:.2f}',
                'DDM': '{:.2f}',
                'Graham': '{:.2f}',
                'VI Price': '{:.2f}',
                'VI MOS%': '{:.2f}',
                'MOS%': '{:.2f}',
                'P/E': '{:.2f}',
                'P/BV': '{:.2f}',
                'EPS': '{:.2f}',
                'ROA%': '{:.2%}',
                'ROE%': '{:.2%}',
                'D/E': '{:.2f}',
                'Liquidity': '{:.2f}',
                'NPM%': '{:.2%}',
                'ปันผล(฿)': '{:.2f}',
                'ปันผล(%)': '{:.2%}',
                'ROC%': '{:.2%}',
                'EY%': '{:.2%}',
                'Score': '{:.0f}',
                'F-Score': '{:.0f}',
                'VI Score': '{:.0f}',
                'FCF%': '{:.2%}',
                'Z-Score': '{:.2f}',
                'SGR%': '{:.2%}',
                'G%': '{:.2f}',
                'K%': '{:.2f}'
            }
            
            # Determine which MOS column to use for gradient
            mos_col = 'VI MOS%' if 'VI MOS%' in top_display.columns else 'MOS%'
            
            def highlight_vi_price(x):
                # Create a DataFrame of styles
                df_st = pd.DataFrame('', index=x.index, columns=x.columns)
                if 'VI Price' in x.columns:
                    df_st['VI Price'] = 'background-color: #fff9c4; color: black; font-weight: bold' # Light Yellow
                return df_st

            st.dataframe(
                top_display.style.format(fmt_dict)
                .background_gradient(subset=[mos_col], cmap='Greens')
                .apply(highlight_vi_price, axis=None)
                .apply(highlight_price_ddm, axis=None),
                use_container_width=True
            )
        else:
            st.warning("ไม่พบหุ้นที่ผ่านเกณฑ์พื้นฐาน (MOS > 0, ROE > 10%, ปันผล > 3%) ลองปรับเกณฑ์ความเสี่ยงดูครับ")

        # Main Screener Results
        st.markdown("---")
        st.subheader(f"ผลการคัดกรองหุ้นทั้งหมด (พบ: {len(filtered_df)} ตัว)")
        
        # Formatting for display
        
        # Calculate P/E and P/BV
        # P/E = Price / EPS
        # P/BV = Price / Book Value
        filtered_df['P/E'] = filtered_df.apply(
            lambda row: (row['price'] / row['trailingEps']) if (pd.notna(row['trailingEps']) and row['trailingEps'] != 0) else 0, 
            axis=1
        )
        filtered_df['P/BV'] = filtered_df.apply(lambda row: row['price'] / row['bookValue'] if row['bookValue'] > 0 else 0, axis=1)
        
        filtered_df['dividendYield_pct'] = filtered_df.apply(
            lambda row: (
                (row.get('dividendYield') / 100) if (pd.notna(row.get('dividendYield')) and row.get('dividendYield') > 1)
                else (row.get('dividendYield') if pd.notna(row.get('dividendYield')) else (row.get('dividendRate') / row.get('price') if row.get('price') and row.get('price') > 0 else 0))
            ),
            axis=1
        )

        

        # --- Calculate Graham Number & VI Price (Consistency with Super Stocks) ---
        # Graham Number = Sqrt(22.5 * EPS * BVPS)
        filtered_df['graham_num'] = filtered_df.apply(
            lambda row: (22.5 * row['trailingEps'] * row['bookValue'])**0.5 
            if (row['trailingEps'] > 0 and row['bookValue'] > 0) else 0, 
            axis=1
        )

        # VI Price = Average(Fair Value, Graham Number)
        def calc_vi_price_main(row):
             vals = []
             if row['fair_value'] > 0: vals.append(row['fair_value'])
             if row['graham_num'] > 0: vals.append(row['graham_num'])
             return sum(vals) / len(vals) if vals else 0

        filtered_df['vi_price'] = filtered_df.apply(calc_vi_price_main, axis=1)
        
        # Override MOS% to be based on Selected Base (val_base)
        mos_target_map = {'DDM': 'valuation_ddm', 'Fair': 'fair_value', 'Graham': 'graham_num', 'VI Price': 'vi_price'}
        mos_col_name = mos_target_map.get(val_base, 'valuation_ddm')
        
        filtered_df['mos_ddm'] = filtered_df.apply(
            lambda row: ((row[mos_col_name] - row['price']) / row[mos_col_name] * 100) 
            if (pd.notna(row[mos_col_name]) and row[mos_col_name] > 0) else -999, 
            axis=1
        )
        
        display_df = filtered_df[[
            'symbol', 'Action', 'price', 'fair_value', 'valuation_ddm', 'graham_num', 'vi_price', 'mos_ddm', 
            'P/E', 'pegRatio', 'P/BV', 'trailingEps', 
            'returnOnAssets', 'returnOnEquity', 
            'grossMargins', 'operatingMargins', 'profitMargins',
            'debtToEquityRatio', 'ibdToEquity', 'currentRatio', 'quickRatio',
            'revenueGrowth', 'enterpriseToEbitda',
            'dividendRate', 'dividendYield_pct', 'Quality Score',
            'terminal_growth_percent', 'k_percent'
        ]].copy()
        
        # Convert decimals to percentages for display (x100)
        cols_to_percent = [
            'returnOnAssets', 'returnOnEquity', 
            'grossMargins', 'operatingMargins', 'profitMargins',
            'revenueGrowth', 'dividendYield_pct'
        ]
        
        for col in cols_to_percent:
            if col in display_df.columns:
                display_df[col] = display_df[col] * 100

        # Rename columns for readable headers
        display_df.columns = [
            'หุ้น', 'สถานะ', 'ราคา', 'Fair', 'DDM', 'Graham', 'VI Price', 'MOS%',
            'P/E', 'PEG', 'P/BV', 'EPS',
            'ROA%', 'ROE%',
            'GPM%', 'OPM%', 'NPM%',
            'D/E', 'IBD/E', 'Liquidity', 'Quick',
            'Growth%', 'EV/EBITDA',
            'ปันผล(฿)', 'ปันผล(%)', 'Q-Score',
            'G%', 'K%'
        ]
        
        def highlight_fair_main(x):
            df_st = pd.DataFrame('', index=x.index, columns=x.columns)
            
            # Highlight ONLY the selected valuation base column
            target_col = val_base
            if target_col in x.columns:
                df_st[target_col] = 'background-color: #fff9c4; color: black; font-weight: bold'

            # MOS% styling: Light green if > 20%
            if 'MOS%' in x.columns:
                mask_mos = x['MOS%'] > 20
                df_st.loc[mask_mos, 'MOS%'] = 'background-color: #dcfce7; color: black; font-weight: bold'

            if 'สถานะ' in x.columns:
                mask_sbuy = x['สถานะ'] == 'Strong Buy'
                mask_buy = x['สถานะ'] == 'Buy'
                mask_sell = x['สถานะ'] == 'Sell'
                mask_hold = x['สถานะ'] == 'Hold'
                df_st.loc[mask_sbuy, 'สถานะ'] = 'background-color: #10b981; color: white; font-weight: bold'
                df_st.loc[mask_buy, 'สถานะ'] = 'background-color: #d1fae5; color: #065f46; font-weight: bold'
                df_st.loc[mask_sell, 'สถานะ'] = 'background-color: #fee2e2; color: #991b1b; font-weight: bold'
                df_st.loc[mask_hold, 'สถานะ'] = 'background-color: #fef3c7; color: #92400e'
            return df_st

        # Apply formatting
        embed_trend_col = st.checkbox("ฝัง Trend (5Y) ในคอลัมน์ของตาราง", value=True)
        if embed_trend_col:
            try:
                max_rows_embed = 100
                target_symbols = display_df['หุ้น'].head(max_rows_embed).tolist()
                
                # 1. Fetch EPS Trend
                eps_map_embed = utils.get_eps_10_years(target_symbols, years=6)
                
                # 2. Fetch Dividend Trend (New)
                # Changed to 10 years per user request
                div_map_embed = utils.get_dividends_batch(target_symbols, years=11) # Fetch 11 to be safe
                
                import datetime as _dt2
                current_year2 = _dt2.datetime.now().year
                target_years2 = list(range(current_year2 - 5, current_year2))
                target_years_div = list(range(current_year2 - 10, current_year2)) # 10 Years for Div Trend
                
                def _make_trend_image(sym):
                    item = eps_map_embed.get(sym, {})
                    hist = (item.get('history') or {})
                    values = []
                    import math as _math
                    import pandas as _p
                    for y in target_years2:
                        v = hist.get(y, 0.0)
                        try:
                            val = float(v) if v is not None else 0.0
                            if _p.isna(val) or not _math.isfinite(val):
                                val = 0.0
                            values.append(val)
                        except Exception:
                            values.append(0.0)
                    
                    # Create Sparkline Image
                    width, height = 100, 30
                    img = Image.new('RGBA', (width, height), (255, 255, 255, 0))
                    draw = ImageDraw.Draw(img)
                    
                    if not values:
                        return None
                    
                    min_val = min(values + [0]) # Ensure 0 is in range
                    max_val = max(values + [0])
                    
                    val_range = max_val - min_val
                    if val_range == 0:
                        val_range = 1 # Avoid div by zero
                        
                    # Config
                    bar_count = len(values)
                    bar_width = width / bar_count
                    padding = 2
                    
                    for i, val in enumerate(values):
                        # Normalize height
                        # Bottom is 0 (or min_val)
                        # We want 0 axis to be relative? Or just min-max scaling?
                        # Usually sparklines are min-max.
                        # But if we have negative values, we want 0 line?
                        # Let's stick to simple min-max scaling for visibility.
                        
                        # Scale value to 0..height
                        normalized = (val - min_val) / val_range
                        bar_h = normalized * height
                        if bar_h < 1: bar_h = 1 # Min 1px
                        
                        # Coordinates
                        x0 = i * bar_width + padding
                        x1 = (i + 1) * bar_width - padding
                        y1 = height # Bottom
                        y0 = height - bar_h # Top
                        
                        # Color: Green if Max, else Grey
                        color = "#22c55e" if val == max_val and val != 0 else "#94a3b8"
                        if val == 0 and max_val == 0 and min_val == 0:
                             color = "#e2e8f0" # All zero
                        
                        draw.rectangle([x0, y0, x1, y1], fill=color)
                        
                    # Convert to base64
                    buffer = io.BytesIO()
                    img.save(buffer, format="PNG")
                    img_str = base64.b64encode(buffer.getvalue()).decode()
                    return f"data:image/png;base64,{img_str}"

                def _make_div_trend_image(sym):
                    hist = div_map_embed.get(sym, {})
                    values = []
                    years_list = []
                    import math as _math
                    import pandas as _p
                    for y in target_years_div:
                        v = hist.get(str(y), 0.0)
                        try:
                            val = float(v) if v is not None else 0.0
                            if _p.isna(val) or not _math.isfinite(val):
                                val = 0.0
                            values.append(val)
                            years_list.append(y)
                        except Exception:
                            values.append(0.0)
                            years_list.append(y)
                    
                    if not values:
                        return None
                    
                    # SVG Generation
                    width, height = 120, 30
                    min_val = 0
                    max_val = max(values + [0])
                    val_range = max_val - min_val if (max_val - min_val) > 0 else 1
                    
                    bar_count = len(values)
                    bar_width = width / bar_count
                    padding = 1
                    
                    rects = []
                    for i, val in enumerate(values):
                        normalized = (val - min_val) / val_range
                        bar_h = normalized * height
                        if bar_h < 1 and val > 0: bar_h = 1
                        
                        x = i * bar_width + padding
                        y = height - bar_h
                        w = bar_width - (2 * padding)
                        if w < 1: w = 1
                        
                        color = "#22c55e" if val == max_val and val > 0 else "#94a3b8"
                        if val == 0: color = "#e2e8f0"
                        
                        # Add tooltip via title
                        tooltip = f"Year: {years_list[i]} &#10;Div: {val:.2f}"
                        rects.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{w:.1f}" height="{bar_h:.1f}" fill="{color}"><title>{tooltip}</title></rect>')
                        
                    svg = f'<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg">{"".join(rects)}</svg>'
                    
                    # Return SVG string directly (Streamlit ImageColumn supports raw SVG)
                    return f"data:image/svg+xml;base64,{base64.b64encode(svg.encode('utf-8')).decode('utf-8')}"

                display_df_embed = display_df.copy()
                display_df_embed['Trend (5Y)'] = display_df_embed['หุ้น'].apply(_make_trend_image)
                display_df_embed['Div Trend'] = display_df_embed['หุ้น'].apply(_make_div_trend_image)
                
                # Insert Div Trend after Trend (5Y)
                cols = list(display_df_embed.columns)
                # Remove special cols to re-insert
                base_cols = [c for c in cols if c not in ['Trend (5Y)', 'Div Trend']]
                # Find index of 'หุ้น'
                idx_sym = base_cols.index('หุ้น')
                
                # Reconstruct order: หุ้น, Trend, Div Trend, ...rest
                col_order = ['หุ้น', 'Trend (5Y)', 'Div Trend'] + base_cols[idx_sym+1:]
                
                cfg_embed = {
                    'หุ้น': st.column_config.TextColumn('หุ้น', width='small', pinned=True),
                    'Trend (5Y)': st.column_config.ImageColumn('Trend (5Y)', width=100, help="กำไรต่อหุ้น (EPS) ย้อนหลัง 5 ปี"),
                    'Div Trend': st.column_config.ImageColumn('Div Trend', width=120, help="เงินปันผล (Dividend) ย้อนหลัง 10 ปี (แท่งเขียว=สูงสุด)"),
                    'ราคา': st.column_config.NumberColumn('ราคา', format='%.2f', width='small'),
                    'Fair': st.column_config.NumberColumn('Fair', format='%.2f', width='small'),
                    'DDM': st.column_config.NumberColumn('DDM', format='%.2f', width='small'),
                    'Graham': st.column_config.NumberColumn('Graham', format='%.2f', width='small'),
                    'VI Price': st.column_config.NumberColumn('VI Price', format='%.2f', width='small'),
                    'MOS%': st.column_config.NumberColumn('MOS%', format='%.2f', width='small'),
                    'P/E': st.column_config.NumberColumn('P/E', format='%.2f', width='small'),
                    'PEG': st.column_config.NumberColumn('PEG', format='%.2f', width='small'),
                    'P/BV': st.column_config.NumberColumn('P/BV', format='%.2f', width='small'),
                    'EPS': st.column_config.NumberColumn('EPS', format='%.2f', width='small'),
                    'ROA%': st.column_config.NumberColumn('ROA%', format='%.2f%%', width='small'),
                    'ROE%': st.column_config.NumberColumn('ROE%', format='%.2f%%', width='small'),
                    'GPM%': st.column_config.NumberColumn('GPM%', format='%.2f%%', width='small'),
                    'OPM%': st.column_config.NumberColumn('OPM%', format='%.2f%%', width='small'),
                    'NPM%': st.column_config.NumberColumn('NPM%', format='%.2f%%', width='small'),
                    'D/E': st.column_config.NumberColumn('D/E', format='%.2f', width='small'),
                    'IBD/E': st.column_config.NumberColumn('IBD/E', format='%.2f', width='small'),
                    'Liquidity': st.column_config.NumberColumn('Liquidity', format='%.2f', width='small'),
                    'Quick': st.column_config.NumberColumn('Quick', format='%.2f', width='small'),
                    'Growth%': st.column_config.NumberColumn('Growth%', format='%.2f%%', width='small'),
                    'EV/EBITDA': st.column_config.NumberColumn('EV/EBITDA', format='%.2f', width='small'),
                    'ปันผล(฿)': st.column_config.NumberColumn('ปันผล(฿)', format='%.2f', width='small'),
                    'ปันผล(%)': st.column_config.NumberColumn('ปันผล(%)', format='%.2f%%', width='small'),
                    'Q-Score': st.column_config.NumberColumn('Q-Score', format='%.0f', width='small'),
                    'G%': st.column_config.NumberColumn('G%', format='%.2f', width='small'),
                    'K%': st.column_config.NumberColumn('K%', format='%.2f', width='small'),
                    'สถานะ': st.column_config.TextColumn('สถานะ', width=110),
                }
                
                fmt_map = {
                    'ราคา': '{:.2f}', 
                    'Fair': '{:.2f}', 
                    'DDM': '{:.2f}',
                    'Graham': '{:.2f}',
                    'VI Price': '{:.2f}',
                    'MOS%': '{:.2f}',
                    'P/E': '{:.2f}',
                    'PEG': '{:.2f}',
                    'P/BV': '{:.2f}',
                    'EPS': '{:.2f}',
                    'ROA%': '{:.2%}',
                    'ROE%': '{:.2%}',
                    'GPM%': '{:.2%}',
                    'OPM%': '{:.2%}',
                    'NPM%': '{:.2%}',
                    'D/E': '{:.2f}',
                    'IBD/E': '{:.2f}',
                    'Liquidity': '{:.2f}',
                    'Quick': '{:.2f}',
                    'Growth%': '{:.2%}',
                    'EV/EBITDA': '{:.2f}',
                    'ปันผล(฿)': '{:.2f}',
                    'ปันผล(%)': '{:.2%}',
                    'G%': '{:.2f}',
                    'K%': '{:.2f}'
                }
                styled_embed = (
                    display_df_embed
                    .style
                    .format(fmt_map)
                    .apply(highlight_fair_main, axis=None)
                    .apply(highlight_price_ddm, axis=None)
                )
                st.dataframe(
                    styled_embed,
                    column_order=[c for c in col_order if c in display_df_embed.columns],
                    column_config=cfg_embed,
                    use_container_width=True,
                    height=600,
                    on_select="rerun",
                    selection_mode="multi-row",
                    key="main_table_selection" # Add key for accessing state
                )
            except Exception:
                # Fallback to original styled table if anything fails
                st.dataframe(
                    display_df.style.format({
                        'ราคา': '{:.2f}', 
                        'Fair': '{:.2f}', 
                        'DDM': '{:.2f}',
                        'Graham': '{:.2f}',
                        'VI Price': '{:.2f}',
                        'MOS%': '{:.2f}',
                        'P/E': '{:.2f}',
                        'PEG': '{:.2f}',
                        'P/BV': '{:.2f}',
                        'EPS': '{:.2f}',
                        'ROA%': '{:.2%}',
                        'ROE%': '{:.2%}',
                        'GPM%': '{:.2%}',
                        'OPM%': '{:.2%}',
                        'NPM%': '{:.2%}',
                        'D/E': '{:.2f}',
                        'Liquidity': '{:.2f}',
                        'Quick': '{:.2f}',
                        'Growth%': '{:.2%}',
                        'EV/EBITDA': '{:.2f}',
                        'ปันผล(฿)': '{:.2f}',
                        'ปันผล(%)': '{:.2%}',
                        'G%': '{:.2f}',
                        'K%': '{:.2f}'
                    })
                    .apply(highlight_fair_main, axis=None)
                    .apply(highlight_price_ddm, axis=None),
                    use_container_width=True,
                    height=600
                )
        else:
            st.dataframe(
                display_df.style.format({
                    'ราคา': '{:.2f}', 
                    'Fair': '{:.2f}', 
                    'DDM': '{:.2f}',
                    'Graham': '{:.2f}',
                    'VI Price': '{:.2f}',
                    'MOS%': '{:.2f}',
                    'P/E': '{:.2f}',
                    'PEG': '{:.2f}',
                    'P/BV': '{:.2f}',
                    'EPS': '{:.2f}',
                    'ROA%': '{:.2%}',
                    'ROE%': '{:.2%}',
                    'GPM%': '{:.2%}',
                    'OPM%': '{:.2%}',
                    'NPM%': '{:.2%}',
                    'D/E': '{:.2f}',
                    'Liquidity': '{:.2f}',
                    'Quick': '{:.2f}',
                    'Growth%': '{:.2%}',
                    'EV/EBITDA': '{:.2f}',
                    'ปันผล(฿)': '{:.2f}',
                    'ปันผล(%)': '{:.2%}',
                    'G%': '{:.2f}',
                    'K%': '{:.2f}'
                })
                .apply(highlight_fair_main, axis=None)
                .apply(highlight_price_ddm, axis=None),
                use_container_width=True,
                height=600
            )
        
        # --- NEW: Dividend History Chart ---
        st.markdown("---")
        st.subheader("💰 ประวัติการจ่ายปันผลย้อนหลัง 10 ปี (Dividend History)")
        
        # Check for selection
        selected_tickers_from_table = []
        if "main_table_selection" in st.session_state:
            selection = st.session_state["main_table_selection"]
            # Streamlit 1.35+ returns structure with 'selection': {'rows': [...], 'columns': [...]}
            if selection and "selection" in selection and "rows" in selection["selection"]:
                 selected_indices = selection["selection"]["rows"]
                 if selected_indices:
                      try:
                         # Ensure indices are integers
                         # display_df_embed has default RangeIndex or similar?
                         # We need to be careful if display_df_embed index is not 0..N
                         # display_df_embed was created from display_df.copy().
                         # display_df was created from filtered_df.
                         # If filtered_df has non-standard index, we might need iloc
                         # selection['rows'] are integer positions (0-based) of the displayed data?
                         # Docs: "The selection state contains the indices of the selected rows."
                         # "When the user sorts the dataframe, the indices returned are the indices of the sorted dataframe."
                         # Wait, actually recent Streamlit updates say:
                         # "selection.rows returns the index labels of the selected rows."
                         # NO, it returns the integer index (iloc) if index is not set, or labels (loc) if set?
                         # Let's assume iloc for now, but to be safe, let's use the index of display_df_embed
                         
                         # Best practice: 
                         # st.dataframe(df, on_select="rerun", selection_mode="multi-row")
                         # event.selection.rows -> list of row INDICES (labels).
                         
                         selected_rows = display_df_embed.iloc[selected_indices]
                         selected_tickers_from_table = selected_rows['หุ้น'].tolist()
                      except Exception as e:
                         # st.write(f"Debug Selection Error: {e}")
                         pass

        default_div_targets = filtered_df['symbol'].head(3).tolist()
        if selected_tickers_from_table:
            # Filter to ensure they exist in current filtered_df (though they should)
            valid_selections = [t for t in selected_tickers_from_table if t in filtered_df['symbol'].values]
            if valid_selections:
                default_div_targets = valid_selections

        div_targets = st.multiselect("เลือกหุ้นเพื่อดูกราฟปันผล:", filtered_df['symbol'].unique(), default=default_div_targets)
        
        if div_targets:
            col_div_chart, col_div_table = st.columns([3, 1])
            
            with st.spinner("กำลังดึงข้อมูลปันผล..."):
                div_data_list = []
                for t in div_targets:
                    d_hist = utils.get_dividend_history(t, years=10)
                    if not d_hist.empty:
                        d_hist['Symbol'] = t
                        div_data_list.append(d_hist)
                
                if div_data_list:
                    all_div_df = pd.concat(div_data_list)
                    
                    with col_div_chart:
                        fig_div = px.bar(
                            all_div_df, 
                            x='Year', 
                            y='Dividend', 
                            color='Symbol',
                            barmode='group',
                            title='เงินปันผลต่อหุ้น (บาท/ปี)',
                            text_auto='.2f',
                            labels={'Dividend': 'เงินปันผล (บาท)', 'Year': 'ปี'}
                        )
                        fig_div.update_layout(xaxis_type='category')
                        st.plotly_chart(fig_div, use_container_width=True)
                        
                    with col_div_table:
                        st.dataframe(
                            all_div_df.pivot(index='Year', columns='Symbol', values='Dividend').sort_index(ascending=False),
                            use_container_width=True
                        )
                else:
                    st.info("ไม่พบข้อมูลปันผลสำหรับหุ้นที่เลือก")

        st.info("💡 **เกร็ดความรู้:** หุ้นที่มี 'MOS (%)' เขียว (> 20%) คือหุ้นที่มีส่วนลดจากมูลค่าจริงมาก")
        
        with st.expander("📖 อธิบายความหมายอัตราส่วนทางการเงิน (Financial Glossary)"):
            st.markdown(r"""
            ### 🧮 สูตรการคำนวณและคำอธิบาย (Formulas & Definitions)

            #### 1. ความถูกแพง (Valuation)
            *   **P/E (Price-to-Earnings Ratio):** ความถูกแพงของหุ้นเทียบกับกำไรสุทธิ
                $$ \text{P/E} = \frac{\text{Price}}{\text{EPS}} $$
            *   **PEG (P/E to Growth):** P/E เทียบกับการเติบโตของกำไร
                $$ \text{PEG} = \frac{\text{P/E}}{\text{Earnings Growth (\%)}} $$
            *   **P/BV (Price-to-Book Ratio):** ราคาหุ้นเทียบกับมูลค่าทางบัญชี
                $$ \text{P/BV} = \frac{\text{Price}}{\text{Book Value per Share}} $$
            *   **EV/EBITDA:** มูลค่ากิจการเทียบกับกำไรเงินสด
                $$ \text{EV/EBITDA} = \frac{\text{Market Cap + Debt - Cash}}{\text{EBITDA}} $$

            #### 2. ประสิทธิภาพ (Efficiency)
            *   **ROE (Return on Equity):** ผลตอบแทนต่อส่วนของผู้ถือหุ้น
                $$ \text{ROE} = \frac{\text{Net Income}}{\text{Shareholders' Equity}} \times 100 $$
            *   **ROA (Return on Assets):** ความสามารถในการทำกำไรจากสินทรัพย์
                $$ \text{ROA} = \frac{\text{Net Income}}{\text{Total Assets}} \times 100 $$
            *   **ROC (Return on Capital):** ผลตอบแทนจากเงินลงทุนดำเนินงาน (Magic Formula)
                $$ \text{ROC} = \frac{\text{EBIT}}{\text{Net Working Capital} + \text{Net Fixed Assets}} $$

            #### 3. สุขภาพทางการเงิน (Health)
            *   **D/E (Debt-to-Equity Ratio):** หนี้สินต่อทุน
                $$ \text{D/E} = \frac{\text{Total Debt}}{\text{Shareholders' Equity}} $$
            *   **Current Ratio:** สภาพคล่องหมุนเวียน
                $$ \text{Current Ratio} = \frac{\text{Current Assets}}{\text{Current Liabilities}} $$
            *   **Z-Score (Altman Z-Score):** ดัชนีชี้วัดความเสี่ยงล้มละลาย (Manufacturing Model)
                $$ Z = 1.2A + 1.4B + 3.3C + 0.6D + 1.0E $$
                (A=WC/TA, B=RE/TA, C=EBIT/TA, D=MktCap/Liab, E=Sales/TA)

            #### 4. การประเมินมูลค่า (Valuation Models)
            *   **Fair Price (ราคาเหมาะสม):** ค่าเฉลี่ยของ 3 วิธี (DDM, Target P/E, Target P/BV)
            *   **DDM (Dividend Discount Model):** คิดลดเงินปันผล 2 ช่วง (5 ปีแรก + Terminal Value)
                $$ \text{Value} = \sum_{t=1}^{5} \frac{D_0(1+g)^t}{(1+k)^t} + \frac{D_5(1+g)}{(k-g)(1+k)^5} $$
            *   **Graham Number:** ราคาที่เหมาะสมตามสูตร Benjamin Graham
                $$ \text{Graham Num} = \sqrt{22.5 \times \text{EPS} \times \text{BVPS}} $$
            *   **VI Price:** ราคาเหมาะสมแบบ VI ประยุกต์
                $$ \text{VI Price} = \frac{\text{Fair Price} + \text{Graham Number}}{2} $$

            #### 5. ตัวแปรสมมติฐาน (Assumptions)
            *   **G% (Terminal Growth Rate):** อัตราการเติบโตระยะยาวที่ใช้ในสูตร DDM และ Target Multiples
            *   **K% (Required Return):** ผลตอบแทนคาดหวัง (Discount Rate) คำนวณจาก CAPM หรือกำหนดเอง
                $$ k = R_f + \beta (R_m - R_f) $$
            *   **MOS% (Margin of Safety):** ส่วนเผื่อความปลอดภัย (เทียบกับ DDM)
                $$ \text{MOS\%} = \frac{\text{DDM} - \text{Price}}{\text{DDM}} \times 100 $$
            """)
        
        # --- Display Advanced Results if available (Optional: Keep it hidden or move to debug) ---
        # User requested to combine into one table, so we hide the separate Magic Formula table
        # but we keep the logic above to feed the "Super Stocks" table.
        
    else:
        st.error("ไม่สามารถโหลดข้อมูลได้ โปรดตรวจสอบการเชื่อมต่ออินเทอร์เน็ต")


        # Sector Heatmap
        st.markdown("---")
        st.subheader("🗺️ แผนภาพความร้อนรายอุตสาหกรรม (Sector Heatmap)")
        st.markdown("ขนาดกล่อง = มูลค่าตลาด (Market Cap), สี = ความถูกแพง (Margin of Safety)")
        
        # Prepare Data for Heatmap
        # Ignore huge outliers for color scale or clamp them?
        heat_df = df[df['marketCap'] > 0].copy()
        
        fig_treemap = px.treemap(
            heat_df, 
            path=[px.Constant("SET100"), 'sector', 'symbol'], 
            values='marketCap',
            color='margin_of_safety',
            color_continuous_scale='RdYlGn',
            color_continuous_midpoint=0,
            hover_data=['price', 'fair_value']
        )
        fig_treemap.update_layout(height=600)
        st.plotly_chart(fig_treemap, use_container_width=True)

elif page == "🔍 วิเคราะห์หุ้นรายตัว":
    st.title("🔎 วิเคราะห์หุ้นเจาะลึก (Pro Stock Analysis)")
    
    # Select Stock
    selected_ticker = st.selectbox("เลือกหุ้นที่ต้องการวิเคราะห์", SET100_TICKERS)
    
    if st.button("เริ่มวิเคราะห์"):
        with st.spinner(f"กำลังวิเคราะห์ {selected_ticker}..."):
            # Get fresh data (or we could use cached if passed, but let's fetch fresh deeper data)
            stock_data = utils.get_stock_data(selected_ticker)
            valuation = utils.calculate_valuations(stock_data)
            fin_hist = utils.get_financial_history(selected_ticker)
            
            if valuation:
                # --- HEADER SECTION ---
                st.markdown(f"## {valuation['longName']} ({valuation['symbol']})")
                st.markdown(f"**อุตสาหกรรม:** {valuation.get('sector')} | **ธุรกิจ:** {valuation.get('summary')[:150]}...")
                
                # Gauge / Recommendation
                rec_val = valuation.get('recommendation', 3.0) # 1=Buy, 5=Sell
                target_price = valuation.get('targetPrice', 0)
                current_price = valuation.get('price', 0)
                fair_val = valuation.get('fair_value', 0)
                
                col_head1, col_head2, col_head3 = st.columns([1, 2, 1])
                
                with col_head1:
                    st.metric("ราคาปัจจุบัน", f"฿{current_price:.2f}")
                    
                    # Simple Sentiment Color
                    if rec_val <= 2.0:
                        st.success("นักวิเคราะห์: แนะนำซื้อ (BUY)")
                    elif rec_val >= 4.0:
                        st.error("นักวิเคราะห์: แนะนำขาย (SELL)")
                    else:
                        st.warning("นักวิเคราะห์: แนะนำถือ (HOLD)")
                        
                with col_head2:
                    # Comparison Bar
                    st.markdown("##### ราคาตลาด vs มูลค่าที่เหมาะสม")
                    comp_data = pd.DataFrame({
                        'Type': ['ราคาปัจจุบัน', 'เป้านักวิเคราะห์', 'มูลค่าพื้นฐาน (VI)'],
                        'Price': [current_price, target_price, fair_val]
                    })
                    fig_comp = px.bar(comp_data, x='Price', y='Type', orientation='h', text='Price', 
                                      color='Type', color_discrete_map={'ราคาปัจจุบัน': 'grey', 'เป้านักวิเคราะห์': '#3b82f6', 'มูลค่าพื้นฐาน (VI)': '#10b981'})
                    fig_comp.update_layout(height=200, margin=dict(l=0, r=0, t=0, b=0))
                    fig_comp.update_traces(texttemplate='฿%{text:.2f}')
                    st.plotly_chart(fig_comp, use_container_width=True)

                with col_head3:
                    mos = valuation.get('margin_of_safety', 0)
                    st.metric("MOS (ส่วนเผื่อความปลอดภัย)", f"{mos:.2f}%", 
                              delta="ราคาถูก (Undervalued)" if mos > 0 else "ราคาแพง (Overvalued)",
                              delta_color="normal" if mos > 0 else "inverse")
                
                # --- KEY STATS GRID ---
                st.subheader("📊 อัตราส่วนทางการเงินที่สำคัญ (Key Ratios)")
                k1, k2, k3, k4 = st.columns(4)
                
                with k1:
                    st.markdown("**ความถูกแพง (Valuation)**")
                    st.metric("P/E Ratio", f"{valuation.get('price') / valuation.get('trailingEps') if valuation.get('trailingEps') else 0:.2f}") 
                    st.metric("P/BV Ratio", f"{valuation.get('price') / valuation.get('bookValue') if valuation.get('bookValue') else 0:.2f}")
                    st.metric("PEG Ratio", f"{valuation.get('pegRatio', 0):.2f}")
                
                with k2:
                    st.markdown("**ประสิทธิภาพ (Efficiency)**")
                    st.metric("ROE (ผลตอบแทนส่วนผู้ถือหุ้น)", f"{valuation.get('returnOnEquity', 0)*100:.2f}%")
                    st.metric("ROA (ผลตอบแทนสินทรัพย์)", f"{valuation.get('returnOnAssets', 0)*100:.2f}%")
                    st.metric("Profit Margin (อัตรากำไร)", f"{valuation.get('profitMargins', 0)*100:.2f}%")
                    
                with k3:
                    st.markdown("**สุขภาพการเงิน (Health)**")
                    st.metric("D/E Ratio (หนี้สิน/ทุน)", f"{valuation.get('debtToEquity', 0)/100:.2f}") 
                    st.metric("Current Ratio (สภาพคล่อง)", f"{valuation.get('currentRatio', 0):.2f}")
                    st.metric("Beta (ความผันผวน)", f"{valuation.get('beta', 1.0):.2f}")

                with k4:
                    st.markdown("**ปันผล (Dividend)**")
                    st.metric("Yield (ผลตอบแทน)", f"{(valuation.get('dividendRate',0) / current_price * 100) if current_price else 0:.2f}%")
                    st.metric("Payout Ratio (สัดส่วนจ่าย)", f"{valuation.get('payoutRatio', 0)*100:.2f}%")
                
                # --- FINANCIAL TRENDS & FORECAST ---
                st.markdown("---")
                st.subheader("📈 ผลประกอบการย้อนหลัง & คาดการณ์อนาคต")
                st.info("ℹ️ **หมายเหตุข้อมูล:** ข้อมูลย้อนหลังประมาณ 4 ปีล่าสุด | ตัวเลขคาดการณ์อ้างอิงจากบทวิเคราะห์ (Analyst Estimates)")
                
                # Tabs for different views
                tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 การเติบโต & กำไร", "💪 ประสิทธิภาพการทำกำไร", "🔮 คาดการณ์อนาคต", "📉 PE Band & Matrix", "📰 ข่าวล่าสุด"])
                
                if not fin_hist.empty:
                    with tab1:
                        # Revenue & Profit Combo
                        f1, f2 = st.columns(2)
                        with f1:
                            fig_fin = go.Figure()
                            fig_fin.add_trace(go.Bar(x=fin_hist.index, y=fin_hist['Revenue'], name='รายได้ (Revenue)', marker_color='#60a5fa'))
                            fig_fin.add_trace(go.Scatter(x=fin_hist.index, y=fin_hist['Net Profit'], name='กำไรสุทธิ (Net Profit)', mode='lines+markers', line=dict(color='#10b981', width=3)))
                            fig_fin.update_layout(title="แนวโน้มรายได้ vs กำไรสุทธิ", legend=dict(orientation="h"))
                            st.plotly_chart(fig_fin, use_container_width=True)
                            
                        with f2:
                            # EPS Trend
                            if 'EPS' in fin_hist.columns:
                                fig_eps = px.bar(fin_hist, x=fin_hist.index, y='EPS', title="กำไรต่อหุ้น (EPS)", text_auto='.2f')
                                fig_eps.update_traces(marker_color='#8b5cf6')
                                st.plotly_chart(fig_eps, use_container_width=True)
                                
                    with tab2:
                        # Ratios Triple Chart
                        pass # Content continues below...
                
                # --- CONTENT FOR MAIN TAB 1 CONTINUED ---
                with st.container():
                    with tab2:
                        r1, r2, r3 = st.columns(3)
                        
                        with r1:
                            if 'ROE (%)' in fin_hist.columns:
                                fig_roe = px.line(fin_hist, x=fin_hist.index, y='ROE (%)', markers=True, title="ROE (%)")
                                fig_roe.update_traces(line_color='#ef4444')
                                st.plotly_chart(fig_roe, use_container_width=True)
                        
                        with r2:
                            if 'NPM (%)' in fin_hist.columns:
                                fig_npm = px.line(fin_hist, x=fin_hist.index, y='NPM (%)', markers=True, title="Net Profit Margin (%)")
                                fig_npm.update_traces(line_color='#f59e0b')
                                st.plotly_chart(fig_npm, use_container_width=True)

                        with r3:
                            if 'D/E (x)' in fin_hist.columns:
                                fig_de = px.bar(fin_hist, x=fin_hist.index, y='D/E (x)', title="D/E Ratio (เท่า)", text_auto='.2f')
                                fig_de.update_traces(marker_color='#64748b')
                                st.plotly_chart(fig_de, use_container_width=True)
                
                    with tab3:
                        # Forecast Logic
                        # We have Trailing EPS and Forward EPS.
                        # Let's project 2 years
                        current_year_eps = valuation.get('trailingEps')
                        next_year_eps = valuation.get('forwardEps')
                    
                        if current_year_eps and next_year_eps:
                            # Simple 2-point projection
                            # Avoid div by zero
                            denom = abs(current_year_eps) if current_year_eps != 0 else 1
                            growth = (next_year_eps - current_year_eps) / denom
                            
                            # Project Year+2 with same growth rate (Conservative)
                            year_2_eps = next_year_eps * (1 + (growth * 0.8)) # Decay growth slightly
                            
                            forecast_data = pd.DataFrame({
                                'Year': ['ปีปัจจุบัน (TTM)', 'ปีหน้า (คาดการณ์)', 'ปีถัดไป (คาดการณ์)'],
                                'EPS': [current_year_eps, next_year_eps, year_2_eps],
                                'Type': ['ของจริง', 'คาดการณ์', 'คาดการณ์']
                            })
                            
                            f_col1, f_col2 = st.columns([2, 1])
                            with f_col1:
                                fig_fore = px.line(forecast_data, x='Year', y='EPS', markers=True, title="คาดการณ์กำไรต่อหุ้น (Earnings Forecast)", text='EPS')
                                fig_fore.update_traces(texttemplate='%{text:.2f}', textposition="top center", line=dict(color='#0ea5e9', width=3, dash='dot'))
                                st.plotly_chart(fig_fore, use_container_width=True)
                                
                            with f_col2:
                                st.metric("การเติบโตคาดหวัง (1Y)", f"{growth*100:.2f}%")
                                st.metric("Forward EPS", f"{next_year_eps:.2f}")
                                st.markdown("*(E) = ตัวเลขประมาณการ*")
                        else:
                            st.info("ไม่มีข้อมูลประมาณการจากนักวิเคราะห์")
                    
                    with tab4:
                        st.subheader("📉 Historical PE Band")
                        st.info("กราฟแสดงราคาหุ้นเทียบกับกรอบราคาที่คิดจากค่า PE ย้อนหลัง 5 ปี (ช่วยดูว่าตอนนี้ถูกหรือแพงเมื่อเทียบกับตัวเองในอดีต)")
                        
                        pe_band_data = utils.get_historical_pe_bands(selected_ticker)
                        
                        if pe_band_data:
                             band_df = pe_band_data['data']
                             
                             fig_band = go.Figure()
                             
                             # Price
                             fig_band.add_trace(go.Scatter(x=band_df['Date'], y=band_df['Close'], name='ราคาหุ้น (Price)', line=dict(color='black', width=3)))
                             
                             # Bands
                             fig_band.add_trace(go.Scatter(x=band_df['Date'], y=band_df['Mean PE'], name=f'Avg PE ({pe_band_data["avg_pe"]:.1f}x)', line=dict(color='orange', dash='dash')))
                             fig_band.add_trace(go.Scatter(x=band_df['Date'], y=band_df['+1 SD'], name='+1 SD', line=dict(color='red', width=1)))
                             fig_band.add_trace(go.Scatter(x=band_df['Date'], y=band_df['+2 SD'], name='+2 SD (แพงมาก)', line=dict(color='darkred', width=1, dash='dot')))
                             fig_band.add_trace(go.Scatter(x=band_df['Date'], y=band_df['-1 SD'], name='-1 SD', line=dict(color='green', width=1)))
                             fig_band.add_trace(go.Scatter(x=band_df['Date'], y=band_df['-2 SD'], name='-2 SD (ถูกมาก)', line=dict(color='darkgreen', width=1, dash='dot')))
                             
                             fig_band.update_layout(title=f"PE Band: {selected_ticker}", hovermode="x unified")
                             st.plotly_chart(fig_band, use_container_width=True)
                             
                             st.markdown(f"**ค่าเฉลี่ย PE 5 ปีย้อนหลัง:** {pe_band_data['avg_pe']:.2f} เท่า | **PE ปัจจุบัน:** {pe_band_data['current_pe']:.2f} เท่า")
                        else:
                             st.error("ข้อมูลไม่เพียงพอสำหรับสร้าง PE Band (ต้องการกำไรย้อนหลังต่อเนื่อง)")

                with tab5:
                    # News Section
                    st.markdown("##### 📰 ข่าวล่าสุด (Latest News)")
                    st.caption("ดึงข้อมูลข่าวจาก Yahoo Finance")
                    
                    news_list = utils.fetch_stock_news(selected_ticker)
                    
                    if news_list:
                        for news in news_list[:5]: # Show top 5
                            with st.expander(f"{news.get('title', 'No Title')}"):
                                pub_time = "N/A"
                                if 'providerPublishTime' in news:
                                    import datetime
                                    pub_time = datetime.datetime.fromtimestamp(news['providerPublishTime']).strftime('%Y-%m-%d %H:%M')
                                    
                                st.caption(f"Source: {news.get('publisher', 'Unknown')} | Time: {pub_time}")
                                st.markdown(f"[อ่านข่าวฉบับเต็ม]({news.get('link', '#')})")
                                if 'thumbnail' in news and 'resolutions' in news['thumbnail']:
                                     # Try to get thumbnail
                                     try:
                                         thumb_url = news['thumbnail']['resolutions'][0]['url']
                                         st.image(thumb_url, width=200)
                                     except:
                                         pass
                    else:
                        st.info("ไม่พบข่าวล่าสุดในระบบ")

                # --- 8 Qualities Checklist (Enhanced) ---
                st.markdown("---")
                
                # --- RAW DATA VERIFICATION (NEW) ---
                with st.expander("🔍 ตรวจสอบความถูกต้องของข้อมูล (Data Verification)", expanded=False):
                    st.markdown("### แหล่งที่มาและเวลาของข้อมูล (Data Source & Timestamp)")
                    
                    # Convert timestamp to readable format
                    last_ts = valuation.get('last_price_time', 0)
                    last_time_str = "N/A"
                    if last_ts > 0:
                        import datetime
                        last_time_str = datetime.datetime.fromtimestamp(last_ts).strftime('%Y-%m-%d %H:%M:%S')
                    
                    st.info(f"""
                    **แหล่งข้อมูล:** Yahoo Finance (Real-time delay 15-20 mins)
                    **เวลาล่าสุดของราคา (Last Price Time):** {last_time_str}
                    **สกุลเงิน (Currency):** {valuation.get('currency', 'THB')}
                    **ตลาด (Exchange):** {valuation.get('exchange', 'SET')}
                    """)

                    st.markdown("### ข้อมูลดิบจากระบบ (Raw Data Inspector)")
                    st.json(valuation)
                    st.caption("*หากพบข้อมูลไม่ตรงกับ SETSMART หรือ Streaming อาจเกิดจากความล่าช้าของ Source หรือรอบบัญชีที่แตกต่างกัน (TTM vs Annual)*")

                st.markdown("---")
                st.subheader("📋 แบบประเมินคุณภาพหุ้น VI (Checklist)")
                
                score = 0
                total = 8
                
                check_col1, check_col2 = st.columns(2)
                
                roe = valuation.get('returnOnEquity', 0)
                npm = valuation.get('profitMargins', 0)
                de = valuation.get('debtToEquity', 0) # yfinance returns e.g. 150 for 1.5 ratio often, need to verify.
                # Usually debtToEquity is a percentage in yfinance (e.g. 221.35 means 2.21)
                
                # Logic helpers
                is_strong_roe = roe > 0.15
                is_strong_npm = npm > 0.10
                is_low_debt = de < 200 # < 2.0 D/E
                is_undervalued = mos > 0
                
                with check_col1:
                    c1 = st.checkbox("1. ผู้นำตลาด / ผูกขาด (Market Leader)", help="บริษัทมีส่วนแบ่งการตลาดสูง อำนาจต่อรองสูง?")
                    c2 = st.checkbox("2. คู่แข่งเข้ามายาก (High Barriers to Entry)", help="ธุรกิจเลียนแบบยาก หรือต้องใช้เงินลงทุนสูงมาก?")
                    c3 = st.checkbox("3. กำหนดราคาเองได้ (Pricing Power)", help="ขึ้นราคาสินค้าได้โดยที่ลูกค้าไม่หนีไปไหน?")
                    c4 = st.checkbox("4. ยังมีโอกาสเติบโต (Growth Potential)", help="อุตสาหกรรมยังไม่ตะวันตกดิน ยังโตได้อีก?")
                    
                with check_col2:
                    c5 = st.checkbox(f"5. คุมต้นทุนดี / หนี้ต่ำ (D/E < 2) [Current: {de/100:.2f}x]", value=is_low_debt, help=f"ค่า D/E ปัจจุบัน: {de/100:.2f} เท่า")
                    c6 = st.checkbox(f"6. การเงินแกร่ง (ROE > 15%) [Current: {roe*100:.2f}%]", value=is_strong_roe, help=f"ค่า ROE ปัจจุบัน: {roe*100:.2f}%")
                    c7 = st.checkbox(f"7. กำไรสูง (NPM > 10%) [Current: {npm*100:.2f}%]", value=is_strong_npm, help=f"ค่า NPM ปัจจุบัน: {npm*100:.2f}%")
                    c8 = st.checkbox(f"8. ราคาถูก (MOS > 0%) [Current: {mos:.2f}%]", value=is_undervalued, disabled=True)
                
                # Manual Score Calculation
                manual_checks = sum([c1, c2, c3, c4])
                auto_checks = sum([is_low_debt, is_strong_roe, is_strong_npm, is_undervalued])
                final_score = manual_checks + auto_checks
                
                st.markdown(f"#### **คะแนนคุณภาพรวม: {final_score} / 8**")
                st.progress(final_score / 8)

            else:
                st.error("ไม่สามารถดึงข้อมูลหุ้นตัวนี้ได้")


elif page == "⚖️ เปรียบเทียบคู่แข่ง":
    st.title("⚔️ เปรียบเทียบคู่แข่ง (Competitor Analysis)")
    st.markdown("เปรียบเทียบตัวเลขทางการเงินของหุ้นหลายตัวแบบตัวต่อตัว")
    
    # Multiselect
    selected_tickers = st.multiselect("เลือกหุ้นมาชนกัน (สูงสุด 5 ตัว)", SET100_TICKERS, default=["ADVANC", "TRUE"] if "TRUE" in SET100_TICKERS else ["ADVANC"])
    
    if len(selected_tickers) > 0:
        if len(selected_tickers) > 5:
            st.warning("เลือกได้สูงสุด 5 ตัวเท่านั้น")
        else:
            with st.spinner("กำลังดึงข้อมูลเปรียบเทียบ..."):
                # Fetch data directly or via utils
                # Use ThreadPool to fetch detailed history for all selected
                
                # 1. Comparison Table (Current Stats)
                # Filter 'df' (global) for efficiency for current stats
                comp_df = df[df['symbol'].isin(selected_tickers)].set_index('symbol')
                
                # Select interesting columns
                cols_to_show = ['price', 'fair_value', 'margin_of_safety', 'dividendRate', 'returnOnEquity', 'profitMargins', 'debtToEquityRatio', 'valuation_pe']
                comp_table = comp_df[cols_to_show].T
                
                # Rename Index for TH
                comp_table.index = ['ราคา', 'มูลค่าเหมาะสม', 'MOS (%)', 'ปันผล (บาท)', 'ROE (%)', 'NPM (%)', 'D/E (เท่า)', 'P/E (เท่า)']
                
                st.subheader("📊 ตารางวัดพลังพื้นฐาน")
                st.dataframe(comp_table.style.format("{:.2f}").background_gradient(axis=1), use_container_width=True)
                
                # 2. Historical Charts Comparison
                st.subheader("📈 กราฟวัดพลังย้อนหลัง")
                
                # We need to fetch history for each
                hist_data = {}
                metrics = ['Revenue', 'Net Profit', 'ROE (%)', 'NPM (%)']
                
                # Fetch history logic
                # For charts we need a combined dataframe
                combined_hist = pd.DataFrame()
                
                for t in selected_tickers:
                     h = utils.get_financial_history(t)
                     if not h.empty:
                         h['Symbol'] = t
                         combined_hist = pd.concat([combined_hist, h])
                
                if not combined_hist.empty:
                    # Choose Metric to compare
                    metric_choice = st.radio("เลือกหัวข้อเปรียบเทียบ", metrics, horizontal=True)
                    
                    if metric_choice in combined_hist.columns:
                        fig_comp = px.bar(combined_hist, x=combined_hist.index, y=metric_choice, color='Symbol', barmode='group', title=f"เปรียบเทียบ {metric_choice}")
                        st.plotly_chart(fig_comp, use_container_width=True)
                    else:
                        st.info(f"ไม่มีข้อมูล {metric_choice}")
                else:
                    st.error("ไม่สามารถดึงข้อมูลย้อนหลังได้")


                    st.error("ไม่สามารถดึงข้อมูลย้อนหลังได้")


elif page == "💡 แนะนำพอร์ตการลงทุน":
    st.title("🍰 แนะนำพอร์ตการลงทุน (Asset Allocation)")
    
    st.markdown("สร้างพอร์ตการลงทุนที่สมดุล ตามหลักการกระจายความเสี่ยง")
    
    # Input with cleaner integer format (Note: Commas in input fields are not supported by Streamlit for editing, so we show a caption)
    capital = st.number_input("เงินลงทุนตั้งต้น (บาท)", min_value=1000, value=100000, step=1000, format="%d")
    st.caption(f"💰 จำนวนเงินที่ระบุ: **{capital:,.0f}** บาท")
    
    # Risk Profile Selector
    st.markdown("---")
    risk_level = st.radio("ระดับความเสี่ยงที่รับได้ (Risk Profile)", ["ต่ำ (Conservative)", "ปานกลาง (Moderate)", "สูง (Aggressive)"], index=1)
    
    # Define Allocations based on Risk (Thai Stocks Only)
    if "ต่ำ" in risk_level:
        # Conservative: Large Cap 60%, REITs 40%
        allocation = {
            "หุ้นไทยขนาดใหญ่ (SET50)": 0.60,
            "กองทุนอสังหาฯ (REITs)": 0.40,
            "หุ้นเล็ก / หุ้นเติบโต (Growth)": 0.00
        }
        alloc_rules = {
            "Thai Large Cap": 0.60,
            "REITs": 0.40,
            "Growth Stocks": 0.00
        }
    elif "สูง" in risk_level:
        # Aggressive: Thai Large 40%, Growth 50%, REITs 10%
        allocation = {
            "หุ้นไทยขนาดใหญ่ (SET50)": 0.40,
            "หุ้นเล็ก / หุ้นเติบโต (Growth)": 0.50,
            "กองทุนอสังหาฯ (REITs)": 0.10
        }
        alloc_rules = {
            "Thai Large Cap": 0.40,
            "Growth Stocks": 0.50,
            "REITs": 0.10
        }
    else:
        # Moderate: Thai Large 50%, Growth 20%, REITs 30%
        allocation = {
            "หุ้นไทยขนาดใหญ่ (SET50)": 0.50,
            "กองทุนอสังหาฯ (REITs)": 0.30,
            "หุ้นเล็ก / หุ้นเติบโต (Growth)": 0.20
        }
        alloc_rules = {
            "Thai Large Cap": 0.50,
            "REITs": 0.30,
            "Growth Stocks": 0.20
        }
    
    if st.button("คำนวณสัดส่วนการลงทุน"):
        amounts = utils.calculate_portfolio(capital, allocation)
        
        col1, col2 = st.columns([1, 1])
        
        with col1:
            st.subheader("สัดส่วนที่แนะนำ (Target Allocation)")
            # Create DataFrame with Thai columns
            df_port = pd.DataFrame(list(amounts.items()), columns=['ประเภทสินทรัพย์', 'มูลค่า (บาท)'])
            df_port['สัดส่วน (%)'] = df_port['ประเภทสินทรัพย์'].map(allocation) * 100
            
            # Format numbers with commas
            st.dataframe(
                df_port.style.format({
                    'มูลค่า (บาท)': '{:,.2f}', 
                    'สัดส่วน (%)': '{:.1f}%'
                }),
                use_container_width=True
            )
            
        with col2:
            fig = px.pie(
                values=list(amounts.values()), 
                names=list(amounts.keys()), 
                title="แผนภูมิพอร์ตการลงทุน",
                hole=0.4
            )
            st.plotly_chart(fig)
            
        # --- ASSET RECOMMENDATION EXPANDER ---
        st.markdown("---")
        st.subheader("💡 แนะนำสินทรัพย์น่าลงทุน (Asset Recommendations)")
        st.info("รายชื่อสินทรัพย์ยอดนิยมสำหรับคนไทย (คำเตือน: ไม่ใช่คำแนะนำการลงทุน เป็นเพียงตัวอย่างศึกษา)")
        
        with st.expander("🛡️ ตราสารหนี้ & พันธบัตร (40%)", expanded=True):
             st.markdown("""
             **แนวคิด:** รักษาเงินต้น ความเสี่ยงต่ำ
             *   **พันธบัตรไทย:** `LB296A`, `LB31DA` (ซื้อผ่านแอปเป๋าตัง/ธนาคาร)
             *   **กองทุนตราสารหนี้:** `K-FIXED`, `SCBFIXED`, `TMBABF`
             *   **เงินฝาก:** บัญชีออมทรัพย์ดอกเบี้ยสูง (Kept, Dime, etc.)
             """)
             
        with st.expander("🏢 หุ้นไทยขนาดใหญ่ (15%)", expanded=True):
            st.markdown("""
            **แนวคิด:** เติบโตมั่นคง + ปันผลสม่ำเสมอ
            *   **หุ้นเด่น SET100:** `ADVANC`, `PTT`, `AOT`, `KBANK`, `CPALL`
            *   **กองทุนดัชนี (ETF):** `TDEX` (อ้างอิงดัชนี SET50)
            """)
            
        with st.expander("🏬 กองทุนอสังหาฯ (REITs)", expanded=True):
            st.markdown("""
            **แนวคิด:** รับค่าเช่า (Passive Income)
            *   **ห้าง/ออฟฟิศ:** `CPNREIT`, `ALLY`
            *   **คลังสินค้า/นิคม:** `WHAIR`, `FTREIT`
            *   **โครงสร้างพื้นฐาน:** `DIF` (เสาสัญญาณ), `TFFIF` (ทางด่วน)
            """)
            
        with st.expander("🌱 หุ้นเติบโต / หุ้นเล็ก (Growth Stocks)", expanded=True):
            st.markdown("""
            **แนวคิด:** เน้นกำไรเติบโตสูง (High Risk High Return)
            *   **รายตัว:** `JMT`, `FORTH`, `XO`, `SIS`, `COM7`
            *   **กองทุน:** `K-STAR`, `SCBSE`
            """)

    # --- PORTFOLIO SIMULATOR ---
    st.markdown("---")
    st.subheader("🛠️ จำลองพอร์ตหุ้น (Portfolio Simulator)")
    
    # Dividend Goal Input
    with st.expander("🎯 เป้าหมายเงินปันผล (Dividend Goal)", expanded=True):
        st.caption("กรองรายชื่อหุ้นเพื่อแสดงเฉพาะตัวที่ให้ปันผลตามเป้าหมาย (เฉพาะหุ้นไทยและ REITs)")
        target_yield_req = st.number_input("ต้องการปันผลขั้นต่ำ (%)", min_value=0.0, max_value=20.0, value=0.0, step=0.5, help="ใส่ 0 หากไม่ต้องการกรอง")
        
        if target_yield_req > 0:
            st.success(f"✅ ระบบจะกรองและเลือกหุ้นที่ปันผล > {target_yield_req}% ให้โดยอัตโนมัติ")
    
    st.caption("จัดพอร์ตตาม Asset Allocation ที่แนะนำ เลือกสินทรัพย์ในแต่ละกลุ่มเพื่อคำนวณผลตอบแทนคาดหวัง")

    # Helper Data for Non-Stock Assets (Estimated Yields & Proxy Prices)
    # Price is dummy 10.0 just for calculating quantity roughly if needed, mostly for amount allocation
    ASSET_PROXY = {}
    
    # Calculate Yield for all stocks in DF for filtering
    # Reuse logic from simulator loop
    def get_stock_yield(row):
        price = row.get('price', 0)
        div_yield = 0
        d_rate = row.get('dividendRate', 0)
        if price > 0 and pd.notnull(d_rate) and d_rate > 0:
            div_yield = d_rate / price
        else:
            y_val = row.get('dividendYield', 0)
            if pd.notnull(y_val) and y_val > 0:
                if y_val > 1: div_yield = y_val / 100.0
                else: div_yield = y_val
        return div_yield * 100 # Return as percentage

    # Create Filtered Lists
    # Filter Logic: Yield >= target_yield_req
    
    # 1. Thai Large Cap
    large_cap_all = df[df['marketCap'] > 50_000_000_000]['symbol'].tolist()
    
    # 2. Growth Stocks
    small_cap_all = df[df['marketCap'] <= 50_000_000_000]['symbol'].tolist()
    
    # 3. REITs (Approximate)
    known_reits = ['CPNREIT', 'WHAIR', 'FTREIT', 'ALLY', 'DIF', 'TFFIF', 'LHHOTEL', 'GVREIT', 'AIMIRT', 'PROSPECT']
    reit_all = [x for x in known_reits if x in df['symbol'].values]
    
    # Apply Filter if Target > 0
    if target_yield_req > 0:
        # Pre-calc yields map
        yield_map = {row['symbol']: get_stock_yield(row) for _, row in df.iterrows()}
        
        large_cap_list = [s for s in large_cap_all if yield_map.get(s, 0) >= target_yield_req]
        small_cap_list = [s for s in small_cap_all if yield_map.get(s, 0) >= target_yield_req]
        reit_list = [s for s in reit_all if yield_map.get(s, 0) >= target_yield_req]
        
        # Auto-select defaults: Top 3 yielders in each category
        def get_top_yielders(tickers, n=3):
            sorted_t = sorted(tickers, key=lambda x: yield_map.get(x, 0), reverse=True)
            return sorted_t[:n]
            
        def_large = get_top_yielders(large_cap_list)
        def_small = get_top_yielders(small_cap_list)
        def_reit = get_top_yielders(reit_list)
        
    else:
        # No filter
        large_cap_list = large_cap_all
        small_cap_list = small_cap_all
        reit_list = sorted(df['symbol'].unique()) # Allow all for REITs if no filter, or stick to known? Stick to known + valid
        reit_list = [x for x in df['symbol'].unique() if any(k in x for k in ['REIT', 'PF', 'IF']) or x in known_reits] # Simple heuristic
        
        # Original Defaults
        def_large = [x for x in ['ADVANC', 'PTT', 'AOT', 'KBANK', 'CPALL'] if x in large_cap_list]
        def_small = [x for x in ['JMT', 'FORTH', 'XO', 'SIS', 'COM7'] if x in small_cap_list]
        def_reit = [x for x in known_reits if x in df['symbol'].values]

    # Categories mapping to Logic
    # 1. Fixed Income (40%) -> Manual Selection (Mock List)
    # 2. Thai Large (15%) -> SET50 from df
    # 3. Global (15%) -> Manual Selection (Mock List)
    # 4. REITs (10%) -> REITs from df (Filter by name/sector?)
    # 5. Growth (10%) -> Non-SET50 from df
    # 6. Emerging (10%) -> Manual Selection (Mock List)

    sim_budget = st.number_input("เงินลงทุนสำหรับพอร์ตนี้ (บาท)", min_value=1000.0, value=float(capital), step=1000.0)
    
    # --- SELECTION SECTION ---
    st.markdown("#### 1. เลือกสินทรัพย์เข้าพอร์ต")
    
    col_sel1, col_sel2 = st.columns(2)
    
    selected_assets = {} # Store {category: [list of assets]}

    with col_sel1:
        st.markdown(f"**1. หุ้นไทยขนาดใหญ่ (Thai Large Cap) {f'(Yield > {target_yield_req}%)' if target_yield_req > 0 else ''}**")
        selected_assets["Thai Large Cap"] = st.multiselect("เลือกหุ้นขนาดใหญ่ (SET50):", sorted(large_cap_list), default=def_large)

    with col_sel2:
        st.markdown(f"**2. กองทุนอสังหาฯ (REITs) {f'(Yield > {target_yield_req}%)' if target_yield_req > 0 else ''}**")
        selected_assets["REITs"] = st.multiselect("เลือกกองทุนอสังหาฯ (REITs):", sorted(reit_list), default=def_reit)
        
        st.markdown(f"**3. หุ้นเติบโต / หุ้นเล็ก (Growth) {f'(Yield > {target_yield_req}%)' if target_yield_req > 0 else ''}**")
        selected_assets["Growth Stocks"] = st.multiselect("เลือกหุ้นเติบโต/หุ้นเล็ก:", sorted(small_cap_list), default=def_small)

    # --- CALCULATION ---
    # Allocation Rules (Moved up to dynamic section based on Risk Level)
    # alloc_rules variable is already defined above
    
    sim_rows = []
    
    for cat, pct in alloc_rules.items():
        cat_budget = sim_budget * pct
        picks = selected_assets.get(cat, [])
        
        if picks:
            budget_per_asset = cat_budget / len(picks)
            for asset in picks:
                # Determine Price & Yield
                price = 0
                div_yield = 0
                
                # Check if it's a real stock in df
                if asset in df['symbol'].values:
                    row = df[df['symbol'] == asset].iloc[0]
                    price = row.get('price', 0)
                    
                    # Try to get yield from multiple sources
                    div_yield = 0
                    
                    # 1. Prioritize calculated from Dividend Rate (Most reliable: Rate / Price)
                    d_rate = row.get('dividendRate', 0)
                    if price > 0 and pd.notnull(d_rate) and d_rate > 0:
                        div_yield = d_rate / price
                    else:
                        # 2. Fallback to explicit dividendYield
                        y_val = row.get('dividendYield', 0)
                        if pd.notnull(y_val) and y_val > 0:
                            # Normalize scale: If > 1, assume it's percentage (e.g. 4.5 means 4.5%), so divide by 100
                            # If < 1, assume it's decimal (e.g. 0.045 means 4.5%)
                            if y_val > 1:
                                div_yield = y_val / 100.0
                            else:
                                div_yield = y_val
                else:
                    # Fallback to Proxy
                    # Default fallback
                    price = 10.0
                    div_yield = 0.0
                
                qty = int(budget_per_asset / price) if price > 0 else 0
                actual_invest = qty * price
                div_amt = actual_invest * div_yield
                
                sim_rows.append({
                    "หมวดหมู่ (Category)": cat,
                    "ชื่อ (Asset)": asset,
                    "จำนวนเงิน (Invested)": actual_invest,
                    "จำนวนหุ้น (Qty)": qty,
                    "%ปันผล (Yield)": div_yield * 100,
                    "ปันผล (บาท)": div_amt
                })
    
    if sim_rows:
        # Move out of columns to ensure full width
        st.markdown("---")
        st.markdown("#### 2. ตารางสรุปพอร์ตโฟลิโอ (Portfolio Summary)")
        df_sim_final = pd.DataFrame(sim_rows)
        
        # Show DataFrame with use_container_width=True
        st.dataframe(
            df_sim_final.style.format({
                'จำนวนเงิน (Invested)': '{:,.2f}',
                'จำนวนหุ้น (Qty)': '{:,}',
                '%ปันผล (Yield)': '{:.2f}%',
                'ปันผล (บาท)': '{:,.2f}'
            }),
            use_container_width=True,
            hide_index=True,
            height=(len(df_sim_final) + 1) * 35 + 3
        )
        
        # Summary Metrics
        total_inv = df_sim_final['จำนวนเงิน (Invested)'].sum()
        total_div = df_sim_final['ปันผล (บาท)'].sum()
        avg_yield_port = (total_div / total_inv * 100) if total_inv > 0 else 0
        
        m1, m2, m3 = st.columns(3)
        m1.metric("มูลค่าพอร์ตคาดการณ์", f"{total_inv:,.0f} บาท")
        m2.metric("เงินปันผลรายปี (โดยประมาณ)", f"{total_div:,.2f} บาท")
        m3.metric("อัตราผลตอบแทนเฉลี่ย (Yield)", f"{avg_yield_port:.2f}%")
        
        st.caption("*หมายเหตุ: ข้อมูลหุ้นไทยอ้างอิงราคาล่าสุด | กองทุนและตราสารหนี้ใช้ราคาและผลตอบแทนสมมติเพื่อการคำนวณเท่านั้น")

elif page == "🎒 พอร์ตของฉัน (My Portfolio)":
    st.title("🎒 พอร์ตของฉัน (My Portfolio)")
    st.markdown("บันทึกการซื้อขายและติดตามผลกำไรขาดทุนของพอร์ตโฟลิโอ")
    
    # 1. Add Transaction Form
    with st.expander("➕ เพิ่มรายการซื้อ/ขาย (Add Transaction)", expanded=False):
        t_col1, t_col2, t_col3, t_col4, t_col5 = st.columns(5)
        with t_col1:
            t_open_action = st.selectbox("ทำรายการ", ["Buy", "Sell"])
        with t_col2:
            t_symbol = st.selectbox("หุ้น (Symbol)", SET100_TICKERS)
        with t_col3:
            t_date = st.date_input("วันที่ (Date)")
        with t_col4:
            t_price = st.number_input("ราคา (Price)", min_value=0.01, step=0.05)
        with t_col5:
            t_qty = st.number_input("จำนวน (Qty)", min_value=100, step=100)
            
        if st.button("บันทึกรายการ"):
            utils.save_transaction(t_symbol, t_date, t_price, t_qty, t_open_action)
            st.success(f"บันทึก {t_open_action} {t_symbol} เรียบร้อย!")
            st.rerun()

    # 2. Portfolio View
    # Create price map from loaded df
    if not df.empty:
        price_map = df.set_index('symbol')['price'].to_dict()
    else:
        price_map = {}
        
    port_df, port_val, cost_val = utils.get_portfolio_summary(price_map)
    
    if not port_df.empty:
        # Metrics
        m1, m2, m3 = st.columns(3)
        unrealized_pl = port_val - cost_val
        pl_pct = (unrealized_pl / cost_val * 100) if cost_val > 0 else 0
        
        m1.metric("มูลค่าพอร์ตปัจจุบัน", f"{port_val:,.2f} บาท")
        m2.metric("ทุนรวม", f"{cost_val:,.2f} บาท")
        m3.metric("กำไร/ขาดทุน (Unrealized)", f"{unrealized_pl:,.2f} บาท", f"{pl_pct:.2f}%")
        
        st.subheader("📜 รายการถือครอง (Current Holdings)")
        # Show specific columns
        display_port = port_df[['Symbol', 'Qty', 'Avg Price', 'Market Price', 'Cost Value', 'Market Value', 'P/L %']]
        st.dataframe(display_port.style.format({
            'Qty': '{:,.0f}',
            'Avg Price': '{:,.2f}',
            'Market Price': '{:,.2f}',
            'Cost Value': '{:,.2f}',
            'Market Value': '{:,.2f}',
            'P/L %': '{:+.2f}%'
        }))
        
        # Pie Chart
        st.subheader("🍰 สัดส่วนพอร์ต (Allocation)")
        fig_port = px.pie(port_df, values='Market Value', names='Symbol', title='Portfolio Allocation by Value', hole=0.4)
        st.plotly_chart(fig_port)
    else:
        st.info("ยังไม่มีข้อมูลในพอร์ต กรุณาเพิ่มรายการซื้อขาย")

elif page == "⏳ จำลองการออมหุ้น (DCA Backtester)":
    st.title("⏳ จำลองการออมหุ้น (DCA Backtester)")
    st.markdown("ทดสอบผลตอบแทนย้อนหลัง หากเราลงทุนแบบ Dollar Cost Average (DCA) อย่างมีวินัย")
    
    col_d1, col_d2 = st.columns(2)
    
    with col_d1:
        dca_ticker = st.selectbox("เลือกหุ้นที่จะออม", SET100_TICKERS, index=SET100_TICKERS.index('CPALL') if 'CPALL' in SET100_TICKERS else 0)
        dca_amount = st.number_input("เงินออมต่อเดือน (บาท)", value=5000, step=1000)
    
    with col_d2:
        dca_years = st.slider("ระยะเวลาลงทุน (ปี)", 1, 10, 5)
        dca_day = st.slider("วันที่ลงทุนของทุกเดือน", 1, 28, 25)
        
    if st.button("เริ่มการจำลอง (Run Simulation)"):
        with st.spinner("กำลังคำนวณผลตอบแทนย้อนหลัง..."):
            ledger, total_inv, final_val, prof_pct = utils.calculate_dca_simulation(dca_ticker, dca_amount, dca_years, dca_day)
            
            if not ledger.empty:
                st.success("การคำนวณเสร็จสิ้น!")
                
                # Metrics
                r1, r2, r3 = st.columns(3)
                r1.metric("เงินต้นรวม (Total Invested)", f"{total_inv:,.2f} บาท")
                r2.metric("มูลค่าพอร์ตปลายทาง", f"{final_val:,.2f} บาท")
                r3.metric("กำไร/ขาดทุน (%)", f"{prof_pct:+.2f}%", delta_color="normal")
                
                # Chart
                st.subheader("📈 การเติบโตของพอร์ต DCA")
                fig_dca = go.Figure()
                fig_dca.add_trace(go.Scatter(x=ledger['Date'], y=ledger['Value'], fill='tozeroy', name='มูลค่าพอร์ต (Portfolio Value)', line=dict(color='#10b981')))
                fig_dca.add_trace(go.Scatter(x=ledger['Date'], y=ledger['Invested'], name='เงินต้นสะสม (Invested)', line=dict(color='#6b7280', dash='dash')))
                fig_dca.update_layout(title=f"DCA Simulation for {dca_ticker} ({dca_years} Years)", hovermode="x unified")
                st.plotly_chart(fig_dca, use_container_width=True)
                
                # Data Table
                with st.expander("ดูตารางข้อมูลละเอียด (Detailed Ledger)"):
                    st.dataframe(ledger.style.format({'Invested': '{:,.2f}', 'Value': '{:,.2f}', 'Cost': '{:,.2f}'}))
            else:
                st.error("ไม่สามารถดึงข้อมูลย้อนหลังได้ หรือข้อมูลไม่เพียงพอ")
            
elif page == "⚙️ ตั้งค่า":
    st.title("⚙️ ตั้งค่า (Settings)")
    
    st.subheader("จัดการรายชื่อหุ้น (SET100)")
    st.markdown("เพิ่ม/ลด รายชื่อหุ้นที่ต้องการสแกน (คั่นด้วยเครื่องหมายจุลภาค , หรือขึ้นบรรทัดใหม่)")
    
    current_tickers = ", ".join(SET100_TICKERS)
    new_tickers_text = st.text_area("รายชื่อหุ้น (Ticker Symbols)", value=current_tickers, height=300)
    
    if st.button("บันทึกรายชื่อ"):
        # Process input
        raw_tickers = new_tickers_text.replace("\n", ",").split(",")
        clean_tickers = [t.strip().upper() for t in raw_tickers if t.strip()]
        
        # Save to file
        utils.save_tickers(clean_tickers)
        st.success(f"บันทึกเรียบร้อย! มีหุ้นทั้งหมด {len(clean_tickers)} ตัว (กรุณารีโหลดหน้าเว็บใหม่)")
        
        # Clear cache so new tickers are used next time
        st.cache_data.clear()

elif page == "💰 พอร์ตปันผล Value Growth":
    # Sidebar inputs for this page
    with st.sidebar:
        st.markdown("### ⚙️ ตั้งค่าพอร์ต (Portfolio Settings)")
        capital = st.number_input("เงินลงทุนเริ่มต้น (บาท)", value=6000000, step=100000, format="%d")
        st.caption(f"💰 {capital:,.0f} บาท")
        monthly_target = st.number_input("เป้าหมายปันผล (บาท/เดือน)", value=50000, step=5000, format="%d")
        st.caption(f"💰 {monthly_target:,.0f} บาท/เดือน")
        num_stocks = st.number_input("จำนวนหุ้นในพอร์ต (ตัว)", value=8, min_value=1, max_value=20, step=1)
        risk_level = st.select_slider("ระดับความเสี่ยง (Risk Level)", options=["Conservative", "Balanced", "Aggressive"], value="Balanced")
        
        st.markdown("---")
        st.markdown("**🚀 เร่งการเติบโต (Growth Booster)**")
        monthly_injection = st.number_input("เติมเงินลงทุนต่อเดือน (บาท)", value=0, step=1000, format="%d", help="จำนวนเงินที่ออมเพิ่มเพื่อซื้อหุ้นทุกเดือน")
        reinvest_dividends = st.checkbox("ทบต้นดอกเบี้ย (Reinvest Dividends)", value=False, help="นำเงินปันผลที่ได้กลับมาซื้อหุ้นเพิ่มอัตโนมัติ (Compound Interest)")
        
        st.info(f"""
        **เป้าหมาย:**
        - Yield: 7.5-9.0%
        - Div Growth: 4-8%
        - สุทธิ: {monthly_target:,.0f} บาท/เดือน
        """)

    st.title("💰 สร้างพอร์ตปันผล Value Growth")
    st.markdown("---")

    st.markdown(f"""
    ### 🎯 เป้าหมาย: สร้าง Cash Flow เดือนละ {monthly_target:,.0f} บาท
    จากเงินลงทุน **{capital:,.0f} บาท** ด้วยหุ้นปันผลคุณภาพสูง (High Quality Dividend Growth)
    """)

    # Initialize session state for portfolio if not exists
    if 'portfolio_data' not in st.session_state:
        st.session_state.portfolio_data = None

    if st.button("🚀 สร้างพอร์ตปันผลยั่งยืนให้ผม (Create Portfolio)", type="primary", use_container_width=True):
        with st.spinner("กำลังสแกนหาหุ้นปันผลที่ดีที่สุดจากตลาด (SET)... อาจใช้เวลา 1-2 นาที"):
            # Use SET100 + High Priority
            universe = SET100_TICKERS
            # Pass version=5 to force cache invalidation and use new logic
            portfolio, projection, avg_yield, warnings = portfolio_builder.build_dividend_portfolio(universe, capital, monthly_target, risk_level, max_stocks=num_stocks, version=5, monthly_injection=monthly_injection, reinvest_dividends=reinvest_dividends)
            
            # Save to session state
            st.session_state.portfolio_data = {
                'portfolio': portfolio,
                'projection': projection,
                'avg_yield': avg_yield,
                'warnings': warnings,
                'monthly_target': monthly_target # Save target to compare later
            }

    # Display Results if available
    if st.session_state.portfolio_data is not None:
        data = st.session_state.portfolio_data
        portfolio = data['portfolio']
        projection = data['projection']
        avg_yield = data['avg_yield']
        warnings = data['warnings']
        target_used = data.get('monthly_target', monthly_target)
            
        if not portfolio.empty:
            # 1. Summary Card
            st.success("✅ สร้างพอร์ตสำเร็จ!")
            
            # Show Warnings
            for w in warnings:
                st.warning(w)
            
            total_income_net = portfolio['Monthly_Income_Net'].sum()
            
            c1, c2, c3 = st.columns(3)
            c1.metric("Yield เฉลี่ย (Average Yield)", f"{avg_yield:.2%}") # Use .2% for decimal
            c2.metric("ปันผลสุทธิ/เดือน (Net Income)", f"{total_income_net:,.0f} บาท", delta=f"{total_income_net - target_used:,.0f} vs Target")
            c3.metric("ปันผลสุทธิ/ปี (Yearly)", f"{total_income_net*12:,.0f} บาท")
            
            # 2. Charts
            chart_c1, chart_c2 = st.columns(2)
            
            with chart_c1:
                st.subheader("📈 แนวโน้มการเติบโต (Growth Projection)")
                
                tab_inc, tab_val = st.tabs(["💰 เงินปันผล (Income)", "💎 มูลค่าพอร์ต (Portfolio Value)"])
                
                if not projection.empty:
                    with tab_inc:
                        # Line Chart - Income
                        fig_proj = px.line(projection, x='Year', y='Monthly_Income', markers=True, 
                                           title="คาดการณ์เงินปันผลสุทธิต่อเดือน (Projected Monthly Net Income)",
                                           labels={'Monthly_Income': 'บาท/เดือน', 'Year': 'ปี'})
                        # Add target line
                        fig_proj.add_hline(y=target_used, line_dash="dash", line_color="green", annotation_text="Target")
                        fig_proj.update_layout(yaxis_tickformat=",.0f")
                        st.plotly_chart(fig_proj, use_container_width=True)
                        
                    with tab_val:
                        # Line Chart - Value
                        fig_val = px.line(projection, x='Year', y='Portfolio_Value', markers=True,
                                          title="คาดการณ์มูลค่าพอร์ต (Projected Portfolio Value)",
                                          labels={'Portfolio_Value': 'มูลค่าพอร์ต (บาท)', 'Year': 'ปี'})
                        fig_val.update_traces(line_color='#8b5cf6') # Purple
                        fig_val.update_layout(yaxis_tickformat=",.0f")
                        st.plotly_chart(fig_val, use_container_width=True)
            
            with chart_c2:
                # Pie Chart Allocation
                st.subheader("🍰 สัดส่วนการลงทุน (Allocation)")
                fig_pie = px.pie(portfolio, values='Investment', names='Ticker', title=f"Portfolio Allocation ({len(portfolio)} Stocks)", hole=0.4)
                st.plotly_chart(fig_pie, use_container_width=True)
            
            # 3. Table
            st.subheader("📋 รายชื่อหุ้นแนะนำ (Recommended Stocks)")
            
            # Format for display
            # Extract useful metrics for easy understanding
            portfolio['Payout'] = portfolio['Details'].apply(lambda x: x.get('Payout_Ratio', 0))
            portfolio['ROE'] = portfolio['Details'].apply(lambda x: x.get('ROE', 0))
            portfolio['D/E'] = portfolio['Details'].apply(lambda x: x.get('DE_Ratio', 0))
            portfolio['Annual_Income_Net'] = portfolio['Monthly_Income_Net'] * 12
            
            # Calculate Investment Weight
            total_inv = portfolio['Investment'].sum()
            portfolio['Weight'] = portfolio['Investment'] / total_inv if total_inv > 0 else 0

            display_df = portfolio[['Ticker', 'Price', 'Score', 'Weight', 'Yield', 'DPS_Growth', 'Payout', 'ROE', 'D/E', 'Shares', 'Investment', 'Monthly_Income_Net', 'Annual_Income_Net']].copy()
            
            # Format Percentage
            for col in ['Yield', 'DPS_Growth', 'Payout', 'ROE', 'Weight']:
                display_df[col] = display_df[col].map(lambda x: f"{x:.2%}" if pd.notnull(x) else "-")
            
            # Format Numbers
            display_df['Price'] = display_df['Price'].map('{:,.2f}'.format)
            display_df['D/E'] = display_df['D/E'].map('{:.2f}'.format)
            display_df['Shares'] = display_df['Shares'].map('{:,.0f}'.format)
            display_df['Investment'] = display_df['Investment'].map('{:,.0f}'.format)
            display_df['Monthly_Income_Net'] = display_df['Monthly_Income_Net'].map('{:,.0f}'.format)
            display_df['Annual_Income_Net'] = display_df['Annual_Income_Net'].map('{:,.0f}'.format)
            
            # Rename columns to Thai
            display_df = display_df.rename(columns={
                'Ticker': 'ชื่อหุ้น',
                'Price': 'ราคา',
                'Score': 'คะแนน',
                'Weight': 'สัดส่วน (%)',
                'Yield': 'ปันผล (%)',
                'DPS_Growth': 'Growth (%)',
                'Payout': 'Payout',
                'ROE': 'ROE',
                'D/E': 'D/E',
                'Shares': 'จำนวนหุ้น',
                'Investment': 'เงินลงทุน (บาท)',
                'Monthly_Income_Net': 'ปันผลสุทธิ/เดือน',
                'Annual_Income_Net': 'ปันผลสุทธิ/ปี'
            })
            
            st.dataframe(display_df, use_container_width=True, hide_index=True)
            
            # Details Expander
            with st.expander("ดูรายละเอียดคะแนน (Scoring Details)"):
                st.write("เกณฑ์คะแนน (100 คะแนนเต็ม):")
                # Convert details dict to string or json for display
                st.dataframe(pd.json_normalize(portfolio['Details']), use_container_width=True)

            # 4. Actions
            csv = portfolio.to_csv(index=False).encode('utf-8')
            
            c_action1, c_action2 = st.columns([1, 2])
            with c_action1:
                st.download_button(
                    label="📥 Export Portfolio to Excel/CSV",
                    data=csv,
                    file_name='gg_dividend_portfolio.csv',
                    mime='text/csv',
                    use_container_width=True
                )
            
            st.markdown("---")
            st.caption("""
            **⚠️ หมายเหตุและข้อควรระวัง (Disclaimer):**
            1. **แหล่งข้อมูล:** ข้อมูลทั้งหมดดึงมาจาก **Yahoo Finance** แบบเรียลไทม์ (อาจมีความล่าช้า 15-20 นาที) และงบการเงินย้อนหลัง
            2. **ความถูกต้อง:** โปรแกรมใช้อัลกอริทึมในการคำนวณและคัดกรองเบื้องต้น ข้อมูลอาจมีความคลาดเคลื่อนจากแหล่งข้อมูลต้นทาง
            3. **ไม่ใช่คำแนะนำการลงทุน:** ผลลัพธ์นี้เป็นเพียงแนวทาง (Guide) สำหรับการศึกษาและวิเคราะห์เบื้องต้นเท่านั้น ผู้ใช้งานควรตรวจสอบข้อมูลจาก **SET.or.th** หรือหนังสือชี้ชวนก่อนตัดสินใจลงทุนจริง
            4. **การคำนวณ:** การคาดการณ์ปันผลในอนาคต (Projection) ใช้สมมติฐานการเติบโตในอดีต ซึ่งอาจไม่สะท้อนอนาคตที่แท้จริง
            """)
        
        else:
            st.error("ไม่พบหุ้นที่ผ่านเกณฑ์ หรือข้อมูลไม่เพียงพอ กรุณาลองใหม่อีกครั้ง")
