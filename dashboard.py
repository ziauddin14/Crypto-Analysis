import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timezone
from streamlit_autorefresh import st_autorefresh
from db_mongo import get_db
from etl_pipeline import run_etl

# --- Page Configuration ---
st.set_page_config(
    page_title="Real-Time Crypto Analytics",
    page_icon="🚀",
    layout="wide",
)

# --- Auto-Refresh (every 60 seconds) ---
count = st_autorefresh(interval=60 * 1000, key="statrefresh")

# --- CSS for modern look ---
st.markdown("""
    <style>
    .main {
        background-color: #0e1117;
    }
    .stMetric {
        background-color: #1e2130;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    </style>
    """, unsafe_allow_html=True)

# --- Helper Functions ---
def load_data():
    db = get_db()
    cursor = db.crypto_market.find({}, {"_id": 0})
    df = pd.DataFrame(list(cursor))
    return df

# --- Sidebar ---
st.sidebar.title("🛠️ Actions")
if st.sidebar.button("🚀 Run ETL Now"):
    with st.spinner("Executing Pipeline..."):
        result = run_etl(save_history=True)
        if result["status"] == "success":
            st.sidebar.success(f"ETL Complete! Fetched {result['fetched']} coins.")
        else:
            st.sidebar.error("ETL Failed! Check logs.")

st.sidebar.info("Auto-refreshing every 60 seconds.")

# --- Main Dashboard ---
st.title("🚀 Real-Time Crypto Analytics Platform")
st.markdown("---")

# Load Data from DB
df = load_data()

if df.empty:
    st.warning("No data found in database. Please run ETL from the sidebar.")
else:
    # 1. KPIs / Metrics Row
    col1, col2, col3, col4 = st.columns(4)
    
    total_mcap = df['market_cap'].sum()
    top_gainer = df.loc[df['price_change_24h'].idxmax()]
    most_volatile = df.loc[df['volatility_score'].idxmax()]
    avg_price = df['current_price'].mean()
    last_updated = df['extracted_at'].max()

    col1.metric("Total Market Cap (20)", f"${total_mcap/1e9:.2f}B")
    col2.metric("Top Gainer (24h%)", f"{top_gainer['symbol']}", f"{top_gainer['price_change_24h']:.2f}%")
    col3.metric("Most Volatile", f"{most_volatile['symbol']}")
    col4.metric("Avg Price", f"${avg_price:,.2f}")

    st.caption(f"Last Database Update: {last_updated.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    
    st.markdown("---")

    # 2. Charts Row 1
    c1, c2 = st.columns(2)

    with c1:
        st.subheader("📊 Market Capitalization (Top 20)")
        fig_mcap = px.bar(
            df, x='symbol', y='market_cap', 
            color='market_cap', 
            hover_data=['name', 'current_price'],
            labels={'market_cap': 'Market Cap ($)', 'symbol': 'Coin'},
            template="plotly_dark",
            color_continuous_scale='Viridis'
        )
        st.plotly_chart(fig_mcap, use_container_width=True)

    with c2:
        st.subheader("🔥 Volatility Ranking")
        # Top 10 by volatility_score
        df_vol = df.sort_values(by='volatility_score', ascending=False).head(10)
        fig_vol = px.bar(
            df_vol, x='volatility_score', y='symbol', 
            orientation='h',
            color='volatility_score',
            labels={'volatility_score': 'Volatility Score', 'symbol': 'Coin'},
            template="plotly_dark",
            color_continuous_scale='Bluered'
        )
        st.plotly_chart(fig_vol, use_container_width=True)

    # 3. Charts Row 2
    c3, c4 = st.columns(2)

    with c3:
        st.subheader("📈 24h Price Change (%)")
        fig_price = px.scatter(
            df, x='symbol', y='price_change_24h',
            size='total_volume', color='price_change_24h',
            hover_data=['name'],
            labels={'price_change_24h': 'Price Change % (24h)'},
            template="plotly_dark",
            color_continuous_scale='RdYlGn'
        )
        st.plotly_chart(fig_price, use_container_width=True)

    with c4:
        st.subheader("💰 Trading Volume Analysis")
        fig_vol_bar = px.pie(
            df.head(10), values='total_volume', names='symbol',
            title="Top 10 Volume Distribution",
            template="plotly_dark",
            hole=0.4
        )
        st.plotly_chart(fig_vol_bar, use_container_width=True)

    # 4. Data Table
    st.markdown("---")
    st.subheader("📄 Detailed Market Data")
    st.dataframe(df[['market_cap_rank', 'symbol', 'name', 'current_price', 'price_change_24h', 'market_cap', 'volatility_score']], use_container_width=True)
