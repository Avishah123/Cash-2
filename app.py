import streamlit as st
import pandas as pd
import numpy as np
import pyotp
from SmartApi import SmartConnect
import time
import concurrent.futures
from collections import deque
import threading
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# Set page config
st.set_page_config(
    page_title="Market Data Monitor",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Load credentials from secrets or environment variables
# In production, use st.secrets or environment variables instead of hardcoded values
@st.cache_data
def get_credentials():
    return {
        "api_key": "taADOnb8",
        "client_id": "SERT1017",
        "password": "2589",
        "totp_token": "JAZJT3WVP4JXREBJARRWXQSMRE"
    }

class RateLimiter:
    """Implements a token bucket rate limiter to manage API request rates"""
    
    def __init__(self, rate_per_second=10, rate_per_minute=500):
        self.rate_per_second = rate_per_second
        self.rate_per_minute = rate_per_minute
        
        # Token buckets
        self.second_tokens = rate_per_second
        self.minute_tokens = rate_per_minute
        
        # Last refill times
        self.last_second_refill = time.time()
        self.last_minute_refill = time.time()
        
        # Tracking recent requests (sliding window for per-minute)
        self.recent_requests = deque()
        
        # Lock for thread safety
        self.lock = threading.Lock()
    
    def wait_if_needed(self):
        """Wait if necessary to comply with rate limits"""
        with self.lock:
            current_time = time.time()
            
            # Refill second bucket if needed
            seconds_elapsed = current_time - self.last_second_refill
            if seconds_elapsed >= 1.0:
                self.second_tokens = self.rate_per_second
                self.last_second_refill = current_time
            
            # Clean up minute tracking (remove entries older than 60 seconds)
            while self.recent_requests and current_time - self.recent_requests[0] > 60:
                self.recent_requests.popleft()
            
            # Check if we need to wait (either second or minute limit)
            wait_for_second = self.second_tokens <= 0
            wait_for_minute = len(self.recent_requests) >= self.rate_per_minute
            
            if wait_for_second or wait_for_minute:
                if wait_for_second:
                    # Wait until next second refill
                    wait_time = 1.0 - (current_time - self.last_second_refill)
                    if wait_time > 0:
                        time.sleep(wait_time)
                        # Recursively check again after waiting
                        self.wait_if_needed()
                        return
                
                if wait_for_minute:
                    # Wait until oldest request falls out of the minute window
                    wait_time = 60 - (current_time - self.recent_requests[0]) + 0.01
                    if wait_time > 0:
                        time.sleep(wait_time)
                        # Recursively check again after waiting
                        self.wait_if_needed()
                        return
            
            # If we get here, we can make a request
            self.second_tokens -= 1
            self.recent_requests.append(time.time())

@st.cache_resource
def get_authenticated_client():
    """Create and authenticate SmartConnect client"""
    credentials = get_credentials()
    totp = pyotp.TOTP(credentials["totp_token"]).now()
    obj = SmartConnect(credentials["api_key"])
    data = obj.generateSession(credentials["client_id"], credentials["password"], totp)
    
    if data.get('status'):
        return obj
    else:
        st.error(f"Login failed: {data.get('message')}")
        return None

def fetch_market_data_chunk(obj, exchange, tokens):
    """Fetch market data for a chunk of tokens from one exchange"""
    try:
        exchangeTokens = {exchange: tokens}
        response = obj.getMarketData(mode="FULL", exchangeTokens=exchangeTokens)
        
        if response and response.get('status'):
            result = {}
            fetched_data = response.get('data', {}).get('fetched', [])
            
            for item in fetched_data:
                token = str(item.get('symbolToken'))
                result[(exchange, token)] = item
                
            return result
        else:
            error_message = response.get('message', 'Unknown error')
            st.warning(f"Error fetching data for {exchange}: {error_message}")
            return {}
    except Exception as e:
        st.warning(f"Exception in fetch_market_data_chunk for {exchange}: {e}")
        return {}

def fetch_market_data_with_rate_limiting(obj, df, rate_limiter, status_placeholder, chunk_size=50):
    """Fetch market data with proper rate limiting"""
    # Similar to batch_fetch_market_data but uses the rate limiter
    start_time = time.time()
    
    # Group tokens by exchange
    exchange_tokens = {}
    for _, row in df.iterrows():
        if row['exch_seg_x'] not in exchange_tokens:
            exchange_tokens[row['exch_seg_x']] = set()
        if row['exch_seg_y'] not in exchange_tokens:
            exchange_tokens[row['exch_seg_y']] = set()
        
        exchange_tokens[row['exch_seg_x']].add(str(row['token_x']))
        exchange_tokens[row['exch_seg_y']].add(str(row['token_y']))
    
    # Create batches
    batches = []
    for exchange, tokens in exchange_tokens.items():
        unique_tokens = list(tokens)
        for i in range(0, len(unique_tokens), chunk_size):
            batches.append((exchange, unique_tokens[i:i + chunk_size]))
    
    # Setup progress bar
    progress_bar = status_placeholder.progress(0)
    status_text = status_placeholder.empty()
    
    # Fetch data sequentially with rate limiting
    all_market_data = {}
    for i, (exchange, tokens) in enumerate(batches):
        status_text.text(f"Processing batch {i+1}/{len(batches)}: {exchange} with {len(tokens)} tokens")
        progress_bar.progress((i+1)/len(batches))
        
        rate_limiter.wait_if_needed()
        try:
            result = fetch_market_data_chunk(obj, exchange, tokens)
            all_market_data.update(result)
        except Exception as e:
            status_text.text(f"Error processing batch {i+1}: {e}")
    
    # Update dataframe
    def get_ltp_from_cache(exchange, token):
        token_str = str(token)
        key = (exchange, token_str)
        
        if key in all_market_data:
            return all_market_data[key].get('ltp')
        
        for (ex, tk), data in all_market_data.items():
            if ex == exchange and tk == token_str:
                return data.get('ltp')
                
        return None
    
    df['ltp_x'] = df.apply(lambda row: get_ltp_from_cache(row['exch_seg_x'], row['token_x']), axis=1)
    df['ltp_y'] = df.apply(lambda row: get_ltp_from_cache(row['exch_seg_y'], row['token_y']), axis=1)
    df['ltp_difference'] = df['ltp_x'] - df['ltp_y']
    
    # Add timestamp
    df['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    end_time = time.time()
    
    # Update status
    status_text.text(f"Data fetch complete in {end_time - start_time:.2f} seconds")
    progress_bar.progress(100)
    
    return df

# Helper function to log differences
def log_differences(df, history_dict):
    """Track important values over time for visualization"""
    current_time = datetime.now()
    
    # For each row in dataframe, update history
    for _, row in df.iterrows():
        pair_key = f"{row['symbol_x']}_{row['symbol_y']}"
        
        if pair_key not in history_dict:
            history_dict[pair_key] = {
                'times': [],
                'differences': []
            }
        
        # Add current values to history
        history_dict[pair_key]['times'].append(current_time)
        history_dict[pair_key]['differences'].append(row['ltp_difference'])
        
        # Keep only last 100 data points to avoid memory issues
        if len(history_dict[pair_key]['times']) > 100:
            history_dict[pair_key]['times'].pop(0)
            history_dict[pair_key]['differences'].pop(0)
    
    return history_dict

def create_difference_chart(history_dict, selected_pairs):
    """Create time series chart of price differences"""
    fig = go.Figure()
    
    for pair in selected_pairs:
        if pair in history_dict:
            # Convert datetime objects to strings
            times = [t.strftime("%H:%M:%S") for t in history_dict[pair]['times']]
            differences = history_dict[pair]['differences']
            
            fig.add_trace(go.Scatter(
                x=times,
                y=differences,
                mode='lines+markers',
                name=pair
            ))
    
    fig.update_layout(
        title="Price Difference Over Time",
        xaxis_title="Time",
        yaxis_title="Price Difference",
        height=400,
        margin=dict(l=40, r=40, t=40, b=40)
    )
    
    return fig

def create_heatmap(df):
    """Create heatmap of price differences"""
    # Prepare data for heatmap
    pairs = [f"{row['symbol_x']}_{row['symbol_y']}" for _, row in df.iterrows()]
    differences = df['ltp_difference'].values
    
    # Create a dataframe for the heatmap
    heatmap_df = pd.DataFrame({
        'Pair': pairs,
        'Difference': differences,
        'AbsDifference': np.abs(differences)
    })
    
    # Sort by absolute difference for better visualization
    heatmap_df = heatmap_df.sort_values('AbsDifference', ascending=False)
    
    # Create heatmap
    fig = px.bar(
        heatmap_df, 
        x='Pair', 
        y='Difference',
        color='Difference',
        color_continuous_scale='RdBu_r',  # Red for negative, Blue for positive
        title="Current Price Differences"
    )
    
    fig.update_layout(
        height=400,
        margin=dict(l=40, r=40, t=40, b=40)
    )
    
    return fig

def main():
    st.title("Market Data Monitor")
    
    # Sidebar configuration
    st.sidebar.header("Configuration")
    
    # File uploader
    uploaded_file = st.sidebar.file_uploader("Upload token_data_final.csv", type=["csv"])
    
    # Set update interval
    update_interval = st.sidebar.slider(
        "Update Interval (seconds)", 
        min_value=15, 
        max_value=60, 
        value=30,
        step=5
    )
    
    # Rate limit settings
    st.sidebar.subheader("API Rate Limits")
    req_per_second = st.sidebar.slider("Requests per second", 1, 10, 8)
    req_per_minute = st.sidebar.slider("Requests per minute", 100, 500, 450)
    
    # Auto-refresh option
    auto_refresh = st.sidebar.checkbox("Enable auto-refresh", value=True)
    
    # Initialize session state
    if 'data_history' not in st.session_state:
        st.session_state.data_history = {}
    
    if 'last_update' not in st.session_state:
        st.session_state.last_update = None
    
    if 'df' not in st.session_state:
        st.session_state.df = None
    
    if 'update_counter' not in st.session_state:
        st.session_state.update_counter = 0
    
    # Create placeholders
    status_container = st.container()
    data_container = st.container()
    
    # Main dashboard layout (two columns)
    col1, col2 = st.columns(2)
    
    # Chart placeholders
    with col1:
        heatmap_placeholder = st.empty()
    
    with col2:
        time_series_placeholder = st.empty()
    
    # Data table placeholder
    data_table_placeholder = st.empty()
    
    # Manual refresh button
    if not auto_refresh:
        refresh_button = st.button("Refresh Data Now")
    
    if uploaded_file is not None:
        # Create rate limiter
        rate_limiter = RateLimiter(rate_per_second=req_per_second, rate_per_minute=req_per_minute)
        
        # Read CSV file (only once)
        if st.session_state.df is None:
            df = pd.read_csv(uploaded_file)
            st.session_state.df = df
        
        # Authenticate client
        with status_container:
            st.info("Authenticating with API...")
            obj = get_authenticated_client()
            
            if obj is None:
                st.error("Failed to authenticate. Please check your credentials.")
                return
            
            st.success("Authentication successful!")
        
        # Determine if we should update
        current_time = datetime.now()
        should_update = (
            (auto_refresh and (st.session_state.last_update is None or 
             (current_time - st.session_state.last_update).total_seconds() >= update_interval)) or
            (not auto_refresh and refresh_button)
        )
        
        if should_update:
            # Increment counter for unique keys
            st.session_state.update_counter += 1
            
            with status_container:
                st.subheader("Status")
                status_placeholder = st.empty()
                
                with status_placeholder.container():
                    st.write(f"Updating data at {current_time.strftime('%Y-%m-%d %H:%M:%S')}...")
                    progress_placeholder = st.empty()
                    status_text = st.empty()
                    
                    # Fetch data with rate limiting
                    df = fetch_market_data_with_rate_limiting(
                        obj, 
                        st.session_state.df, 
                        rate_limiter, 
                        progress_placeholder
                    )
                    
                    # Update session state
                    st.session_state.df = df
                    st.session_state.last_update = current_time
                    
                    # Log differences for time series
                    st.session_state.data_history = log_differences(df, st.session_state.data_history)
                    
                    status_text.success(f"Data updated successfully at {current_time.strftime('%H:%M:%S')}")
            
            # Select top pairs by absolute difference for chart
            sorted_df = df.sort_values(by='ltp_difference', key=abs, ascending=False)
            top_pairs = [f"{row['symbol_x']}_{row['symbol_y']}" for _, row in sorted_df.head(5).iterrows()]
            
            # Create and display heatmap
            with heatmap_placeholder.container():
                heatmap_fig = create_heatmap(df)
                st.plotly_chart(heatmap_fig, use_container_width=True, key=f"heatmap_{st.session_state.update_counter}")
            
            # Create and display time series
            with time_series_placeholder.container():
                time_series_fig = create_difference_chart(st.session_state.data_history, top_pairs)
                st.plotly_chart(time_series_fig, use_container_width=True, key=f"timeseries_{st.session_state.update_counter}")
            
            # Display filtered data table
            with data_table_placeholder.container():
                st.subheader("Current Market Data")
                
                # Create filtered dataframe for display
                display_df = df[['symbol_x', 'symbol_y', 'ltp_x', 'ltp_y', 'ltp_difference', 'timestamp']]
                
                # Sort by absolute difference and display
                sorted_display_df = display_df.sort_values(by='ltp_difference', key=abs, ascending=False)
                st.dataframe(
                    sorted_display_df.style.apply(
                        lambda x: ['color: red' if v < 0 else 'color: green' if v > 0 else '' for v in x], 
                        subset=['ltp_difference']
                    ),
                    use_container_width=True,
                    key=f"datatable_{st.session_state.update_counter}"
                )
        else:
            # Just display existing data without fetching
            if st.session_state.df is not None and not st.session_state.df.empty:
                df = st.session_state.df
                
                # Select top pairs by absolute difference for chart
                sorted_df = df.sort_values(by='ltp_difference', key=abs, ascending=False)
                top_pairs = [f"{row['symbol_x']}_{row['symbol_y']}" for _, row in sorted_df.head(5).iterrows()]
                
                # Display existing charts and data
                with heatmap_placeholder.container():
                    heatmap_fig = create_heatmap(df)
                    st.plotly_chart(heatmap_fig, use_container_width=True, key=f"heatmap_static_{st.session_state.update_counter}")
                
                with time_series_placeholder.container():
                    time_series_fig = create_difference_chart(st.session_state.data_history, top_pairs)
                    st.plotly_chart(time_series_fig, use_container_width=True, key=f"timeseries_static_{st.session_state.update_counter}")
                
                with data_table_placeholder.container():
                    st.subheader("Current Market Data")
                    display_df = df[['symbol_x', 'symbol_y', 'ltp_x', 'ltp_y', 'ltp_difference', 'timestamp']]
                    sorted_display_df = display_df.sort_values(by='ltp_difference', key=abs, ascending=False)
                    st.dataframe(
                        sorted_display_df.style.apply(
                            lambda x: ['color: red' if v < 0 else 'color: green' if v > 0 else '' for v in x], 
                            subset=['ltp_difference']
                        ),
                        use_container_width=True,
                        key=f"datatable_static_{st.session_state.update_counter}"
                    )
        
        # Auto-refresh mechanism using Streamlit's way (no while loops)
        if auto_refresh:
            time_until_refresh = update_interval
            if st.session_state.last_update is not None:
                elapsed = (datetime.now() - st.session_state.last_update).total_seconds()
                time_until_refresh = max(0, update_interval - elapsed)
            
            st.sidebar.info(f"Next refresh in: {int(time_until_refresh)} seconds")
            
            # This is the proper way to trigger refreshes in Streamlit
            time.sleep(1)  # Short sleep to prevent consuming too much CPU
            st.rerun()
    else:
        st.info("Please upload the token_data_final.csv file to start monitoring.")

if __name__ == "__main__":
    main()