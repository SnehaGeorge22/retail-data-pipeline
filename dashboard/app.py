"""
Retail Analytics Dashboard
Interactive dashboard for retail sales insights
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import snowflake.connector

# Page configuration
st.set_page_config(
    page_title="Retail Analytics Dashboard",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        margin-bottom: 1rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    </style>
""", unsafe_allow_html=True)

@st.cache_resource
def init_connection():
    """Initialize Snowflake connection"""
    try:
        return snowflake.connector.connect(
            user=st.secrets["snowflake"]["user"],
            password=st.secrets["snowflake"]["password"],
            account=st.secrets["snowflake"]["account"],
            warehouse=st.secrets["snowflake"]["warehouse"],
            database=st.secrets["snowflake"]["database"],
            schema=st.secrets["snowflake"]["schema"]
        )
    except Exception as e:
        st.error(f"Connection error: {e}")
        return None

@st.cache_data(ttl=600)
def run_query(query):
    """Run a query and return results as dataframe"""
    try:
        conn = init_connection()
        if conn is None:
            return pd.DataFrame()
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()

def main():
    # Header
    st.markdown('<p class="main-header">🛒 Retail Analytics Dashboard</p>', unsafe_allow_html=True)
    st.markdown("---")
    
    # Sidebar filters
    with st.sidebar:
        st.header("📊 Filters")
        
        # Date range filter
        date_range = st.date_input(
            "Select Date Range",
            value=(datetime.now() - timedelta(days=30), datetime.now()),
            max_value=datetime.now()
        )
        
        # Get available categories
        categories_query = """
        SELECT DISTINCT category 
        FROM fact_sales 
        WHERE category IS NOT NULL 
        ORDER BY category
        """
        categories_df = run_query(categories_query)
        
        if not categories_df.empty:
            selected_categories = st.multiselect(
                "Product Categories",
                options=categories_df['CATEGORY'].tolist(),
                default=categories_df['CATEGORY'].tolist()  # Select ALL by default
            )
        else:
            selected_categories = []
        
        # Get available store types
        store_types_query = """
        SELECT DISTINCT store_type 
        FROM fact_sales 
        WHERE store_type IS NOT NULL 
        ORDER BY store_type
        """
        store_types_df = run_query(store_types_query)
        
        if not store_types_df.empty:
            selected_store_types = st.multiselect(
                "Store Types",
                options=store_types_df['STORE_TYPE'].tolist(),
                default=store_types_df['STORE_TYPE'].tolist()  # Select ALL by default
            )
        else:
            selected_store_types = []
        
        st.markdown("---")
        st.info("💡 Data refreshes every 10 minutes")
    
    # Build filter conditions (FIXED for empty selections)
    date_filter = f"transaction_date BETWEEN '{date_range[0]}' AND '{date_range[1]}'"
    
    # Category filter - handles empty list
    if selected_categories and len(selected_categories) > 0:
        category_list = ','.join([f"'{c}'" for c in selected_categories])
        category_filter = f"category IN ({category_list})"
    else:
        category_filter = "1=1"
    
    # Store filter - handles empty list  
    if selected_store_types and len(selected_store_types) > 0:
        store_list = ','.join([f"'{s}'" for s in selected_store_types])
        store_filter = f"store_type IN ({store_list})"
    else:
        store_filter = "1=1"
    
    # Key Metrics
    st.subheader("📈 Key Performance Indicators")
    
    metrics_query = f"""
    SELECT
        COUNT(DISTINCT transaction_id) as total_transactions,
        COUNT(DISTINCT customer_id) as unique_customers,
        SUM(total_amount) as total_revenue,
        AVG(total_amount) as avg_order_value,
        SUM(gross_profit) as total_profit,
        AVG(gross_profit) as avg_profit_per_transaction
    FROM fact_sales
    WHERE {date_filter}
        AND {category_filter}
        AND {store_filter}
    """
    
    metrics = run_query(metrics_query)
    
    if not metrics.empty:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="💰 Total Revenue",
                value=f"${metrics['TOTAL_REVENUE'].iloc[0]:,.0f}" if pd.notna(metrics['TOTAL_REVENUE'].iloc[0]) else "$0"
            )
        
        with col2:
            st.metric(
                label="🛍️ Total Transactions",
                value=f"{metrics['TOTAL_TRANSACTIONS'].iloc[0]:,.0f}" if pd.notna(metrics['TOTAL_TRANSACTIONS'].iloc[0]) else "0"
            )
        
        with col3:
            st.metric(
                label="👥 Unique Customers",
                value=f"{metrics['UNIQUE_CUSTOMERS'].iloc[0]:,.0f}" if pd.notna(metrics['UNIQUE_CUSTOMERS'].iloc[0]) else "0"
            )
        
        with col4:
            st.metric(
                label="📊 Avg Order Value",
                value=f"${metrics['AVG_ORDER_VALUE'].iloc[0]:,.2f}" if pd.notna(metrics['AVG_ORDER_VALUE'].iloc[0]) else "$0.00"
            )
    
    st.markdown("---")
    
    # Row 1: Revenue Trends and Category Performance
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📅 Daily Revenue Trend")
        daily_revenue_query = f"""
        SELECT
            transaction_date,
            SUM(total_amount) as daily_revenue,
            COUNT(DISTINCT transaction_id) as daily_transactions
        FROM fact_sales
        WHERE {date_filter}
            AND {category_filter}
            AND {store_filter}
        GROUP BY transaction_date
        ORDER BY transaction_date
        """
        daily_revenue = run_query(daily_revenue_query)
        
        if not daily_revenue.empty:
            fig_revenue = go.Figure()
            fig_revenue.add_trace(go.Scatter(
                x=daily_revenue['TRANSACTION_DATE'],
                y=daily_revenue['DAILY_REVENUE'],
                mode='lines+markers',
                name='Revenue',
                line=dict(color='#1f77b4', width=2),
                fill='tozeroy'
            ))
            fig_revenue.update_layout(
                height=350,
                xaxis_title="Date",
                yaxis_title="Revenue ($)",
                hovermode='x unified'
            )
            st.plotly_chart(fig_revenue, use_container_width=True)
        else:
            st.info("No data available for the selected filters")
    
    with col2:
        st.subheader("🏷️ Revenue by Category")
        category_query = f"""
        SELECT
            category,
            SUM(total_amount) as revenue,
            COUNT(DISTINCT transaction_id) as transactions
        FROM fact_sales
        WHERE {date_filter}
            AND {category_filter}
            AND {store_filter}
            AND category IS NOT NULL
        GROUP BY category
        ORDER BY revenue DESC
        LIMIT 10
        """
        category_data = run_query(category_query)
        
        if not category_data.empty:
            fig_category = px.bar(
                category_data,
                x='REVENUE',
                y='CATEGORY',
                orientation='h',
                color='REVENUE',
                color_continuous_scale='Blues'
            )
            fig_category.update_layout(height=350, showlegend=False)
            st.plotly_chart(fig_category, use_container_width=True)
        else:
            st.info("No data available for the selected filters")
    
    # Row 2: Top Products and Store Performance
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🌟 Top 10 Products by Revenue")
        top_products_query = f"""
        SELECT
            product_id,
            category,
            SUM(total_amount) as revenue,
            SUM(quantity) as units_sold
        FROM fact_sales
        WHERE {date_filter}
            AND {category_filter}
            AND {store_filter}
        GROUP BY product_id, category
        ORDER BY revenue DESC
        LIMIT 10
        """
        top_products = run_query(top_products_query)
        
        if not top_products.empty:
            st.dataframe(
                top_products.style.format({
                    'REVENUE': '${:,.2f}',
                    'UNITS_SOLD': '{:,.0f}'
                }),
                use_container_width=True,
                height=350
            )
        else:
            st.info("No data available")
    
    with col2:
        st.subheader("🏪 Store Performance")
        store_performance_query = f"""
        SELECT
            store_type,
            store_city,
            SUM(total_amount) as revenue,
            COUNT(DISTINCT transaction_id) as transactions
        FROM fact_sales
        WHERE {date_filter}
            AND {category_filter}
            AND {store_filter}
            AND store_type IS NOT NULL
        GROUP BY store_type, store_city
        ORDER BY revenue DESC
        LIMIT 10
        """
        store_performance = run_query(store_performance_query)
        
        if not store_performance.empty:
            fig_stores = px.treemap(
                store_performance,
                path=['STORE_TYPE', 'STORE_CITY'],
                values='REVENUE',
                color='REVENUE',
                color_continuous_scale='RdYlGn'
            )
            fig_stores.update_layout(height=350)
            st.plotly_chart(fig_stores, use_container_width=True)
        else:
            st.info("No data available")
    
    # Row 3: Customer Insights
    st.markdown("---")
    st.subheader("👥 Customer Insights")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("**Customer Segments**")
        segment_query = f"""
        SELECT
            customer_segment,
            COUNT(DISTINCT customer_id) as customers,
            SUM(total_amount) as revenue
        FROM fact_sales
        WHERE {date_filter} AND customer_segment IS NOT NULL
        GROUP BY customer_segment
        ORDER BY revenue DESC
        """
        segments = run_query(segment_query)
        
        if not segments.empty:
            fig_segments = px.pie(
                segments,
                values='REVENUE',
                names='CUSTOMER_SEGMENT',
                hole=0.4
            )
            fig_segments.update_layout(height=300)
            st.plotly_chart(fig_segments, use_container_width=True)
        else:
            st.info("No data available")
    
    with col2:
        st.markdown("**Loyalty Program Impact**")
        loyalty_query = f"""
        SELECT
            CASE WHEN loyalty_member THEN 'Member' ELSE 'Non-Member' END as loyalty_status,
            AVG(total_amount) as avg_transaction_value,
            COUNT(DISTINCT customer_id) as customer_count
        FROM fact_sales
        WHERE {date_filter}
        GROUP BY loyalty_status
        """
        loyalty_data = run_query(loyalty_query)
        
        if not loyalty_data.empty:
            fig_loyalty = px.bar(
                loyalty_data,
                x='LOYALTY_STATUS',
                y='AVG_TRANSACTION_VALUE',
                color='LOYALTY_STATUS',
                text='AVG_TRANSACTION_VALUE'
            )
            fig_loyalty.update_traces(texttemplate='$%{text:.2f}', textposition='outside')
            fig_loyalty.update_layout(height=300, showlegend=False)
            st.plotly_chart(fig_loyalty, use_container_width=True)
        else:
            st.info("No data available")
    
    with col3:
        st.markdown("**Sales by Day Type**")
        daytype_query = f"""
        SELECT
            day_type,
            SUM(total_amount) as revenue,
            COUNT(DISTINCT transaction_id) as transactions
        FROM fact_sales
        WHERE {date_filter}
            AND {category_filter}
            AND {store_filter}
            AND day_type IS NOT NULL
        GROUP BY day_type
        """
        daytype_data = run_query(daytype_query)
        
        if not daytype_data.empty:
            fig_daytype = px.bar(
                daytype_data,
                x='DAY_TYPE',
                y='REVENUE',
                color='DAY_TYPE',
                text='REVENUE'
            )
            fig_daytype.update_traces(texttemplate='$%{text:,.0f}', textposition='outside')
            fig_daytype.update_layout(height=300, showlegend=False)
            st.plotly_chart(fig_daytype, use_container_width=True)
        else:
            st.info("No data available")

if __name__ == "__main__":
    main()