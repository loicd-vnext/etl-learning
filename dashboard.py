"""
Streamlit Dashboard - ETL Pipeline Dashboard và BI
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

from src.dashboard import Dashboard
from src.utils.database import db_manager

# Page config
st.set_page_config(
    page_title="ETL Pipeline Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Title
st.title("📊 ETL Pipeline Dashboard")
st.markdown("---")

# Initialize dashboard
dashboard = Dashboard()

# Sidebar
st.sidebar.title("🔧 Navigation")
page = st.sidebar.selectbox(
    "Chọn trang",
    ["🚀 Run Pipeline", "📈 Overview", "👥 Customers", "📦 Products", "💰 Sales", "⚙️ Pipeline Status"]
)

# Check database connection
if not db_manager.test_connection():
    st.error("❌ Không thể kết nối database. Vui lòng kiểm tra database connection.")
    st.stop()

# Run Pipeline Page
if page == "🚀 Run Pipeline":
    st.header("🚀 Run ETL Pipeline")
    st.markdown("Chạy ETL pipeline với các tùy chọn tùy chỉnh")
    
    # Pipeline Configuration Form
    with st.form("pipeline_config"):
        st.subheader("📋 Pipeline Configuration")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Data Sources**")
            orders_path = st.text_input("Orders CSV Path", value="data/sample/orders.csv")
            customers_path = st.text_input("Customers JSON Path", value="data/sample/customers.json")
            products_path = st.text_input("Products JSON Path", value="data/sample/products.json")
        
        with col2:
            st.markdown("**Pipeline Options**")
            validate_data = st.checkbox("Validate Data", value=True)
            clean_data = st.checkbox("Clean Data", value=True)
            transform_data = st.checkbox("Transform Data", value=True)
            save_to_lake = st.checkbox("Save to Data Lake", value=True)
            load_to_warehouse = st.checkbox("Load to Warehouse", value=True)
            continue_on_error = st.checkbox("Continue on Error", value=False)
        
        batch_size = st.slider("Batch Size", min_value=100, max_value=10000, value=1000, step=100)
        
        # Submit button
        run_button = st.form_submit_button("🚀 Run Pipeline", type="primary", use_container_width=True)
    
    # Run pipeline when button is clicked
    if run_button:
        st.markdown("---")
        st.subheader("📊 Pipeline Execution")
        
        # Create containers
        progress_container = st.container()
        result_container = st.container()
        
        with progress_container:
            progress_bar = st.progress(0)
            status_text = st.empty()
            log_expander = st.expander("📝 Execution Logs", expanded=True)
        
        try:
            # Import pipeline
            from src.pipeline import ETLPipeline, PipelineConfig
            import io
            import sys
            from contextlib import redirect_stdout, redirect_stderr
            
            # Create config
            config = PipelineConfig(
                orders_path=orders_path,
                customers_path=customers_path,
                products_path=products_path,
                validate_data=validate_data,
                clean_data=clean_data,
                transform_data=transform_data,
                save_to_lake=save_to_lake,
                load_to_warehouse=load_to_warehouse,
                continue_on_error=continue_on_error,
                batch_size=batch_size
            )
            
            # Update progress
            status_text.info("🔄 Initializing pipeline...")
            progress_bar.progress(5)
            
            # Create pipeline
            pipeline = ETLPipeline(config)
            
            # Capture logs
            log_output = io.StringIO()
            
            # Run pipeline with log capture
            status_text.info("🔄 Running pipeline... Please wait...")
            progress_bar.progress(10)
            
            # Run pipeline (logs will go to file, we'll read them)
            result = pipeline.run()
            
            # Update progress
            progress_bar.progress(100)
            status_text.empty()
            
            # Display results
            with result_container:
                st.markdown("---")
                st.subheader("✅ Pipeline Execution Results")
                
                # Success/Failure banner
                if result.success:
                    st.success(f"🎉 Pipeline completed successfully in {result.duration_seconds:.2f} seconds!")
                else:
                    st.error("❌ Pipeline failed!")
                
                # Execution Summary Cards
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    status_icon = "✅" if result.success else "❌"
                    st.metric("Status", f"{status_icon} {'Success' if result.success else 'Failed'}")
                with col2:
                    st.metric("Duration", f"{result.duration_seconds:.2f}s")
                with col3:
                    st.metric("Steps Completed", len(result.steps_completed))
                with col4:
                    st.metric("Steps Failed", len(result.steps_failed))
                
                # Steps Timeline
                st.markdown("#### 📋 Execution Steps:")
                steps_data = []
                for i, step in enumerate(result.steps_completed, 1):
                    steps_data.append({
                        "Step": i,
                        "Name": step.replace("_", " ").title(),
                        "Status": "✅ Completed",
                        "Time": "✓"
                    })
                
                for step in result.steps_failed:
                    steps_data.append({
                        "Step": len(steps_data) + 1,
                        "Name": step.replace("_", " ").title(),
                        "Status": "❌ Failed",
                        "Time": "✗"
                    })
                
                if steps_data:
                    steps_df = pd.DataFrame(steps_data)
                    st.dataframe(steps_df, use_container_width=True, hide_index=True)
                
                # Statistics
                if result.statistics:
                    st.markdown("#### 📊 Statistics:")
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        if "extract" in result.statistics:
                            st.markdown("**Extract:**")
                            st.json(result.statistics["extract"])
                    
                    with col2:
                        if "transform" in result.statistics:
                            st.markdown("**Transform:**")
                            st.json(result.statistics["transform"])
                    
                    if "clean" in result.statistics:
                        st.markdown("**Clean:**")
                        st.json(result.statistics["clean"])
                
                # Errors
                if result.errors:
                    st.markdown("#### ⚠️ Errors:")
                    for error in result.errors:
                        st.error(f"❌ {error}")
                
                # Show log file content
                log_expander.markdown("#### Recent Logs:")
                try:
                    from pathlib import Path
                    log_dir = Path("logs")
                    if log_dir.exists():
                        log_files = sorted(log_dir.glob("*.log"), key=lambda x: x.stat().st_mtime, reverse=True)
                        if log_files:
                            with open(log_files[0], 'r', encoding='utf-8') as f:
                                lines = f.readlines()
                                # Show last 50 lines
                                recent_logs = "".join(lines[-50:])
                                log_expander.code(recent_logs, language="text")
                except Exception as e:
                    log_expander.warning(f"Could not read log file: {e}")
                
                # Action buttons
                if result.success:
                    st.markdown("---")
                    st.subheader("⚡ Next Actions")
                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button("🔄 Refresh All Data", key="refresh_after_run", use_container_width=True):
                            st.rerun()
                    with col2:
                        st.info("💡 Sử dụng sidebar để xem Overview, Sales, hoặc các pages khác")
        
        except Exception as e:
            progress_bar.progress(100)
            status_text.error("❌ Pipeline execution failed")
            st.error(f"**Error:** {str(e)}")
            import traceback
            with st.expander("🔍 Error Details", expanded=False):
                st.code(traceback.format_exc(), language="python")
    
    # Show last pipeline run info
    st.markdown("---")
    st.subheader("ℹ️ Pipeline Information")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        **Pipeline Steps:**
        1. 📥 **Extract** - Đọc dữ liệu từ CSV, JSON
        2. 💾 **Save Raw Data** - Lưu raw data vào data lake
        3. ✅ **Validate** - Kiểm tra data quality
        4. 🧹 **Clean** - Làm sạch data
        5. 🔄 **Transform** - Transform và enrich data
        6. 💾 **Save Processed** - Lưu processed data
        7. 📊 **Load to Warehouse** - Load vào database
        """)
    
    with col2:
        st.markdown("""
        **Configuration Options:**
        - ✅ **Validate Data**: Kiểm tra schema và business rules
        - 🧹 **Clean Data**: Remove duplicates, handle nulls
        - 🔄 **Transform Data**: Join, enrich, calculate
        - 💾 **Save to Lake**: Lưu raw và processed data
        - 📊 **Load to Warehouse**: Load vào PostgreSQL
        - ⚠️ **Continue on Error**: Tiếp tục khi có lỗi
        """)
    
    # Quick actions
    st.markdown("---")
    st.subheader("⚡ Quick Actions")
    st.info("💡 Sử dụng sidebar dropdown ở trên để chuyển giữa các pages: Overview, Customers, Products, Sales, Pipeline Status")

# Overview Page
elif page == "📈 Overview":
    st.header("📈 Tổng Quan")
    
    # Get statistics
    stats = dashboard.get_pipeline_stats()
    
    if stats["database_connected"]:
        # Key Metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("📊 Total Records", f"{stats['total_records']:,}")
        
        with col2:
            table_count = len(stats["tables"])
            st.metric("🗄️ Tables", table_count)
        
        with col3:
            if stats["tables"].get("fact_sales"):
                sales_count = stats["tables"]["fact_sales"]["row_count"]
                st.metric("💰 Sales Records", f"{sales_count:,}")
            else:
                st.metric("💰 Sales Records", "0")
        
        with col4:
            if stats["last_update"]:
                st.metric("🕐 Last Update", "Connected")
            else:
                st.metric("🕐 Last Update", "N/A")
        
        st.markdown("---")
        
        # Table Statistics
        st.subheader("📋 Table Statistics")
        table_data = []
        for table_name, table_info in stats["tables"].items():
            table_data.append({
                "Table": table_name,
                "Row Count": table_info["row_count"]
            })
        
        if table_data:
            df_tables = pd.DataFrame(table_data)
            st.dataframe(df_tables, use_container_width=True)
        
        # Sales Summary
        st.subheader("💰 Sales Summary")
        sales_summary = dashboard.get_sales_summary()
        if not sales_summary.empty and sales_summary['total_orders'].iloc[0] > 0:
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Orders", f"{int(sales_summary['total_orders'].iloc[0]):,}")
            with col2:
                st.metric("Total Revenue", f"${sales_summary['total_revenue'].iloc[0]:,.2f}")
            with col3:
                st.metric("Avg Order Value", f"${sales_summary['avg_order_value'].iloc[0]:,.2f}")
            with col4:
                st.metric("Total Discount", f"${sales_summary['total_discount'].iloc[0]:,.2f}")
        else:
            st.info("💡 No sales data available. Run pipeline to load data: `python scripts/run_pipeline.py`")
        
        # Customer & Product Summary
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("👥 Customer Summary")
            customer_summary = dashboard.get_customer_summary()
            if not customer_summary.empty:
                st.metric("Total Customers", f"{int(customer_summary['total_customers'].iloc[0]):,}")
                st.metric("Cities", f"{int(customer_summary['cities'].iloc[0]):,}")
                st.metric("Countries", f"{int(customer_summary['countries'].iloc[0]):,}")
        
        with col2:
            st.subheader("📦 Product Summary")
            product_summary = dashboard.get_product_summary()
            if not product_summary.empty:
                st.metric("Total Products", f"{int(product_summary['total_products'].iloc[0]):,}")
                st.metric("Categories", f"{int(product_summary['categories'].iloc[0]):,}")
                st.metric("Brands", f"{int(product_summary['brands'].iloc[0]):,}")
                st.metric("Avg Price", f"${product_summary['avg_price'].iloc[0]:,.2f}")

# Customers Page
elif page == "👥 Customers":
    st.header("👥 Customer Analytics")
    
    # Top Customers
    st.subheader("🏆 Top Customers by Revenue")
    top_customers = dashboard.get_top_customers(limit=10)
    
    if not top_customers.empty:
        # Display table
        st.dataframe(top_customers, use_container_width=True)
        
        # Chart
        if len(top_customers) > 0 and 'total_revenue' in top_customers.columns:
            fig = px.bar(
                top_customers.head(10),
                x='customer_name',
                y='total_revenue',
                title="Top 10 Customers by Revenue",
                labels={'customer_name': 'Customer', 'total_revenue': 'Revenue ($)'}
            )
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No customer data available")

# Products Page
elif page == "📦 Products":
    st.header("📦 Product Analytics")
    
    # Top Products
    st.subheader("🏆 Top Products by Sales")
    top_products = dashboard.get_top_products(limit=10)
    
    if not top_products.empty:
        # Display table
        st.dataframe(top_products, use_container_width=True)
        
        # Chart
        if len(top_products) > 0 and 'total_revenue' in top_products.columns:
            fig = px.bar(
                top_products.head(10),
                x='product_name',
                y='total_revenue',
                title="Top 10 Products by Revenue",
                labels={'product_name': 'Product', 'total_revenue': 'Revenue ($)'}
            )
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
    
    # Category Performance
    st.subheader("📊 Category Performance")
    category_perf = dashboard.get_category_performance()
    
    if not category_perf.empty:
        st.dataframe(category_perf, use_container_width=True)
        
        if len(category_perf) > 0 and 'total_revenue' in category_perf.columns:
            fig = px.pie(
                category_perf,
                values='total_revenue',
                names='category',
                title="Revenue by Category"
            )
            st.plotly_chart(fig, use_container_width=True)

# Sales Page
elif page == "💰 Sales":
    st.header("💰 Sales Analytics")
    
    # Daily Sales
    st.subheader("📈 Daily Sales Trend")
    daily_sales = dashboard.get_daily_sales()
    
    if not daily_sales.empty and 'date' in daily_sales.columns and len(daily_sales) > 0:
        # Line chart
        fig = px.line(
            daily_sales,
            x='date',
            y='total_revenue',
            title="Daily Sales Revenue",
            labels={'date': 'Date', 'total_revenue': 'Revenue ($)'},
            markers=True
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Display data
        st.dataframe(daily_sales, use_container_width=True)
    else:
        st.info("No daily sales data available. Run pipeline to load data.")
    
    # Sales Summary
    st.subheader("📊 Sales Summary")
    sales_summary = dashboard.get_sales_summary()
    if not sales_summary.empty:
        st.dataframe(sales_summary, use_container_width=True)

# Pipeline Status Page
elif page == "⚙️ Pipeline Status":
    st.header("⚙️ Pipeline Status")
    
    stats = dashboard.get_pipeline_stats()
    
    if stats["database_connected"]:
        st.success("✅ Database Connected")
        
        # Table Status
        st.subheader("📋 Table Status")
        for table_name, table_info in stats["tables"].items():
            col1, col2 = st.columns([3, 1])
            with col1:
                st.write(f"**{table_name}**")
            with col2:
                st.metric("Rows", f"{table_info['row_count']:,}")
        
        # Connection Info
        st.subheader("🔌 Connection Info")
        st.info(f"Last Update: {stats.get('last_update', 'N/A')}")
        st.info(f"Total Records: {stats['total_records']:,}")
    else:
        st.error("❌ Database Not Connected")

# Footer
st.markdown("---")
st.markdown("**ETL Pipeline Dashboard** | Built with Streamlit")

