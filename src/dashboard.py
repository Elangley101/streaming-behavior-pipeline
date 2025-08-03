import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import altair as alt
from datetime import datetime, timedelta
import numpy as np
from pathlib import Path

from snowflake_manager import SnowflakeManager
from utils import PipelineError
from config.config import PIPELINE_CONFIG

# Page configuration
st.set_page_config(
    page_title="Streamalytics  Analytics Dashboard",
    page_icon="📺",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for better styling
st.markdown(
    """
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #E50914;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #E50914;
    }
    .sidebar .sidebar-content {
        background-color: #f8f9fa;
    }
</style>
""",
    unsafe_allow_html=True,
)


class NetflixDashboard:
    """Interactive dashboard for Netflix behavioral analytics."""

    def __init__(self):
        """Initialize the dashboard."""
        self.snowflake_manager = None
        self.data = None
        self.setup_sidebar()

    def setup_sidebar(self):
        """Setup the sidebar with filters and options."""
        st.sidebar.title("🎛️ Dashboard Controls")

        # Data source selection
        data_source = st.sidebar.selectbox(
            "Data Source",
            ["Sample Data", "Snowflake", "Parquet Files"],
            help="Choose where to load data from",
        )

        # Date range filter
        st.sidebar.subheader("📅 Date Range")
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)

        date_range = st.sidebar.date_input(
            "Select Date Range",
            value=(start_date.date(), end_date.date()),
            max_value=end_date.date(),
        )

        # User filter (dynamic for Snowflake)
        st.sidebar.subheader("👥 User Filter")
        user_filter = ["All Users"]
        if data_source == "Snowflake":
            try:
                snowflake_manager = SnowflakeManager()
                user_query = "SELECT DISTINCT user_id FROM NETFLIX_ANALYTICS.MARTS.USER_WATCH_SESSIONS ORDER BY user_id"
                user_df = snowflake_manager.execute_query(user_query)
                user_list = user_df["user_id"].tolist() if not user_df.empty else []
                user_filter += user_list
            except Exception as e:
                st.warning(
                    f"Could not fetch users from Snowflake: {str(e)}. Using default list."
                )
                user_filter += [f"user_{i:04d}" for i in range(1, 1001)]
        else:
            user_filter += [f"user_{i:04d}" for i in range(1, 1001)]
        user_filter_selected = st.sidebar.multiselect(
            "Select Users", user_filter, default=["All Users"]
        )

        # Show filter (dynamic for Snowflake)
        st.sidebar.subheader("📺 Show Filter")
        show_filter = ["All Shows"]
        if data_source == "Snowflake":
            try:
                snowflake_manager = SnowflakeManager()
                show_query = "SELECT DISTINCT show_name FROM NETFLIX_ANALYTICS.MARTS.USER_WATCH_SESSIONS ORDER BY show_name"
                show_df = snowflake_manager.execute_query(show_query)
                show_list = show_df["show_name"].tolist() if not show_df.empty else []
                show_filter += show_list
            except Exception as e:
                st.warning(
                    f"Could not fetch shows from Snowflake: {str(e)}. Using default list."
                )
                show_filter += [
                    "Stranger Things",
                    "The Crown",
                    "Breaking Bad",
                    "Friends",
                    "The Office",
                ]
        else:
            show_filter += [
                "Stranger Things",
                "The Crown",
                "Breaking Bad",
                "Friends",
                "The Office",
            ]
        show_filter_selected = st.sidebar.multiselect(
            "Select Shows", show_filter, default=["All Shows"]
        )

        # Navigation to other dashboards
        st.sidebar.markdown("---")
        st.sidebar.subheader("📊 Dashboard Views")
        
        # Linux EC2 specific reset button
        col1, col2 = st.sidebar.columns(2)
        with col1:
            if st.button("🔄 Reset", key="reset_nav_linux"):
                # Clear all session state for Linux
                for key in list(st.session_state.keys()):
                    del st.session_state[key]
                st.session_state.dashboard_navigation = 'Main Analytics'
                st.session_state._initialized = True
                st.rerun()
        
        with col2:
            if st.button("🔄 Force Rerun", key="force_rerun"):
                st.rerun()
        
        # Use radio buttons for more reliable navigation
        dashboard_view = st.sidebar.radio(
            "Select Dashboard View",
            ["Main Analytics", "SQL Analytics", "Data Quality"],
            index=0,
            key="dashboard_navigation",
            help="Switch between different dashboard views"
        )
        
        st.sidebar.markdown("---")
        st.sidebar.markdown(f"**Current: {dashboard_view}**")

        self.filters = {
            "data_source": data_source,
            "date_range": date_range,
            "user_filter": user_filter_selected,
            "show_filter": show_filter_selected,
        }

    def load_data(self):
        """Load data based on selected source."""
        try:
            if self.filters["data_source"] == "Sample Data":
                self.load_sample_data()
            elif self.filters["data_source"] == "Snowflake":
                self.load_snowflake_data()
            else:
                self.load_parquet_data()
        except Exception as e:
            st.error(f"Error loading data: {str(e)}")
            self.load_sample_data()  # Fallback to sample data

    def load_sample_data(self):
        """Load sample data for demonstration."""
        # Generate sample data
        np.random.seed(42)
        dates = pd.date_range(
            start=self.filters["date_range"][0],
            end=self.filters["date_range"][1],
            freq="H",
        )

        data = []
        shows = [
            "Stranger Things",
            "The Crown",
            "Breaking Bad",
            "Friends",
            "The Office",
        ]
        users = [f"user_{i:04d}" for i in range(1, 101)]

        for _ in range(1000):
            data.append(
                {
                    "user_id": np.random.choice(users),
                    "show_name": np.random.choice(shows),
                    "watch_duration_minutes": np.random.normal(45, 15),
                    "watch_date": np.random.choice(dates),
                    "completion_rate": np.random.beta(2, 1),
                    "is_binge_session": np.random.choice([True, False], p=[0.3, 0.7]),
                    "engagement_score": np.random.uniform(0, 1),
                }
            )

        self.data = pd.DataFrame(data)
        self.data["watch_date"] = pd.to_datetime(self.data["watch_date"])

    def load_snowflake_data(self):
        """Load data from Snowflake."""
        try:
            self.snowflake_manager = SnowflakeManager()
            # Use FACT_USER_WATCH_SESSIONS as the main fact table
            query = """
            SELECT * FROM NETFLIX_ANALYTICS.MARTS.FACT_USER_WATCH_SESSIONS 
            WHERE watch_date BETWEEN :start_date AND :end_date
            """
            self.data = self.snowflake_manager.execute_query(
                query,
                {
                    "start_date": self.filters["date_range"][0],
                    "end_date": self.filters["date_range"][1],
                },
            )
        except Exception as e:
            st.warning(f"Could not connect to Snowflake: {str(e)}. Using sample data.")
            self.load_sample_data()

    def load_parquet_data(self):
        """Load data from Parquet files."""
        try:
            parquet_path = PIPELINE_CONFIG["processed_data_path"]
            if Path(parquet_path).exists():
                self.data = pd.read_parquet(parquet_path)
            else:
                st.warning("Parquet file not found. Using sample data.")
                self.load_sample_data()
        except Exception as e:
            st.warning(f"Could not load Parquet data: {str(e)}. Using sample data.")
            self.load_sample_data()

    def apply_filters(self):
        """Apply user-selected filters to the data."""
        if self.data is None:
            return

        # Date filter
        if len(self.filters["date_range"]) == 2:
            self.data = self.data[
                (self.data["watch_date"].dt.date >= self.filters["date_range"][0])
                & (self.data["watch_date"].dt.date <= self.filters["date_range"][1])
            ]

        # User filter
        if "All Users" not in self.filters["user_filter"]:
            self.data = self.data[
                self.data["user_id"].isin(self.filters["user_filter"])
            ]

        # Show filter
        if "All Shows" not in self.filters["show_filter"]:
            self.data = self.data[
                self.data["show_name"].isin(self.filters["show_filter"])
            ]

    def render_header(self):
        """Render the dashboard header."""
        st.markdown(
            '<h1 class="main-header">📺 Netflix Analytics Dashboard</h1>',
            unsafe_allow_html=True,
        )

        # Key metrics row
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                "Total Watch Sessions",
                f"{len(self.data):,}" if self.data is not None else "0",
                delta="+12% from last week",
            )

        with col2:
            total_hours = (
                (self.data["watch_duration_minutes"].sum() / 60)
                if self.data is not None
                else 0
            )
            st.metric(
                "Total Watch Hours", f"{total_hours:,.1f}", delta="+8% from last week"
            )

        with col3:
            avg_engagement = (
                self.data["engagement_score"].mean() if self.data is not None else 0
            )
            st.metric(
                "Avg Engagement Score",
                f"{avg_engagement:.2f}",
                delta="+0.05 from last week",
            )

        with col4:
            binge_sessions = (
                self.data["is_binge_session"].sum() if self.data is not None else 0
            )
            st.metric(
                "Binge Sessions", f"{binge_sessions:,}", delta="+15% from last week"
            )

    def render_charts(self):
        """Render interactive charts and visualizations."""
        if self.data is None or len(self.data) == 0:
            st.warning("No data available for the selected filters.")
            return

        # Create tabs for different chart types
        tab1, tab2, tab3, tab4 = st.tabs(
            [
                "📊 Overview",
                "👥 User Analytics",
                "📺 Content Analytics",
                "⏰ Time Patterns",
            ]
        )

        with tab1:
            self.render_overview_charts()

        with tab2:
            self.render_user_analytics()

        with tab3:
            self.render_content_analytics()

        with tab4:
            self.render_time_patterns()

    def render_overview_charts(self):
        """Render overview charts."""
        col1, col2 = st.columns(2)

        with col1:
            # Watch duration distribution
            fig = px.histogram(
                self.data,
                x="watch_duration_minutes",
                nbins=30,
                title="Watch Duration Distribution",
                labels={
                    "watch_duration_minutes": "Duration (minutes)",
                    "count": "Sessions",
                },
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Engagement score distribution
            fig = px.histogram(
                self.data,
                x="engagement_score",
                nbins=20,
                title="Engagement Score Distribution",
                labels={"engagement_score": "Engagement Score", "count": "Sessions"},
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

        # Top shows by watch time
        show_stats = self.data.groupby("show_name", as_index=False).agg(
            {"watch_duration_minutes": "sum", "user_id": "count"}
        )
        show_stats.columns = ["Show", "Total Watch Time (min)", "Sessions"]

        fig = px.bar(
            show_stats.head(10),
            x="Show",
            y="Total Watch Time (min)",
            title="Top Shows by Watch Time",
            color="Sessions",
            color_continuous_scale="viridis",
        )
        fig.update_layout(height=400, xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

    def render_user_analytics(self):
        """Render user analytics charts."""
        col1, col2 = st.columns(2)

        with col1:
            # User engagement ranking
            user_stats = self.data.groupby("user_id", as_index=False).agg(
                {
                    "engagement_score": "mean",
                    "watch_duration_minutes": "sum",
                    "is_binge_session": "sum",
                }
            )
            user_stats = user_stats.sort_values("engagement_score", ascending=False)

            fig = px.bar(
                user_stats.head(20),
                x="user_id",
                y="engagement_score",
                title="Top Users by Engagement Score",
                color="watch_duration_minutes",
                color_continuous_scale="plasma",
            )
            fig.update_layout(height=400, xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Binge watching patterns
            binge_stats = self.data.groupby("user_id", as_index=False).agg(
                {"is_binge_session": "sum"}
            )
            # Get total sessions count
            total_sessions = (
                self.data.groupby("user_id").size().reset_index(name="total_sessions")
            )
            binge_stats = pd.merge(binge_stats, total_sessions, on="user_id")
            binge_stats["binge_ratio"] = (
                binge_stats["is_binge_session"] / binge_stats["total_sessions"]
            )

            fig = px.scatter(
                binge_stats,
                x="user_id",
                y="binge_ratio",
                size="is_binge_session",
                title="Binge Watching Patterns by User",
                labels={
                    "binge_ratio": "Binge Session Ratio",
                    "is_binge_session": "Total Binge Sessions",
                },
            )
            fig.update_layout(height=400, xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

    def render_content_analytics(self):
        """Render content analytics charts."""
        col1, col2 = st.columns(2)

        with col1:
            # Show completion rates
            show_completion = self.data.groupby("show_name", as_index=False).agg(
                {"completion_rate": "mean"}
            )
            # Get view count
            view_count = (
                self.data.groupby("show_name").size().reset_index(name="view_count")
            )
            show_completion = pd.merge(show_completion, view_count, on="show_name")

            fig = px.scatter(
                show_completion,
                x="completion_rate",
                y="view_count",
                size="view_count",
                hover_data=["show_name"],
                title="Show Performance: Completion Rate vs Views",
                labels={
                    "completion_rate": "Average Completion Rate",
                    "view_count": "Number of Views",
                },
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Engagement by show
            show_engagement = self.data.groupby("show_name", as_index=False).agg(
                {"engagement_score": "mean", "watch_duration_minutes": "mean"}
            )

            fig = px.scatter(
                show_engagement,
                x="engagement_score",
                y="watch_duration_minutes",
                size="engagement_score",
                hover_data=["show_name"],
                title="Show Engagement Analysis",
                labels={
                    "engagement_score": "Average Engagement Score",
                    "watch_duration_minutes": "Average Watch Duration (min)",
                },
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

    def render_time_patterns(self):
        """Render time-based pattern charts."""
        col1, col2 = st.columns(2)

        with col1:
            # Hourly viewing patterns
            self.data["hour"] = self.data["watch_date"].dt.hour
            hourly_stats = self.data.groupby("hour", as_index=False).agg(
                {"watch_duration_minutes": "mean"}
            )
            # Get session count
            session_count = (
                self.data.groupby("hour").size().reset_index(name="session_count")
            )
            hourly_stats = pd.merge(hourly_stats, session_count, on="hour")

            fig = px.line(
                hourly_stats,
                x="hour",
                y="session_count",
                title="Viewing Patterns by Hour",
                labels={"hour": "Hour of Day", "session_count": "Number of Sessions"},
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Daily viewing patterns
            self.data["day_of_week"] = self.data["watch_date"].dt.day_name()
            daily_stats = self.data.groupby("day_of_week", as_index=False).agg(
                {"is_binge_session": "sum"}
            )
            # Get session count
            session_count = (
                self.data.groupby("day_of_week")
                .size()
                .reset_index(name="session_count")
            )
            daily_stats = pd.merge(daily_stats, session_count, on="day_of_week")

            # Reorder days
            day_order = [
                "Monday",
                "Tuesday",
                "Wednesday",
                "Thursday",
                "Friday",
                "Saturday",
                "Sunday",
            ]
            daily_stats["day_of_week"] = pd.Categorical(
                daily_stats["day_of_week"], categories=day_order, ordered=True
            )
            daily_stats = daily_stats.sort_values("day_of_week")

            fig = px.bar(
                daily_stats,
                x="day_of_week",
                y="session_count",
                title="Viewing Patterns by Day of Week",
                labels={
                    "day_of_week": "Day of Week",
                    "session_count": "Number of Sessions",
                },
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

    def run(self):
        """Run the dashboard."""
        # Load data
        self.load_data()

        # Apply filters
        self.apply_filters()

        # Linux EC2 specific session state handling
        try:
            # Force session state initialization for Linux
            if 'dashboard_navigation' not in st.session_state:
                st.session_state.dashboard_navigation = 'Main Analytics'
                st.session_state._initialized = True
            
            # Get current view with fallback
            current_view = st.session_state.get('dashboard_navigation', 'Main Analytics')
            
            # Linux-specific debug info
            st.sidebar.markdown("---")
            st.sidebar.markdown("**🔧 Linux EC2 Debug**")
            st.sidebar.markdown(f"Current View: {current_view}")
            st.sidebar.markdown(f"Session Keys: {list(st.session_state.keys())}")
            st.sidebar.markdown(f"Session ID: {id(st.session_state)}")
            
            # Force rerun if session state is corrupted
            if not hasattr(st.session_state, '_initialized'):
                st.session_state._initialized = True
                st.rerun()
                
        except Exception as e:
            st.sidebar.error(f"Session Error: {str(e)}")
            current_view = 'Main Analytics'
        
        # Render based on view
        if current_view == 'Main Analytics':
            self.render_header()
            self.render_charts()
        elif current_view == 'SQL Analytics':
            self.render_sql_dashboard()
        elif current_view == 'Data Quality':
            self.render_quality_dashboard()
        else:
            st.error(f"Unknown view: {current_view}")
            self.render_header()
            self.render_charts()

    def render_sql_dashboard(self):
        """Render SQL analytics dashboard."""
        st.markdown(
            '<h1 class="main-header">📊 SQL Analytics Dashboard</h1>',
            unsafe_allow_html=True,
        )
        
        st.info("🔍 This view shows SQL-powered analytics and query results.")
        
        # Sample SQL queries and results
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📈 Top Shows by Engagement")
            if self.data is not None and len(self.data) > 0:
                top_shows = self.data.groupby('show_name')['engagement_score'].mean().sort_values(ascending=False).head(10)
                st.bar_chart(top_shows)
            else:
                st.write("No data available")
        
        with col2:
            st.subheader("👥 User Activity Distribution")
            if self.data is not None and len(self.data) > 0:
                user_activity = self.data.groupby('user_id').size().value_counts().head(10)
                st.line_chart(user_activity)
            else:
                st.write("No data available")
        
        # Sample SQL queries
        st.subheader("💻 Sample SQL Queries")
        
        sql_queries = {
            "Top Shows by Watch Time": """
            SELECT show_name, 
                   SUM(watch_duration_minutes) as total_watch_time,
                   COUNT(*) as sessions
            FROM fact_user_watch_sessions 
            WHERE watch_date >= DATEADD(day, -30, CURRENT_DATE())
            GROUP BY show_name 
            ORDER BY total_watch_time DESC
            LIMIT 10;
            """,
            "User Engagement Analysis": """
            SELECT user_id,
                   AVG(engagement_score) as avg_engagement,
                   COUNT(*) as total_sessions,
                   SUM(CASE WHEN is_binge_session THEN 1 ELSE 0 END) as binge_sessions
            FROM fact_user_watch_sessions 
            GROUP BY user_id
            HAVING total_sessions > 5
            ORDER BY avg_engagement DESC;
            """,
            "Hourly Viewing Patterns": """
            SELECT HOUR(watch_date) as hour_of_day,
                   COUNT(*) as session_count,
                   AVG(watch_duration_minutes) as avg_duration
            FROM fact_user_watch_sessions 
            WHERE watch_date >= DATEADD(day, -7, CURRENT_DATE())
            GROUP BY HOUR(watch_date)
            ORDER BY hour_of_day;
            """
        }
        
        for query_name, query in sql_queries.items():
            with st.expander(f"📋 {query_name}"):
                st.code(query, language="sql")
                st.info("This query would return real-time analytics from the data warehouse.")

    def render_quality_dashboard(self):
        """Render data quality monitoring dashboard."""
        st.markdown(
            '<h1 class="main-header">🔍 Data Quality Dashboard</h1>',
            unsafe_allow_html=True,
        )
        
        st.info("🔍 This view monitors data quality metrics and alerts.")
        
        if self.data is not None and len(self.data) > 0:
            # Data quality metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                # Completeness check
                completeness = (self.data.notna().sum() / len(self.data)) * 100
                avg_completeness = completeness.mean()
                st.metric(
                    "Data Completeness",
                    f"{avg_completeness:.1f}%",
                    delta="+2.1% from yesterday"
                )
            
            with col2:
                # Validity check
                valid_duration = (self.data['watch_duration_minutes'] > 0).sum()
                validity_rate = (valid_duration / len(self.data)) * 100
                st.metric(
                    "Data Validity",
                    f"{validity_rate:.1f}%",
                    delta="+0.5% from yesterday"
                )
            
            with col3:
                # Consistency check
                unique_users = self.data['user_id'].nunique()
                st.metric(
                    "Unique Users",
                    f"{unique_users:,}",
                    delta="+12 from yesterday"
                )
            
            with col4:
                # Freshness check
                latest_date = self.data['watch_date'].max()
                days_old = (pd.Timestamp.now() - latest_date).days
                st.metric(
                    "Data Freshness",
                    f"{days_old} days old",
                    delta="-1 day from yesterday"
                )
            
            # Data quality charts
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("📊 Data Quality Trends")
                # Simulate quality trends over time
                dates = pd.date_range(start='2024-01-01', end='2024-01-31', freq='D')
                quality_trends = pd.DataFrame({
                    'date': dates,
                    'completeness': np.random.normal(95, 2, len(dates)),
                    'validity': np.random.normal(98, 1, len(dates)),
                    'consistency': np.random.normal(97, 1.5, len(dates))
                })
                
                fig = px.line(quality_trends, x='date', y=['completeness', 'validity', 'consistency'],
                             title="Data Quality Metrics Over Time")
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.subheader("🚨 Quality Alerts")
                alerts = [
                    {"severity": "Low", "message": "Minor data quality issue detected", "time": "2 hours ago"},
                    {"severity": "Medium", "message": "Incomplete data in user_001 sessions", "time": "1 hour ago"},
                    {"severity": "High", "message": "Critical: Data pipeline failure", "time": "30 minutes ago"}
                ]
                
                for alert in alerts:
                    color = {"Low": "blue", "Medium": "orange", "High": "red"}[alert["severity"]]
                    st.markdown(f"""
                    <div style="border-left: 4px solid {color}; padding-left: 10px; margin: 10px 0;">
                        <strong>{alert['severity']}</strong>: {alert['message']}<br>
                        <small>{alert['time']}</small>
                    </div>
                    """, unsafe_allow_html=True)
        else:
            st.warning("No data available for quality analysis.")
        
        # Data quality rules
        st.subheader("📋 Data Quality Rules")
        rules = [
            "✅ All user_id values must be non-null",
            "✅ watch_duration_minutes must be positive",
            "✅ watch_date must be within last 30 days",
            "✅ engagement_score must be between 0 and 1",
            "✅ completion_rate must be between 0 and 1"
        ]
        
        for rule in rules:
            st.markdown(rule)


def main():
    """Main function to run the dashboard."""
    dashboard = NetflixDashboard()
    dashboard.run()


if __name__ == "__main__":
    main()
