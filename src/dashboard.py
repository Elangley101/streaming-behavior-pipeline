import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import altair as alt
from datetime import datetime, timedelta
import numpy as np
from pathlib import Path

from src.snowflake_manager import SnowflakeManager
from src.utils import PipelineError
from config.config import PIPELINE_CONFIG

# Page configuration
st.set_page_config(
    page_title="Netflix Analytics Dashboard",
    page_icon="📺",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
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
""", unsafe_allow_html=True)

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
            help="Choose where to load data from"
        )
        
        # Date range filter
        st.sidebar.subheader("📅 Date Range")
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        date_range = st.sidebar.date_input(
            "Select Date Range",
            value=(start_date.date(), end_date.date()),
            max_value=end_date.date()
        )
        
        # User filter
        st.sidebar.subheader("👥 User Filter")
        user_filter = st.sidebar.multiselect(
            "Select Users",
            ["All Users"] + [f"user_{i:04d}" for i in range(1, 101)],
            default=["All Users"]
        )
        
        # Show filter
        st.sidebar.subheader("📺 Show Filter")
        show_filter = st.sidebar.multiselect(
            "Select Shows",
            ["All Shows", "Stranger Things", "The Crown", "Breaking Bad", "Friends"],
            default=["All Shows"]
        )
        
        self.filters = {
            "data_source": data_source,
            "date_range": date_range,
            "user_filter": user_filter,
            "show_filter": show_filter
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
        dates = pd.date_range(start=self.filters["date_range"][0], 
                            end=self.filters["date_range"][1], freq='H')
        
        data = []
        shows = ["Stranger Things", "The Crown", "Breaking Bad", "Friends", "The Office"]
        users = [f"user_{i:04d}" for i in range(1, 101)]
        
        for _ in range(1000):
            data.append({
                "user_id": np.random.choice(users),
                "show_name": np.random.choice(shows),
                "watch_duration_minutes": np.random.normal(45, 15),
                "watch_date": np.random.choice(dates),
                "completion_rate": np.random.beta(2, 1),
                "is_binge_session": np.random.choice([True, False], p=[0.3, 0.7]),
                "engagement_score": np.random.uniform(0, 1)
            })
        
        self.data = pd.DataFrame(data)
        self.data["watch_date"] = pd.to_datetime(self.data["watch_date"])
    
    def load_snowflake_data(self):
        """Load data from Snowflake."""
        try:
            self.snowflake_manager = SnowflakeManager()
            query = """
            SELECT * FROM WATCH_FACTS 
            WHERE watch_date BETWEEN %s AND %s
            """
            self.data = self.snowflake_manager.execute_query(
                query, 
                {"start_date": self.filters["date_range"][0], 
                 "end_date": self.filters["date_range"][1]}
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
                (self.data["watch_date"].dt.date >= self.filters["date_range"][0]) &
                (self.data["watch_date"].dt.date <= self.filters["date_range"][1])
            ]
        
        # User filter
        if "All Users" not in self.filters["user_filter"]:
            self.data = self.data[self.data["user_id"].isin(self.filters["user_filter"])]
        
        # Show filter
        if "All Shows" not in self.filters["show_filter"]:
            self.data = self.data[self.data["show_name"].isin(self.filters["show_filter"])]
    
    def render_header(self):
        """Render the dashboard header."""
        st.markdown('<h1 class="main-header">📺 Netflix Analytics Dashboard</h1>', 
                   unsafe_allow_html=True)
        
        # Key metrics row
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Total Watch Sessions",
                f"{len(self.data):,}" if self.data is not None else "0",
                delta="+12% from last week"
            )
        
        with col2:
            total_hours = (self.data["watch_duration_minutes"].sum() / 60) if self.data is not None else 0
            st.metric(
                "Total Watch Hours",
                f"{total_hours:,.1f}",
                delta="+8% from last week"
            )
        
        with col3:
            avg_engagement = self.data["engagement_score"].mean() if self.data is not None else 0
            st.metric(
                "Avg Engagement Score",
                f"{avg_engagement:.2f}",
                delta="+0.05 from last week"
            )
        
        with col4:
            binge_sessions = self.data["is_binge_session"].sum() if self.data is not None else 0
            st.metric(
                "Binge Sessions",
                f"{binge_sessions:,}",
                delta="+15% from last week"
            )
    
    def render_charts(self):
        """Render interactive charts and visualizations."""
        if self.data is None or len(self.data) == 0:
            st.warning("No data available for the selected filters.")
            return
        
        # Create tabs for different chart types
        tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "👥 User Analytics", "📺 Content Analytics", "⏰ Time Patterns"])
        
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
                labels={"watch_duration_minutes": "Duration (minutes)", "count": "Sessions"}
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
                labels={"engagement_score": "Engagement Score", "count": "Sessions"}
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        # Top shows by watch time
        show_stats = self.data.groupby("show_name").agg({
            "watch_duration_minutes": "sum",
            "user_id": "count"
        }).reset_index()
        show_stats.columns = ["Show", "Total Watch Time (min)", "Sessions"]
        
        fig = px.bar(
            show_stats.head(10),
            x="Show",
            y="Total Watch Time (min)",
            title="Top Shows by Watch Time",
            color="Sessions",
            color_continuous_scale="viridis"
        )
        fig.update_layout(height=400, xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
    
    def render_user_analytics(self):
        """Render user analytics charts."""
        col1, col2 = st.columns(2)
        
        with col1:
            # User engagement ranking
            user_stats = self.data.groupby("user_id").agg({
                "engagement_score": "mean",
                "watch_duration_minutes": "sum",
                "is_binge_session": "sum"
            }).reset_index()
            user_stats = user_stats.sort_values("engagement_score", ascending=False)
            
            fig = px.bar(
                user_stats.head(20),
                x="user_id",
                y="engagement_score",
                title="Top Users by Engagement Score",
                color="watch_duration_minutes",
                color_continuous_scale="plasma"
            )
            fig.update_layout(height=400, xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Binge watching patterns
            binge_stats = self.data.groupby("user_id").agg({
                "is_binge_session": "sum"
            }).reset_index()
            binge_stats["total_sessions"] = self.data.groupby("user_id").size().reset_index(name="total_sessions")["total_sessions"]
            binge_stats["binge_ratio"] = binge_stats["is_binge_session"] / binge_stats["total_sessions"]
            
            fig = px.scatter(
                binge_stats,
                x="user_id",
                y="binge_ratio",
                size="is_binge_session",
                title="Binge Watching Patterns by User",
                labels={"binge_ratio": "Binge Session Ratio", "is_binge_session": "Total Binge Sessions"}
            )
            fig.update_layout(height=400, xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
    
    def render_content_analytics(self):
        """Render content analytics charts."""
        col1, col2 = st.columns(2)
        
        with col1:
            # Show completion rates
            show_completion = self.data.groupby("show_name").agg({
                "completion_rate": "mean"
            }).reset_index()
            show_completion["view_count"] = self.data.groupby("show_name").size().reset_index(name="view_count")["view_count"]
            
            fig = px.scatter(
                show_completion,
                x="completion_rate",
                y="view_count",
                size="view_count",
                hover_data=["show_name"],
                title="Show Performance: Completion Rate vs Views",
                labels={"completion_rate": "Average Completion Rate", "view_count": "Number of Views"}
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Engagement by show
            show_engagement = self.data.groupby("show_name").agg({
                "engagement_score": "mean",
                "watch_duration_minutes": "mean"
            }).reset_index()
            
            fig = px.scatter(
                show_engagement,
                x="engagement_score",
                y="watch_duration_minutes",
                size="engagement_score",
                hover_data=["show_name"],
                title="Show Engagement Analysis",
                labels={"engagement_score": "Average Engagement Score", 
                       "watch_duration_minutes": "Average Watch Duration (min)"}
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    def render_time_patterns(self):
        """Render time-based pattern charts."""
        col1, col2 = st.columns(2)
        
        with col1:
            # Hourly viewing patterns
            self.data["hour"] = self.data["watch_date"].dt.hour
            hourly_stats = self.data.groupby("hour").agg({
                "watch_duration_minutes": "mean"
            }).reset_index()
            hourly_stats["session_count"] = self.data.groupby("hour").size().reset_index(name="session_count")["session_count"]
            
            fig = px.line(
                hourly_stats,
                x="hour",
                y="session_count",
                title="Viewing Patterns by Hour",
                labels={"hour": "Hour of Day", "session_count": "Number of Sessions"}
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Daily viewing patterns
            self.data["day_of_week"] = self.data["watch_date"].dt.day_name()
            daily_stats = self.data.groupby("day_of_week").agg({
                "is_binge_session": "sum"
            }).reset_index()
            daily_stats["session_count"] = self.data.groupby("day_of_week").size().reset_index(name="session_count")["session_count"]
            
            # Reorder days
            day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            daily_stats["day_of_week"] = pd.Categorical(daily_stats["day_of_week"], categories=day_order, ordered=True)
            daily_stats = daily_stats.sort_values("day_of_week")
            
            fig = px.bar(
                daily_stats,
                x="day_of_week",
                y="session_count",
                title="Viewing Patterns by Day of Week",
                labels={"day_of_week": "Day of Week", "session_count": "Number of Sessions"}
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    def run(self):
        """Run the dashboard."""
        # Load data
        self.load_data()
        
        # Apply filters
        self.apply_filters()
        
        # Render dashboard
        self.render_header()
        self.render_charts()

def main():
    """Main function to run the dashboard."""
    dashboard = NetflixDashboard()
    dashboard.run()

if __name__ == "__main__":
    main() 