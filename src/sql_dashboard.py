"""
Netflix Analytics Dashboard - SQL Version
Leverages DBT models for data transformations
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import logging
from typing import Optional, Dict, Any

# Import SQL manager
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.snowflake_manager import SnowflakeManager

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NetflixSQLDashboard:
    """Netflix Analytics Dashboard using SQL/DBT models."""

    def __init__(self):
        """Initialize the dashboard with SQL connection."""
        try:
            self.snowflake = SnowflakeManager()
            self.snowflake_enabled = getattr(self.snowflake, "enabled", False)
        except Exception as e:
            logger.warning(f"Failed to initialize Snowflake: {str(e)}")
            self.snowflake_enabled = False
            self.snowflake = None

        self.setup_page()

    def setup_page(self):
        """Setup Streamlit page configuration."""
        st.set_page_config(
            page_title="Netflix Analytics - SQL Dashboard",
            page_icon="📊",
            layout="wide",
            initial_sidebar_state="expanded",
        )

        st.title("🎬 Netflix Analytics Dashboard")
        st.markdown("**Powered by DBT & Snowflake**")

        # Add navigation sidebar
        self.setup_navigation()

    def setup_navigation(self):
        """Setup navigation to other dashboards."""
        st.sidebar.markdown("---")
        st.sidebar.subheader("📊 Other Dashboards")
        col1, col2 = st.sidebar.columns(2)

        with col1:
            if st.button("🎬 Main", key="nav_main", help="Main analytics dashboard"):
                # Use JavaScript to get current host and redirect
                st.markdown("""
                <script>
                var currentHost = window.location.hostname;
                var currentPort = window.location.port;
                var mainPort = parseInt(currentPort) - 1;
                var mainUrl = 'http://' + currentHost + ':' + mainPort;
                window.location.href = mainUrl;
                </script>
                """, unsafe_allow_html=True)
                st.success("Redirecting to Main Dashboard...")

        with col2:
            if st.button(
                "🔍 Quality", key="nav_quality", help="Data quality monitoring"
            ):
                # Use JavaScript to get current host and redirect
                st.markdown("""
                <script>
                var currentHost = window.location.hostname;
                var currentPort = window.location.port;
                var qualityPort = parseInt(currentPort) + 1;
                var qualityUrl = 'http://' + currentHost + ':' + qualityPort;
                window.location.href = qualityUrl;
                </script>
                """, unsafe_allow_html=True)
                st.success("Redirecting to Data Quality Dashboard...")

        st.sidebar.markdown("---")
        st.sidebar.markdown("**Current: SQL Dashboard**")
        st.sidebar.markdown("*DBT & Snowflake powered*")

    def run_sql_query(self, query: str) -> pd.DataFrame:
        """Execute SQL query and return DataFrame."""
        if not self.snowflake_enabled:
            st.warning(
                "⚠️ Snowflake is not connected. Running in demo mode with sample data."
            )
            return self._simulate_sql_query(query)

        try:
            # Debug: Print the actual query being sent
            print(f"DEBUG: Executing query: {query}")
            return self.snowflake.execute_query(query)
        except Exception as e:
            st.error(f"SQL Query Error: {str(e)}")
            return pd.DataFrame()

    def _simulate_sql_query(self, query: str) -> pd.DataFrame:
        """Simulate SQL query results using demo data."""
        demo_df = self.get_demo_data()

        # Handle different types of queries
        query_lower = query.lower()

        # Overview metrics query
        if "count(distinct user_id)" in query_lower and "total_users" in query_lower:
            return pd.DataFrame(
                [
                    {
                        "total_users": len(demo_df["user_id"].unique()),
                        "total_sessions": len(demo_df),
                        "total_watch_time": demo_df["watch_duration_minutes"].sum(),
                        "avg_engagement": demo_df["engagement_score"].mean(),
                        "total_binge_sessions": demo_df["is_binge_session"].sum(),
                        "binge_ratio": (
                            demo_df["is_binge_session"].sum() / len(demo_df)
                        )
                        * 100,
                    }
                ]
            )

        # User analytics queries
        elif "mart_user_analytics" in query_lower:
            if "engagement_level" in query_lower and "count(*)" in query_lower:
                # User segments distribution
                return (
                    demo_df.groupby("engagement_level")
                    .size()
                    .reset_index(name="user_count")
                )
            elif "retention_status" in query_lower and "percentage" in query_lower:
                # Retention analysis with percentage calculation
                retention_counts = (
                    demo_df.groupby("retention_status")
                    .size()
                    .reset_index(name="user_count")
                )
                total_users = retention_counts["user_count"].sum()
                retention_counts["percentage"] = (
                    retention_counts["user_count"] / total_users * 100
                ).round(2)
                return retention_counts
            elif "retention_status" in query_lower:
                # Retention analysis without percentage
                return (
                    demo_df.groupby("retention_status")
                    .size()
                    .reset_index(name="user_count")
                )
            else:
                # Top users by engagement
                return demo_df[
                    [
                        "user_id",
                        "avg_engagement_score",
                        "total_watch_time_minutes",
                        "total_sessions",
                        "engagement_level",
                    ]
                ].head(20)

        # Content analytics queries
        elif "mart_content_analytics" in query_lower:
            if "content_stickiness" in query_lower:
                # Content stickiness analysis
                return (
                    demo_df.groupby("content_stickiness")
                    .agg({"show_name": "count", "avg_views_per_viewer": "mean"})
                    .reset_index()
                    .rename(columns={"show_name": "show_count"})
                )
            elif "avg_completion_rate" in query_lower:
                # Show performance scatter plot data
                return demo_df[
                    [
                        "show_name",
                        "avg_engagement_score",
                        "avg_completion_rate",
                        "popularity_category",
                    ]
                ].head(15)
            else:
                # Top shows by popularity
                return demo_df[
                    [
                        "show_name",
                        "total_views",
                        "avg_engagement_score",
                        "popularity_category",
                    ]
                ].head(15)

        # Time patterns queries
        elif "watch_hour" in query_lower:
            # Hourly patterns
            return (
                demo_df.groupby("watch_hour")
                .agg({"user_id": "count", "watch_duration_minutes": "mean"})
                .reset_index()
                .rename(
                    columns={
                        "user_id": "session_count",
                        "watch_duration_minutes": "avg_duration",
                    }
                )
            )

        elif "day_name" in query_lower:
            # Daily patterns
            return (
                demo_df.groupby("day_name")
                .agg({"user_id": "count", "is_binge_session": "sum"})
                .reset_index()
                .rename(
                    columns={
                        "user_id": "session_count",
                        "is_binge_session": "binge_sessions",
                    }
                )
            )

        # Data quality query
        elif "total_records" in query_lower:
            return pd.DataFrame(
                [
                    {
                        "total_records": len(demo_df),
                        "records_last_24h": len(demo_df) // 3,  # Simulate recent data
                        "records_last_7d": len(demo_df) // 2,
                        "earliest_record": demo_df["watch_date"].min(),
                        "latest_record": demo_df["watch_date"].max(),
                    }
                ]
            )

        # Default: return demo data
        return demo_df

    def get_demo_data(self) -> pd.DataFrame:
        """Return demo data when Snowflake is not available."""
        # Create sample data for demo purposes
        import numpy as np
        from datetime import datetime, timedelta

        np.random.seed(42)
        n_users = 100

        # Generate consistent data with same length arrays
        user_ids = [f"user_{i:03d}" for i in range(1, n_users + 1)]
        show_names = np.random.choice([f"Show {i}" for i in range(1, 21)], n_users)
        watch_durations = np.random.exponential(45, n_users)
        engagement_scores = np.random.uniform(60, 95, n_users)
        total_watch_times = np.random.uniform(100, 1000, n_users)
        total_sessions = np.random.poisson(15, n_users)
        engagement_levels = np.random.choice(
            ["Low", "Medium", "High"], n_users, p=[0.2, 0.5, 0.3]
        )
        is_binge_session = np.random.choice([True, False], n_users, p=[0.3, 0.7])
        watch_hours = np.random.randint(0, 24, n_users)
        day_names = np.random.choice(
            [
                "Monday",
                "Tuesday",
                "Wednesday",
                "Thursday",
                "Friday",
                "Saturday",
                "Sunday",
            ],
            n_users,
        )
        watch_dates = [
            datetime.now() - timedelta(days=np.random.randint(1, 30))
            for _ in range(n_users)
        ]

        # Create comprehensive demo data that matches SQL query expectations
        data = {
            "user_id": user_ids,
            "show_name": show_names,
            "watch_duration_minutes": watch_durations,
            "engagement_score": engagement_scores,
            "total_watch_time_minutes": total_watch_times,
            "total_sessions": total_sessions,
            "engagement_level": engagement_levels,
            "is_binge_session": is_binge_session,
            "watch_hour": watch_hours,
            "day_name": day_names,
            "watch_date": watch_dates,
            "avg_engagement_score": engagement_scores,  # For user analytics
            "total_views": np.random.randint(
                10, 1000, n_users
            ),  # For content analytics
            "avg_completion_rate": np.random.uniform(
                0.5, 1.0, n_users
            ),  # For content analytics
            "popularity_category": np.random.choice(
                ["Low", "Medium", "High"], n_users, p=[0.3, 0.5, 0.2]
            ),  # For content analytics
            "retention_status": np.random.choice(
                ["Active", "At Risk", "Churned"], n_users, p=[0.6, 0.3, 0.1]
            ),  # For retention analysis
            "content_stickiness": np.random.choice(
                ["Low", "Medium", "High"], n_users, p=[0.4, 0.4, 0.2]
            ),  # For stickiness analysis
            "avg_views_per_viewer": np.random.uniform(
                1.0, 5.0, n_users
            ),  # For stickiness analysis
        }

        return pd.DataFrame(data)

    def render_overview_metrics(self):
        """Render overview KPI metrics using SQL."""
        st.header("📈 Overview Metrics")

        # Get key metrics from DBT models
        metrics_query = """
        SELECT 
            COUNT(DISTINCT user_id) as total_users,
            COUNT(*) as total_sessions,
            SUM(watch_duration_minutes) as total_watch_time,
            AVG(engagement_score) as avg_engagement,
            SUM(is_binge_session) as total_binge_sessions,
            ROUND(SUM(is_binge_session) * 100.0 / COUNT(*), 2) as binge_ratio
        FROM NETFLIX_ANALYTICS.MARTS.FACT_USER_WATCH_SESSIONS
        """

        metrics_df = self.run_sql_query(metrics_query)

        if not metrics_df.empty:
            col1, col2, col3 = st.columns(3)

            with col1:
                st.metric(
                    "Total Users",
                    f"{metrics_df['total_users'].iloc[0]:,}",
                    help="Unique users who have watched content",
                )
                st.metric(
                    "Total Sessions",
                    f"{metrics_df['total_sessions'].iloc[0]:,}",
                    help="Total watch sessions",
                )

            with col2:
                st.metric(
                    "Total Watch Time",
                    f"{metrics_df['total_watch_time'].iloc[0]:,.0f} min",
                    help="Cumulative watch time in minutes",
                )
                st.metric(
                    "Avg Engagement",
                    f"{metrics_df['avg_engagement'].iloc[0]:.1f}%",
                    help="Average user engagement score",
                )

            with col3:
                st.metric(
                    "Binge Sessions",
                    f"{metrics_df['total_binge_sessions'].iloc[0]:,}",
                    help="Total binge watching sessions",
                )
                st.metric(
                    "Binge Ratio",
                    f"{metrics_df['binge_ratio'].iloc[0]:.1f}%",
                    help="Percentage of binge sessions",
                )

    def render_user_analytics(self):
        """Render user analytics using SQL."""
        st.header("👥 User Analytics")

        col1, col2 = st.columns(2)

        with col1:
            # Top users by engagement
            top_users_query = """
            SELECT 
                user_id,
                avg_engagement_score,
                total_watch_time_minutes,
                total_sessions,
                engagement_level
            FROM NETFLIX_ANALYTICS.MARTS.MART_USER_ANALYTICS
            ORDER BY avg_engagement_score DESC
            LIMIT 20
            """

            top_users_df = self.run_sql_query(top_users_query)

            if not top_users_df.empty:
                fig = px.bar(
                    top_users_df,
                    x="user_id",
                    y="avg_engagement_score",
                    color="total_watch_time_minutes",
                    title="Top Users by Engagement Score",
                    color_continuous_scale="plasma",
                )
                fig.update_layout(height=400, xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            # User segments distribution
            segments_query = """
            SELECT 
                engagement_level,
                COUNT(*) as user_count
            FROM NETFLIX_ANALYTICS.MARTS.MART_USER_ANALYTICS
            GROUP BY engagement_level
            """

            segments_df = self.run_sql_query(segments_query)

            if not segments_df.empty:
                fig = px.pie(
                    segments_df,
                    values="user_count",
                    names="engagement_level",
                    title="User Engagement Distribution",
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)

    def render_content_analytics(self):
        """Render content analytics using SQL."""
        st.header("🎭 Content Analytics")

        col1, col2 = st.columns(2)

        with col1:
            # Top shows by popularity
            top_shows_query = """
            SELECT 
                show_name,
                total_views,
                avg_engagement_score,
                popularity_category
            FROM NETFLIX_ANALYTICS.MARTS.MART_CONTENT_ANALYTICS
            ORDER BY total_views DESC
            LIMIT 15
            """

            top_shows_df = self.run_sql_query(top_shows_query)

            if not top_shows_df.empty:
                fig = px.bar(
                    top_shows_df,
                    x="show_name",
                    y="total_views",
                    color="avg_engagement_score",
                    title="Top Shows by Views",
                    color_continuous_scale="viridis",
                )
                fig.update_layout(height=400, xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Show performance scatter
            performance_query = """
            SELECT 
                show_name,
                avg_engagement_score,
                avg_completion_rate,
                popularity_category
            FROM NETFLIX_ANALYTICS.MARTS.MART_CONTENT_ANALYTICS
            """

            performance_df = self.run_sql_query(performance_query)

            if not performance_df.empty:
                fig = px.scatter(
                    performance_df,
                    x="avg_engagement_score",
                    y="avg_completion_rate",
                    size="avg_engagement_score",
                    hover_data=["show_name"],
                    color="popularity_category",
                    title="Show Performance: Engagement vs Completion",
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)

    def render_time_patterns(self):
        """Render time-based patterns using SQL."""
        st.header("⏰ Time Patterns")

        col1, col2 = st.columns(2)

        with col1:
            # Hourly patterns
            hourly_query = """
            SELECT 
                watch_hour,
                COUNT(*) as session_count,
                AVG(watch_duration_minutes) as avg_duration
            FROM NETFLIX_ANALYTICS.MARTS.FACT_USER_WATCH_SESSIONS
            GROUP BY watch_hour
            ORDER BY watch_hour
            """

            hourly_df = self.run_sql_query(hourly_query)

            if not hourly_df.empty:
                fig = px.line(
                    hourly_df,
                    x="watch_hour",
                    y="session_count",
                    title="Viewing Patterns by Hour",
                    labels={"watch_hour": "Hour of Day", "session_count": "Sessions"},
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Daily patterns
            daily_query = """
            SELECT 
                day_name,
                COUNT(*) as session_count,
                SUM(is_binge_session) as binge_sessions
            FROM NETFLIX_ANALYTICS.MARTS.FACT_USER_WATCH_SESSIONS
            GROUP BY day_name
            ORDER BY 
                CASE day_name
                    WHEN 'Monday' THEN 1
                    WHEN 'Tuesday' THEN 2
                    WHEN 'Wednesday' THEN 3
                    WHEN 'Thursday' THEN 4
                    WHEN 'Friday' THEN 5
                    WHEN 'Saturday' THEN 6
                    WHEN 'Sunday' THEN 7
                END
            """

            daily_df = self.run_sql_query(daily_query)

            if not daily_df.empty:
                fig = px.bar(
                    daily_df,
                    x="day_name",
                    y="session_count",
                    title="Viewing Patterns by Day of Week",
                    labels={"day_name": "Day of Week", "session_count": "Sessions"},
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)

    def render_advanced_analytics(self):
        """Render advanced analytics using SQL."""
        st.header("🔍 Advanced Analytics")

        # User retention analysis
        retention_query = """
        SELECT 
            retention_status,
            COUNT(*) as user_count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
        FROM NETFLIX_ANALYTICS.MARTS.MART_USER_ANALYTICS
        GROUP BY retention_status
        """

        retention_df = self.run_sql_query(retention_query)

        if not retention_df.empty:
            col1, col2 = st.columns(2)

            with col1:
                fig = px.pie(
                    retention_df,
                    values="user_count",
                    names="retention_status",
                    title="User Retention Status",
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                st.subheader("Retention Metrics")
                for _, row in retention_df.iterrows():
                    st.metric(
                        row["retention_status"],
                        f"{row['user_count']:,} users ({row['percentage']}%)",
                    )

        # Content stickiness analysis
        stickiness_query = """
        SELECT 
            content_stickiness,
            COUNT(*) as show_count,
            AVG(avg_views_per_viewer) as avg_views_per_viewer
        FROM NETFLIX_ANALYTICS.MARTS.MART_CONTENT_ANALYTICS
        GROUP BY content_stickiness
        """

        stickiness_df = self.run_sql_query(stickiness_query)

        if not stickiness_df.empty:
            st.subheader("Content Stickiness Analysis")
            fig = px.bar(
                stickiness_df,
                x="content_stickiness",
                y="show_count",
                color="avg_views_per_viewer",
                title="Content Stickiness Distribution",
                color_continuous_scale="plasma",
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

    def render_data_quality(self):
        """Render data quality metrics."""
        st.header("✅ Data Quality")

        # Data freshness
        freshness_query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN watch_date >= DATEADD('day', -1, CURRENT_DATE()) THEN 1 END) as records_last_24h,
            COUNT(CASE WHEN watch_date >= DATEADD('day', -7, CURRENT_DATE()) THEN 1 END) as records_last_7d,
            MIN(watch_date) as earliest_record,
            MAX(watch_date) as latest_record
        FROM NETFLIX_ANALYTICS.MARTS.FACT_USER_WATCH_SESSIONS
        """

        freshness_df = self.run_sql_query(freshness_query)

        if not freshness_df.empty:
            col1, col2, col3 = st.columns(3)

            with col1:
                st.metric("Total Records", f"{freshness_df['total_records'].iloc[0]:,}")

            with col2:
                st.metric("Last 24h", f"{freshness_df['records_last_24h'].iloc[0]:,}")

            with col3:
                st.metric("Last 7 Days", f"{freshness_df['records_last_7d'].iloc[0]:,}")

            st.info(
                f"Data Range: {freshness_df['earliest_record'].iloc[0]} to {freshness_df['latest_record'].iloc[0]}"
            )

    def run(self):
        """Run the SQL-based dashboard."""
        try:
            # Render dashboard sections
            self.render_overview_metrics()
            self.render_user_analytics()
            self.render_content_analytics()
            self.render_time_patterns()
            self.render_advanced_analytics()
            self.render_data_quality()

        except Exception as e:
            st.error(f"Dashboard Error: {str(e)}")
            logger.error(f"Dashboard error: {str(e)}")
        finally:
            # Close connection safely
            if hasattr(self, "snowflake") and self.snowflake is not None:
                try:
                    self.snowflake.close()
                except Exception as e:
                    logger.warning(f"Error closing Snowflake connection: {str(e)}")


def main():
    """Main function to run the SQL dashboard."""
    dashboard = NetflixSQLDashboard()
    dashboard.run()


if __name__ == "__main__":
    main()
