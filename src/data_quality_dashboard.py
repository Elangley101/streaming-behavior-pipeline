"""
Data Quality Dashboard
Monitors data quality metrics and provides insights into data health.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np
from typing import Dict, List, Any, Optional
import json

# Import monitoring and data lineage modules
from monitoring import log_data_quality_issue
from data_lineage import get_data_lineage, lineage_tracker


class DataQualityMetrics:
    """Calculate and track data quality metrics."""

    def __init__(self):
        self.metrics_history = []

    def calculate_completeness(self, df: pd.DataFrame) -> Dict[str, float]:
        """Calculate completeness metrics for each column."""
        completeness = {}
        for column in df.columns:
            non_null_count = df[column].notna().sum()
            total_count = len(df)
            completeness[column] = (non_null_count / total_count) * 100
        return completeness

    def calculate_accuracy(self, df: pd.DataFrame) -> Dict[str, float]:
        """Calculate accuracy metrics based on business rules."""
        accuracy = {}

        # Check for valid user IDs (should be strings and not empty)
        if "user_id" in df.columns:
            valid_user_ids = df["user_id"].notna() & (df["user_id"] != "")
            accuracy["user_id"] = (valid_user_ids.sum() / len(df)) * 100

        # Check for valid watch durations (should be positive and reasonable)
        if "watch_duration_minutes" in df.columns:
            valid_durations = (df["watch_duration_minutes"] > 0) & (
                df["watch_duration_minutes"] <= 1440
            )
            accuracy["watch_duration_minutes"] = (valid_durations.sum() / len(df)) * 100

        # Check for valid dates (should be in reasonable range)
        if "watch_date" in df.columns:
            current_date = pd.Timestamp.now()
            one_year_ago = current_date - pd.DateOffset(years=1)
            one_year_future = current_date + pd.DateOffset(years=1)

            valid_dates = (df["watch_date"] >= one_year_ago) & (
                df["watch_date"] <= one_year_future
            )
            accuracy["watch_date"] = (valid_dates.sum() / len(df)) * 100

        return accuracy

    def calculate_consistency(self, df: pd.DataFrame) -> Dict[str, float]:
        """Calculate consistency metrics."""
        consistency = {}

        # Check for consistent data types
        if "user_id" in df.columns:
            consistent_user_ids = df["user_id"].apply(lambda x: isinstance(x, str))
            consistency["user_id_type"] = (consistent_user_ids.sum() / len(df)) * 100

        if "watch_duration_minutes" in df.columns:
            consistent_durations = df["watch_duration_minutes"].apply(
                lambda x: isinstance(x, (int, float))
            )
            consistency["watch_duration_type"] = (
                consistent_durations.sum() / len(df)
            ) * 100

        return consistency

    def calculate_uniqueness(self, df: pd.DataFrame) -> Dict[str, float]:
        """Calculate uniqueness metrics."""
        uniqueness = {}

        # Check for duplicate records
        total_records = len(df)
        unique_records = len(df.drop_duplicates())
        uniqueness["overall_uniqueness"] = (unique_records / total_records) * 100

        # Check for duplicate user IDs (if that's expected)
        if "user_id" in df.columns:
            unique_users = df["user_id"].nunique()
            uniqueness["user_uniqueness"] = (unique_users / total_records) * 100

        return uniqueness

    def calculate_timeliness(self, df: pd.DataFrame) -> Dict[str, float]:
        """Calculate timeliness metrics."""
        timeliness = {}

        if "watch_date" in df.columns:
            current_time = pd.Timestamp.now()

            # Check if data is recent (within last 24 hours)
            recent_data = df["watch_date"] >= (current_time - pd.Timedelta(hours=24))
            timeliness["recent_data"] = (recent_data.sum() / len(df)) * 100

            # Check if data is not too old (within last 30 days)
            not_too_old = df["watch_date"] >= (current_time - pd.Timedelta(days=30))
            timeliness["not_too_old"] = (not_too_old.sum() / len(df)) * 100

        return timeliness

    def calculate_overall_quality_score(self, metrics: Dict[str, Any]) -> float:
        """Calculate overall data quality score."""
        scores = []

        # Weight different aspects of data quality
        weights = {
            "completeness": 0.25,
            "accuracy": 0.30,
            "consistency": 0.20,
            "uniqueness": 0.15,
            "timeliness": 0.10,
        }

        for aspect, weight in weights.items():
            if aspect in metrics:
                aspect_scores = list(metrics[aspect].values())
                if aspect_scores:
                    scores.append(np.mean(aspect_scores) * weight)

        return sum(scores) if scores else 0.0

    def analyze_data_quality(
        self, df: pd.DataFrame, dataset_name: str
    ) -> Dict[str, Any]:
        """Perform comprehensive data quality analysis."""
        analysis = {
            "dataset_name": dataset_name,
            "timestamp": datetime.utcnow().isoformat(),
            "total_records": len(df),
            "total_columns": len(df.columns),
            "completeness": self.calculate_completeness(df),
            "accuracy": self.calculate_accuracy(df),
            "consistency": self.calculate_consistency(df),
            "uniqueness": self.calculate_uniqueness(df),
            "timeliness": self.calculate_timeliness(df),
        }

        # Calculate overall score
        analysis["overall_quality_score"] = self.calculate_overall_quality_score(
            analysis
        )

        # Store in history
        self.metrics_history.append(analysis)

        return analysis


class DataQualityDashboard:
    """Streamlit dashboard for data quality monitoring."""

    def __init__(self):
        self.quality_metrics = DataQualityMetrics()
        self.setup_page()

    def setup_page(self):
        """Setup Streamlit page configuration."""
        st.set_page_config(
            page_title="Data Quality Dashboard",
            page_icon="🔍",
            layout="wide",
            initial_sidebar_state="expanded",
        )

        st.title("🔍 Data Quality Dashboard")
        st.markdown("**Monitor and track data quality across your pipeline**")

        # Add navigation sidebar
        self.setup_navigation()

    def setup_navigation(self):
        """Setup navigation to other dashboards."""
        st.sidebar.markdown("---")
        st.sidebar.subheader("📊 Other Dashboards")
        col1, col2 = st.sidebar.columns(2)

        with col1:
            if st.button("🎬 Main", key="nav_main", help="Main analytics dashboard"):
                st.markdown(
                    f'<meta http-equiv="refresh" content="0;url=http://localhost:8501">',
                    unsafe_allow_html=True,
                )
                st.success("Redirecting to Main Dashboard...")

        with col2:
            if st.button("📊 SQL", key="nav_sql", help="SQL-powered analytics"):
                st.markdown(
                    f'<meta http-equiv="refresh" content="0;url=http://localhost:8502">',
                    unsafe_allow_html=True,
                )
                st.success("Redirecting to SQL Dashboard...")

        st.sidebar.markdown("---")
        st.sidebar.markdown("**Current: Data Quality**")
        st.sidebar.markdown("*Pipeline health monitoring*")

    def render_quality_overview(self, analysis: Dict[str, Any]):
        """Render overall quality overview."""
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                "Overall Quality Score",
                f"{analysis['overall_quality_score']:.1f}%",
                delta=(
                    f"{analysis['overall_quality_score'] - 85:.1f}%"
                    if analysis["overall_quality_score"] > 85
                    else f"{analysis['overall_quality_score'] - 85:.1f}%"
                ),
            )

        with col2:
            st.metric(
                "Total Records",
                f"{analysis['total_records']:,}",
                delta="+1,234",  # This would be calculated from history
            )

        with col3:
            st.metric(
                "Data Completeness",
                f"{np.mean(list(analysis['completeness'].values())):.1f}%",
            )

        with col4:
            st.metric(
                "Data Accuracy", f"{np.mean(list(analysis['accuracy'].values())):.1f}%"
            )

    def render_quality_charts(self, analysis: Dict[str, Any]):
        """Render quality metric charts."""
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("📊 Completeness by Column")
            completeness_df = pd.DataFrame(
                [
                    {"Column": col, "Completeness": val}
                    for col, val in analysis["completeness"].items()
                ]
            )

            fig = px.bar(
                completeness_df,
                x="Column",
                y="Completeness",
                title="Data Completeness by Column",
                color="Completeness",
                color_continuous_scale="RdYlGn",
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("🎯 Accuracy by Field")
            accuracy_df = pd.DataFrame(
                [
                    {"Field": field, "Accuracy": val}
                    for field, val in analysis["accuracy"].items()
                ]
            )

            fig = px.pie(
                accuracy_df,
                values="Accuracy",
                names="Field",
                title="Data Accuracy by Field",
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

    def render_quality_trends(self):
        """Render quality trends over time."""
        if len(self.quality_metrics.metrics_history) < 2:
            st.info("Need more data points to show trends")
            return

        st.subheader("📈 Quality Trends Over Time")

        # Create trend data
        trend_data = []
        for analysis in self.quality_metrics.metrics_history[-10:]:  # Last 10 points
            trend_data.append(
                {
                    "timestamp": analysis["timestamp"],
                    "overall_score": analysis["overall_quality_score"],
                    "completeness": np.mean(list(analysis["completeness"].values())),
                    "accuracy": np.mean(list(analysis["accuracy"].values())),
                    "consistency": np.mean(list(analysis["consistency"].values())),
                }
            )

        trend_df = pd.DataFrame(trend_data)
        trend_df["timestamp"] = pd.to_datetime(trend_df["timestamp"])

        fig = px.line(
            trend_df,
            x="timestamp",
            y=["overall_score", "completeness", "accuracy", "consistency"],
            title="Data Quality Trends",
            labels={"value": "Quality Score (%)", "variable": "Metric"},
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    def render_data_issues(self, analysis: Dict[str, Any]):
        """Render data quality issues and recommendations."""
        st.subheader("🚨 Data Quality Issues")

        issues = []
        df = (
            self.quality_metrics.metrics_history[-1]["dataframe"]
            if "dataframe" in self.quality_metrics.metrics_history[-1]
            else None
        )
        if df is None:
            # Fallback: use sample data
            df = self.generate_sample_data()
        # --- Existing checks ---
        # Check for completeness issues
        for col, completeness in analysis["completeness"].items():
            if completeness < 95:
                issues.append(
                    {
                        "type": "Completeness",
                        "field": col,
                        "severity": "High" if completeness < 80 else "Medium",
                        "description": f"Missing {100-completeness:.1f}% of data in {col}",
                        "recommendation": f"Investigate data source for {col} and implement data validation",
                    }
                )
        # Check for accuracy issues
        for field, accuracy in analysis["accuracy"].items():
            if accuracy < 90:
                issues.append(
                    {
                        "type": "Accuracy",
                        "field": field,
                        "severity": "High" if accuracy < 70 else "Medium",
                        "description": f"Data accuracy is {accuracy:.1f}% for {field}",
                        "recommendation": f"Review business rules and data validation for {field}",
                    }
                )
        # Check for consistency issues
        for field, consistency in analysis["consistency"].items():
            if consistency < 95:
                issues.append(
                    {
                        "type": "Consistency",
                        "field": field,
                        "severity": "Medium",
                        "description": f"Data type consistency is {consistency:.1f}% for {field}",
                        "recommendation": f"Standardize data types for {field}",
                    }
                )
        # --- Advanced checks ---
        # Duplicate records
        dupe_count = len(df) - len(df.drop_duplicates())
        if dupe_count > 0:
            issues.append(
                {
                    "type": "Duplicates",
                    "field": "All Columns",
                    "severity": "High" if dupe_count > 10 else "Medium",
                    "description": f"{dupe_count} duplicate records found.",
                    "recommendation": "Remove duplicates and enforce unique constraints.",
                }
            )
        # Invalid completion rates
        if "completion_rate" in df.columns:
            invalid_completion = df[
                (df["completion_rate"] < 0) | (df["completion_rate"] > 1)
            ]
            if not invalid_completion.empty:
                issues.append(
                    {
                        "type": "Range",
                        "field": "completion_rate",
                        "severity": "High",
                        "description": f"{len(invalid_completion)} records with invalid completion_rate (<0 or >1)",
                        "recommendation": "Ensure completion_rate is always between 0 and 1.",
                    }
                )
        # Future dates
        if "watch_date" in df.columns:
            future_dates = df[df["watch_date"] > pd.Timestamp.now()]
            if not future_dates.empty:
                issues.append(
                    {
                        "type": "Date",
                        "field": "watch_date",
                        "severity": "Medium",
                        "description": f"{len(future_dates)} records with watch_date in the future.",
                        "recommendation": "Check data ingestion timestamps.",
                    }
                )
        # Unusual user behavior: >10 sessions/hour
        if "user_id" in df.columns and "watch_date" in df.columns:
            df["watch_hour"] = df["watch_date"].dt.floor("H")
            session_counts = (
                df.groupby(["user_id", "watch_hour"])
                .size()
                .reset_index(name="sessions")
            )
            suspicious = session_counts[session_counts["sessions"] > 10]
            if not suspicious.empty:
                issues.append(
                    {
                        "type": "Anomaly",
                        "field": "user_id",
                        "severity": "Medium",
                        "description": f"{len(suspicious)} user-hour pairs with >10 sessions (possible bot or error)",
                        "recommendation": "Investigate for bot activity or data errors.",
                    }
                )
        # Orphaned shows (if DIM_SHOWS exists)
        # (For demo, just check if show_name is in a list)
        known_shows = set(
            [
                "Stranger Things",
                "The Crown",
                "Wednesday",
                "Bridgerton",
                "The Witcher",
                "Squid Game",
                "Black Mirror",
                "The Umbrella Academy",
                "You",
                "Sex Education",
            ]
        )
        if "show_name" in df.columns:
            orphaned = df[~df["show_name"].isin(known_shows)]
            if not orphaned.empty:
                issues.append(
                    {
                        "type": "Orphaned FK",
                        "field": "show_name",
                        "severity": "Low",
                        "description": f"{len(orphaned)} records with show_name not in DIM_SHOWS.",
                        "recommendation": "Check for referential integrity with DIM_SHOWS.",
                    }
                )

        if issues:
            issues_df = pd.DataFrame(issues)

            # Color code by severity
            def color_severity(val):
                if val == "High":
                    return "background-color: #ffcccc"
                elif val == "Medium":
                    return "background-color: #fff2cc"
                else:
                    return "background-color: #ccffcc"

            st.dataframe(
                issues_df.style.applymap(color_severity, subset=["severity"]),
                use_container_width=True,
            )
        else:
            st.success("✅ No significant data quality issues detected!")

    def render_data_lineage(self, dataset_name: str):
        """Render data lineage information."""
        st.subheader("🔄 Data Lineage")

        lineage = get_data_lineage(dataset_name)
        if lineage:
            col1, col2 = st.columns(2)

            with col1:
                st.write("**Data Sources:**")
                for source in lineage.sources:
                    st.write(f"• {source.name} ({source.type.value})")

            with col2:
                st.write("**Transformations:**")
                for trans in lineage.transformations:
                    st.write(f"• {trans.name} ({trans.type.value})")
        else:
            st.info("No lineage information available for this dataset")

    def run_dashboard(self):
        """Run the data quality dashboard."""
        # Sidebar for controls
        st.sidebar.header("Dashboard Controls")
        # Table selection
        table_options = [
            "USER_WATCH_SESSIONS",
            "FACT_USER_WATCH_SESSIONS",
            "DIM_USERS",
            "DIM_SHOWS",
            "MART_CONTENT_ANALYTICS",
            "MART_USER_ANALYTICS",
        ]
        selected_table = st.sidebar.selectbox(
            "Select Table for Data Quality Analysis", table_options
        )
        # For demo, use sample data; in production, fetch from Snowflake based on selected_table
        sample_data = self.generate_sample_data()
        # Timestamp
        last_run = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        st.markdown(f"**Last Run:** {last_run}")
        # Analyze data quality
        analysis = self.quality_metrics.analyze_data_quality(
            sample_data, selected_table
        )
        # Store dataframe for CSV download
        analysis["dataframe"] = sample_data
        # Render dashboard sections
        self.render_quality_overview(analysis)
        st.markdown("---")
        self.render_quality_charts(analysis)
        st.markdown("---")
        self.render_quality_trends()
        st.markdown("---")
        self.render_data_issues(analysis)
        # Download issues as CSV
        if "dataframe" in analysis:
            issues = []
            # Re-run the same logic as render_data_issues to get issues
            # (For brevity, just use the issues from the last call if available)
            # In a real implementation, refactor to return issues from render_data_issues
            # For now, just allow download of the sample data
            st.download_button(
                label="Download Data as CSV",
                data=analysis["dataframe"].to_csv(index=False),
                file_name=f"{selected_table}_data_quality.csv",
                mime="text/csv",
            )
        st.markdown("---")
        self.render_data_lineage(selected_table)

    def generate_sample_data(self) -> pd.DataFrame:
        """Generate sample data for demonstration."""
        np.random.seed(42)
        n_records = 1000

        # Generate realistic Netflix-like data
        user_ids = [f"user_{i:06d}" for i in range(1, n_records + 1)]
        shows = [
            "Stranger Things",
            "The Crown",
            "Wednesday",
            "Bridgerton",
            "The Witcher",
            "Squid Game",
            "Black Mirror",
            "The Umbrella Academy",
            "You",
            "Sex Education",
        ]

        data = {
            "user_id": np.random.choice(user_ids, n_records),
            "show_name": np.random.choice(shows, n_records),
            "watch_duration_minutes": np.random.normal(45, 15, n_records),
            "watch_date": pd.date_range(
                start="2024-01-01", periods=n_records, freq="H"
            ),
            "engagement_score": np.random.uniform(0, 100, n_records),
            "completion_rate": np.random.uniform(0, 1, n_records),
        }

        # Introduce some data quality issues
        df = pd.DataFrame(data)

        # Add some missing values
        df.loc[np.random.choice(df.index, 50), "user_id"] = None
        df.loc[np.random.choice(df.index, 30), "watch_duration_minutes"] = np.nan

        # Add some invalid values
        df.loc[np.random.choice(df.index, 20), "watch_duration_minutes"] = -10
        df.loc[np.random.choice(df.index, 10), "engagement_score"] = 150

        return df


def main():
    """Main function to run the dashboard."""
    dashboard = DataQualityDashboard()
    dashboard.run_dashboard()


if __name__ == "__main__":
    main()
