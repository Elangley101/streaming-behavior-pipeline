import pandas as pd

def transform_watch_data(df):
    try:
        # Drop rows with nulls in key columns
        df = df.dropna(subset=['User ID', 'Show Name', 'Watch Duration (min)', 'Watch Date'])

        # Rename columns for consistency
        df = df.rename(columns={
            'User ID': 'user_id',
            'Show Name': 'show_name',
            'Watch Duration (min)': 'watch_duration_minutes',
            'Watch Date': 'watch_date'
        })

        # Convert watch_date to datetime
        df['watch_date'] = pd.to_datetime(df['watch_date'])

        # Feature: Completion rate (assuming 60 min avg episode)
        df['completion_rate'] = (df['watch_duration_minutes'] / 60).clip(upper=1.0)

        # Feature: Is binge session (watching more than 180 mins)
        df['is_binge_session'] = df['watch_duration_minutes'] > 180

        print(f"✅ Transformed dataset with {df.shape[0]} rows and {df.shape[1]} columns")
        return df

    except Exception as e:
        print(f"❌ Transformation failed: {e}")
        return None

# Example usage
if __name__ == '__main__':
    import extract
    raw_df = extract.extract_from_csv('../data/netflix_user_data.csv')
    clean_df = transform_watch_data(raw_df)
    print(clean_df.head())
