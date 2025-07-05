import pandas as pd

def load_to_parquet(df, output_path):
    try:
        df.to_parquet(output_path, index=False)
        print(f"✅ Data successfully saved to {output_path}")
    except Exception as e:
        print(f"❌ Failed to save data: {e}")

# Example usage
if __name__ == '__main__':
    import extract
    import transform

    raw_df = extract.extract_from_csv('../data/netflix_user_data.csv')
    clean_df = transform.transform_watch_data(raw_df)
    load_to_parquet(clean_df, '../data/cleaned_netflix_data.parquet')
