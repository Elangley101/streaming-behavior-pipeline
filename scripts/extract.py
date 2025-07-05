import pandas as pd
import os

def extract_from_csv(file_path):
    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        df = pd.read_csv(file_path)
        print(f"✅ Extracted {len(df)} rows from {file_path}")
        return df
    except Exception as e:
        print(f"❌ Extraction failed: {e}")
        return None

# Example usage
if __name__ == '__main__':
    df = extract_from_csv('../data/netflix_user_data.csv')
    print(df.head())
