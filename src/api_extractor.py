import requests
import pandas as pd
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import time
import json

logger = logging.getLogger(__name__)

class APIExtractor:
    def __init__(self, base_url: str, api_key: Optional[str] = None):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.session = requests.Session()
        
        if api_key:
            self.session.headers.update({'Authorization': f'Bearer {api_key}'})
        
        self.session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': 'Streamlytics-Pipeline/1.0'
        })
    
    def fetch_streaming_data(self, endpoint: str = '/streaming-data', 
                           params: Optional[Dict] = None) -> pd.DataFrame:
        try:
            url = f"{self.base_url}{endpoint}"
            logger.info(f"Fetching data from: {url}")
            
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if isinstance(data, dict) and 'data' in data:
                records = data['data']
            elif isinstance(data, list):
                records = data
            else:
                records = [data]
            
            df = pd.DataFrame(records)
            logger.info(f"Successfully fetched {len(df)} records from API")
            
            return df
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Error processing API response: {e}")
            raise
    
    def fetch_mock_streaming_data(self, num_records: int = 100) -> pd.DataFrame:
        import random
        
        platforms = ['Netflix', 'Disney+', 'Hulu', 'Amazon Prime', 'HBO Max', 'Peacock']
        content_types = ['Movie', 'TV Show', 'Documentary', 'Stand-up Comedy', 'Reality TV']
        genres = ['Action', 'Comedy', 'Drama', 'Horror', 'Sci-Fi', 'Romance', 'Thriller', 'Documentary']
        
        data = []
        base_time = datetime.now() - timedelta(days=30)
        
        for i in range(num_records):
            user_id = f"user_{random.randint(1, 1000):04d}"
            platform = random.choice(platforms)
            content_type = random.choice(content_types)
            genre = random.choice(genres)
            
            watch_duration = random.randint(15, 240)
            
            hour = random.choices(
                range(24), 
                weights=[0.5, 0.3, 0.2, 0.1, 0.1, 0.1, 0.2, 0.3, 0.5, 0.7, 0.8, 0.9, 
                        1.0, 1.0, 0.9, 0.8, 0.7, 0.6, 0.8, 1.0, 1.2, 1.3, 1.1, 0.8]
            )[0]
            
            watch_time = base_time + timedelta(
                days=random.randint(0, 30),
                hours=hour,
                minutes=random.randint(0, 59)
            )
            
            completion_rate = min(watch_duration / 60, 1.0)
            is_binge = watch_duration > 180
            
            record = {
                'user_id': user_id,
                'platform': platform,
                'content_type': content_type,
                'genre': genre,
                'watch_duration_minutes': watch_duration,
                'watch_date': watch_time.strftime('%Y-%m-%d %H:%M:%S'),
                'completion_rate': round(completion_rate, 2),
                'is_binge_session': is_binge,
                'engagement_score': round(random.uniform(0.1, 1.0), 2)
            }
            
            data.append(record)
        
        df = pd.DataFrame(data)
        logger.info(f"Generated {len(df)} mock streaming records")
        
        return df
    
    def fetch_from_public_api(self) -> pd.DataFrame:
        try:
            time.sleep(0.5)
            
            if random.random() < 0.1:
                raise requests.exceptions.RequestException("Simulated API failure")
            
            return self.fetch_mock_streaming_data(num_records=50)
            
        except Exception as e:
            logger.warning(f"Public API call failed, falling back to mock data: {e}")
            return self.fetch_mock_streaming_data(num_records=50)
    
    def stream_data_incremental(self, endpoint: str, 
                               batch_size: int = 100,
                               delay: float = 1.0) -> pd.DataFrame:
        all_data = []
        page = 1
        
        while True:
            try:
                params = {
                    'page': page,
                    'limit': batch_size,
                    'timestamp': datetime.now().isoformat()
                }
                
                batch_df = self.fetch_streaming_data(endpoint, params)
                
                if batch_df.empty:
                    break
                
                all_data.append(batch_df)
                logger.info(f"Fetched batch {page} with {len(batch_df)} records")
                
                page += 1
                time.sleep(delay)
                
                if page > 10:
                    break
                    
            except Exception as e:
                logger.error(f"Error fetching batch {page}: {e}")
                break
        
        if all_data:
            return pd.concat(all_data, ignore_index=True)
        else:
            return pd.DataFrame()

if __name__ == "__main__":
    extractor = APIExtractor("https://api.example.com")
    
    df = extractor.fetch_mock_streaming_data(100)
    print(f"Fetched {len(df)} records")
    print(df.head())
    
    df.to_csv('data/raw/api_streaming_data.csv', index=False)
    print("Data saved to data/raw/api_streaming_data.csv") 