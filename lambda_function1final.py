import praw
import requests
import boto3
import os
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import pandas as pd
from datetime import datetime, timedelta
import io

# Ensure required libraries are available
try:
    import openpyxl  # Required for reading Excel files
except ImportError:
    print("Missing 'openpyxl' module. Install it using 'pip install openpyxl'.")
    exit(1)

# AWS Credentials from Environment Variables
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")  # Default to eu-north-1 if not set

# Debugging: Print AWS Credentials to logs
print("Checking AWS Credentials...")
print(f"AWS Access Key: {'SET' if AWS_ACCESS_KEY_ID else 'NOT SET'}")
print(f"AWS Secret Key: {'SET' if AWS_SECRET_ACCESS_KEY else 'NOT SET'}")
print(f"AWS Region: {AWS_REGION}")

# Ensure AWS credentials exist before proceeding
if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
    print("ERROR: AWS credentials are missing. Exiting.")
    exit(1)

# AWS S3 Configuration
S3_BUCKET = "stock-sentiment-list"  # Change to your actual S3 bucket name
S3_FILE_KEY = "List of Analysed Stocks.xlsx"

# Initialize S3 Client
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

def download_stock_list_from_s3():
    """Download stock list from S3 bucket and return as DataFrame."""
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_FILE_KEY)
        return pd.read_excel(io.BytesIO(response['Body'].read()), engine="openpyxl")
    except Exception as e:
        print(f"Error downloading stock list from S3: {e}")
        exit(1)

# Load stock list from S3
stock_list_df = download_stock_list_from_s3()
stocks = [(row["ticker"], row["company"]) for _, row in stock_list_df.iterrows()]

# Reddit API Credentials
REDDIT_CLIENT_ID = "iD4oybpiOZkLmYh4foX5vA"
REDDIT_CLIENT_SECRET = "r9p34KaFw3yUL0nxin16bkGqBc_O_A"
REDDIT_USER_AGENT = "TestBot7 by Impressive_Plate3586"

# NewsAPI Key
NEWS_API_KEY = "8affd82647134b6f81048c1c4a876319"

# YouTube API Key
YOUTUBE_API_KEY = "AIzaSyAObKmyVWJg9eTY2hJxYn8AfK5iu3iUAz4"

# Initialize Reddit API
reddit = praw.Reddit(
    client_id=REDDIT_CLIENT_ID,
    client_secret=REDDIT_CLIENT_SECRET,
    user_agent=REDDIT_USER_AGENT,
)

# Sentiment Analyzer
analyzer = SentimentIntensityAnalyzer()

def fetch_reddit_posts(asset):
    try:
        subreddit = reddit.subreddit("stocks")
        posts = [post.title for post in subreddit.search(asset, limit=10)]
        return posts
    except Exception as e:
        print(f"Error fetching Reddit posts: {e}")
        return []

def fetch_news_articles(asset):
    url = f"https://newsapi.org/v2/everything?q={asset}&apiKey={NEWS_API_KEY}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return [article['title'] for article in response.json().get("articles", [])[:10]]
        return []
    except Exception as e:
        print(f"Error: {e}")
        return []

def fetch_youtube_videos(asset):
    url = f"https://www.googleapis.com/youtube/v3/search?part=snippet&q={asset} stock&key={YOUTUBE_API_KEY}&maxResults=10"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return [video['snippet']['title'] for video in response.json().get("items", [])]
        return []
    except Exception as e:
        print(f"Error: {e}")
        return []

def analyze_sentiment(texts):
    return [(text, analyzer.polarity_scores(text)['compound']) for text in texts]

def save_to_s3(filename, df):
    try:
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        
        print(f"Uploading {filename} to S3 bucket {S3_BUCKET}...")  # Debug print
        s3_client.put_object(Bucket=S3_BUCKET, Key=filename, Body=csv_buffer.getvalue())

        print(f"✅ Successfully uploaded {filename} to S3.")
    except Exception as e:
        print(f"❌ Error uploading {filename}: {e}")

def save_to_s3(filename, df):
    """Save DataFrame to S3 as a CSV file."""
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    try:
        s3_client.put_object(Bucket=S3_BUCKET, Key=filename, Body=csv_buffer.getvalue())
        print(f"File saved successfully: {filename}")
    except Exception as e:
        print(f"Error saving to S3: {e}")

def lambda_handler(event, context):
    """AWS Lambda entry point."""
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=24)
    timestamp_range = f"{start_time.strftime('%Y-%m-%d %H:%M UTC')} to {end_time.strftime('%Y-%m-%d %H:%M UTC')}"
    
    all_sentiments, daily_averages, extreme_sentiments = [], [], []
    
    for ticker, search_term in stocks:
        reddit_posts = fetch_reddit_posts(search_term)
        news_articles = fetch_news_articles(search_term)
        youtube_videos = fetch_youtube_videos(search_term)
        
        all_titles = reddit_posts + news_articles + youtube_videos
        sentiment_results = analyze_sentiment(all_titles)
        avg_sentiment = sum([s for _, s in sentiment_results]) / len(sentiment_results) if sentiment_results else 0
        
        for title, sentiment in sentiment_results:
            all_sentiments.append((ticker, search_term, title, sentiment, timestamp_range))
        
        daily_averages.append((ticker, search_term, avg_sentiment, timestamp_range))
        
        if abs(avg_sentiment) > 0.10:  # Extreme threshold at 10%
            extreme_sentiments.append((ticker, search_term, avg_sentiment, timestamp_range))
    
    all_sentiments_df = pd.DataFrame(all_sentiments, columns=["Ticker", "Asset", "Title", "Sentiment", "Timestamp"])
    daily_averages_df = pd.DataFrame(daily_averages, columns=["Ticker", "Asset", "Sentiment", "Timestamp"])
    extreme_sentiments_df = pd.DataFrame(extreme_sentiments, columns=["Ticker", "Asset", "Sentiment", "Timestamp"])
    
    daily_averages_df = daily_averages_df.sort_values(by="Sentiment", ascending=False)
    extreme_sentiments_df = extreme_sentiments_df.sort_values(by="Sentiment", ascending=False)
    
    current_date = datetime.now().strftime('%Y-%m-%d')
    save_to_s3(f"extreme_sentiment_{current_date}.csv", extreme_sentiments_df)
    save_to_s3(f"daily_avg_sentiment_{current_date}.csv", daily_averages_df)
    save_to_s3(f"detailed_sentiment_{current_date}.csv", all_sentiments_df)
    
    return {
        'statusCode': 200,
        'body': "Sentiment analysis completed successfully!"
    }
def test_s3_upload():
    """Test function to upload a small test file to S3."""
    test_filename = "test_upload.txt"
    test_content = "This is a test file to verify S3 upload permissions."

    try:
        s3_client.put_object(Bucket=S3_BUCKET, Key=test_filename, Body=test_content)
        print(f"✅ Uploaded {test_filename} to {S3_BUCKET}")
    except Exception as e:
        print(f"❌ S3 Upload Failed: {e}")

# Run test
test_s3_upload()
