from flask import Flask, request, jsonify
import praw
import requests
import boto3
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import pandas as pd
from datetime import datetime, timedelta
import io

app = Flask(__name__)

# AWS S3 Configuration
S3_BUCKET = "stock-sentiment-list"
S3_FILE_KEY = "List of Analysed Stocks.xlsx"
AWS_REGION = "us-east-1"

s3_client = boto3.client('s3')

def download_stock_list_from_s3():
    response = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_FILE_KEY)
    return pd.read_excel(io.BytesIO(response['Body'].read()))

stock_list_df = download_stock_list_from_s3()
stocks = [(row["ticker"], row["company"]) for _, row in stock_list_df.iterrows()]

REDDIT_CLIENT_ID = "your_reddit_client_id"
REDDIT_CLIENT_SECRET = "your_reddit_client_secret"
REDDIT_USER_AGENT = "your_reddit_user_agent"

NEWS_API_KEY = "your_newsapi_key"
YOUTUBE_API_KEY = "your_youtube_api_key"

reddit = praw.Reddit(
    client_id=REDDIT_CLIENT_ID,
    client_secret=REDDIT_CLIENT_SECRET,
    user_agent=REDDIT_USER_AGENT,
)

analyzer = SentimentIntensityAnalyzer()

def fetch_reddit_posts(asset):
    subreddit = reddit.subreddit("stocks")
    posts = [post.title for post in subreddit.search(asset, limit=10)]
    return posts

def fetch_news_articles(asset):
    url = f"https://newsapi.org/v2/everything?q={asset}&apiKey={NEWS_API_KEY}"
    response = requests.get(url)
    return [article['title'] for article in response.json().get("articles", [])[:10]]

def fetch_youtube_videos(asset):
    url = f"https://www.googleapis.com/youtube/v3/search?part=snippet&q={asset} stock&key={YOUTUBE_API_KEY}&maxResults=10"
    response = requests.get(url)
    return [video['snippet']['title'] for video in response.json().get("items", [])]

def analyze_sentiment(texts):
    return [(text, analyzer.polarity_scores(text)['compound']) for text in texts]

@app.route('/analyze', methods=['GET'])
def analyze():
    results = []
    for ticker, search_term in stocks:
        reddit_posts = fetch_reddit_posts(search_term)
        news_articles = fetch_news_articles(search_term)
        youtube_videos = fetch_youtube_videos(search_term)

        all_titles = reddit_posts + news_articles + youtube_videos
        sentiment_results = analyze_sentiment(all_titles)

        avg_sentiment = sum([s for _, s in sentiment_results]) / len(sentiment_results) if sentiment_results else 0
        results.append({"ticker": ticker, "asset": search_term, "sentiment": avg_sentiment})

    return jsonify(results)

if __name__ == '__main__':
    app.run(debug=True)
