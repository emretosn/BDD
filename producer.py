import tweepy
from kafka import KafkaProducer
import logging
import json
from decouple import config

"""API ACCESS KEYS"""
consumerKey = config('egvT4V8OclDFH9lzF6lnMTVy3')
consumerSecret = config('njJJzDu6A0sADYSjVhe2KwF08s445oOm4t1A6qwt6OIphLgzeH')
accessToken = config('1744266282088722432-uuHzANw9WcdcyErnCoguENmG92d8Bj')
accessTokenSecret = config('K3nsyrWYSpANGCIApF967gpcgkv7p4fBEI6yGnCX7u6dD')
bearerToken = config('AAAAAAAAAAAAAAAAAAAAADZUrwEAAAAAdVtQOn44x0jSyTfZOy%2BKJSqonvw%3DhwP9lWXVtrP4dTBTiBrdIggCaythIX2QdbWNGpMQ9aTCdjHLoh')

logging.basicConfig(level=logging.INFO)
producer = KafkaProducer(bootstrap_servers='localhost:9092')
user_to_follow = 'example_user'
topic_name = 'twitter'

def twitterAuth():
    # create the authentication object
    authenticate = tweepy.OAuthHandler(consumerKey, consumerSecret)
    # set the access token and the access token secret
    authenticate.set_access_token(accessToken, accessTokenSecret)
    authenticate.secure = True
    # create the API object
    api = tweepy.API(authenticate, wait_on_rate_limit=True)
    return api

class TweetListener(tweepy.StreamingClient):

    def on_data(self, raw_data):
        logging.info(raw_data)

        tweet = json.loads(raw_data)

        if tweet['data']:
            data = {
                'message': tweet['data']['text'].replace(',', '')
            }
            producer.send(topic_name, value=json.dumps(data).encode('utf-8'))

        return True

    @staticmethod
    def on_error(status_code):
        if status_code == 420:
            # returning False in on_data disconnects the stream
            return False

    def start_streaming_tweets(self, user_to_follow):
        self.add_rules(tweepy.StreamRule(follow=user_to_follow))
        self.filter()

if __name__ == '__main__':
    twitter_stream = TweetListener(bearerToken)
    twitter_stream.start_streaming_tweets(user_to_follow)
