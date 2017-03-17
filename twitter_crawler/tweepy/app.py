from __future__ import print_function

import accounts2 as accounts
# import tweepy
import twitter
import boto3
import json

consumer_key = accounts.consumer_key
consumer_secret = accounts.consumer_secret
access_token = accounts.access_token
access_token_secret = accounts.access_token_secret

from_queue_name = 'to_crawl_followers'
to_queue_name = 'to_crawl_followers'

def lambda_handler(event, context):
    print(event)
# auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
# auth.set_access_token(access_token, access_token_secret)
# api = tweepy.API(auth)

    api = twitter.Api(
        consumer_key = consumer_key,
        consumer_secret = consumer_secret,
        access_token_key = access_token,
        access_token_secret = access_token_secret,
    )

    # SQS からクロールすべきユーザとカーソルを取得
    from_queue = boto3.resource('sqs').get_queue_by_name(
        QueueName = from_queue_name,
    )
    messages = from_queue.receive_messages(MaxNumberOfMessages=1)
    message = messages[0]
    print(json.loads(message.body))
    user_id = json.loads(message.body)['user_id']
    cursor = json.loads(message.body)['cursor']
    print("user_id:")
    print(user_id)
    print("cursor:")
    print(cursor)
    

    response = api.GetFollowerIDsPaged(user_id=user_id,cursor=cursor)
    next_cursor = response[0]
    followers = response[2]
    print("follower size: ")
    print(len(followers))
    print(next_cursor)

    targets = followers
    if next_cursor != 0:
      targets.append(user_id)
    followers_data = [{"user_id": follower, "cursor": -1} for follower in followers]

    print(followers_data[0:2])

    # クロール結果を SQS に格納
    to_queue = boto3.resource('sqs').get_queue_by_name(
        QueueName = to_queue_name,
    )
    response = queue.send_messages(Entries=targets)
   
    return user_id

if __name__ == '__main__':
  #lambda_handler({'user_id': 232838593, 'cursor': 0}, 10)
  #lambda_handler({'user_id': 7080152, 'cursor': 1555516028321571380}, 10)
  lambda_handler({}, 10)

