from __future__ import print_function

import accounts2 as accounts
# import tweepy
import twitter
import boto3
import json
import redis
import random
from boto3.dynamodb.conditions import Key, Attr

accounts = accounts.accounts
account = random.choice(accounts)

consumer_key = account['consumer_key']
consumer_secret = account['consumer_secret']
access_token = account['access_token']
access_token_secret = account['access_token_secret']

from_queue_name = 'to_check_users'
to_queue_name = 'to_crawl_followers'

redis_host = 'crawled-users.g80yt7.0001.apne1.cache.amazonaws.com'

def send_users_to_SQS(users, queue_name):
    #Convert from crawling targets to SQS's entries
    entries = []
    for i, user in enumerate(users):
      message = '{"user_id": %d, "cursor": %d}' % (user['user_id'], user['cursor'])
      entries.append({"Id": str(i), "MessageBody": message})

    print(entries[0:2])

    #Send crawl result to SQS
    if entries:
      queue = boto3.resource('sqs').get_queue_by_name(
          QueueName = queue_name,
      )
      response = queue.send_messages(Entries=entries)
      print(response)

def lambda_handler(event, context):
    api = twitter.Api(
        consumer_key = consumer_key,
        consumer_secret = consumer_secret,
        access_token_key = access_token,
        access_token_secret = access_token_secret,
    )

    # Get users to crawle and cursors from SQS
    from_queue = boto3.resource('sqs').get_queue_by_name(
        QueueName = from_queue_name,
    )
    messages = from_queue.receive_messages(MaxNumberOfMessages=1)
    message = messages
    to_check_users = []
    for message in messages:
      user_id = json.loads(message.body)['user_id']
      cursor = json.loads(message.body)['cursor']
      user = {'user_id': user_id, 'cursor': cursor}
      to_check_users.append(user)

    print(to_check_users)
    
    # Check whether the user is Japanese or not
    try:
      user_ids = [user['user_id'] for user in to_check_users]
      japanese_user_ids = []
      if user_ids:
        users = api.UsersLookup(user_id=user_ids) 
        japanese_user_ids = [user.id for user in users if user.lang == 'ja']

      print(japanese_user_ids)
      to_check_users = [user for user in to_check_users if user['user_id'] in japanese_user_ids]
    except Exception as e:
      # Return user_id to SQS
      print(e)
      send_users_to_SQS(to_check_users, from_queue_name)
      return
    
    # Check duplicated users by DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('crawled_followers')
    if not to_check_users:
      return
    for user in to_check_users:
      user_id = user['user_id']
      item = table.get_item(
        Key={
          "user_id": user_id
        }
      )
      if item.has_key('Item'):
        print("Already crawled")
        print(user_id)
        print(item)
      else:
        table.put_item(
          Item={
            "user_id": user_id,
          }
        )


    send_users_to_SQS(to_check_users, to_queue_name)


    # Delete all messages got from SQS
    for message in messages:
      message.delete()

if __name__ == '__main__':
  lambda_handler({}, 10)
