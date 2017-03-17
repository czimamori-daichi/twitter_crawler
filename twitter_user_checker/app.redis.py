from __future__ import print_function

import accounts2 as accounts
# import tweepy
import twitter
import boto3
import json
import redis

consumer_key = accounts.consumer_key
consumer_secret = accounts.consumer_secret
access_token = accounts.access_token
access_token_secret = accounts.access_token_secret

from_queue_name = 'to_check_users'
to_queue_name = 'to_crawl_followers'

redis_host = 'crawled-users.g80yt7.0001.apne1.cache.amazonaws.com'

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
    
    # Check duplicated users by Redis
    r = redis.Redis(host=redis_host, port=6379, db=0)
    for user in to_check_users:
      user_id = user['user_id']
      if r.hkeys(user_id):
        print("Already crawled")
        print(user_id)
      else:
        r.hset(user_id, "checked", 1)


    #Convert from crawling targets to SQS's entries
    entries = []
    for i, user in enumerate(to_check_users):
      message = '{"user_id": %d, "cursor": %d}' % (user['user_id'], user['cursor'])
      entries.append({"Id": str(i), "MessageBody": message})

    print(entries[0:2])

    #Send crawl result to SQS
    if len(entries) > 0:
      to_queue = boto3.resource('sqs').get_queue_by_name(
          QueueName = to_queue_name,
      )
      response = to_queue.send_messages(Entries=entries)
      print(response)


    # Delete all messages got from SQS
    for message in messages:
      message.delete()

    return user_id

if __name__ == '__main__':
  lambda_handler({}, 10)

