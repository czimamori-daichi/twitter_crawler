from __future__ import print_function

import accounts2 as accounts
# import tweepy
import twitter
import random
import boto3
import json
from multiprocessing import Process

accounts = accounts.accounts
account = random.choice(accounts)

consumer_key = account['consumer_key']
consumer_secret = account['consumer_secret']
access_token = account['access_token']
access_token_secret = account['access_token_secret']

from_queue_name = 'to_crawl_followers'
to_queue_name = 'to_check_users'
s3_bucket_name = 'twitter-crawler'
s3_dir_name = 'followers'


to_queue = boto3.resource('sqs').get_queue_by_name(
    QueueName = to_queue_name,
)

from_queue = boto3.resource('sqs').get_queue_by_name(
    QueueName = from_queue_name,
)

def delete_all_messages(messages):
  for message in messages:
    message.delete()


def chunked(iterable, n):
  return [iterable[x:x + n] for x in range(0, len(iterable), n)]

def send_followers(entries):
    to_queue.send_messages(Entries=entries)

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
    print('Start get users from SQS')
    messages = from_queue.receive_messages(MaxNumberOfMessages=1)
    print('End get users from SQS')

    if len(messages) == 0:
      print("SQS is empty.")
      return
    message = messages[0]
    print(message.body)
    print(json.loads(message.body))
    user_id = json.loads(message.body)['user_id']
    cursor = json.loads(message.body)['cursor']
    user = {'user_id': int(user_id), 'cursor': int(cursor)}
    print("user_id:")
    print(user_id)
    print("cursor:")
    print(cursor)
    

    # Twitter API for crawling followers
    try:
      response = api.GetFollowerIDsPaged(user_id=user_id, cursor=cursor)
      next_cursor = response[0]
      followers = response[2]
      print("follower size: ")
      print(len(followers))
      print(next_cursor)
    except Exception as e:
      # Return user_id to SQS
      print('=========')
      if "Not authorized." in str(e):
        print("Not authrorized exception")
        delete_all_messages(messages)
      print(e)
      print(type(e))
      print('=========')
      return

    #Convert from crawling targets to SQS's entries
    targets = followers[:]
    if next_cursor != 0:
      targets.append(user_id)
    entries = []
    for i, t in enumerate(targets):
      message = '{"user_id": %d, "cursor": -1}' % t
      entries.append({"Id": str(i), "MessageBody": message})

    print(entries[0:2])

    #Send crawl result to SQS
    if len(entries) > 0:
      chunked_entries = chunked(entries, 10)
      for entries in chunked_entries:
        p = Process(target=send_followers, args=(entries,))
        p.start()
      if p:
        p.join()

    # Save followers on S3
    if len(followers) > 0:
      s3 = boto3.resource('s3')
      bucket = s3.Bucket(s3_bucket_name)
      s3_object_name = s3_dir_name + "/" + str(user_id) + "/" + str(cursor)
      obj = bucket.Object(s3_object_name)

      s3_body = '\n'.join(map(lambda x: "%d,%d" % (user_id, x), followers))

      response = obj.put(
          Body=s3_body,
          ContentEncoding='utf-8',
          ContentType='text/plane'
      )

    # Delete all messages got from SQS
    delete_all_messages(messages)
   
    return user_id

if __name__ == '__main__':
  #lambda_handler({'user_id': 232838593, 'cursor': 0}, 10)
  #lambda_handler({'user_id': 7080152, 'cursor': 1555516028321571380}, 10)
  lambda_handler({}, 10)

