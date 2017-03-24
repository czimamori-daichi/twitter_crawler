from __future__ import print_function

import accounts
import twitter
import random
import boto3
import json
import time
import threading

ACCOUNTS = accounts.accounts
ACCOUNT = random.choice(ACCOUNTS)

CONSUMER_KEY = ACCOUNT['consumer_key']
CONSUMER_SECRET = ACCOUNT['consumer_secret']
ACCESS_TOKEN = ACCOUNT['access_token']
ACCESS_TOKEN_SECRET = ACCOUNT['access_token_secret']

FROM_QUEUE_NAME = 'to_crawl_followers'
TO_QUEUE_NAME = 'to_check_users'
S3_BUCKET_NAME = 'twitter-crawler'
S3_DIR_NAME = 'followers'

CRAWL_SIZE = 100

to_queue = boto3.resource('sqs').get_queue_by_name(
    QueueName=TO_QUEUE_NAME,
)

from_queue = boto3.resource('sqs').get_queue_by_name(
    QueueName=FROM_QUEUE_NAME,
)

def save_followers_on_s3(user_id, cursor, followers):
    if len(followers) > 0:
      s3 = boto3.resource('s3')
      bucket = s3.Bucket(S3_BUCKET_NAME)
      s3_object_name = S3_DIR_NAME + "/" + str(user_id) + "/" + str(cursor)
      obj = bucket.Object(s3_object_name)

      s3_body = '\n'.join(map(lambda x: "%d,%d" % (user_id, x), followers))

      response = obj.put(
          Body=s3_body,
          ContentEncoding='utf-8',
          ContentType='text/plane'
      )

def convert_followers_to_sqs_entry(targets):
    entries = []
    for i, t in enumerate(targets):
        message = '{"user_id": %d, "cursor": -1}' % t
        entries.append({"Id": str(i), "MessageBody": message})
    return entries

def delete_all_messages(messages):
  for message in messages:
    message.delete()

def chunked(iterable, n):
  return [iterable[x:x + n] for x in range(0, len(iterable), n)]

def send_followers(entries):
    to_queue.send_messages(Entries=entries)

def lambda_handler(event, context):

    api = twitter.Api(
        consumer_key=CONSUMER_KEY,
        consumer_secret=CONSUMER_SECRET,
        access_token_key=ACCESS_TOKEN,
        access_token_secret=ACCESS_TOKEN_SECRET,
    )

    # Get users to crawle and cursors from SQS
    print('Start get users from SQS')
    messages = from_queue.receive_messages(MaxNumberOfMessages=1)

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
      response = api.GetFollowerIDsPaged(user_id=user_id, cursor=cursor, count=CRAWL_SIZE)
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
    entries = convert_followers_to_sqs_entry(targets)

    print(entries[0:2])

    #Send crawl result to SQS
    if next_cursor != 0:
      message = '{"user_id": %d, "cursor": %d}' % (user_id, next_cursor)
      from_queue.send_message(MessageBody=message)

    if len(entries) > 0:
      chunked_entries = chunked(entries, 10)
      threads = []
      for entries in chunked_entries:
        t = threading.Thread(target=send_followers, args=(entries,))
        threads.append(t)
        t.start()
      for t in threads:
        t.join()

    # Save followers on S3
    save_followers_on_s3(user_id, cursor, followers)

    # Delete all messages got from SQS
    delete_all_messages(messages)
   
    return user_id

if __name__ == '__main__':
  #lambda_handler({'user_id': 232838593, 'cursor': 0}, 10)
  #lambda_handler({'user_id': 7080152, 'cursor': 1555516028321571380}, 10)
  lambda_handler({}, 10)

