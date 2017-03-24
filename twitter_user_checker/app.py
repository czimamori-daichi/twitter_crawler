from __future__ import print_function

import accounts2 as accounts
import lib.twitter
import boto3
import json
import random
from boto3.dynamodb.conditions import Key, Attr

ACCOUNTS = accounts.accounts
ACCOUNT = random.choice(ACCOUNTS)

CONSUMER_KEY = ACCOUNT['consumer_key']
CONSUMER_SECRET = ACCOUNT['consumer_secret']
ACCESS_TOKEN = ACCOUNT['access_token']
ACCESS_TOKEN_SECRET = ACCOUNT['access_token_secret']

FROM_QUEUE_NAME = 'to_check_users'
TO_QUEUE_NAME = 'to_crawl_followers'

def send_users_to_SQS(users, queue_name):
    #Convert from crawling targets to SQS's entries
    entries = []
    for i, user in enumerate(users):
      message = '{"user_id": %d, "cursor": %d}' % (user['user_id'], user['cursor'])
      entries.append({"Id": str(i), "MessageBody": message})

    print(entries)

    #Send crawl result to SQS
    if entries:
      queue = boto3.resource('sqs').get_queue_by_name(
          QueueName = queue_name,
      )
      response = queue.send_messages(Entries=entries)
      print(response)

def lambda_handler(event, context):
    api = twitter.Api(
        consumer_key=CONSUMER_KEY,
        consumer_secret=CONSUMER_SECRET,
        access_token_key=ACCESS_TOKEN,
        access_token_secret=ACCESS_TOKEN_SECRET,
    )

    # Get users to crawle and cursors from SQS
    from_queue = boto3.resource('sqs').get_queue_by_name(
        QueueName = FROM_QUEUE_NAME,
    )
    messages = from_queue.receive_messages(MaxNumberOfMessages=10)
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
      send_users_to_SQS(to_check_users, FROM_QUEUE_NAME)
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


    send_users_to_SQS(to_check_users, TO_QUEUE_NAME)


    # Delete all messages got from SQS
    for message in messages:
      message.delete()

if __name__ == '__main__':
  lambda_handler({}, 10)

