
import os
import json
import logging
import urllib

from dotenv import load_dotenv
import telebot

logging.basicConfig(level=logging.INFO)

load_dotenv()

# Telebot API Key
api_key = os.getenv('APIKEY')
bot = telebot.TeleBot(api_key, threaded=False)


def lambda_handler(event, context):
    # getting query
    logging.info(event)
    logging.info(context)
    query = json.loads(event['body'])

    # load data
    chatid = query['chatid']
    content = query['content']
    type = content.get('type', 'text')

    # send notification
    if type == 'text':
        bot.send_message(chatid, content['message'])
        return {
            'statusCode': 200,
            'body': 'Text sent: {}'.format(content['message'])
        }
    elif type == 'picture':
        f = urllib.request.urlopen(content['url'])
        bot.send_photo(chatid, f)
        return {
            'statusCode': 200,
            'body': 'Picture sent.'
        }
    else:
        return {
            'statusCode': 500,
            'body': 'Invalid payload.'
        }
