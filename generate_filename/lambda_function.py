
import random
from datetime import datetime


def lambda_handler(event, context):
    name = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=20))
    timestr = datetime.strftime(datetime.utcnow(), '%Y%m%d%H%M%SUTC%z')
    return {
        'statusCode': 200,
        'body': '{}_{}'.format(timestr, name)
    }

# calling another lambda: https://www.sqlshack.com/calling-an-aws-lambda-function-from-another-lambda-function/
