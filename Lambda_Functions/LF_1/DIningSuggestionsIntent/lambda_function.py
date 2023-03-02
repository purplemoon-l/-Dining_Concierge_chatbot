"""
 This code sample demonstrates an implementation of the Lex Code Hook Interface
 in order to serve a bot which manages dentist appointments.
 Bot, Intent, and Slot models which are compatible with this sample can be found in the Lex Console
 as part of the 'MakeAppointment' template.

 For instructions on how to set up and test this bot, as well as additional samples,
 visit the Lex Getting Started documentation http://docs.aws.amazon.com/lex/latest/dg/getting-started.html.
"""
import json
import dateutil.parser
import datetime
import time
import os
import math
import random
import logging
import boto3
import traceback

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def push_sqs(data):
    sqs_client = boto3.client('sqs')
    sqs_result = {}
    try:
        sqs_client.send_message(
            QueueUrl = "https://sqs.us-east-1.amazonaws.com/819007789055/DiningConcierge_SQS",
            MessageBody = "Details , "+ data['Cuisine']+" , "+data['Location'] + " , "+str(data['Noofppl'])+ " , "+str(data['PhoneNumber'])
        )
        
        sqs_result["sqs_push"] = "Successful"
        return sqs_result
    except:
        logging.info(traceback.print_exc())
        return sqs_result


def close(session_attributes, fulfillment_state, message):
    logging.info('sqs close')
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

    return response




""" --- Intents --- """


def dispatch(intent_request):
    """
    Called when the user specifies an intent for this bot.
    """

    logging.info(intent_request)
    data = intent_request['currentIntent']['slots']
    logging.info('slots after')
    sqs_result = push_sqs(data)
    

    # Dispatch to your bot's intent handlers
    return close(
        intent_request['sessionAttributes'],
        'Fulfilled',
        {'contentType': 'PlainText','content': 'Thank you for choosing us. We will let you know once we have curated a list of restaurants that offer {} dinner for {} in {}, Pushed to sqs status{}'.format(data['Cuisine'],data['Noofppl'],data['Location'],sqs_result["sqs_push"])})


""" --- Main handler --- """


def lambda_handler(event, context):
    """
    Route the incoming request based on intent.
    The JSON body of the request is provided in the event slot.
    """
    # By default, treat the user request as coming from the America/New_York time zone.
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    #logger.debug('event.bot.name={}'.format(event['bot']['name']))

    return dispatch(event)
