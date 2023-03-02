import json

import json
import boto3
import datetime
sessionAttributes = {}


def lambda_handler(event, context):

    msg = event['messages'][0]['unstructured']['text']
    lex_return = pushChat(msg, sessionAttributes);

    response = {
        'statusCode': 200,
        'messages' : [{
            'type': 'unstructured',
            'unstructured': {
                'text':lex_return['message']
            }
        }]
    };
    return response;
    
    
def pushChat(msg, sessionAttributes):
    client = boto3.client('lex-runtime')
    response = client.post_text(
        botName='DiningConcierge',
        botAlias='DC',
        userId='hw1cc24thfeb',
        inputText=msg
        )

    return response
    