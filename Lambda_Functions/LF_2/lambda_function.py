import json
import boto3
import math
import random
import logging
import boto3
import traceback
import requests
import ast

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def send_email(emailid, message_to_be_sent, subject):
    ses_client = boto3.client('ses')
    
    logging.info("message_to_be_sent")
    logging.info(type(message_to_be_sent))
    logging.info("subject")
    logging.info(type(subject))
    logging.info("emailid")
    logging.info(type(emailid))
    CHARSET = "UTF-8"
    response_ses = ses_client.send_email(
        Destination={
            "ToAddresses": [
                emailid,
            ],
        },
        Message={
            "Body": {
                "Text": {
                    "Charset": CHARSET,
                    "Data": message_to_be_sent,
                }
            },
            "Subject": {
                "Charset": CHARSET,
                "Data": subject,
            },
        },
        Source="violetorigin1999@gmail.com",
    )
    return response_ses
   
   
def dynamotable_getitem(restaurant_id, cuisine):
    dynamodb_data = {
        'id':  {
            'S':restaurant_id
                },
        'Cuisine': {
            'S':cuisine
                }
        }
            
    logging.info(dynamodb_data)
    client = boto3.client('dynamodb')
    return client.get_item(TableName="yelp-restaurants", Key=dynamodb_data)


def lambda_handler(event, context):
    sqs_client = boto3.client('sqs')
    sqs_result = {}
    try:
        
        #SQS
        sqs_url = 'https://sqs.us-east-1.amazonaws.com/819007789055/DiningConcierge_SQS'
        response_sqs = sqs_client.receive_message(QueueUrl=sqs_url,MaxNumberOfMessages=1)
        logging.info(response_sqs)
        r_handle = response_sqs["Messages"][0]["ReceiptHandle"]
        response_delete_sqs = sqs_client.delete_message(QueueUrl=sqs_url,ReceiptHandle=r_handle)
        _, cuisine, country, ppl, phonenumber = response_sqs["Messages"][0]["Body"].split(" , ")
        print("SQS Processed")
        
        
        #OpenSearch
        headers = { "Content-Type": "application/json" }
        opensearch_url = "https://search-restaurants-6sswz6lhlzzdqfto66xrukjhye.us-east-1.es.amazonaws.com/restaurant"
        response_opensearch = requests.get(opensearch_url+"/_search?q="+cuisine+"&pretty=true", headers = headers, auth=('TestRest', 'TestRest@1'))
        hit_results = response_opensearch.json()["hits"]["hits"]
        logging.info(hit_results)
        logging.info("OPEN SEARCH PROCESSED")
        
        if len(hit_results) > 0:
            ind = random.randrange(len(hit_results))    
            restaurant_id = hit_results[ind]["_source"]["restaurantId"]
            cuisine_opensearch = hit_results[ind]["_source"]["cuisine"]
            #DynamoDB
            response_dynamodb = dynamotable_getitem(restaurant_id, cuisine_opensearch)
            logger.info(response_dynamodb)
            message_to_be_sent = "You can check out "+response_dynamodb["Item"]["Name"]["S"]+" which has a rating of " + response_dynamodb["Item"]["Rating"]["S"] + " in the location " + ast.literal_eval(response_dynamodb["Item"]["Location"]["S"])["address1"]
            logging.info("DYNAMO DB PROCESSED")
            
            #SES
            subject = "Suggested Options"
            
            dynamo = boto3.resource('dynamodb')
            table_session = dynamo.Table("Cookies_session")
            response = table_session.put_item(
            Item = {
                'id':'1',
                'message': response_dynamodb["Item"]["Name"]["S"]+" which has a rating of " + response_dynamodb["Item"]["Rating"]["S"] + " in the location " + ast.literal_eval(response_dynamodb["Item"]["Location"]["S"])["address1"],
            })
            
        else:
            message_to_be_sent = "Sorry we could not find any restaurants that match your taste"
            subject = "No Options"
            
        return send_email(phonenumber,message_to_be_sent,subject)
        

        
    except:
        logging.info(traceback.print_exc())


        
