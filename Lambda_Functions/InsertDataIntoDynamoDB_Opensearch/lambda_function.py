import json
import boto3
import logging
from botocore.client import Config
import ast
import traceback
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
import requests

def lambda_handler(event, context):

    dynamo = boto3.resource('dynamodb')
    s3_client = boto3.client('s3')
    bucket = "yelprestaurantresults"
    dynamo_table = "yelp-restaurants"
    json_file_name = "yelp_data.json"

    json_object = s3_client.get_object(Bucket=bucket,Key=json_file_name)
    file_readers = json.loads(json_object['Body'].read().decode("utf-8"))
    
    countofcuisine = {}
    countofcuisine["Indian"] = 0
    countofcuisine["Italian"] = 0
    countofcuisine["Chinese"] = 0
    table = dynamo.Table(dynamo_table)

    for file_reader in file_readers:
        cnt = 1
        try:
            #Insert into DynamoDB
            if(countofcuisine[file_reader['cuisine']] < 10):
                countofcuisine[file_reader['cuisine']]+=1
                response_dynamodb = table.put_item(
                    Item = {
                        'id':str(file_reader['id']),
                        'coordinates': str(file_reader['coordinates']),
                        'Cuisine' : str(file_reader['cuisine']),
                        'Country' : str(ast.literal_eval(file_reader['location'])['country']),
                        'City' : str(ast.literal_eval(file_reader['location'])['city']),
                        'State' : str(ast.literal_eval(file_reader['location'])['state']),
                        'Name' : str(file_reader['name']),
                        'Location' : str(file_reader['location']),
                        'Review_count':str(file_reader['review_count']),
                        'Rating':str(file_reader['rating']),
                        'Zip_code':str(ast.literal_eval(file_reader['location'])['zip_code'])
                        }
                )
                logging.info("DynamoDB")
                logging.info(response_dynamodb)
                
                headers = { "Content-Type": "application/json" }
                data = json.dumps({"restaurant":  file_reader['id'], "cuisine": file_reader['cuisine']})
                url ="https://search-restaurants-6sswz6lhlzzdqfto66xrukjhye.us-east-1.es.amazonaws.com/restaurant"
                
                #Insert into OpenSearch
                response_opensearch = requests.put(url+"/_doc/"+str(cnt), data=data, headers = headers, auth=('TestRest', 'TestRest@1'))
                cnt = cnt+1
                
                statusCode = "200"
                message =" Pushed to Dynamo Db + OpenSearch"
                
                logging.info("OpenSearch")
                logging.info(response_opensearch)
            
        except:
            logging.info(traceback.print_exc())
            statusCode = "500"
            message = "Error"
    
    logger.info(countofcuisine)
    return {
        'statusCode': statusCode,
        'body': json.dumps('Pushed to DynamoDB and OpenSearch')
    }
