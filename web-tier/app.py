from flask import Flask, request
import boto3
import uuid
import time
from collections import defaultdict

app = Flask(__name__)

ASU_ID = '1231868809'
input_bucket = f"{ASU_ID}-in-bucket"
request_queue_url = f'https://sqs.us-east-1.amazonaws.com/967211778521/1231868809-req-queue'
response_queue_url = f'https://sqs.us-east-1.amazonaws.com/967211778521/1231868809-resp-queue'

sqs = boto3.client('sqs')
s3 = boto3.client('s3')

request_queue_name = f'{ASU_ID}-req-queue'
response_queue_name = f'{ASU_ID}-resp-queue'
responses = defaultdict(str)

def upload_to_s3(bucket, key, data):
    """Upload data to S3."""
    s3.put_object(Bucket=bucket, Key=key, Body=data)
    
@app.route('/', methods=['POST'])
def handle_image_upload():
    if 'inputFile' not in request.files:
        return "No inputFile part in the request", 400
    
    file = request.files['inputFile']
    if file.filename == '':
        return "No file selected", 400

    filename = file.filename

    request_id = str(uuid.uuid4())
    print("filename: " + filename)

    try:
        upload_to_s3(input_bucket, filename, file)
        print(f"File uploaded to S3: {filename}")
    except Exception as e:
        return f"Failed to upload file to S3: {e}", 500
    
    sqs.send_message(
        QueueUrl=request_queue_url,
        MessageBody=request_id,
        MessageAttributes={
            'filename': {
                'StringValue': filename,
                'DataType': 'String'
            }
        }
    )
    
    while True:
        response = sqs.receive_message(
            QueueUrl=response_queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=5,
            MessageAttributeNames=['All']
        )
        fname = ''
        response_body = ''
        if 'Messages' in response:
            message = response['Messages'][0]
            receipt_handle = message['ReceiptHandle']
            response_body = message['Body']

            if 'MessageAttributes' in message:
                    for attribute_name, attribute_value in message['MessageAttributes'].items():
                        if 'StringValue' in attribute_value:
                            fname = attribute_value['StringValue']
                            
            sqs.delete_message(
                QueueUrl=response_queue_url,
                ReceiptHandle=receipt_handle
            )
        if fname != '' and fname not in responses:
            responses[fname] = response_body
        
        filename = filename.split('.')[0]
        print(responses)

        if filename in responses:
            response_value = responses[filename]

            del responses[filename]

            return f"{filename}:{response_value}", 200

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
  #  app.run(debug=True)