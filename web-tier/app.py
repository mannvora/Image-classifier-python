from flask import Flask, request
import boto3
import uuid
import time
from collections import defaultdict

app = Flask(__name__)

# Set up SQS
ASU_ID = '1231868809'  # Replace with your ASU ID
input_bucket = f"{ASU_ID}-in-bucket"
request_queue_url = f'https://sqs.us-east-1.amazonaws.com/597088032987/{ASU_ID}-req-queue'
response_queue_url = f'https://sqs.us-east-1.amazonaws.com/597088032987/{ASU_ID}-resp-queue'

sqs = boto3.client('sqs')
s3 = boto3.client('s3')

def get_queue_url(queue_name):
    response = sqs.get_queue_url(QueueName=queue_name)
    return response['QueueUrl']

request_queue_name = f'{ASU_ID}-req-queue'
response_queue_name = f'{ASU_ID}-resp-queue'

request_queue_url = get_queue_url(request_queue_name)
response_queue_url = get_queue_url(response_queue_name)
responses = defaultdict(str)

def upload_to_s3(bucket, key, data):
    """Upload data to S3."""
    s3.put_object(Bucket=bucket, Key=key, Body=data)
    
@app.route('/', methods=['POST'])
def handle_image_upload():
    # Get the image file from the request
    if 'inputFile' not in request.files:
        return "No inputFile part in the request", 400
    
    file = request.files['inputFile']
    if file.filename == '':
        return "No file selected", 400

    filename = file.filename
    # Generate a unique request ID for tracking
    request_id = str(uuid.uuid4())
    print("filename: " + filename)
    # Send the image to SQS Request Queue
    
    # Upload the file to S3
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
    
    # Wait for response from SQS Response Queue
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
    app.run(debug=True)