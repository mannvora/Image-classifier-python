import boto3
import json
import time
import uuid
from facenet_pytorch import InceptionResnetV1, MTCNN
from PIL import Image
import torch
import io

ASU_ID = "1231868809" 
INPUT_BUCKET = f"{ASU_ID}-in-bucket"
OUTPUT_BUCKET = f"{ASU_ID}-out-bucket"

# SQS Configuration
sqs = boto3.client('sqs')
def get_queue_url(queue_name):
    response = sqs.get_queue_url(QueueName=queue_name)
    return response['QueueUrl']

request_queue_name = f'{ASU_ID}-req-queue'
response_queue_name = f'{ASU_ID}-resp-queue'

request_queue_url = f'https://sqs.us-east-1.amazonaws.com/967211778521/1231868809-req-queue'
response_queue_url = f'https://sqs.us-east-1.amazonaws.com/967211778521/1231868809-resp-queue'

s3 = boto3.client('s3')

resnet = InceptionResnetV1(pretrained='vggface2').eval()
mtcnn = MTCNN(image_size=240, margin=0, min_face_size=20)

def run_deep_learning_model(image_bytes):
    img = Image.open(io.BytesIO(image_bytes))
    face, prob = mtcnn(img, return_prob=True) 
    emb = resnet(face.unsqueeze(0)).detach() 

    saved_data = torch.load('data.pt', weights_only=True) 
    embedding_list = saved_data[0]
    name_list = saved_data[1]
    dist_list = [] 

    for idx, emb_db in enumerate(embedding_list):
        dist = torch.dist(emb, emb_db).item()
        dist_list.append(dist)

    idx_min = dist_list.index(min(dist_list))
    return name_list[idx_min]

def upload_to_s3(bucket, key, data):

    s3.put_object(Bucket=bucket, Key=key, Body=data)

def download_from_s3(bucket, key):

    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj['Body'].read()

def main():
    while True:
        response = sqs.receive_message(
            QueueUrl=request_queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10, 
            MessageAttributeNames=['All']

        )
        if 'Messages' in response:
            
            message = response['Messages'][0]
            receipt_handle = message['ReceiptHandle']
            message_body = message['Body']

            request_id = message_body
            filename = message['MessageAttributes']['filename']['StringValue']

            s3_response = s3.get_object(Bucket=INPUT_BUCKET, Key=filename)
            image_data = s3_response['Body'].read()

            classification_result = run_deep_learning_model(image_data)

            if classification_result:
                output_key = f"{filename.split('.')[0]}"  
                upload_to_s3(OUTPUT_BUCKET, output_key, classification_result.encode('utf-8'))
                sqs.send_message(
                    QueueUrl=response_queue_url,
                    MessageBody=classification_result,
                    MessageAttributes={
                        'filename': {
                            'StringValue': filename.split('.')[0],
                            'DataType': 'String'
                        }
                    }
                )

            sqs.delete_message(
                QueueUrl=request_queue_url,
                ReceiptHandle=receipt_handle
            )

        time.sleep(5)

if __name__ == '__main__':
    main()
