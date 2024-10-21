import boto3
import time

ASU_ID = '1231868809'
app_instance_name_prefix = 'app-tier-instance-'
max_instances = 20
ami_id = 'ami-0248ff7b537f13cf2' 
instance_type = 't2.micro'
key_name = 'project2part2'

sqs = boto3.client('sqs')
ec2 = boto3.client('ec2')

request_queue_name = f'{ASU_ID}-req-queue'

def get_queue_values(queue_url):
    try:
        response = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['ApproximateNumberOfMessages']
        )

        return int(response['Attributes']['ApproximateNumberOfMessages'])
    except Exception as e:
        print(f"Error getting queue metrics: {e}")
        return 0

def find_instances():
    response = ec2.describe_instances(
        Filters=[{
            'Name': 'tag:Name',
            'Values': [f'{app_instance_name_prefix}*']
        }, {
            'Name': 'instance-state-name',
            'Values': ['running']
        }]
    )
    instances = []
    for reservation in response['Reservations']:
        for instance in reservation['Instances']:
            instances.append({
                'InstanceId': instance['InstanceId'],
                'LaunchTime': instance['LaunchTime']
            })

    instances.sort(key=lambda x: x['LaunchTime'], reverse=True)
    return instances

def create_new_instance(instance_number):
    instance_name = f'{app_instance_name_prefix}{instance_number}'
    ec2.run_instances(
        ImageId=ami_id,
        InstanceType=instance_type,
        MinCount=1,
        MaxCount=1,
        KeyName=key_name,
        SecurityGroupIds = ['sg-06207987a2ce72c67'],
        TagSpecifications=[{
            'ResourceType': 'instance',
            'Tags': [{'Key': 'Name', 'Value': instance_name}]
        }]
    )
    print(f'Launched {instance_name}')

def terminate_instance(instance_id):
    ec2.terminate_instances(InstanceIds=[instance_id])
    print(f'Terminated instance {instance_id}')

def scale_out(current_instances, target_instances):

    for i in range(current_instances + 1, target_instances + 1):
        create_new_instance(i)

def scale_in(current_instances, target_instances):

    running_instances = find_instances()
    instances_to_terminate = current_instances - target_instances
    for i in range(instances_to_terminate):
        instance_id = running_instances[i]['InstanceId']
        terminate_instance(instance_id)

def scaler():
    while True:
        try:
            total_available_messages = int(get_queue_values(f'https://sqs.us-east-1.amazonaws.com/967211778521/1231868809-req-queue'))
            current_instance_count = len(find_instances())

            print(f'Available Messages: {total_available_messages}')
            print(f'Currently total Running Instances: {current_instance_count}')

            if total_available_messages == 0 and current_instance_count > 0:

                scale_in(current_instance_count, 0)

            elif 1 <= total_available_messages <= 10:

                if current_instance_count < 10:
                    scale_out(current_instance_count, min(total_available_messages, 10))

            elif 11 <= total_available_messages <= 50:

                if current_instance_count < 20:
                    scale_out(current_instance_count, min(total_available_messages, 20))

            elif total_available_messages > 50:

                if current_instance_count < 20:
                    scale_out(current_instance_count, 20)

            if current_instance_count > 0:

                if 1 <= total_available_messages <= 10 and current_instance_count > 10:
                    scale_in(current_instance_count, 10)

                elif total_available_messages <= 50 and current_instance_count > 20:
                    scale_in(current_instance_count, 20)
            
            time.sleep(10)
            
        except Exception as e:
            print(f"Error scaling the instances: {e}")

if __name__ == "__main__":
    scaler()

