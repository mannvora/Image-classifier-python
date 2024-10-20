import boto3
import time

ASU_ID = '1231868809'
app_instance_name_prefix = 'app-tier-instance-'
max_instances = 20
ami_id = 'ami-0248ff7b537f13cf2'  # Update with your AMI ID
instance_type = 't2.micro'
key_name = 'project2part2'  # Replace with your EC2 key pair

# Initialize SQS, EC2 clients
sqs = boto3.client('sqs')
ec2 = boto3.client('ec2')

request_queue_name = f'{ASU_ID}-req-queue'

def get_queue_metrics(queue_url):
    try:
        # Get the attributes of the queue
        response = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        
        # Return the number of visible messages
        return int(response['Attributes']['ApproximateNumberOfMessages'])
    except Exception as e:
        print(f"Error getting queue metrics: {e}")
        return 0

def get_running_instances():
    # Describe instances that are running with our specific tag (app-tier-instance)
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
    # Sort instances by LaunchTime in reverse order (newest first)
    instances.sort(key=lambda x: x['LaunchTime'], reverse=True)
    return instances

def launch_instance(instance_number):
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
    # Launch the required number of instances
    for i in range(current_instances + 1, target_instances + 1):
        launch_instance(i)

def scale_in(current_instances, target_instances):
    # Terminate extra instances
    running_instances = get_running_instances()
    instances_to_terminate = current_instances - target_instances
    for i in range(instances_to_terminate):
        instance_id = running_instances[i]['InstanceId']
        terminate_instance(instance_id)

def manage_scaling():
    while True:
        try:
            num_messages = int(get_queue_metrics(f'https://sqs.us-east-1.amazonaws.com/967211778521/1231868809-req-queue'))
            current_instance_count = len(get_running_instances())

            print(f'Approximate Number of Messages: {num_messages}')
            print(f'Current Running Instances: {current_instance_count}')

            # Scaling logic
            if num_messages == 0 and current_instance_count > 0:
                # Terminate all instances if no messages in the queue
                scale_in(current_instance_count, 0)

            elif 1 <= num_messages <= 10:
                # Scale out to match the number of messages up to 10 instances
                if current_instance_count < 10:
                    scale_out(current_instance_count, min(num_messages, 10))

            elif 11 <= num_messages <= 50:
                # Scale out to match the number of messages up to 20 instances
                if current_instance_count < 20:
                    scale_out(current_instance_count, min(num_messages, 20))

            elif num_messages > 50:
                # Ensure 20 instances are running if there are more than 50 messages
                if current_instance_count < 20:
                    scale_out(current_instance_count, 20)

            # Scale in logic
            if current_instance_count > 0:
                # Scale down to 10 instances if there are 1 to 10 messages and more than 10 instances running
                if 1 <= num_messages <= 10 and current_instance_count > 10:
                    scale_in(current_instance_count, 10)

                # Scale down to 20 instances if there are up to 50 messages and more than 20 instances running
                elif num_messages <= 50 and current_instance_count > 20:
                    scale_in(current_instance_count, 20)

            # Wait before checking again
            time.sleep(6)  # Wait 10 seconds before checking again
            
        except Exception as e:
            print(f"Error managing scaling: {e}")

if __name__ == "__main__":
    manage_scaling()

