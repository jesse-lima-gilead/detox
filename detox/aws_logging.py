import time
import yaml
import json
import boto3

'''def log_data_quality(json_result, client, log_group_name='your-log-group', log_stream_name='your-log-stream'):
 
    # Create log group if it doesn't exist
    try:
        client.create_log_group(logGroupName=log_group_name)
    except client.exceptions.ResourceAlreadyExistsException:
        pass

    # Create log stream if it doesn't exist
    try:
        client.create_log_stream(logGroupName=log_group_name, logStreamName=log_stream_name)
    except client.exceptions.ResourceAlreadyExistsException:
        pass

    # Prepare log events from the JSON result
    log_events = []
    for log in json_result:
        log_entry = {
            'check_status': log['check_status'],
            'check_level': log['check_level'],
            'constraint_status': log['constraint_status'],
            'check': log['check'],
            'constraint_message': log['constraint_message'],
            'constraint': log['constraint']
        }
        log_events.append({
            'timestamp': int(time.time() * 1000),  # Current timestamp in milliseconds
            'message': json.dumps(log_entry)  # Convert log entry to string
        })

    # Send logs to CloudWatch
    sequence_token = None
    while log_events:
        chunk = log_events[:100]  # Limit to 100 log events per request
        del log_events[:100]

        # Get sequence token
        response = client.describe_log_streams(logGroupName=log_group_name, logStreamNamePrefix=log_stream_name)
        sequence_token = response['logStreams'][0].get('uploadSequenceToken')

        # Put log events
        if sequence_token:
            client.put_log_events(
                logGroupName=log_group_name,
                logStreamName=log_stream_name,
                logEvents=chunk,
                sequenceToken=sequence_token
            )
        else:
            client.put_log_events(
                logGroupName=log_group_name,
                logStreamName=log_stream_name,
                logEvents=chunk
            )

        time.sleep(1)  # Avoid hitting rate limits
'''
        
def create_log_group(client, log_group_name):
    """Create a CloudWatch log group if it doesn't already exist."""
    try:
        client.create_log_group(logGroupName=log_group_name)
    except client.exceptions.ResourceAlreadyExistsException:
        pass

def create_log_stream(client, log_group_name, log_stream_name):
    """Create a CloudWatch log stream if it doesn't already exist."""
    try:
        client.create_log_stream(logGroupName=log_group_name, logStreamName=log_stream_name)
    except client.exceptions.ResourceAlreadyExistsException:
        pass

def prepare_log_events(json_result):
    """Prepare log events from the JSON result."""
    log_events = []
    for log in json_result:
        log_entry = {
            'check_status': log['check_status'],
            'check_level': log['check_level'],
            'constraint_status': log['constraint_status'],
            'check': log['check'],
            'constraint_message': log['constraint_message'],
            'constraint': log['constraint']
        }
        log_events.append({
            'timestamp': int(time.time() * 1000),  # Current timestamp in milliseconds
            'message': json.dumps(log_entry)  # Convert log entry to string
        })
    return log_events

def send_log_events(client, log_group_name, log_stream_name, log_events):
    """Send log events to CloudWatch in chunks."""
    while log_events:
        chunk = log_events[:100]  # Limit to 100 log events per request
        del log_events[:100]

        # Get sequence token
        response = client.describe_log_streams(logGroupName=log_group_name, logStreamNamePrefix=log_stream_name)
        sequence_token = response['logStreams'][0].get('uploadSequenceToken')

        # Put log events
        if sequence_token:
            client.put_log_events(
                logGroupName=log_group_name,
                logStreamName=log_stream_name,
                logEvents=chunk,
                sequenceToken=sequence_token
            )
        else:
            client.put_log_events(
                logGroupName=log_group_name,
                logStreamName=log_stream_name,
                logEvents=chunk
            )

        time.sleep(1)  # Avoid hitting rate limits

def log_data_quality(json_result, client, log_group_name, log_stream_name):
    """
    Log data quality results to AWS CloudWatch.

    Parameters:
    - json_result: List of dictionaries containing log data.
    - log_group_name: Name of the CloudWatch log group.
    - log_stream_name: Name of the CloudWatch log stream.
    - client: Boto3 CloudWatch client instance.
    """
    create_log_group(client, log_group_name)
    create_log_stream(client, log_group_name, log_stream_name)
    
    log_events = prepare_log_events(json_result)
    send_log_events(client, log_group_name, log_stream_name, log_events)

    
def log_message(log_message, client, log_group_name, log_stream_name):
    """
    Log a generic message to AWS CloudWatch.

    Parameters:
    - log_message: The message to log (string).
    - log_group_name: Name of the CloudWatch log group.
    - log_stream_name: Name of the CloudWatch log stream.
    - client: Boto3 CloudWatch client instance.
    """
    create_log_group(client, log_group_name)
    create_log_stream(client, log_group_name, log_stream_name)

    # Prepare log event
    log_event = {
        'timestamp': int(time.time() * 1000),  # Current timestamp in milliseconds
        'message': log_message  # Use the provided log message
    }

    # Send log event
    send_log_events(client, log_group_name, log_stream_name, [log_event])