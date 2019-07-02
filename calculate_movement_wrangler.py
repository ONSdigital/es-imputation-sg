import json
import boto3
import traceback
import pandas as pd
import os
import random

# Set up clients
s3 = boto3.resource('s3')
sqs = boto3.client('sqs', region_name='eu-west-2')
sns = boto3.client('sns', region_name='eu-west-2')


def _get_traceback(exception):
    """
    Given an exception, returns the traceback as a string.
    :param exception: Exception object
    :return: string
    """

    return ''.join(
        traceback.format_exception(
            etype=type(exception), value=exception, tb=exception.__traceback__
        )
    )


def strata_mismatch_detector(data, current_period):
    """
    Looks only at id and strata columns. Then drops any duplicated rows(keep=false means that if
    there is a dupe it'll drop both). If there are any rows in this DataFrame it shows that the
    reference-strata combination was unique, and therefore the strata is different between periods
    :param data: The data the miss-match detection will be performed on.
    :param current_period: The current period of the run.
    :return:
    """
    time = os.environ['time']  # Currently set as "period"
    reference = os.environ['reference']  # Currently set as "responder_id"
    segmentation = os.environ['segmentation']  # Currently set as "strata"
    stored_segmentation = os.environ['stored_segmentation']  # Currently set as "goodstrata"
    current_time = os.environ['current_time']  # Currently set as "current_period"
    previous_time = os.environ['previous_time']  # Currently set as "previous_period"
    current_segmentation = os.environ['current_segmentation']  # Currently set as "current_strata"
    previous_segmentation = os.environ['previous_segmentation']  # Currently set as "previous_strata"

    data_anomalies = data[[reference, segmentation, time]]

    data_anomalies = data_anomalies.drop_duplicates(subset=[reference, segmentation], keep=False)

    if data_anomalies.size > 0:
        # Filter so we only have current period stuff
        fix_data = data_anomalies[data_anomalies[time] == int(current_period)][[reference, segmentation]]
        fix_data = fix_data.rename(columns={segmentation: stored_segmentation})

        # Now merge these so that the fix_data strata is added as an extra column to the input data
        data = pd.merge(data, fix_data, on=reference, how='left')

        # We should now have a good Strata column in the dataframe - mostly containing null values,
        # containing strata where there was anomoly using an apply method, set strata to be the
        # goodstrata
        data[segmentation] = data.apply(
            lambda x: x[stored_segmentation] if str(x[stored_segmentation]) != 'nan' else x[
                segmentation], axis=1)
        data = data.drop(stored_segmentation, axis=1)

        # Split on period then merge together so they're same row.
        current_period_anomalies = data_anomalies[
            data_anomalies[time] == int(current_period)].rename(
            columns={segmentation: current_segmentation, time: current_time})
        prev_period_anomalies = data_anomalies[data_anomalies[time] != int(current_period)].rename(
            columns={segmentation: previous_segmentation, time: previous_time})

        data_anomalies = pd.merge(current_period_anomalies, prev_period_anomalies, on=reference)

    return data, data_anomalies


def lambda_handler(event, context):
    """
    This wrangler is used to prepare the data for the calculate movements statistical method.
    The method requires a column per question to store the movements, named as follows:
    'movement_questionNameAndNumber'. The wrangler checks for non response and if everyone has
    responded the calculate movements is skipped.
    :param event: N/A
    :param context: N/A
    :return: Success - True/False & Checkpoint
    """
    try:
        # Setting up clients
        lambda_client = boto3.client('lambda')

        # Setting up environment variables
        s3_file = os.environ['s3_file']
        bucket_name = os.environ['bucket_name']

        queue_url = os.environ['queue_url']
        sqs_messageid_name = os.environ['sqs_messageid_name']

        checkpoint = os.environ['checkpoint']
        period = os.environ['period']
        method_name = os.environ['method_name']

        time = os.environ['time']  # Currently set as "period"
        response_type = os.environ['response_type']  # Currently set as "response_type"
        questions_list = os.environ['questions_list']
        output_file = os.environ['output_file']

        previous_period_json = read_data_from_s3(bucket_name, s3_file)

        response = get_data_from_sqs(queue_url)
        message = response['Messages'][0]
        message_json = json.loads(message['Body'])

        # Used for deleting data from the Queue
        receipt_handle = message['ReceiptHandle']

        data = pd.DataFrame(message_json)

        # Create a Dataframe where the response column value is set as 1 i.e non responders
        filtered_non_responders = data.loc[(data[response_type] == 1) & (data[time] == int(period))]
        response_check = len(filtered_non_responders.index)

        # If greater than 0 it means there is non-responders so Imputation need to be run
        if response_check > 0:

            non_responders = data[data[response_type] == 1]
            non_responders_json = non_responders.to_json(orient='records')

            save_to_s3(bucket_name, output_file, non_responders_json)

            # Ensure that only responder_ids with a response type of 2 (returned) get picked up
            data = data[data[response_type] == 2]

            # Merged together so it can be sent via the payload to the method
            previous_period_data = pd.DataFrame(previous_period_json)
            merged_data = pd.concat([data, previous_period_data])

            # Pass to mismatch detector to look for and fix strata mismatches
            merged_data, anomalies = strata_mismatch_detector(merged_data, period)

            for question in questions_list.split():
                merged_data['movement_' + question] = 0.0

            json_ordered_data = merged_data.to_json(orient='records')
            imputed_data = lambda_client.invoke(FunctionName=method_name, Payload=json_ordered_data)
            json_response = json.loads(imputed_data.get('Payload').read().decode("UTF-8"))

            imputation_run_type = "Calculate Movement was ran successfully."
            send_sqs_message(queue_url, json_response, sqs_messageid_name, receipt_handle)

        else:

            imputation_run_type = "Imputation was not ran"
            send_sqs_message(queue_url, message, sqs_messageid_name, receipt_handle)

        send_sns_message(imputation_run_type, anomalies)

    except Exception as exc:

        # sqs.purge_queue(
        #     QueueUrl=queue_url
        # )

        return {
            "success": False,
            "checkpoint": checkpoint,
            "error": "Unexpected exception {}".format(_get_traceback(exc))
        }

    return {
        "success": True,
        "checkpoint": checkpoint
    }


def read_data_from_s3(bucket_name, s3_file):
    """
    This method is used to retrieve data from an s3 bucket.
    :param bucket_name: The name of the bucket you are accessing.
    :param s3_file: The file you wish to import.
    :return: previous_period_json - Type: JSON.
    """
    previous_period_object = s3.Object(bucket_name, s3_file)
    previous_period_content = previous_period_object.get()['Body'].read()
    previous_period_json = json.loads(previous_period_content)

    return previous_period_json


def save_to_s3(bucket_name, output_file_name, output_data):
    """
    This function uploads a specified set of data to the s3 bucket under the given name.
    :param bucket_name: Name of the bucket you wish to upload too - Type: String.
    :param output_file_name: Name that you want the file to be called on s3 - Type: String.
    :param output_data: The data that you wish to upload to s3 - Type: JSON.
    :return: None
    """
    s3.Object(bucket_name, output_file_name).put(Body=output_data)


def get_data_from_sqs(queue_url):
    """
    This method retrieves the data from the specified SQS queue.
    :param queue_url: The url of the SQS queue.
    :return: Type: Array of Message objects
    """
    return sqs.receive_message(QueueUrl=queue_url)


def send_sqs_message(queue_url, message, output_message_id, receipt_handle):
    """
    This method is responsible for sending data to the SQS queue and deleting the left-over data.
    :param queue_url: The url of the SQS queue. - Type: String.
    :param message: The message/data you wish to send to the SQS queue - Type: String.
    :param output_message_id: The label of the record in the SQS queue - Type: String
    :param receipt_handle: Received from the sqs payload, used to specify what is to be deleted.
    :return: None
    """
    # MessageDeduplicationId is set to a random hash to overcome de-duplication,otherwise modules
    # could not be re-run in the space of 5 Minutes.
    sqs.send_message(QueueUrl=queue_url,
                     MessageBody=message,
                     MessageGroupId=output_message_id,
                     MessageDeduplicationId=str(random.getrandbits(128))
                     )

    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)


def send_sns_message(imputation_run_type, anomalies):
    """
    This method is responsible for sending a notification to the specified arn, so that it can be
    used to relay information for the BPM to use and handle.
    :param imputation_run_type: A flag to see if imputation ran or not - Type: String.
    :param anomalies: Any discrepancies that have been detected during processing. - Type: DataFrame
    :return: None
    """
    arn = os.environ['arn']
    checkpoint = os.environ['Checkpoint']

    sns_message = {
        "success": True,
        "module": "Imputation",
        "checkpoint": checkpoint,
        "anomalies": anomalies.to_json(orient='records'),
        "message": imputation_run_type
    }

    sns.publish(
        TargetArn=arn,
        Message=json.dumps(sns_message)
    )
