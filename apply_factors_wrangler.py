import json
import boto3
import traceback
import pandas as pd
import os
import random


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


def get_from_sqs(queue_url):
    """
    Gets and formats data from sqs queue
    :param queue_url - String: Name of sqs queue
    :return: factors_dataframe - Dataframe: Data from previous step
             receipt_handle    - String: The id of the sqs message, used to delete from queue at the end of the process
    """

    response = receive_message(queue_url)
    message = response['Messages'][0]
    message_json = json.loads(message['Body'])
    factors_dataframe = pd.DataFrame(message_json)
    # Used for clearing the Queue
    receipt_handle = response['ReceiptHandle']

    return factors_dataframe, receipt_handle


def receive_message(queue_url):
    """
    Gets the raw sqs response.
    :param queue_url - String: Name of sqs queue
    :return: response - Dict: unprocessed(raw) sqs message.
    """

    sqs = boto3.client('sqs')
    return sqs.receive_message(QueueUrl=queue_url)

def delete_sqs_message(sqs, queue_url, receipt_handle):
    """
    Deletes a specified sqs message from the queue
    :param sqs - Boto3 client(SQS): SQS client for use in interacting with sqs
    :param queue_url - String: Name of sqs queue
    :param receipt_handle    - String: The id of the sqs message, used to delete from queue
    :return: None
    """
    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)


def send_output_to_sqs(queue_url, message, output_message_id,receipt_handle):
    """
        Handles sending output data at end of module, and deleting input data from queue
        :param queue_url - String: Name of sqs queue
        :param message - String: Message to send to sqs, string(json) representation of output dataframe
        :param output_message_id  - String: ID of the message to be sent
        :param receipt_handle    - String: The id of the sqs message, used to delete from queue
        :return: None
    """
    sqs = boto3.client('sqs')
    send_sqs_message(sqs, queue_url, message, output_message_id)
    delete_sqs_message(sqs, queue_url, receipt_handle)

def send_sqs_message(sqs, queue_url, message, output_message_id):
    """
    This method is responsible for sending data to the SQS queue.
    :param sqs - Boto3 client(SQS): SQS client for use in interacting with sqs
    :param queue_url: The url of the SQS queue. - Type: String.
    :param message: The message/data you wish to send to the SQS queue - Type: String.
    :param output_message_id: The label of the record in the SQS queue - Type: String
    :return: None
    """

    sqs.send_message(QueueUrl=queue_url,
                     MessageBody=message,
                     MessageGroupId=output_message_id,
                     MessageDeduplicationId=str(random.getrandbits(128))
                     )


def lambda_handler(event, context):
    try:

        # Set up clients
        # # S3
        bucket_name = os.environ['bucket_name']
        non_responder_data_file = os.environ['non_responder_file']  # non_responders_output.json
        # output_file = os.environ['output_file']
        #
        # # Sqs
        queue_url = os.environ['queue_url']
        sqs_messageid_name = os.environ['sqs_messageid_name']
        #
        # # Sns

        #
        checkpoint = os.environ['checkpoint']
        current_period = os.environ['period']
        method_name = os.environ['method_name']
        #

        s3_file = os.environ['s3_file']
        arn = os.environ['arn']
        lambda_client = boto3.client('lambda')

        # get imputation factors from queue

        # run get non responders to get a dataframe of all those that did not respond
        #
        # Join imputation factors to non-responder data[[Matching on region,county,strata]]
        # Apply factors to non-responder data
        #
        # Join imputed data with current period responders

        factors_dataframe,receipt_handle = get_from_sqs(queue_url)
        #using assumption that factors df is granular- unaggregated data

        # Reads in non responder data
        non_responder_dataframe = pd.DataFrame(read_data_from_s3(bucket_name, non_responder_data_file))

        # Read in previous period data for current period non-responders
        prev_period_data = pd.DataFrame(read_data_from_s3(bucket_name, s3_file))

        # Filter so we only have those that responded in prev
        prev_period_data = prev_period_data[prev_period_data['response_type'] == 2]

        question_columns = ['Q601_asphalting_sand', 'Q602_building_soft_sand', 'Q603_concreting_sand', 'Q604_bituminous_gravel', 'Q605_concreting_gravel', 'Q606_other_gravel', 'Q607_constructional_fill']
        prev_question_columns = ['prev_Q601_asphalting_sand', 'prev_Q602_building_soft_sand', 'prev_Q603_concreting_sand', 'prev_Q604_bituminous_gravel', 'prev_Q605_concreting_gravel', 'prev_Q606_other_gravel', 'prev_Q607_constructional_fill', 'responder_id']

        for question in question_columns:
            prev_period_data = prev_period_data.rename(index=str, columns={question: 'prev_' + question})

        # Join prev data so we have those who responded in prev but not in current

        non_responder_dataframe = pd.merge(non_responder_dataframe, prev_period_data[prev_question_columns], on='responder_id')

        # Merge the factors onto the non responders
        non_responders_with_factors = pd.merge(non_responder_dataframe, factors_dataframe[['region', 'strata', 'imputation_factor_Q601_asphalting_sand', 'imputation_factor_Q602_building_soft_sand', 'imputation_factor_Q603_concreting_sand', 'imputation_factor_Q604_bituminous_gravel', 'imputation_factor_Q605_concreting_gravel', 'imputation_factor_Q606_other_gravel', 'imputation_factor_Q607_constructional_fill']], on=["region", "strata"], how='inner')
        #Non responder data should now contain all previous values and 7 imp columns
        imputed_data = lambda_client.invoke(FunctionName=method_name, Payload=non_responders_with_factors.to_json(orient='records'))

        json_response = json.loads(imputed_data.get('Payload').read().decode("ascii"))

        imputed_non_responders = pd.read_json(str(json_response).replace("'",'"'))
        current_responders = factors_dataframe[factors_dataframe['period'] == int(current_period)]

        final_imputed = pd.concat([current_responders, imputed_non_responders])

        imputation_run_type = "Imputation complete"
        message = final_imputed.to_json(orient='records')
        #send_sqs_message(queue_url,message,sqs_messageid_name,receipt_handle)
        send_output_to_sqs(queue_url,message,sqs_messageid_name,receipt_handle)

        send_sns_message(arn, imputation_run_type, checkpoint)

    except Exception as exc:
        print(_get_traceback(exc))
        ### COMMENTED OUT FOR TESTING ###
        # purge = sqs.purge_queue(
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


def send_sns_message(arn, imputation_run_type, checkpoint):
    """
    This method is responsible for sending a notification to the specified arn, so that it can be
    used to relay information for the BPM to use and handle.
    :param imputation_run_type: A flag to see if imputation ran or not - Type: String.
    :param anomalies: Any discrepancies that have been detected during processing. - Type: List.
    :return: None
    """
    sns = boto3.client('sns')

    sns_message = {
        "success": True,
        "module": "Imputation",
        "checkpoint": checkpoint,
        "message": imputation_run_type
    }

    sns.publish(
        TargetArn=arn,
        Message=json.dumps(sns_message)
    )


def read_data_from_s3(bucket_name, s3_file):
    """
    This method is used to retrieve data from an s3 bucket.
    :param bucket_name: The name of the bucket you are accessing.
    :param s3_file: The file you wish to import.
    :return: json_file - Type: JSON.
    """
    s3 = boto3.resource('s3')
    object = s3.Object(bucket_name, s3_file)
    content = object.get()['Body'].read()
    json_file = json.loads(content)

    return json_file
