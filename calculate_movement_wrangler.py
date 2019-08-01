import json
import boto3
import traceback
import pandas as pd
import os
import random
from marshmallow import Schema, fields

# Set up clients
s3 = boto3.resource('s3')
sqs = boto3.client('sqs', region_name='eu-west-2')
sns = boto3.client('sns', region_name='eu-west-2')


class EnvironSchema(Schema):
    """
    Schema to ensure that environment variables are present and in the correct format.
    :param Schema: Schema from marshmallow import
    :return: None
    """
    s3_file = fields.Str(required=True)
    bucket_name = fields.Str(required=True)
    queue_url = fields.Str(required=True)
    sqs_messageid_name = fields.Str(required=True)
    checkpoint = fields.Str(required=True)
    arn = fields.Str(required=True)
    method_name = fields.Str(required=True)
    time = fields.Str(required=True)
    response_type = fields.Str(required=True)
    questions_list = fields.Str(required=True)
    output_file = fields.Str(required=True)
    reference = fields.Str(required=True)
    segmentation = fields.Str(required=True)
    stored_segmentation = fields.Str(required=True)
    current_time = fields.Str(required=True)
    previous_time = fields.Str(required=True)
    current_segmentation = fields.Str(required=True)
    previous_segmentation = fields.Str(required=True)


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


def strata_mismatch_detector(data, current_period, time, reference, segmentation,
                             stored_segmentation, current_time, previous_time,
                             current_segmentation, previous_segmentation):
    """
    Looks only at id and strata columns. Then drops any duplicated rows(keep=false means
    that if there is a dupe it'll drop both). If there are any rows in this DataFrame it
    shows that the reference-strata combination was unique, and therefore the strata is
    different between periods.
    :param data: The data the miss-match detection will be performed on.
    :param current_period: The current period of the run.
    :param time: Field name which is used as a gauge of time'. Added for IAC config.
    :param reference: Field name which is used as a reference for IAC.
    :param segmentation: Field name of the segmentation used for IAC.
    :param stored_segmentation: Field name of stored segmentation for IAC.
    :param current_time: Field name of the current time used for IAC.
    :param previous_time: Field name of the previous time used for IAC.
    :param current_segmentation: Field name of the current segmentation used for IAC.
    :param previous_segmentation: Field name of the current segmentation used for IAC.
    :return: data - Type: DataFrame, data_anomalies - Type: DataFrame
    """
    data_anomalies = data[[reference, segmentation, time]]

    data_anomalies = data_anomalies.drop_duplicates(subset=[reference, segmentation],
                                                    keep=False)

    if data_anomalies.size > 0:
        # Filter so we only have current period stuff
        fix_data = data_anomalies[data_anomalies[time] == int(current_period)][
            [reference, segmentation]]
        fix_data = fix_data.rename(columns={segmentation: stored_segmentation})

        # Now merge these so that the fix_data strata is
        # added as an extra column to the input data
        data = pd.merge(data, fix_data, on=reference, how='left')

        # We should now have a good Strata column in the dataframe - mostly containing
        # null values, containing strata where there was anomoly using an apply method,
        # set strata to be the goodstrata.
        data[segmentation] = data.apply(
            lambda x: x[stored_segmentation]
            if str(x[stored_segmentation]) != 'nan' else x[segmentation], axis=1)
        data = data.drop(stored_segmentation, axis=1)

        # Split on period then merge together so they're same row.
        current_period_anomalies = data_anomalies[
            data_anomalies[time] == int(current_period)].rename(
            columns={segmentation: current_segmentation, time: current_time})

        prev_period_anomalies = data_anomalies[data_anomalies[time]
                                               != int(current_period)].rename(
            columns={segmentation: previous_segmentation, time: previous_time})

        data_anomalies = pd.merge(current_period_anomalies, prev_period_anomalies,
                                  on=reference)

    return data, data_anomalies


def lambda_handler(event, context):
    """
    This wrangler is used to prepare data for the calculate movements statistical method.
    The method requires a column per question to store the movements, named as follows:
    'movement_questionNameAndNumber'. The wrangler checks for non response and if everyone
    has responded the calculate movements is skipped.
    :param event: N/A
    :param context: N/A
    :return: Success - True/False & Checkpoint
    """
    to_be_imputed = True
    try:

        schema = EnvironSchema()
        config, errors = schema.load(os.environ)
        if errors:
            raise ValueError(f"Error validating environment params: {errors}")

        # Needs to be declared inside of the lambda handler
        lambda_client = boto3.client('lambda', region_name="eu-west-2")

        # Setting up environment variables
        s3_file = config['s3_file']
        bucket_name = config['bucket_name']

        queue_url = config['queue_url']
        sqs_messageid_name = config['sqs_messageid_name']

        checkpoint = config['checkpoint']
        arn = config['arn']
        period = event['RuntimeVariables']['period']
        method_name = config['method_name']

        time = config['time']  # Set as "period"
        response_type = config['response_type']  # Set as "response_type"
        questions_list = config['questions_list']
        output_file = config['output_file']

        # Import for strata miss-match
        reference = config['reference']  # Set as "responder_id"
        segmentation = config['segmentation']  # Set as "strata"
        stored_segmentation = config['stored_segmentation']  # Set as "goodstrata"
        current_time = config['current_time']  # Set as "current_period"
        previous_time = config['previous_time']  # Set as "previous_period"
        # Set as "current_strata"
        current_segmentation = config['current_segmentation']
        # Set as "previous_strata"
        previous_segmentation = config['previous_segmentation']

        previous_period_json = read_data_from_s3(bucket_name, s3_file)

        response = get_data_from_sqs(queue_url)
        message = response['Messages'][0]
        message_json = json.loads(message['Body'])

        # Used for deleting data from the Queue
        receipt_handle = message['ReceiptHandle']

        data = pd.DataFrame(message_json)

        # Create a Dataframe where the response column
        # value is set as 1 i.e non responders
        filtered_non_responders = data.loc[(data[response_type] == 1) &
                                           (data[time] == int(period))]

        response_check = len(filtered_non_responders.index)

        # If greater than 0 it means there is non-responders so Imputation need to be run
        if response_check > 0:

            non_responders = data[data[response_type] == 1]
            non_responders_json = non_responders.to_json(orient='records')

            save_to_s3(bucket_name, output_file, non_responders_json)

            # Ensure that only responder_ids with a response
            # type of 2 (returned) get picked up
            data = data[data[response_type] == 2]

            # Merged together so it can be sent via the payload to the method
            previous_period_data = pd.DataFrame(previous_period_json)
            merged_data = pd.concat([data, previous_period_data])

            # Pass to mismatch detector to look for and fix strata mismatches
            merged_data, anomalies = strata_mismatch_detector(merged_data, period, time,
                                                              reference, segmentation,
                                                              stored_segmentation,
                                                              current_time,
                                                              previous_time,
                                                              current_segmentation,
                                                              previous_segmentation)

            for question in questions_list.split():
                merged_data['movement_' + question] = 0.0

            json_ordered_data = merged_data.to_json(orient='records')

            imputed_data = lambda_client.invoke(FunctionName=method_name,
                                                Payload=json_ordered_data)

            json_response = json.loads(imputed_data.get('Payload').read().decode("UTF-8"))

            imputation_run_type = "Calculate Movement was ran successfully."

            send_sqs_message(queue_url, json_response, sqs_messageid_name, receipt_handle)

        else:

            to_be_imputed = False
            imputation_run_type = "Imputation was not ran"
            anomalies = pd.DataFrame
            send_sqs_message(queue_url, message, sqs_messageid_name, receipt_handle)

        send_sns_message(imputation_run_type, anomalies, arn, checkpoint)

    except Exception as exc:

        return {
            "success": False,
            "checkpoint": checkpoint,
            "error": "Unexpected exception {}".format(_get_traceback(exc))
        }

    return {
        "success": True,
        "checkpoint": checkpoint,
        "Impute": to_be_imputed
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
    :param output_file_name: Name you want the file to be called on s3 - Type: String.
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
    This method is responsible for sending data to the SQS queue and deleting the
    left-over data.
    :param queue_url: The url of the SQS queue. - Type: String.
    :param message: The message/data you wish to send to the SQS queue - Type: String.
    :param output_message_id: The label of the record in the SQS queue - Type: String
    :param receipt_handle: Received from the sqs payload, used to
                           specify content to be deleted.
    :return: None
    """
    # MessageDeduplicationId is set to a random hash to overcome de-duplication,
    # otherwise modules could not be re-run in the space of 5 Minutes.
    sqs.send_message(QueueUrl=queue_url,
                     MessageBody=message,
                     MessageGroupId=output_message_id,
                     MessageDeduplicationId=str(random.getrandbits(128))
                     )

    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)


def send_sns_message(imputation_run_type, anomalies, arn, checkpoint):
    """
    This method is responsible for sending a notification to the specified arn,
    so that it can be used to relay information for the BPM to use and handle.
    :param imputation_run_type: A flag to see if imputation ran or not - Type: String.
    :param anomalies: Any discrepancies that have been detected during processing -
                      Type: DataFrame.
    :param arn: The arn of the sns topic you are directing the message at - Type: String.
    :param checkpoint: The current checkpoint location - Type: String.
    :return: None
    """

    sns_message = {
        "success": True,
        "module": "Imputation",
        "checkpoint": checkpoint,
        "anomalies": anomalies.to_json(orient='records'),
        "message": imputation_run_type
    }

    return sns.publish(TargetArn=arn, Message=json.dumps(sns_message))
