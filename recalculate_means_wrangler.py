import json
import random
import os
import boto3
import pandas as pd
import marshmallow
import logging
from botocore.exceptions import ClientError
from botocore.exceptions import IncompleteReadError


class EnvironSchema(marshmallow.Schema):
    """
    Class to set up the environment variables schema.
    """
    arn = marshmallow.fields.Str(required=True)
    queue_url = marshmallow.fields.Str(required=True)
    checkpoint = marshmallow.fields.Str(required=True)
    method_name = marshmallow.fields.Str(required=True)
    questions_list = marshmallow.fields.Str(required=True)
    sqs_messageid_name = marshmallow.fields.Str(required=True)


class NoDataInQueueError(Exception):
    pass


def lambda_handler(event, context):
    """
    prepares the data for the Means method.
    - Read in data from the SQS queue.
    - Invokes the Mean Method.
    - Send data received back from the Mean method to the SQS queue.

    :param event:
    :param context:
    :return: Outcome Message - Type: Json String.
    """
    current_module = "Recalc - Wrangler"
    error_message = ""
    log_message = ""
    logger = logging.getLogger("RecalcMeans")
    logger.setLevel(10)
    try:
        logger.info(current_module + " Begun")
        # Set up clients
        sqs = boto3.client('sqs', 'eu-west-2')
        lambda_client = boto3.client('lambda', 'eu-west-2')

        # Set up Environment variables Schema.
        schema = EnvironSchema()
        config, errors = schema.load(os.environ)
        if errors:
            raise ValueError(f"Error validating environment parameters: {errors}")

        logger.info("Validated params")
        # Set up environment variables
        arn = config["arn"]
        queue_url = config["queue_url"]
        checkpoint = config["checkpoint"]
        method_name = config["method_name"]
        questions_list = config["questions_list"]
        sqs_messageid_name = config["sqs_messageid_name"]

        response = get_sqs_message(queue_url)
        if "Messages" not in response:
            raise NoDataInQueueError("No Messages in queue")

        message = response['Messages'][0]
        message_json = json.loads(message['Body'])
        receipt_handle = message['ReceiptHandle']

        logger.info("Successfully retrieved data from sqs")

        data = pd.DataFrame(message_json)
        # Add means columns
        qno = 1
        for question in questions_list.split(' '):
            data.drop(['movement_' + question + '_count'], axis=1, inplace=True)
            data.drop(['movement_' + question + '_sum'], axis=1, inplace=True)
            data.drop(['atyp60' + str(qno), "iqrs60" + str(qno)], axis=1, inplace=True)
            qno += 1
            data['mean_' + question] = 0.0

        data_json = data.to_json(orient='records')

        returned_data = lambda_client.invoke(FunctionName=method_name, Payload=data_json)

        json_response = returned_data.get('Payload').read().decode("UTF-8")

        logger.info("Successfully invoked lambda")

        send_sqs_message(queue_url, json_response, sqs_messageid_name)

        logger.info("Successfully sent data to sqs")

        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

        logger.info("Successfully deleted input data from sqs")

        send_sns_message(arn, checkpoint)

        logger.info("Successfully sent data to sns")

    except NoDataInQueueError as e:
        error_message = (
            "There was no data in sqs queue in:  "
            + current_module
            + " |-  | Request ID: "
            + str(context["aws_request_id"])
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except AttributeError as e:
        error_message = (
            "Bad data encountered in "
            + current_module
            + " |- "
            + str(e.args)
            + " | Request ID: "
            + str(context["aws_request_id"])
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except ValueError as e:
        error_message = (
            "Parameter validation error"
            + current_module
            + " |- "
            + str(e.args)
            + " | Request ID: "
            + str(context["aws_request_id"])
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except ClientError as e:
        error_message = (
            "AWS Error ("
            + str(e.response["Error"]["Code"])
            + ") "
            + current_module
            + " |- "
            + str(e.args)
            + " | Request ID: "
            + str(context["aws_request_id"])
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except KeyError as e:
        error_message = (
            "Key Error in "
            + current_module
            + " |- "
            + str(e.args)
            + " | Request ID: "
            + str(context["aws_request_id"])
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except IncompleteReadError as e:
        error_message = (
            "Incomplete Lambda response encountered in "
            + current_module
            + " |- "
            + str(e.args)
            + " | Request ID: "
            + str(context["aws_request_id"])
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except Exception as e:
        error_message = (
            "General Error in "
            + current_module
            + " ("
            + str(type(e))
            + ") |- "
            + str(e.args)
            + " | Request ID: "
            + str(context["aws_request_id"])
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    finally:
        if (len(error_message)) > 0:
            logger.error(log_message)
            return {"success": False, "error": error_message}
        else:
            logger.info("Successfully completed module: " + current_module)
            return {"success": True, "checkpoint": checkpoint}


def send_sns_message(arn, checkpoint):
    """
    This function is responsible for sending notifications to the SNS Topic.
    Notifications will be used to relay information to the BPM.

    :param arn: The Address of the SNS topic - Type: String.
    :param imputation_run_type: Imputation ran flag. - Type: String.
    :param checkpoint: Location of process - Type: String.
    :return: None.
    """

    sns = boto3.client('sns', region_name='eu-west-2')

    sns_message = {
        "success": True,
        "module": "Imputation ReCalculate Means",
        "checkpoint": checkpoint
    }

    sns.publish(
        TargetArn=arn,
        Message=json.dumps(sns_message)
    )


def send_sqs_message(queue_url, message, output_message_id):
    """
    This method is responsible for sending data to the SQS queue.

    :param queue_url: The url of the SQS queue. - Type: String.
    :param message: The message/data you wish to send to the SQS queue - Type: String.
    :param output_message_id: The label of the record in the SQS queue - Type: String
    :return: None
    """

    sqs = boto3.client('sqs', region_name='eu-west-2')

    # MessageDeduplicationId is set to a random hash to overcome de-duplication,
    # otherwise modules could not be re-run in the space of 5 Minutes.
    sqs.send_message(QueueUrl=queue_url,
                     MessageBody=message,
                     MessageGroupId=output_message_id,
                     MessageDeduplicationId=str(random.getrandbits(128)))


def get_sqs_message(queue_url):
    """
    Retrieves message from the SQS queue.

    :param queue_url: The url of the SQS queue. - Type: String.
    :return: Message from queue - Type: String.
    """
    sqs = boto3.client('sqs', region_name='eu-west-2')
    return sqs.receive_message(QueueUrl=queue_url)
