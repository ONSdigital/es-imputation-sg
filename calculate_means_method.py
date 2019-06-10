import traceback
import json
import boto3
import os

def _get_clients():
    # Set up clients
    lambda_client = boto3.client('lambda')
    sqs = boto3.client('sqs')
    return(lambda_client, sqs)

# ENV vars
queue_url = os.environ.get('queue_url', None)


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
    

def lambda_handler(event, context):
    """
    Generates an aggregated DataFrame containing the mean value for 
    each of the period on period percentage movements, gropued by 
    region and strata.
    :param event: Event object
    :param context: Context object
    :return: JSON string
    """
    lambda_client, sqs = _get_clients()
    try:
        
        input_json = event
    
        means_df = pd.DataFrame(json.laods(input_json))
        
        means_df = means_df['Q601_asphalting_sand',
        'Q602_building_soft_sand',
        'Q603_concreting_sand',
        'Q604_bituminous_sand',
        'Q605_concreting_gravel',
        'Q606_other_gravel',
        'Q607_constructional_fill'].mean()
        
        final_output = means_df.to_json(orient='records')
        
    except Exception as exc:
        # Invoke error handler lambda
        # lambda_client.invoke(
        #     FunctionName=error_handler_arn,
        #     InvocationType='Event',
        #     Payload=json.loads(_get_traceback(exc))
        # )

        # purge = sqs.purge_queue(
        #   QueueUrl=queue_url
        # )

        return {
            "success": False,
            "error": "Unexpected exception {}".format(_get_traceback(exc))
        }

    return final_output
