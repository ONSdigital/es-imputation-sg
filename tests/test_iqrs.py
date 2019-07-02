import unittest
import unittest.mock as mock
import json
import pandas as pd
import iqrs_wrangler, iqrs_method
from pandas.util.testing import assert_frame_equal



class test_wrangler_and_method(unittest.TestCase):



    @classmethod
    def setup_class(cls):
        # Mock out all the things that need mocking out, here - and start them

        cls.mock_boto_wrangler_patcher = mock.patch('iqrs_wrangler.boto3')
        cls.mock_boto_wrangler = cls.mock_boto_wrangler_patcher.start()

        cls.mock_boto_method_patcher = mock.patch('iqrs_method.boto3')
        cls.mock_boto_method = cls.mock_boto_method_patcher.start()

        cls.mock_os_patcher = mock.patch.dict('os.environ', {
            'queue_url': 'mock_queue',
            'sqs_messageid_name': 'mock_message',
            'arn': 'mock_arn',
            'checkpoint': 'mock_checkpoint',
            'method_name': 'mock_method',
            'input_data': 'mock_data',
            'error_handler_arn': 'mock_arn',
            'iqrs_columns': 'iqrs601,iqrs602,iqrs603,iqrs604,iqrs605,iqrs606,iqrs607',
            'movement_columns': 'movement_Q601_asphalting_sand,movement_Q602_building_soft_sand,movement_Q603_concreting_sand,movement_Q604_bituminous_gravel,movement_Q605_concreting_gravel,movement_Q606_other_gravel,movement_Q607_constructional_fill'
            })

        cls.mock_os = cls.mock_os_patcher.start()

    @classmethod
    def teardown_class(cls):

        # Stop the mocking of the boto stuff and the os

        cls.mock_boto_wrangler_patcher.stop()
        cls.mock_boto_method_patcher.stop()
        cls.mock_os_patcher.stop()


    def test_wrangler(self):  # This function tests the wrangler

        # Load the input file - which includes means and movements

        input_file = 'iqrs_input.json'

        with open(input_file, "r") as file:
            json_content = json.loads(file.read())

        # Set the mock_json value to the json_content variable above (ie the input file)
        # Also mock out the call to the iqrs_method within the wrangler

        with mock.patch('json.loads') as mock_json:
            mock_json.return_value = json_content
            mocked_client = mock.Mock()
            self.mock_boto_wrangler.client.return_value.invoke = mocked_client

        # Call the wrangler and pass the output into the payload variable

            iqrs_wrangler.lambda_handler(None,None)
            payload = mocked_client.call_args[1]['Payload']

        # Output the payload to a file called "Iqrs_with_columns.json". This is not needed for testing
        # the wrangler, but we need this data for testing the method, below, so we output it to a file here.

        with open("Iqrs_with_columns.json", "w+") as file:
             file.write(payload)

        # Read the payload file into a dataframe.

        payloadDF = pd.read_json(json.loads(payload))

        # We want to check that the subset of "required_cols" - ie the iqrs cols are in the set of columns
        # which are on the payloadDF dataframe. The "assert" command below, performs this test

        required_cols = {'iqrs601','iqrs602','iqrs603','iqrs604','iqrs605','iqrs606','iqrs607'}

        self.assertTrue(required_cols.issubset(set(payloadDF.columns)),'IQRS Columns are not in the DataFrame')

        # Set up a new dataframe, new_cols, which contains just the iqrs columns from payloadDF
        # We then check whether any of the iqrs values are set to null - which they shouldn't be
        # The assert does this, any prints out any values which are set to null.

        new_cols = payloadDF[required_cols]
        self.assertFalse(new_cols.isnull().values.any())


    def test_method(self): # This function tests the method

        # Load the input data - which is the output data from the wrangler, created above

        input_file = 'Iqrs_with_columns.json'

        with open(input_file, "r") as file:
            json_content = json.loads(file.read())

        # Call the method. We can pass the json_content file in as a parameter, as the method loads this data
        # into a json file within the code.
        # Output the result to a file called output

        output = iqrs_method.lambda_handler(json_content, None)

        # Set up some variables to be used in the next step

        IQRS_COLS = 'iqrs601,iqrs602,iqrs603,iqrs604,iqrs605,iqrs606,iqrs607'
        SORTING_COLS = ['region', 'strata']
        SELECTED_COLS = IQRS_COLS.split(',') + SORTING_COLS

        # Load the output file into a dataframe. We want to sort it by the sorting cols, region and strata and we only want to keep
        # the selected cols as defined above. We want to drop duplicates to ensure we only have one row per region/strata on the dataset,
        # so it matches the scala output (as opposed to one row per responder_id)

        outputDF = pd.DataFrame(output).sort_values(SORTING_COLS)[SELECTED_COLS].drop_duplicates(keep='first').reset_index(drop=True)

        # Load the data we have output from the Scala process. This data will have IQRS values on it, and should match the outputDF.
        # Again, we want to sort it by region and strata and only keep the selected columns

        ScalaDF = pd.read_csv("iqrs_scala_output.csv").sort_values(SORTING_COLS).reset_index()[SELECTED_COLS]

        # Round both dataframes to 5dp to ensure any differences between the dataframes are not caused by rounding
        # Finally, assert that the two frames are equal.

        responseDF = outputDF.round(5)
        expectedDF = ScalaDF.round(5)
        assert_frame_equal(responseDF, expectedDF)

    # The tests below, all test the exception handling

    def test_wrangler_exception_handling(self):
        response = iqrs_wrangler.lambda_handler(None, None)
        assert not response['success']

    def test_method_exception_handling(self):
        json_content ='[{"movement_Q601_asphalting_sand":0.0},{"movement_Q601_asphalting_sand":0.857614899}]'

        response = iqrs_method.lambda_handler(json_content, None)
        assert not response['success']

    def test_wrangler_success_responses(self):
        with mock.patch('iqrs_wrangler.json') as mock_json:
            response = iqrs_wrangler.lambda_handler(None, None)
            assert mock_json.dumps.call_args[0][0]['success']
            assert response
