import json
from copy import deepcopy
from unittest import mock

import pandas as pd
import pytest
from es_aws_functions import test_generic_library, exception_classes
from moto import mock_s3
from pandas.util.testing import assert_frame_equal

import add_regionless_method as lambda_regionless_method_function
import add_regionless_wrangler as lambda_regionless_wrangler_function
import apply_factors_method as lambda_apply_method_function
import apply_factors_wrangler as lambda_apply_wrangler_function
import atypicals_method as lambda_atypicals_method_function
import atypicals_wrangler as lambda_atypicals_wrangler_function
import calculate_imputation_factors_method as lambda_factors_method_function
import calculate_imputation_factors_wrangler as lambda_factors_wrangler_function
import calculate_means_method as lambda_means_method_function
import calculate_means_wrangler as lambda_means_wrangler_function
import calculate_movement_method as lambda_movement_method_function
import calculate_movement_wrangler as lambda_movement_wrangler_function
import imputation_functions as lambda_imputation_function
import iqrs_method as lambda_iqrs_method_function
import iqrs_wrangler as lambda_iqrs_wrangler_function
import recalculate_means_wrangler as lambda_recalc_wrangler_function

factors_parameters = {
    "RuntimeVariables": {
        "factors_type": "factors_calculation_a",
        "first_imputation_factor": 0,
        "first_threshold": 3,
        "percentage_movement": True,
        "region_column": "region",
        "regional_mean": "third_imputation_factors",
        "regionless_code": 14,
        "second_imputation_factor": 1,
        "second_threshold": 3,
        "survey_column": "survey",
        "third_threshold": 5
    }
}

generic_environment_variables = {
    "bucket_name": "test_bucket",
    "checkpoint": "999",
    "method_name": "test_method",
    "response_type": "response_type",
    "run_environment": "temporary"
}

imputation_functions = {
    "first_threshold": 2,
    "second_threshold": 2,
    "third_threshold": 2,
    "first_imputation_factor": 5,
    "second_imputation_factor": 10,
    "third_imputation_factors": pd.DataFrame(
        [
            {
                "imputation_factor_question_1": 55,
                "region": 1,
                "strata_A": "A",
                "strata_B": "B",
                "survey": "066"
            }
        ]
    ),
    "region_column": "region",
    "regionless_code": 14,
    "survey_column": "survey",
    "percentage_movement": True,
    "distinct_values": ["region"]
}

questions_list = [
    "Q601_asphalting_sand",
    "Q602_building_soft_sand",
    "Q603_concreting_sand",
    "Q604_bituminous_gravel",
    "Q605_concreting_gravel",
    "Q606_other_gravel",
    "Q607_constructional_fill"
  ]

method_apply_runtime_variables = {
    "RuntimeVariables": {
        "data": None,
        "questions_list": questions_list,
        "run_id": "bob",
        "sum_columns": [
            {
              "column_name": "Q608_total",
              "data": {
                "Q601_asphalting_sand": "+",
                "Q602_building_soft_sand": "+",
                "Q603_concreting_sand": "+",
                "Q604_bituminous_gravel": "+",
                "Q605_concreting_gravel": "+",
                "Q606_other_gravel": "+",
                "Q607_constructional_fill": "-"
              }
            }
          ]
    }
}

method_atypicals_runtime_variables = {
    "RuntimeVariables": {
        "data": None,
        "questions_list": questions_list,
        "run_id": "bob"
    }
}

method_factors_runtime_variables = {
    "RuntimeVariables": {
        "distinct_values": ["region", "strata"],
        "factors_parameters": factors_parameters,
        "data": None,
        "questions_list": questions_list,
        "run_id": "bob"
    }
}

method_iqrs_runtime_variables = {
    "RuntimeVariables": {
        "distinct_values": ["region", "strata"],
        "data": None,
        "questions_list": questions_list,
        "run_id": "bob"
    }
}

method_means_runtime_variables = {
    "RuntimeVariables": {
        "distinct_values": ["region", "strata"],
        "data": None,
        "questions_list": questions_list,
        "run_id": "bob"
    }
}

method_movement_runtime_variables = {
    "RuntimeVariables": {
        "current_period": "201809",
        "data": None,
        "movement_type": "movement_calculation_a",
        "period_column": "period",
        "previous_period": "201806",
        "questions_list": questions_list,
        "run_id": "bob"
    }
}

method_regionless_runtime_variables = {
    "RuntimeVariables": {
        "data": None,
        "region_column": "region",
        "regionless_code": 14,
        "run_id": "bob"
    }
}

wrangler_apply_runtime_variables_1 = {
    "RuntimeVariables": {
        "current_data": "test_wrangler_movement_current_data_prepared_output",
        "distinct_values": ["region", "strata"],
        "factors_parameters": {
            "RuntimeVariables": {
                "region_column": "region",
                "regionless_code": 14
            }
        },
        "in_file_name": "test_wrangler_apply_input_1",
        "incoming_message_group_id": "test_group",
        "location": "",
        "out_file_name": "test_wrangler_apply_output.json",
        "outgoing_message_group_id": "test_id1",
        "previous_data": "test_wrangler_movement_previous_data_prepared_output",
        "questions_list": questions_list,
        "queue_url": "Earl",
        "run_id": "bob",
        "sns_topic_arn": "fake_sns_arn",
        "sum_columns": [
            {
              "column_name": "Q608_total",
              "data": {
                "Q601_asphalting_sand": "+",
                "Q602_building_soft_sand": "+",
                "Q603_concreting_sand": "+",
                "Q604_bituminous_gravel": "+",
                "Q605_concreting_gravel": "+",
                "Q606_other_gravel": "+",
                "Q607_constructional_fill": "-"
              }
            }
          ],
        "unique_identifier": ["responder_id"]
    }
}

wrangler_apply_runtime_variables_2 = {
    "RuntimeVariables": {
        "current_data": "test_wrangler_movement_current_data_prepared_output",
        "distinct_values": ["region"],
        "factors_parameters": {
            "RuntimeVariables": {
                "region_column": "region",
                "regionless_code": 14
            }
        },
        "in_file_name": "test_wrangler_apply_input_2",
        "incoming_message_group_id": "test_group",
        "location": "",
        "out_file_name": "test_wrangler_apply_output.json",
        "outgoing_message_group_id": "test_id",
        "previous_data": "test_wrangler_movement_previous_data_prepared_output",
        "questions_list": questions_list,
        "queue_url": "Earl",
        "run_id": "bob",
        "sns_topic_arn": "fake_sns_arn",
        "sum_columns": [
            {
              "column_name": "Q608_total",
              "data": {
                "Q601_asphalting_sand": "+",
                "Q602_building_soft_sand": "+",
                "Q603_concreting_sand": "+",
                "Q604_bituminous_gravel": "+",
                "Q605_concreting_gravel": "+",
                "Q606_other_gravel": "+",
                "Q607_constructional_fill": "-"
              }
            }
          ],
        "unique_identifier": ["responder_id"]
    }
}

wrangler_atypicals_runtime_variables = {
    "RuntimeVariables": {
        "in_file_name": "test_wrangler_atypicals_input",
        "incoming_message_group_id": "test_group",
        "location": "",
        "out_file_name": "test_wrangler_atypicals_output.json",
        "outgoing_message_group_id": "test_id",
        "questions_list": questions_list,
        "queue_url": "Earl",
        "run_id": "bob",
        "sns_topic_arn": "fake_sns_arn"
    }
}

wrangler_factors_runtime_variables = {
    "RuntimeVariables": {
        "distinct_values": ["region", "strata"],
        "factors_parameters": deepcopy(factors_parameters),
        "in_file_name": "test_wrangler_factors_input",
        "incoming_message_group_id": "test_group",
        "location": "",
        "out_file_name": "test_wrangler_factors_output.json",
        "outgoing_message_group_id": "test_id",
        "period_column": "period",
        "questions_list": questions_list,
        "queue_url": "Earl",
        "run_id": "bob",
        "sns_topic_arn": "fake_sns_arn"
    }
}

wrangler_iqrs_runtime_variables = {
    "RuntimeVariables": {
        "distinct_values": ["region", "strata"],
        "in_file_name": "test_wrangler_iqrs_input",
        "incoming_message_group_id": "test_group",
        "location": "",
        "out_file_name": "test_wrangler_iqrs_output.json",
        "outgoing_message_group_id": "test_id",
        "questions_list": questions_list,
        "queue_url": "Earl",
        "run_id": "bob",
        "sns_topic_arn": "fake_sns_arn",
    }
}

wrangler_means_runtime_variables = {
    "RuntimeVariables": {
        "distinct_values": ["region", "strata"],
        "in_file_name": "test_wrangler_means_input",
        "incoming_message_group_id": "test_group",
        "location": "",
        "out_file_name": "test_wrangler_means_output.json",
        "outgoing_message_group_id": "test_id",
        "questions_list": questions_list,
        "queue_url": "Earl",
        "run_id": "bob",
        "sns_topic_arn": "fake_sns_arn",
    }
}

wrangler_movement_runtime_variables = {
    "RuntimeVariables": {
        "current_data": "test_wrangler_movement_current_data_output.json",
        "in_file_name": "test_wrangler_movement_input",
        "incoming_message_group_id": "test_group",
        "location": "",
        "movement_type": "movement_calculation_a",
        "out_file_name": "test_wrangler_movement_output.json",
        "out_file_name_skip": "test_wrangler_movement_skip_output.json",
        "outgoing_message_group_id": "test_id",
        "outgoing_message_group_id_skip": "test_id",
        "period": "201809",
        "period_column": "period",
        "periodicity": "03",
        "previous_data": "test_wrangler_movement_previous_data_output.json",
        "questions_list": questions_list,
        "queue_url": "Earl",
        "run_id": "bob",
        "sns_topic_arn": "fake_sns_arn",
        "unique_identifier": ["responder_id"]
    }
}

wrangler_recalc_runtime_variables = {
    "RuntimeVariables": {
        "distinct_values": ["region", "strata"],
        "in_file_name": "test_wrangler_recalc_input",
        "incoming_message_group_id": "test_group",
        "location": "",
        "out_file_name": "test_wrangler_recalc_output.json",
        "outgoing_message_group_id": "test_id",
        "questions_list": questions_list,
        "queue_url": "Earl",
        "run_id": "bob",
        "sns_topic_arn": "fake_sns_arn",
    }
}

wrangler_regionless_runtime_variables = {
    "RuntimeVariables": {
        "factors_parameters": {
            "RuntimeVariables": {
                "region_column": "region",
                "regionless_code": 14
            }
        },
        "in_file_name": "test_wrangler_regionless_input",
        "incoming_message_group_id": "test_group",
        "location": "",
        "out_file_name": "test_wrangler_regionless_output.json",
        "outgoing_message_group_id": "test_id",
        "queue_url": "Earl",
        "run_id": "bob",
        "sns_topic_arn": "fake_sns_arn"
    }
}


def movement_splitter(which_runtime_variables):
    with open("tests/fixtures/" +
              which_runtime_variables["RuntimeVariables"]["current_data"],
              "r") as file_4:
        test_current_produced = file_4.read()
    produced_current_data = pd.DataFrame(json.loads(test_current_produced))
    with open("tests/fixtures/test_wrangler_movement_current_data_prepared_output.json",
              "r") as file_5:
        test_current_prepared = file_5.read()
    prepared_current_data = pd.DataFrame(json.loads(test_current_prepared))

    assert_frame_equal(produced_current_data, prepared_current_data)

    with open("tests/fixtures/" +
              which_runtime_variables["RuntimeVariables"]["previous_data"],
              "r") as file_6:
        test_previous_produced = file_6.read()
    produced_previous_data = pd.DataFrame(json.loads(test_previous_produced))
    with open("tests/fixtures/test_wrangler_movement_previous_data_prepared_output.json",
              "r") as file_7:
        test_previous_prepared = file_7.read()
    prepared_previous_data = pd.DataFrame(json.loads(test_previous_prepared))

    assert_frame_equal(produced_previous_data, prepared_previous_data)

##########################################################################################
#                                     Generic                                            #
##########################################################################################


@pytest.mark.parametrize(
    "which_lambda,which_runtime_variables,which_environment_variables,"
    "which_data,expected_message,assertion",
    [
        (lambda_regionless_wrangler_function, wrangler_regionless_runtime_variables,
         generic_environment_variables, None,
         "ClientError", test_generic_library.wrangler_assert),
        (lambda_apply_wrangler_function, wrangler_apply_runtime_variables_1,
         generic_environment_variables, None,
         "ClientError", test_generic_library.wrangler_assert),
        (lambda_atypicals_wrangler_function, wrangler_atypicals_runtime_variables,
         generic_environment_variables, None,
         "ClientError", test_generic_library.wrangler_assert),
        (lambda_factors_wrangler_function, wrangler_factors_runtime_variables,
         generic_environment_variables, None,
         "ClientError", test_generic_library.wrangler_assert),
        (lambda_means_wrangler_function, wrangler_means_runtime_variables,
         generic_environment_variables, None,
         "ClientError", test_generic_library.wrangler_assert),
        (lambda_movement_wrangler_function, wrangler_movement_runtime_variables,
         generic_environment_variables, None,
         "ClientError", test_generic_library.wrangler_assert),
        (lambda_iqrs_wrangler_function, wrangler_iqrs_runtime_variables,
         generic_environment_variables, None,
         "ClientError", test_generic_library.wrangler_assert),
        (lambda_recalc_wrangler_function, wrangler_recalc_runtime_variables,
         generic_environment_variables, None,
         "ClientError", test_generic_library.wrangler_assert)
    ])
def test_client_error(which_lambda, which_runtime_variables,
                      which_environment_variables, which_data,
                      expected_message, assertion):
    test_generic_library.client_error(which_lambda, which_runtime_variables,
                                      which_environment_variables, which_data,
                                      expected_message, assertion)


@pytest.mark.parametrize(
    "which_lambda,which_runtime_variables,which_environment_variables,mockable_function,"
    "expected_message,assertion",
    [
        (lambda_regionless_wrangler_function, wrangler_regionless_runtime_variables,
         generic_environment_variables, "add_regionless_wrangler.EnvironSchema",
         "Exception", test_generic_library.wrangler_assert),
        (lambda_apply_wrangler_function, wrangler_apply_runtime_variables_1,
         generic_environment_variables, "apply_factors_wrangler.EnvironSchema",
         "Exception", test_generic_library.wrangler_assert),
        (lambda_atypicals_wrangler_function, wrangler_atypicals_runtime_variables,
         generic_environment_variables, "atypicals_wrangler.EnvironSchema",
         "Exception", test_generic_library.wrangler_assert),
        (lambda_factors_wrangler_function, wrangler_factors_runtime_variables,
         generic_environment_variables,
         "calculate_imputation_factors_wrangler.EnvironSchema",
         "Exception", test_generic_library.wrangler_assert),
        (lambda_means_wrangler_function, wrangler_means_runtime_variables,
         generic_environment_variables, "calculate_means_wrangler.EnvironSchema",
         "Exception", test_generic_library.wrangler_assert),
        (lambda_movement_wrangler_function, wrangler_movement_runtime_variables,
         generic_environment_variables, "calculate_movement_wrangler.EnvironSchema",
         "Exception", test_generic_library.wrangler_assert),
        (lambda_iqrs_wrangler_function, wrangler_iqrs_runtime_variables,
         generic_environment_variables, "iqrs_wrangler.EnvironSchema",
         "Exception", test_generic_library.wrangler_assert),
        (lambda_recalc_wrangler_function, wrangler_recalc_runtime_variables,
         generic_environment_variables, "recalculate_means_wrangler.EnvironSchema",
         "Exception", test_generic_library.wrangler_assert),
        (lambda_regionless_method_function, method_regionless_runtime_variables,
         False, "add_regionless_method.pd.DataFrame",
         "Exception", test_generic_library.method_assert),
        (lambda_apply_method_function, method_apply_runtime_variables,
         False, "apply_factors_method.pd.DataFrame",
         "Exception", test_generic_library.method_assert),
        (lambda_atypicals_method_function, method_atypicals_runtime_variables,
         False, "atypicals_method.pd.DataFrame",
         "Exception", test_generic_library.method_assert),
        (lambda_factors_method_function, method_factors_runtime_variables,
         False,
         "calculate_imputation_factors_method.pd.DataFrame",
         "Exception", test_generic_library.method_assert),
        (lambda_means_method_function, method_means_runtime_variables,
         False, "calculate_means_method.pd.DataFrame",
         "Exception", test_generic_library.method_assert),
        (lambda_movement_method_function, method_movement_runtime_variables,
         False, "calculate_movement_method.pd.DataFrame",
         "Exception", test_generic_library.method_assert),
        (lambda_iqrs_method_function, method_iqrs_runtime_variables,
         False, "iqrs_method.pd.DataFrame",
         "Exception", test_generic_library.method_assert)
    ])
def test_general_error(which_lambda, which_runtime_variables,
                       which_environment_variables, mockable_function,
                       expected_message, assertion):
    test_generic_library.general_error(which_lambda, which_runtime_variables,
                                       which_environment_variables, mockable_function,
                                       expected_message, assertion)


@mock_s3
@pytest.mark.parametrize(
    "which_lambda,which_runtime_variables,which_environment_variables," +
    "file_list,lambda_name,expected_message",
    [
        (lambda_regionless_wrangler_function, wrangler_regionless_runtime_variables,
         generic_environment_variables, ["test_wrangler_regionless_input.json"],
         "add_regionless_wrangler", "IncompleteReadError"),
        (lambda_apply_wrangler_function, deepcopy(wrangler_apply_runtime_variables_1),
         generic_environment_variables,
         ["test_wrangler_apply_input_1.json",
          "test_wrangler_movement_current_data_prepared_output.json",
          "test_wrangler_movement_previous_data_prepared_output.json"],
         "apply_factors_wrangler", "IncompleteReadError"),
        (lambda_atypicals_wrangler_function, wrangler_atypicals_runtime_variables,
         generic_environment_variables, ["test_wrangler_atypicals_input.json"],
         "atypicals_wrangler", "IncompleteReadError"),
        (lambda_factors_wrangler_function, wrangler_factors_runtime_variables,
         generic_environment_variables, ["test_wrangler_factors_input.json"],
         "calculate_imputation_factors_wrangler", "IncompleteReadError"),
        (lambda_means_wrangler_function, wrangler_means_runtime_variables,
         generic_environment_variables, ["test_wrangler_means_input.json"],
         "calculate_means_wrangler", "IncompleteReadError"),
        (lambda_movement_wrangler_function, wrangler_movement_runtime_variables,
         generic_environment_variables, ["test_wrangler_movement_input.json"],
         "calculate_movement_wrangler", "IncompleteReadError"),
        (lambda_iqrs_wrangler_function, wrangler_iqrs_runtime_variables,
         generic_environment_variables, ["test_wrangler_iqrs_input.json"],
         "iqrs_wrangler", "IncompleteReadError"),
        (lambda_recalc_wrangler_function, wrangler_recalc_runtime_variables,
         generic_environment_variables, ["test_wrangler_recalc_input.json"],
         "recalculate_means_wrangler", "IncompleteReadError")
    ])
def test_incomplete_read_error(which_lambda, which_runtime_variables,
                               which_environment_variables, file_list,
                               lambda_name, expected_message):
    with mock.patch(lambda_name + '.aws_functions.get_dataframe',
                    side_effect=test_generic_library.replacement_get_dataframe):
        test_generic_library.incomplete_read_error(which_lambda,
                                                   which_runtime_variables,
                                                   which_environment_variables,
                                                   file_list,
                                                   lambda_name,
                                                   expected_message)


@pytest.mark.parametrize(
    "which_lambda,which_environment_variables,expected_message,assertion",
    [
        (lambda_regionless_method_function, False,
         "KeyError", test_generic_library.method_assert),
        (lambda_regionless_wrangler_function, generic_environment_variables,
         "KeyError", test_generic_library.wrangler_assert),
        (lambda_apply_method_function, False,
         "KeyError", test_generic_library.method_assert),
        (lambda_apply_wrangler_function, generic_environment_variables,
         "KeyError", test_generic_library.wrangler_assert),
        (lambda_atypicals_method_function, False,
         "KeyError", test_generic_library.method_assert),
        (lambda_atypicals_wrangler_function, generic_environment_variables,
         "KeyError", test_generic_library.wrangler_assert),
        (lambda_factors_method_function, False,
         "KeyError", test_generic_library.method_assert),
        (lambda_factors_wrangler_function, generic_environment_variables,
         "KeyError", test_generic_library.wrangler_assert),
        (lambda_means_method_function, False,
         "KeyError", test_generic_library.method_assert),
        (lambda_means_wrangler_function, generic_environment_variables,
         "KeyError", test_generic_library.wrangler_assert),
        (lambda_movement_method_function, False,
         "KeyError", test_generic_library.method_assert),
        (lambda_movement_wrangler_function, generic_environment_variables,
         "KeyError", test_generic_library.wrangler_assert),
        (lambda_iqrs_method_function, False,
         "KeyError", test_generic_library.method_assert),
        (lambda_iqrs_wrangler_function, generic_environment_variables,
         "KeyError", test_generic_library.wrangler_assert),
        (lambda_recalc_wrangler_function, generic_environment_variables,
         "KeyError", test_generic_library.wrangler_assert)
    ])
def test_key_error(which_lambda, which_environment_variables,
                   expected_message, assertion):
    test_generic_library.key_error(which_lambda, which_environment_variables,
                                   expected_message, assertion)


@mock_s3
@pytest.mark.parametrize(
    "which_lambda,which_runtime_variables,which_environment_variables," +
    "file_list,lambda_name",
    [
        (lambda_regionless_wrangler_function, wrangler_regionless_runtime_variables,
         generic_environment_variables, ["test_wrangler_regionless_input.json"],
         "add_regionless_wrangler"),
        (lambda_apply_wrangler_function, deepcopy(wrangler_apply_runtime_variables_1),
         generic_environment_variables,
         ["test_wrangler_apply_input_1.json",
          "test_wrangler_movement_current_data_prepared_output.json",
          "test_wrangler_movement_previous_data_prepared_output.json"],
         "apply_factors_wrangler"),
        (lambda_atypicals_wrangler_function, wrangler_atypicals_runtime_variables,
         generic_environment_variables, ["test_wrangler_atypicals_input.json"],
         "atypicals_wrangler"),
        (lambda_factors_wrangler_function, wrangler_factors_runtime_variables,
         generic_environment_variables, ["test_wrangler_factors_input.json"],
         "calculate_imputation_factors_wrangler"),
        (lambda_means_wrangler_function, wrangler_means_runtime_variables,
         generic_environment_variables, ["test_wrangler_means_input.json"],
         "calculate_means_wrangler"),
        (lambda_movement_wrangler_function, wrangler_movement_runtime_variables,
         generic_environment_variables, ["test_wrangler_movement_input.json"],
         "calculate_movement_wrangler"),
        (lambda_iqrs_wrangler_function, wrangler_iqrs_runtime_variables,
         generic_environment_variables, ["test_wrangler_iqrs_input.json"],
         "iqrs_wrangler"),
        (lambda_recalc_wrangler_function, wrangler_recalc_runtime_variables,
         generic_environment_variables, ["test_wrangler_recalc_input.json"],
         "recalculate_means_wrangler")
    ])
def test_method_error(which_lambda, which_runtime_variables, which_environment_variables,
                      file_list, lambda_name):

    with mock.patch(lambda_name + '.aws_functions.get_dataframe',
                    side_effect=test_generic_library.replacement_get_dataframe):
        test_generic_library.wrangler_method_error(which_lambda,
                                                   which_runtime_variables,
                                                   which_environment_variables,
                                                   file_list,
                                                   lambda_name)


##########################################################################################
#    The Methods Have No Validation. When Add More Marshmallow Add Methods Into Tests.   #
##########################################################################################
@pytest.mark.parametrize(
    "which_lambda,expected_message,assertion",
    [
        (lambda_regionless_wrangler_function, "Error validating environment param",
         test_generic_library.wrangler_assert),
        (lambda_apply_wrangler_function, "Error validating environment param",
         test_generic_library.wrangler_assert),
        (lambda_atypicals_wrangler_function, "Error validating environment param",
         test_generic_library.wrangler_assert),
        (lambda_factors_wrangler_function, "Error validating environment param",
         test_generic_library.wrangler_assert),
        (lambda_means_wrangler_function, "Error validating environment param",
         test_generic_library.wrangler_assert),
        (lambda_movement_wrangler_function, "Error validating environment param",
         test_generic_library.wrangler_assert),
        (lambda_iqrs_wrangler_function, "Error validating environment param",
         test_generic_library.wrangler_assert),
        (lambda_recalc_wrangler_function, "Error validating environment param",
         test_generic_library.wrangler_assert)
    ])
def test_value_error(which_lambda, expected_message, assertion):
    test_generic_library.value_error(
        which_lambda, expected_message, assertion)

##########################################################################################
#                                     Specific                                           #
##########################################################################################


@mock_s3
@pytest.mark.parametrize(
    "which_lambda,which_runtime_variables,input_data,prepared_data",
    [
        (lambda_regionless_method_function, method_regionless_runtime_variables,
         "tests/fixtures/test_method_regionless_input.json",
         "tests/fixtures/test_method_regionless_prepared_output.json"),
        (lambda_apply_method_function, method_apply_runtime_variables,
         "tests/fixtures/test_method_apply_input.json",
         "tests/fixtures/test_method_apply_prepared_output.json"),
        (lambda_atypicals_method_function, method_atypicals_runtime_variables,
         "tests/fixtures/test_method_atypicals_input.json",
         "tests/fixtures/test_method_atypicals_prepared_output.json"),
        (lambda_factors_method_function, deepcopy(method_factors_runtime_variables),
         "tests/fixtures/test_method_factors_input.json",
         "tests/fixtures/test_method_factors_prepared_output.json"),
        (lambda_means_method_function, method_means_runtime_variables,
         "tests/fixtures/test_method_means_input.json",
         "tests/fixtures/test_method_means_prepared_output.json"),
        (lambda_movement_method_function, method_movement_runtime_variables,
         "tests/fixtures/test_method_movement_input.json",
         "tests/fixtures/test_method_movement_prepared_output.json"),
        (lambda_iqrs_method_function, method_iqrs_runtime_variables,
         "tests/fixtures/test_method_iqrs_input.json",
         "tests/fixtures/test_method_iqrs_prepared_output.json")

    ])
def test_method_success(which_lambda, which_runtime_variables, input_data, prepared_data):
    """
    Runs the method function.
    :param which_lambda: Main function.
    :param which_runtime_variables: RuntimeVariables. - Dict.
    :param input_data: File name/location of the data to be passed in. - String.
    :param prepared_data: File name/location of the data
                          to be used for comparison. - String.
    :return Test Pass/Fail
    """

    with open(prepared_data, "r") as file_1:
        file_data = file_1.read()
    prepared_data = pd.DataFrame(json.loads(file_data), dtype=float)

    with open(input_data, "r") as file_2:
        test_data = file_2.read()
    which_runtime_variables["RuntimeVariables"]["data"] = json.loads(test_data)

    output = which_lambda.lambda_handler(
        which_runtime_variables, test_generic_library.context_object)

    produced_data = pd.DataFrame(json.loads(output["data"]), dtype=float)

    assert output["success"]
    assert_frame_equal(produced_data, prepared_data)


@mock_s3
@mock.patch('calculate_movement_wrangler.aws_functions.save_data',
            side_effect=test_generic_library.replacement_save_data)
@mock.patch('calculate_movement_wrangler.aws_functions.get_dataframe',
            side_effect=test_generic_library.replacement_get_dataframe)
def test_wrangler_skip(mock_put_s3, mock_get_s3):
    """
    Runs the calculate_strata function that is called by the method.
    :param mock_put_s3: A replacement function for saving to s3 which saves locally.
    :param mock_get_s3: A replacement function for loading from local s3.
    :return Test Pass/Fail
    """
    bucket_name = generic_environment_variables["bucket_name"]
    client = test_generic_library.create_bucket(bucket_name)

    wrangler_movement_skip_runtime_variables = deepcopy(
        wrangler_movement_runtime_variables)

    wrangler_movement_skip_runtime_variables["RuntimeVariables"]["in_file_name"] =\
        "test_wrangler_movement_skip_input"
    file_list = ["test_wrangler_movement_skip_input.json"]
    test_generic_library.upload_files(client, bucket_name, file_list)

    with open("tests/fixtures/test_wrangler_movement_skip_prepared_output.json",
              "r") as file_1:
        test_data_prepared = file_1.read()
    prepared_data = pd.DataFrame(json.loads(test_data_prepared))

    with mock.patch.dict(lambda_movement_wrangler_function.os.environ,
                         generic_environment_variables):
        with mock.patch("calculate_movement_wrangler.boto3.client") as mock_client:  # noqa: F841 E501
            output = lambda_movement_wrangler_function.lambda_handler(
                wrangler_movement_skip_runtime_variables,
                test_generic_library.context_object
            )

    with open("tests/fixtures/" + wrangler_movement_skip_runtime_variables[
            "RuntimeVariables"]["out_file_name_skip"], "r") as file_3:
        test_data_produced = file_3.read()
    produced_data = pd.DataFrame(json.loads(test_data_produced))

    assert output
    assert_frame_equal(produced_data, prepared_data)


@mock_s3
@mock.patch('calculate_movement_wrangler.aws_functions.save_to_s3',
            side_effect=test_generic_library.replacement_save_to_s3)
@pytest.mark.parametrize(
    "which_lambda,which_environment_variables,which_runtime_variables," +
    "lambda_name,file_list,method_data,which_method_variables",
    [
        (lambda_regionless_wrangler_function, generic_environment_variables,
         wrangler_regionless_runtime_variables, "add_regionless_wrangler",
         ["test_wrangler_regionless_input.json"],
         "tests/fixtures/test_wrangler_regionless_input.json",
         method_regionless_runtime_variables),
        (lambda_apply_wrangler_function, generic_environment_variables,
         wrangler_apply_runtime_variables_1, "apply_factors_wrangler",
         ["test_wrangler_apply_input_1.json",
          "test_wrangler_movement_current_data_prepared_output.json",
          "test_wrangler_movement_previous_data_prepared_output.json"],
         "tests/fixtures/test_method_apply_input.json",
         method_apply_runtime_variables),
        (lambda_apply_wrangler_function, generic_environment_variables,
         wrangler_apply_runtime_variables_2, "apply_factors_wrangler",
         ["test_wrangler_apply_input_2.json",
          "test_wrangler_movement_current_data_prepared_output.json",
          "test_wrangler_movement_previous_data_prepared_output.json"],
         "tests/fixtures/test_method_apply_input.json",
         method_apply_runtime_variables),
        (lambda_atypicals_wrangler_function, generic_environment_variables,
         wrangler_atypicals_runtime_variables, "atypicals_wrangler",
         ["test_wrangler_atypicals_input.json"],
         "tests/fixtures/test_method_atypicals_input.json",
         method_atypicals_runtime_variables),
        (lambda_factors_wrangler_function, generic_environment_variables,
         wrangler_factors_runtime_variables, "calculate_imputation_factors_wrangler",
         ["test_wrangler_factors_input.json"],
         "tests/fixtures/test_method_factors_input.json",
         method_factors_runtime_variables),
        (lambda_means_wrangler_function, generic_environment_variables,
         wrangler_means_runtime_variables, "calculate_means_wrangler",
         ["test_wrangler_means_input.json"],
         "tests/fixtures/test_method_means_input.json",
         method_means_runtime_variables),
        (lambda_movement_wrangler_function, generic_environment_variables,
         wrangler_movement_runtime_variables, "calculate_movement_wrangler",
         ["test_wrangler_movement_input.json"],
         "tests/fixtures/test_method_movement_input.json",
         method_movement_runtime_variables),
        (lambda_iqrs_wrangler_function, generic_environment_variables,
         wrangler_iqrs_runtime_variables, "iqrs_wrangler",
         ["test_wrangler_iqrs_input.json"],
         "tests/fixtures/test_method_iqrs_input.json",
         method_iqrs_runtime_variables),
        (lambda_recalc_wrangler_function, generic_environment_variables,
         wrangler_recalc_runtime_variables, "recalculate_means_wrangler",
         ["test_wrangler_recalc_input.json"],
         "tests/fixtures/test_method_recalc_input.json",
         method_means_runtime_variables)
    ])
def test_wrangler_success_passed(mock_put_s3, which_lambda, which_environment_variables,
                                 which_runtime_variables, lambda_name,
                                 file_list, method_data, which_method_variables):
    """
    Runs the wrangler function.
    :param mock_put_s3: A replacement function for saving to s3 which saves locally.
    :param which_lambda: Main function.
    :param which_environment_variables: Environment Variables. - Dict.
    :param which_runtime_variables: RuntimeVariables. - Dict.
    :param lambda_name: Name of the py file. - String.
    :param file_list: Files to be added to the fake S3. - List(String).
    :param method_data: File name/location of the data
                        to be passed out by the method. - String.
    :param which_method_variables: Variables to compare against. - Dict.
    :return Test Pass/Fail
    """
    bucket_name = which_environment_variables["bucket_name"]
    client = test_generic_library.create_bucket(bucket_name)

    test_generic_library.upload_files(client, bucket_name, file_list)

    with mock.patch.dict(which_lambda.os.environ,
                         which_environment_variables):
        with mock.patch(lambda_name + '.aws_functions.get_dataframe',
                        side_effect=test_generic_library.replacement_get_dataframe):

            with mock.patch(lambda_name + ".boto3.client") as mock_client:
                mock_client_object = mock.Mock()
                mock_client.return_value = mock_client_object

                # Rather than mock the get/decode we tell the code that when the invoke is
                # called pass the variables to this replacement function instead.
                mock_client_object.invoke.side_effect = \
                    test_generic_library.replacement_invoke

                # This stops the Error caused by the replacement function from stopping
                # the test.
                with pytest.raises(exception_classes.LambdaFailure):
                    which_lambda.lambda_handler(
                        which_runtime_variables, test_generic_library.context_object
                    )

    with open(method_data, "r") as file_1:
        test_data_prepared = file_1.read()
    prepared_data = pd.DataFrame(json.loads(test_data_prepared), dtype=float)

    with open("tests/fixtures/test_wrangler_to_method_input.json", "r") as file_2:
        test_data_produced = file_2.read()
    produced_data = pd.DataFrame(json.loads(test_data_produced), dtype=float)

    # Compares the data.
    assert_frame_equal(produced_data, prepared_data)

    with open("tests/fixtures/test_wrangler_to_method_runtime.json", "r") as file_3:
        test_dict_prepared = file_3.read()
    produced_dict = json.loads(test_dict_prepared)

    # Ensures data is not in the RuntimeVariables and then compares.
    which_method_variables["RuntimeVariables"]["data"] = None
    assert produced_dict == which_method_variables["RuntimeVariables"]


@mock_s3
@mock.patch('calculate_movement_wrangler.aws_functions.save_to_s3',
            side_effect=test_generic_library.replacement_save_to_s3)
@pytest.mark.parametrize(
    "which_lambda,which_environment_variables,which_runtime_variables," +
    "lambda_name,file_list,method_data,prepared_data",
    [
        (lambda_regionless_wrangler_function, generic_environment_variables,
         wrangler_regionless_runtime_variables, "add_regionless_wrangler",
         ["test_wrangler_regionless_input.json"],
         "tests/fixtures/test_wrangler_regionless_input.json",
         "tests/fixtures/test_wrangler_regionless_prepared_output.json"),
        (lambda_apply_wrangler_function, generic_environment_variables,
         wrangler_apply_runtime_variables_1, "apply_factors_wrangler",
         ["test_wrangler_apply_input_1.json",
          "test_wrangler_movement_current_data_prepared_output.json",
          "test_wrangler_movement_previous_data_prepared_output.json"],
         "tests/fixtures/test_method_apply_prepared_output.json",
         "tests/fixtures/test_wrangler_apply_prepared_output.json"),
        (lambda_atypicals_wrangler_function, generic_environment_variables,
         wrangler_atypicals_runtime_variables, "atypicals_wrangler",
         ["test_wrangler_atypicals_input.json"],
         "tests/fixtures/test_method_atypicals_prepared_output.json",
         "tests/fixtures/test_wrangler_atypicals_prepared_output.json"),
        (lambda_factors_wrangler_function, generic_environment_variables,
         wrangler_factors_runtime_variables, "calculate_imputation_factors_wrangler",
         ["test_wrangler_factors_input.json"],
         "tests/fixtures/test_method_factors_prepared_output.json",
         "tests/fixtures/test_wrangler_factors_prepared_output.json"),
        (lambda_means_wrangler_function, generic_environment_variables,
         wrangler_means_runtime_variables, "calculate_means_wrangler",
         ["test_wrangler_means_input.json"],
         "tests/fixtures/test_method_means_prepared_output.json",
         "tests/fixtures/test_wrangler_means_prepared_output.json"),
        (lambda_movement_wrangler_function, generic_environment_variables,
         wrangler_movement_runtime_variables, "calculate_movement_wrangler",
         ["test_wrangler_movement_input.json"],
         "tests/fixtures/test_method_movement_prepared_output.json",
         "tests/fixtures/test_wrangler_movement_prepared_output.json"),
        (lambda_iqrs_wrangler_function, generic_environment_variables,
         wrangler_iqrs_runtime_variables, "iqrs_wrangler",
         ["test_wrangler_iqrs_input.json"],
         "tests/fixtures/test_method_iqrs_prepared_output.json",
         "tests/fixtures/test_wrangler_iqrs_prepared_output.json"),
        (lambda_recalc_wrangler_function, generic_environment_variables,
         wrangler_recalc_runtime_variables, "recalculate_means_wrangler",
         ["test_wrangler_recalc_input.json"],
         "tests/fixtures/test_method_recalc_prepared_output.json",
         "tests/fixtures/test_wrangler_recalc_prepared_output.json")
    ])
def test_wrangler_success_returned(mock_put_s3, which_lambda, which_environment_variables,
                                   which_runtime_variables, lambda_name,
                                   file_list, method_data, prepared_data):
    """
    Runs the wrangler function.
    :param mock_put_s3: A replacement function for saving to s3 which saves locally.
    :param which_lambda: Main function.
    :param which_environment_variables: Environment Variables. - Dict.
    :param which_runtime_variables: RuntimeVariables. - Dict.
    :param lambda_name: Name of the py file. - String.
    :param file_list: Files to be added to the fake S3. - List(String).
    :param method_data: File name/location of the data
                        to be passed out by the method. - String.
    :param prepared_data: File name/location of the data
                          to be used for comparison. - String.
    :return Test Pass/Fail
    """
    bucket_name = which_environment_variables["bucket_name"]
    client = test_generic_library.create_bucket(bucket_name)

    test_generic_library.upload_files(client, bucket_name, file_list)

    with open(prepared_data, "r") as file_1:
        test_data_prepared = file_1.read()
    prepared_data = pd.DataFrame(json.loads(test_data_prepared))

    with open(method_data, "r") as file_2:
        test_data_out = file_2.read()

    with mock.patch.dict(which_lambda.os.environ,
                         which_environment_variables):
        with mock.patch(lambda_name + '.aws_functions.save_data',
                        side_effect=test_generic_library.replacement_save_data):
            with mock.patch(lambda_name + '.aws_functions.get_dataframe',
                            side_effect=test_generic_library.replacement_get_dataframe):

                with mock.patch(lambda_name + ".boto3.client") as mock_client:
                    mock_client_object = mock.Mock()
                    mock_client.return_value = mock_client_object

                    mock_client_object.invoke.return_value.get.return_value.read \
                        .return_value.decode.return_value = json.dumps({
                         "data": test_data_out,
                         "success": True,
                         "anomalies": []
                        })

                    output = which_lambda.lambda_handler(
                        which_runtime_variables, test_generic_library.context_object
                    )

    with open("tests/fixtures/" +
              which_runtime_variables["RuntimeVariables"]["out_file_name"],
              "r") as file_3:
        test_data_produced = file_3.read()
    produced_data = pd.DataFrame(json.loads(test_data_produced))

    if lambda_name == "calculate_movement_wrangler":
        movement_splitter(which_runtime_variables)

    assert output
    assert_frame_equal(produced_data, prepared_data)


@pytest.mark.parametrize(
    "input_file,output_file,parameters",
    [
        ("tests/fixtures/test_imputation_functions_factors_a_region_input.json",
         "tests/fixtures/test_imputation_functions_factors_a_region_prepared_output.json",
         False),
        ("tests/fixtures/test_imputation_functions_factors_a_input.json",
         "tests/fixtures/test_imputation_functions_factors_a_prepared_output.json",
         False),
        ("tests/fixtures/test_imputation_functions_factors_a_input.json",
         "tests/fixtures/test_imputation_functions_factors_a_prepared_output.json",
         True)
    ])
def test_factors_calculation_a(input_file, output_file, parameters):
    """
    Runs the factors_calculation_a function.
    :param input_file
    :param output_file
    :param parameters
    :return Test Pass/Fail
    """
    question_list = ["question_1"]
    if parameters:
        imputation_functions["distinct_values"] = ["region", "strata_A", "strata_B"]

    with open(input_file, "r") as file_1:
        test_data_in = file_1.read()
    input_data = pd.DataFrame(json.loads(test_data_in))

    input_data = input_data.apply(
        lambda x: lambda_imputation_function.factors_calculation_a(
            x, question_list, imputation_functions), axis=1)

    # This is for Int, Float mismatch correction.
    json_data = input_data.to_json(orient="records")
    produced_data = pd.DataFrame(json.loads(json_data), dtype=float)

    with open(output_file, "r") as file_2:
        test_data_out = file_2.read()
    prepared_data = pd.DataFrame(json.loads(test_data_out), dtype=float)

    assert_frame_equal(produced_data, prepared_data)


def test_factors_calculation_b():
    """
    Runs the factors_calculation_b function.
    :param None
    :return Test Pass/Fail
    """
    with open("tests/fixtures/test_imputation_functions_factors_b_input.json", "r")\
            as file_1:
        test_data_in = file_1.read()
    produced_data = pd.DataFrame(json.loads(test_data_in))

    produced_data = produced_data.apply(
        lambda x: lambda_imputation_function.factors_calculation_b(
            x, ["question_1"], {"threshold": 2}), axis=1)

    with open("tests/fixtures/test_imputation_functions_factors_b_prepared_output.json",
              "r") as file_2:
        test_data_out = file_2.read()
    prepared_data = pd.DataFrame(json.loads(test_data_out))

    assert_frame_equal(produced_data, prepared_data)


def test_calc_iqrs():
    with open("tests/fixtures/test_calc_iqrs_input.json", "r") as file_1:
        test_data_in = file_1.read()
    input_data = pd.DataFrame(json.loads(test_data_in), dtype=float)

    q_list = method_iqrs_runtime_variables['RuntimeVariables']['questions_list']
    distinct_values = method_iqrs_runtime_variables['RuntimeVariables']["distinct_values"]

    movement_columns = lambda_imputation_function.produce_columns("movement_",
                                                                  q_list)
    iqrs_columns = lambda_imputation_function.produce_columns("iqrs_", q_list)

    produced_data = lambda_iqrs_method_function.calc_iqrs(
        input_data,
        movement_columns,
        iqrs_columns,
        distinct_values
    )
    with open("tests/fixtures/test_calc_iqrs_prepared_output.json",
              "r") as file_2:
        test_data_out = file_2.read()
    prepared_data = pd.DataFrame(json.loads(test_data_out), dtype=float)

    assert_frame_equal(produced_data, prepared_data)


@pytest.mark.parametrize(
    "input_file,quest,prepared_data",
    [
        ("tests/fixtures/test_iqr_sum_even_input.json",
         "movement_Q602_building_soft_sand",
         3.2758616305999997),
        ("tests/fixtures/test_iqr_sum_odd_input.json",
         "movement_Q605_concreting_gravel",
         0.9185712496)
    ])
def test_iqr_sum(input_file, quest, prepared_data):
    with open(input_file, "r") as file_1:
        test_data_in = file_1.read()
    input_data = pd.DataFrame(json.loads(test_data_in))

    produced_data = lambda_iqrs_method_function.iqr_sum(input_data, quest)

    assert produced_data == prepared_data


@pytest.mark.parametrize(
    "input_data,prepared_data",
    [
        ([{"A": 0, "B": 0, "C": 0}], True),
        ([{"A": 1, "B": 2, "C": 3}], False)
    ])
def test_do_check(input_data, prepared_data):

    quest = ["A", "B", "C"]
    working_dataframe = pd.DataFrame(input_data)

    working_dataframe["zero_data"] = working_dataframe.apply(
        lambda x: lambda_apply_wrangler_function.do_check(x, quest), axis=1)

    produced_data = working_dataframe["zero_data"][0]

    assert produced_data == prepared_data


@pytest.mark.parametrize(
    "columns,prepared_data",
    [
        ([{"column_name": "D", "data": {"A": "+", "B": "+", "C": "+"}}], 9),
        ([{"column_name": "D", "data": {"A": "+", "B": "+", "C": "-"}}], 1)
    ])
def test_sum_data_columns(columns, prepared_data):

    working_dataframe = pd.DataFrame(
        [
            {"A": 2, "B": 3, "C": 4, "D": 0}
        ]
    )

    working_dataframe = working_dataframe.apply(
        lambda x: lambda_apply_method_function.sum_data_columns(x, columns), axis=1)

    produced_data = working_dataframe[columns[0]["column_name"]][0]

    assert produced_data == prepared_data


def test_calc_atypicals():

    with open("tests/fixtures/test_calc_atypicals_input.json", "r") as file_1:
        test_data_in = file_1.read()
    input_data = pd.DataFrame(json.loads(test_data_in))

    quest_list = method_atypicals_runtime_variables[
        'RuntimeVariables']['questions_list']

    # Produce columns
    atypical_columns = lambda_imputation_function.produce_columns("atyp_", quest_list)
    movement_columns = lambda_imputation_function.produce_columns("movement_", quest_list)
    iqrs_columns = lambda_imputation_function.produce_columns("iqrs_", quest_list)
    mean_columns = lambda_imputation_function.produce_columns("mean_", quest_list)

    out_data = lambda_atypicals_method_function.calc_atypicals(
        input_data,
        atypical_columns,
        movement_columns,
        iqrs_columns,
        mean_columns
    )

    # This is for Int, Float mismatch correction.
    json_data = out_data.to_json(orient="records")
    produced_data = pd.DataFrame(json.loads(json_data), dtype=float)

    with open("tests/fixtures/test_calc_atypicals_prepared_output.json",
              "r") as file_2:
        test_data_out = file_2.read()
    prepared_data = pd.DataFrame(json.loads(test_data_out), dtype=float)

    assert_frame_equal(produced_data, prepared_data)


@pytest.mark.parametrize(
    "which_current,which_previous,answer",
    [
        (100, 10, 9),
        (6, 5, 0.2)
    ])
def test_movement_calculation_a(which_current, which_previous, answer):
    output = lambda_imputation_function.movement_calculation_a(
        which_current, which_previous)
    assert output == answer


@pytest.mark.parametrize(
    "which_current,which_previous,answer",
    [
        (100, 10, 10),
        (6, 5, 1.2)
    ])
def test_movement_calculation_b(which_current, which_previous, answer):
    output = lambda_imputation_function.movement_calculation_b(
        which_current, which_previous)
    assert output == answer


@pytest.mark.parametrize(
    "which_prefix,which_columns,which_additional,which_suffix,answer",
    [
        ("A_", ["One", "Two"], [], "",
         ['A_One', 'A_Two']),
        ("B_", ["One", "Two"], [], "_X",
         ['B_One_X', 'B_Two_X']),
        ("C_", ["One", "Two"], ["Three", "Four"], "",
         ['C_One', 'C_Two', 'Three', 'Four']),
        ("D_", ["One", "Two"], ["Three", "Four"], "_Z",
         ['D_One_Z', 'D_Two_Z', 'Three', 'Four'])
    ])
def test_produce_columns(which_prefix, which_columns, which_additional, which_suffix,
                         answer):
    output = lambda_imputation_function.produce_columns(which_prefix, which_columns,
                                                        which_additional, which_suffix)
    assert output == answer
