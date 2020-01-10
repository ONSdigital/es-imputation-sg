
import pandas as pd


def movement_calculation_a(current_value, previous_value):
    """
    Movements calculation for Sand and Gravel.
    :param current_value: The current value for the current period - Type: Integer(?)
    :param previous_value: The current value for the previous period - Type: Integer(?)
    :return: Calculation value - Type: Integer(?)
    """
    number = (current_value - previous_value) / previous_value
    return number


def movement_calculation_b(current_value, previous_value):
    """
    Movements calculation for Bricks/Blocks.
    :param current_value: The current value for the current period - Type: Integer(?)
    :param previous_value: The current value for the previous period - Type: Integer(?)
    :return: Calculation value - Type: Integer(?)
    """
    number = current_value / previous_value
    return number


def factors_calcualtion_a(row, question, parameters):
    """
    Calculates the imputation factors for the DataFrame on row by row basis.
    - Calculates imputation factor for each question, in each aggregated group,
      by:
        Region
        Land or Marine (If applicable)
        Count of refs within cell

    :param row: row of DataFrame
    :param question: question
    :param parameters: A dictionary of the following parameters:
        - first_threshold: One of three thresholds to compare the question count to.
        - second_threshold: One of three thresholds to compare the question count to.
        - third_threshold: One of three thresholds to compare the question count to.
        - first_imputation_factor: One of three factors to be assigned to the question.
        - second_imputation_factor: One of three factors to be assigned to the question.
        - third_imputation_factor: One of three factors to be assigned to the question.
        - region_column: The name of the column that holds region.
        - regionless_code: The value used as 'all GB' in the 'region_column'
        - survey_column: Column name of the dataframe containing the survey code.
        - percentage_movement: Indicates if percentage movement was used

    :return: row of DataFrame
    """
    # extract the parameters:
    first_threshold = parameters["first_threshold"]
    second_threshold = parameters["second_threshold"]
    third_threshold = parameters["third_threshold"]
    first_imputation_factor = parameters["first_imputation_factor"]
    second_imputation_factor = parameters["second_imputation_factor"]
    third_imputation_factor = parameters["third_imputation_factor"]
    region_column = parameters["region_column"]
    regionless_code = parameters["regionless_code"]
    survey_column = parameters["survey_column"]
    percentage_movement = parameters["percentage_movement"]

    if row[region_column] == regionless_code:
        if row[survey_column] == "066":
            if row["movement_" + question + "_count"] < int(first_threshold):
                row["imputation_factor_" + question] = float(first_imputation_factor)
            else:
                row["imputation_factor_" + question] =\
                    float(pd.to_numeric(row["mean_" + question]))
        elif row[survey_column] == "076":
            if row["movement_" + question + "_count"] < int(second_threshold):
                row["imputation_factor_" + question] = float(second_imputation_factor)
            else:
                row["imputation_factor_" + question] =\
                    float(pd.to_numeric(row["mean_" + question]))
        else:
            row["imputation_factor_" + question] = 0
    else:
        if row["movement_" + question + "_count"] < int(third_threshold):
            row["imputation_factor_" + question] =\
                float(pd.to_numeric(third_imputation_factor))
        else:
            row["imputation_factor_" + question] =\
                float(pd.to_numeric(row["mean_" + question]))

        # check if the imputation factor needs to be adjusted
        if percentage_movement:
            row["imputation_factor_" + question] =\
                row["imputation_factor_" + question] + 1

    return row


def factors_calcualtion_b(row, question, parameters):
    """
    Calculates the imputation factors for the DataFrame on row by row basis.
    - Calculates imputation factor for each question, in each aggregated group,
      by:
        Count of refs within cell

    :param row: row of DataFrame
    :param question: question
    :param parameters: A dictionary of the following parameters:
        - threshold: The threshold to compare the question count to.

    :return: row of DataFrame
    """
    # extract the parameters:
    threshold = parameters["threshold"]

    if row["movement_" + question + "_count"] < int(threshold):
        row["imputation_factor_" + question] = row["mean_" + question]
    else:
        row["imputation_factor_" + question] = 0

    return row


def produce_columns(prefix, columns, additional=[], suffix=""):
    """
    Produces columns with a prefix, based on standard columns.
    :param prefix: String to be prepended to column name - Type: String
    :param columns: List of columns - Type: List
    :param additional: Any additional columns to be added on - Type: List
    :param suffix: String to be appended to column name - Type: String

    :return: List of column names with desired prefix - Type: List
    """
    new_columns = []
    for column in columns:
        new_value = "%s%s%s" % (prefix, column, suffix)
        new_columns.append(new_value)

    new_columns = new_columns + additional

    return new_columns
