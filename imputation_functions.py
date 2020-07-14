from types import SimpleNamespace

import pandas as pd
from marshmallow import EXCLUDE, Schema, fields


class FactorsSchema(Schema):
    region_column = fields.Str(required=True)
    regional_mean = fields.Str(required=False)
    regionless_code = fields.Int(required=True)
    survey_column = fields.Str(required=True)


class FactorsCalculationASchema(FactorsSchema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        raise ValueError(f"Error validating runtime params: {e}")

    first_imputation_factor = fields.Int()
    first_threshold = fields.Int()
    percentage_movement = fields.Bool(required=True)
    second_imputation_factor = fields.Int()
    second_threshold = fields.Int()
    third_threshold = fields.Int(required=True)


class FactorsCalculationBSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        raise ValueError(f"Error validating runtime params: {e}")

    threshold = fields.Int(required=True)


class ExtendedFactorsCalculationASchema(FactorsCalculationASchema):
    distinct_values = fields.List(fields.String, required=True)
    third_imputation_factors = fields.Field(required=True)


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


def factors_calculation_a(row, questions, **kwargs):
    """
    Calculates the imputation factors for the DataFrame on row by row basis.
    - Calculates imputation factor for each question, in each aggregated group,
      by:
        Region
        Land or Marine (If applicable)
        Count of refs within cell

    :param row: row of DataFrame
    :param questions: question names in columns
    :param kwargs: A dictionary of the following parameters:
        - first_threshold: One of three thresholds to compare the question count to.
        - second_threshold: One of three thresholds to compare the question count to.
        - third_threshold: One of three thresholds to compare the question count to.
        - first_imputation_factor: One of three factors to be assigned to the question.
        - second_imputation_factor: One of three factors to be assigned to the question.
        - third_imputation_factors: A Dataframe containing factors
        to be assigned to the questions.
        - region_column: The name of the column that holds region.
        - regionless_code: The value used as 'all GB' in the 'region_column'
        - survey_column: Column name of the dataframe containing the survey code.
        - percentage_movement: Indicates if percentage movement was used
        - distinct_values: Array of column names to derive distinct values from
                           and store in table. - Type: List

    :return: row of DataFrame
    """

    runtime_object = SimpleNamespace(**kwargs)

    for question in questions:
        if row[runtime_object.region_column] == runtime_object.regionless_code:
            if row[runtime_object.survey_column] == "066":
                if row["movement_" + question + "_count"] < int(
                        runtime_object.first_threshold):
                    row["imputation_factor_" + question] =\
                        float(runtime_object.first_imputation_factor)
                else:
                    row["imputation_factor_" + question] =\
                        float(pd.to_numeric(row["mean_" + question]))
            elif row[runtime_object.survey_column] == "076":
                if row["movement_" + question + "_count"] < int(
                        runtime_object.second_threshold):
                    row["imputation_factor_" + question] =\
                        float(runtime_object.second_imputation_factor)
                else:
                    row["imputation_factor_" + question] =\
                        float(pd.to_numeric(row["mean_" + question]))
            else:
                row["imputation_factor_" + question] = 0
            # check if the imputation factor needs to be adjusted
            if runtime_object.percentage_movement:
                row["imputation_factor_" + question] = \
                    row["imputation_factor_" + question] + 1
        else:
            if row["movement_" + question + "_count"] < int(
                    runtime_object.third_threshold):
                factor_filter = ""

                # Ignore region in matching columns (we need to find the all-gb data)
                if runtime_object.region_column in runtime_object.distinct_values:
                    runtime_object.distinct_values.remove(runtime_object.region_column)

                if len(runtime_object.distinct_values) < 1:
                    row["imputation_factor_" + question] = \
                        float(pd.to_numeric(
                            runtime_object.third_imputation_factors[
                                "imputation_factor_" + question].take([0])))
                else:
                    # Find the correct mean (region or region+strata handling)
                    for value in runtime_object.distinct_values:
                        if value != runtime_object.distinct_values[0]:
                            factor_filter += " & "
                        factor_filter += "(%s == '%s')" % (value, row[value])

                    row["imputation_factor_" + question] = \
                        float(pd.to_numeric(
                            runtime_object.third_imputation_factors.query(
                                str(factor_filter))["imputation_factor_" + question]
                            .take([0])))

            else:
                row["imputation_factor_" + question] =\
                    float(pd.to_numeric(row["mean_" + question]))

                # check if the imputation factor needs to be adjusted
                if runtime_object.percentage_movement:
                    row["imputation_factor_" + question] =\
                        row["imputation_factor_" + question] + 1

    return row


def factors_calculation_b(row, questions, **kwargs):
    """
    Calculates the imputation factors for the DataFrame on row by row basis.
    - Calculates imputation factor for each question, in each aggregated group,
      by:
        Count of refs within cell

    :param row: row of DataFrame
    :param questions: A list of question names
    :param kwargs: A dictionary of the following parameters:
        - threshold: The threshold to compare the question count to.

    :return: row of DataFrame
    """
    runtime_object = SimpleNamespace(**kwargs)

    for question in questions:
        if row["movement_" + question + "_count"] < int(runtime_object.threshold):
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
