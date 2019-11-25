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
