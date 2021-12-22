def change_type(val, value_type):
    """
    converts a string to different type
    :param val:
    :param value_type:
    :return:
    """

    match value_type:
        case 'string':
            return val
        case 'float':
            if val == 'null':
                return float(0)
            return float(val)
        case 'integer':
            if val == 'null':
                return int(0)
            return int(val)
        case 'boolean':
            if val == 'true' or val == 1:
                return True
            return False
        case _:
            return val


def get_key_value(look_for, line):
    """
    :param look_for: tuple of name, type
    :param line: string to search
    :return: value of a 'key' found, from a string
    """

    look_for_item = look_for[0]
    look_for_type = look_for[1]

    key_start_id = line.find('"' + look_for_item + '":')
    val_end_id = line.find(',', key_start_id)
    k, v = line[key_start_id:val_end_id].replace('"', '').split(':')
    return change_type(v, look_for_type)
