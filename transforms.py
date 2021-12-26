def change_type(val, value_type):
    # returns string converted to type specified

    if value_type == 'string':
        return val
    elif value_type == 'float':
        if val == 'null':
            return float(0)
        else:
            return float(val)
    elif value_type == 'integer':
        if val == 'null':
            return int(0)
        else:
            return int(val)
    elif value_type == 'boolean':
        if val == 'true' or val == 1:
            return True
        else:
            return False
    else:
        return val


def get_key_value(look_for, line):
    # looks for a string 'look_for[0]' in string 'line'
    #   find char where <look for> is found, wrapped within '"<look for>":'
    #   last char is next 'comma'
    #   remove quotes and split using ':'
    #   return the value, passing through type converter

    look_for_item = look_for[0]
    look_for_type = look_for[1]

    key_start_id = line.find('"' + look_for_item + '":')
    val_end_id = line.find(',', key_start_id)
    k, v = line[key_start_id:val_end_id].replace('"', '').split(':')
    return change_type(v, look_for_type)
