def change_type(val, type):
    """
    converts a string to different type
    :param val:
    :param type:
    :return:
    """
    match type:
        case 'string':
            return val
        case 'float':
            return float(val)
        case 'boolean':
            return val == 'true'
        case _:
            return val


def get_key_val(look_for, line):
    """
    return dict key and value from a string
    :param look_for: tuple of name, type
    :param line:
    :return:
    """
    key_start_id = line.find('"' + look_for[0] + '":')
    val_end_id = line.find(',', key_start_id)
    k, v = line[key_start_id:val_end_id].replace('"', '').split(':')

    # todo: don't need a key, the overhead is not used - make it a list instead
    return dict([(k, change_type(v, look_for[1]))])


file_path = 'C:\\!\\CODING\\ED\\EDDB_Data\\'
file_name = 'systems_populated.jsonl'

with open(file_path + file_name) as f:
    # load the file
    lines = f.readlines()

systems = []
for line in lines:
    this_system = []
    for f in [['name', 'string'], ['x', 'float'], ['y', 'float'], ['z', 'float'], ['needs_permit', 'boolean']]:
        this_system.append(get_key_val(f, line))
    systems.append(this_system)

xx=systems[:10]
for x in xx:
    print(x)
