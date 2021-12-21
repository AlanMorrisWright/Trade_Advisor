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
            return float(val)
        case 'boolean':
            return val == 'true'
        case _:
            return val


def get_key_val(look_for, line):
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


def get_systems(file_path, file_name):
    """
    grabs system name, co-ords and permit requirements
    :param file_path:
    :param file_name:
    :return:
    """

    # load the file completely
    with open(file_path + file_name) as f:
        lines = f.readlines()

    # systems ..[name, x, y, z, permit]
    systems = []
    for line in lines:
        this_system = []
        for f in [['name', 'string'], ['x', 'float'], ['y', 'float'], ['z', 'float'], ['needs_permit', 'boolean']]:
            this_system.append(get_key_val(f, line))
        systems.append(this_system)
    return systems
