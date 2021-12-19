def get_key_val(look_for, line):
    """
    return dict key and value from a string
    :param look_for:
    :param line:
    :return:
    """
    key_start_id = line.find('"' + look_for + '":')
    val_end_id = line.find(',', key_start_id)
    k, v = line[key_start_id:val_end_id].replace('"', '').split(':')
    return dict([(k, v)])


file_path = 'C:\\!\\CODING\\ED\\EDDB_Data\\'
file_name = 'systems_populated.jsonl'

with open(file_path + file_name) as f:
    # load the file
    lines = f.readlines()

systems = []
for line in lines:
    for f in ['name', 'x', 'y', 'z', 'needs_permit']:
        systems.append(get_key_val(f, line))

print(systems)
