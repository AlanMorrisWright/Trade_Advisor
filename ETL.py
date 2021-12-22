from transforms import get_key_value


def get_json(file_name, file_path, split_chars, fields):
    with open(file_path + file_name) as f:
        if split_chars:
            lines = f.readlines()[0].split(split_chars)
        else:
            lines = f.readlines()

    results = {}
    for line in lines:
        this = []
        for f in fields:
            this.append(get_key_value(f, line))
        results[this[0]] = this[1:]
    return results
