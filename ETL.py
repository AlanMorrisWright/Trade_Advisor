from transforms import get_key_value


def get_json(file_name, file_path, split_chars, fields):
    # returns a dict based on data from a json
    # looks for string in string (from fields) and returns value of it
    #   read the complete json into a list
    #   jsonl loads as line delimited, json just as a single element
    #   so split the single element (making multiple lines)
    #   once read then for each line look for fields and find values
    #    the dict key is the [0] which is the id

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
