import csv
import time
import sqlite3


def count_rows(file_path, file_name):
    # returns count of rows
    # systems.csv shortcut whilst deving
    if file_name == 'systems.csv':
        return 49_817_746

    with open(file_path + file_name, newline='') as f:
        rows = sum(1 for row in f)
    return rows


def change_type(val, value_type):
    # returns string converted to type specified
    if value_type == 'int':
        if val == 'null':
            return int(0)
        else:
            return int(val)
    elif value_type == 'float':
        if val == 'null':
            return float(0)
        else:
            return float(val)
    elif value_type == 'bool':
        if val in ('0', 'false') or val == 0 or val is None:
            return 0
        else:
            return 1
    else:
        return val


def get_key_value(look_for, line, value_type):
    # todo: update this comments - not passing tuple now
    # looks for a string 'look_for[0]' in string 'line'
    #   find char where <look for> is found, wrapped within '"<look for>":'
    #   last char is next comma
    #   remove quotes and split using ':'
    #   return the value, passing through type converter

    key_start_id = line.find('"' + look_for + '":')
    val_end_id = line.find(',', key_start_id)
    k, v = line[key_start_id:val_end_id].replace('"', '').split(':')
    return change_type(v, value_type)


def get_json(file_name, file_path, return_type, split_chars, fields):
    # returns a dict or list (return_type) based on data from a json
    # looks for string in string (from fields) and returns value of it
    #   read the complete json into a list
    #   jsonl loads as line delimited, json just as a single element
    #   so split the single element (making multiple lines)
    #   once read then for each line look for fields and find values
    #   the dict key is element [0] which is the id

    with open(file_path + file_name) as f:
        if split_chars:
            lines = f.readlines()[0].split(split_chars)
        else:
            lines = f.readlines()

    if return_type == 'dictionary':
        results = {}
    else:
        results = []

    for line in lines:
        this = []
        for f in fields:
            this.append(get_key_value(f, line))
        if return_type == 'dictionary':
            results[this[0]] = this[1:]
        else:
            results.append(this)

    return results


def csv_to_db(file_path, file_name, db, tbl, fields):
    con = sqlite3.connect(db)
    cur = con.cursor()
    q = ('?, ' * len(fields[0]))[:-2]

    rows = count_rows(file_path, file_name)
    with open(file_path + file_name, newline='') as f:
        reader = csv.reader(f, delimiter=',', quotechar='"')
        all_headings = next(reader)
        headings = [all_headings[x] for x in fields[0]]
        reader = csv.reader(f, delimiter=',', quotechar='"')
        i = 0
        start_time = time.time()
        for data in reader:
            i += 1
            this = []
            for j, column_value in enumerate([data[x] for x in fields[0]]):
                this.append(change_type(column_value, fields[2][j]))
            if not i % 100_000:
                time_taken = time.time() - start_time
                total_time = time_taken * rows / i
                time_left = total_time - time_taken
                print(f'{i/1_000_000:.1f}mill: {100 * i / rows:.1f}%: {time_left:.1f}s:: {this}')
            cur.execute('INSERT INTO ' + tbl + ' VALUES (' + q + ')', this)
    con.commit()


def json_to_db(file_path, file_name, split_chars, db, tbl, fields):
    # todo: do this?... cur.executemany("insert into stations values (?, ?)", lang_list)
    # looks for string in string (from fields) and returns value of it
    #   read the complete json into a list
    #   jsonl loads as line delimited, json just as a single line
    #   so split the single line (making multiple lines)
    #   once read then for each line look for fields and find values
    #   the dict key is element [0] which is the id
    print(db)
    con = sqlite3.connect(db)
    cur = con.cursor()
    q = ('?, ' * len(fields[0]))[:-2]

    with open(file_path + file_name) as f:
        if split_chars:
            lines = f.readlines()[0].split(split_chars)
        else:
            lines = f.readlines()
    rows = len(lines)

    i = 0
    start_time = time.time()
    for line in lines:
        i += 1
        this = []
        for j, f in enumerate(fields[0]):
            # f: what to look for
            # line: where to look
            # val_type:
            # print(fields[1], j)
            value_type = fields[1][j]
            this.append(get_key_value(f, line, value_type))

        if not i % 100_000:
            time_taken = time.time() - start_time
            total_time = time_taken * rows / i
            time_left = total_time - time_taken
            print(f'{i/1_000_000:.1f}mill: {100 * i / rows:.1f}%: {time_left:.1f}s:: {this}')
        cur.execute('INSERT INTO ' + tbl + ' VALUES (' + q + ')', this)
    con.commit()
