import gl
import csv
import time
from calendar import timegm
import sqlite3


def list_to_csv(this_list, destination):
    with open(destination, 'w', newline='\n') as f:
        writer = csv.writer(f, delimiter=',', quotechar='\"', quoting=csv.QUOTE_MINIMAL)
        writer.writerows(this_list)


def row_count(file_path, file_name, force):
    # returns count of rows
    # systems.csv shortcut whilst deving, use this to get rows..
    #   print(row_count(gl.EDDB_PATH, 'systems.csv', True))
    if not force:
        if file_name == 'systems.csv':
            return 50_253_669

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
        # sqlite3 does not have a bool type, so use 0/1
        if val in ('0', 'false') or val == 0 or val is None:
            return 0
        else:
            return 1
    elif value_type == 'sec1970':
        return timegm(time.strptime(val, '%Y-%m-%dT%H:%M:%SZ'))
    else:
        return val


def key_value(look_for, line, value_type):
    # todo: update this comments - not passing tuple now
    # looks for a string 'look_for[0]' in string 'line'
    #   find char where <look for> is found, wrapped within '"<look for>":'
    #   last char is next comma
    #   remove quotes and split using ':'
    #   return the value, passing through type converter

    key_start_id = line.find('"' + look_for + '":')
    # replace the curley bracket as last item in line does not precede a comma
    val_end_id = line.replace('}', ',').find(',', key_start_id)
    # added replace ": to | and " to null for values with : in them..
    # ..hope it still works for the EDDB datafiles loading!
    if key_start_id > -1 and val_end_id > -1:
        k, v = line[key_start_id:val_end_id].replace('":', '|').replace('"', '').split('|')
        return change_type(v, value_type)


def json_to_list_or_dict(file_name, file_path, return_type, split_chars, fields):
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
            this.append(key_value(f, line))
        if return_type == 'dictionary':
            results[this[0]] = this[1:]
        else:
            results.append(this)

    return results


def csv_to_db(file_path, file_name, db, tbl, fields):
    con = sqlite3.connect(db)
    cur = con.cursor()
    q = ('?, ' * len(fields[0]))[:-2]

    rows = row_count(file_path, file_name, False)
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
            this.append(key_value(f, line, value_type))

        if not i % 100_000:
            time_taken = time.time() - start_time
            total_time = time_taken * rows / i
            time_left = total_time - time_taken
            print(f'{i/1_000_000:.1f}mill: {100 * i / rows:.1f}%: {time_left:.1f}s:: {this}')
        cur.execute('INSERT INTO ' + tbl + ' VALUES (' + q + ')', this)
    con.commit()
