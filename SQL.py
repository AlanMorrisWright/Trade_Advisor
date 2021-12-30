import gl
import csv
import sqlite3
import time
from ETL import json_to_db, csv_to_db


def list_to_csv(this_list):
    with open('c:\\!\\stations.csv', 'w', newline='\n') as f:
        writer = csv.writer(f, delimiter=',', quotechar='\"', quoting=csv.QUOTE_MINIMAL)
        writer.writerows(this_list)


def table_headings(table_name):
    con = gl.DB_CONNECTION
    xx = con.execute('select * from ' + table_name + ' where 1=0;')
    return [description[0] for description in xx.description]


def sql(query_text):
    xx = gl.DB_CONNECTION.execute(query_text)
    gl.DB_CONNECTION.commit()
    return xx


def create_and_fill_stations():
    tbl = 'STATIONS'

    print(tbl + ': drop')
    sql('drop table stations;')

    print(tbl + ': create')
    q = """
        CREATE TABLE stations (
        id int,
        name text,
        system_id int,
        max_landing_pad_size text,
        distance_to_star int,
        type text,
        is_planetary int);
        """
    sql(q)

    print(tbl + ': fill')
    json_to_db(gl.EDDB_PATH,
               'stations.jsonl',
               None,
               gl.DB,
               'stations',
               [['id', 'name', 'system_id', 'max_landing_pad_size', 'distance_to_star', 'type', 'is_planetary'],
                ['int', 'str', 'int', 'str', 'int', 'str', 'bool']])

    print(tbl + ': index')
    sql('create unique index stations_index on stations (id);')


def create_and_fill_systems():
    tbl = 'SYSTEMS'

    print(tbl + ': drop')
    sql('drop table systems;')

    print(tbl + ': create')
    q = """
        CREATE TABLE systems (
        id int,
        name char (255),
        x DOUBLE (15, 3),
        y DOUBLE (15, 3),
        z DOUBLE (15, 3),
        needs_permit int);
        """
    sql(q)

    print(tbl + ': fill')
    csv_to_db(gl.EDDB_PATH,
              'systems.csv',
              'C:\\!\\CODING\\ED\\EDDB_Data\\moz.db',
              'systems',
              [(0, 2, 3, 4, 5, 19),
               ('id', 'name', 'x', 'y', 'z', 'needs_permit'),
               ('int', 'str', 'float', 'float', 'float', 'bool')])

    print(tbl + ': index')
    sql('create unique index systems_index on systems (id);')


def create_stations_full():
    tbl = 'STATIONS_FULL'

    print(tbl + ': drop')
    sql('drop table stations_full;')

    print(tbl + ': create as')
    q = """
        CREATE TABLE stations_full as
        select
        st.id,
        st.name as station_name,
        st.max_landing_pad_size,
        st.distance_to_star,
        st.type,
        st.is_planetary,

        st.system_id,
        sy.name,
        sy.x,
        sy.y,
        sy.z,
        sy.needs_permit

        from
        stations st join
        systems sy on sy.id = st.system_id
        where
        st.max_landing_pad_size = 'L' and
        st.is_planetary = 0 and
        st.type <> 'Fleet Carrier'
        """
    sql(q)

    print(tbl + ': index')
    sql('create unique index stations_full_index on stations_full (id);')


def create_near_stations():
    tbl = 'NEAR_STATIONS'

    print(tbl + ': drop')
    sql('drop table NEAR_stations;')

    print(tbl + ': create as')
    q = """
        CREATE TABLE near_stations as
        select
        t1.id as id_from, t2.id as id_to
        from
        stations_full t1,
        stations_full t2
        where t2.id > t1.id and
          (t2.x - t1.x) * (t2.x - t1.x)
        + (t2.y - t1.y) * (t2.y - t1.y)
        + (t2.z - t1.z) * (t2.z - t1.z) < 100
        """
    sql(q)

    print(tbl + ': index')
    sql('create unique index near_stations_index on near_stations (id_from, id_to);')


result = sql("""select * from stations_full;""")
result = sql("""select * from near_stations ns join stations_full s on s.id = ns.id_from;""")
result_list = list(result)
for x in result_list:
    print(x)

print(len(result_list))
