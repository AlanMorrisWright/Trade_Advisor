import sqlite3
import time


def create_stations():
    con = sqlite3.connect('C:\\!\\CODING\\ED\\EDDB_Data\\moz.db')
    cur = con.cursor()
    qry = 'drop table stations;'
    cur.execute(qry)
    qry = 'CREATE TABLE stations (' \
          'id int, ' \
          'name text, ' \
          'system_id int, ' \
          'max_landing_pad_size text, ' \
          'distance_to_star int, ' \
          'is_planetary text)'
    cur.execute(qry)


def create_systems():
    con = sqlite3.connect('C:\\!\\CODING\\ED\\EDDB_Data\\moz.db')
    cur = con.cursor()
    qry = 'drop table systems;'
    cur.execute(qry)
    qry = 'CREATE TABLE systems (' \
          'id int, ' \
          'name char (255), ' \
          'x DOUBLE (15, 3), ' \
          'y DOUBLE (15, 3), ' \
          'z DOUBLE (15, 3), ' \
          'needs_permit int)'
    cur.execute(qry)
    con.commit()


def set_index():
    con = sqlite3.connect('C:\\!\\CODING\\ED\\EDDB_Data\\moz.db')
    cur = con.cursor()
    cur.execute('create unique index systems_index on systems (id);')
    cur.execute('create unique index stations_index on stations (id);')
    con.commit


def test1():
    con = sqlite3.connect('C:\\!\\CODING\\ED\\EDDB_Data\\moz.db')
    cur = con.cursor()
    qry = """
    select sy1.*, sy2.*, (
      (sy2.x - sy1.x) * (sy2.x - sy1.x)
    + (sy2.y - sy1.y) * (sy2.y - sy1.y)
    + (sy2.z - sy1.z) * (sy2.z - sy1.z)) as dis2
    from systems sy1, systems sy2 where sy2.id > sy1.id and
      (sy2.x - sy1.x) * (sy2.x - sy1.x)
    + (sy2.y - sy1.y) * (sy2.y - sy1.y)
    + (sy2.z - sy1.z) * (sy2.z - sy1.z) < 100
    """

    start_time = time.time()
    xx = cur.execute(qry)
    print(f'{time.time() - start_time:.1f}s')
    i = 0
    for x in xx:
        if i <= 100:
            print(x)
        i += 1



test1()

# sy1.max_landing_pad_size = 'L' and
# sy2.max_landing_pad_size = 'L' and

