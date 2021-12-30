import gl
import csv
import sqlite3
import time
from ETL import json_to_db, csv_to_db


def list_to_csv(this_list, destination):
    with open(destination, 'w', newline='\n') as f:
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


def create_and_fill_commodities():
    tbl = 'COMMODITIES'

    print(tbl + ': drop')
    sql('drop table if exists ' + tbl + ';')

    print(tbl + ': create')
    q = """
        CREATE TABLE """ + tbl + """ (
        id int,
        name text,
        is_rare int);
        """
    sql(q)

    print(tbl + ': fill')
    json_to_db(gl.EDDB_PATH,
               'commodities.json',
               ',{',
               gl.DB,
               'commodities',
               [['id', 'name', 'is_rare'],
                ['int', 'str', 'bool']])

    print(tbl + ': index')
    sql('create unique index commodity_index on commodities (id);')


def create_and_fill_stations():
    tbl = 'STATIONS'

    print(tbl + ': drop')
    sql('drop table if exists ' + tbl + ';')

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
    sql('drop table if exists ' + tbl + ';')

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
              gl.DB,
              'systems',
              [(0, 2, 3, 4, 5, 19),
               ('id', 'name', 'x', 'y', 'z', 'needs_permit'),
               ('int', 'str', 'float', 'float', 'float', 'bool')])

    print(tbl + ': index')
    sql('create unique index systems_index on systems (id);')


def create_and_fill_listings():
    tbl = 'LISTINGS'

    print(tbl + ': drop')
    sql('drop table if exists ' + tbl + ';')

    print(tbl + ': create')
    q = """
        CREATE TABLE """ + tbl + """ (
        station_id int,
        commodity_id int,
        supply int,
        buy_price int,
        sell_price int,
        demand int);
        """
    sql(q)

    print(tbl + ': fill')
    csv_to_db(gl.EDDB_PATH,
              'listings.csv',
              gl.DB,
              'listings',
              [(1, 2, 3, 5, 6, 7),
               ('station_id', 'commodity_id', 'supply', 'buy_price', 'sell_price', 'demand'),
               ('int', 'int', 'int', 'int', 'int', 'int')])

    print(tbl + ': index')
    sql('create unique index listings_index on ' + tbl + ' (station_id, commodity_id);')


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
    jump_range = 30

    print(tbl + ': drop')
    sql('drop table if exists ' + tbl + ';')

    print(tbl + ': create as')
    q = """
        CREATE TABLE """ + tbl + """ as
        select
        t1.id as id_a, t2.id as id_b
        from
        stations_full t1,
        stations_full t2
        where
        t2.id > t1.id and
          (t2.x - t1.x) * (t2.x - t1.x)
        + (t2.y - t1.y) * (t2.y - t1.y)
        + (t2.z - t1.z) * (t2.z - t1.z) < """ + str(jump_range**2)
    sql(q)

    print(tbl + ': adding inbound route')
    q = """
        insert into """ + tbl + """ (id_a, id_b)
        select
        id_b, id_a
        from
        """ + tbl
    sql(q)

    print(tbl + ': index')
    sql('create unique index near_stations_index on near_stations (id_a, id_b);')


def create_outbound():
    tbl = 'OUTBOUND'

    print(tbl + ': drop')
    sql('drop table if exists ' + tbl + ';')

    print(tbl + ': create as')
    q = """
        CREATE TABLE """ + tbl + """ as
        select

        ns.id_a as station_id_ao,
        ns.id_b as station_id_bo,

        -- outbound
        c.name as commodity_o,
        la.supply as supply_o, la.buy_price as buy_price_o,
        lb.demand as demand_o, lb.sell_price as sell_price_o,
        lb.sell_price - la.buy_price as profit_o

        from
        near_stations ns
        join listings la on la.station_id = ns.id_a
        join listings lb on lb.station_id = ns.id_b
        join commodities c on c.id = la.commodity_id

        where
        la.commodity_id = lb.commodity_id and
        la.supply > 400 and
        lb.demand > 400 and
        lb.sell_price - la.buy_price >= 15000
        ;
        """
    sql(q)


def create_inbound():
    tbl = 'INBOUND'

    print(tbl + ': drop')
    sql('drop table if exists ' + tbl + ';')

    print(tbl + ': create as')
    q = """
        CREATE TABLE """ + tbl + """ as
        select

        o.station_id_ao,
        o.station_id_bo,

        c.name as commodity_i,
        lb.supply as supply_i, lb.buy_price as buy_price_i,
        la.demand as demand_i, la.sell_price as sell_price_i,
        la.sell_price - lb.buy_price as profit_i

        from
        outbound o
        join listings la on la.station_id = o.station_id_ao
        join listings lb on lb.station_id = o.station_id_bo
        join commodities c on c.id = la.commodity_id

        where
        la.commodity_id = lb.commodity_id and
        lb.supply > 400 and
        la.demand > 400 and
        la.sell_price > lb.buy_price
        ;
        """
    sql(q)


def create_profits():
    tbl = 'PROFITS'

    print(tbl + ': drop')
    sql('drop table if exists ' + tbl + ';')

    print(tbl + ': create as')
    q = """
        CREATE TABLE """ + tbl + """ as
        select

        o.station_id_ao as station_id_a,
        o.station_id_bo as station_id_b,
        coalesce(o.profit_o, 0) + coalesce(i.profit_i, 0) as profit,
        1.0 * coalesce(o.profit_o, 0) / (coalesce(o.profit_o, 0) + coalesce(i.profit_i, 0)) as profit_o_prop,

        -- outbound
        o.commodity_o,
        o.supply_o, o.buy_price_o,
        o.demand_o, o.sell_price_o,
        o.profit_o,

        --inbound
        i.commodity_i,
        i.supply_i, i.buy_price_i,
        i.demand_i, i.sell_price_i,
        i.profit_i,

        -- station a
        sta.name as system_name_a,
        sta.x as x_a, sta.y as y_a, sta.z as z_a, 
        sta.station_name as station_name_a,
        sta.distance_to_star as distance_to_star_a,

        --station b
        stb.name as system_name_b,
        stb.x as x_b, stb.y as y_b, stb.z as z_b, 
        stb.station_name as station_name_b,
        stb.distance_to_star as distance_to_star_b

        from
        outbound o
        left join inbound i on i.station_id_ao = o.station_id_ao and i.station_id_bo = o.station_id_bo
        left join stations_full sta on sta.id = o.station_id_ao 
        left join stations_full stb on stb.id = o.station_id_bo
        ;
        """
    sql(q)


# done - create_near_stations()
create_outbound()
create_inbound()
create_profits()
tbl = 'profits'
out_csv = [table_headings(tbl)]
q = 'select * from ' + tbl + ';'
result = sql(q)
out_csv.extend(list(result))
list_to_csv(out_csv, gl.DB_PATH + tbl + '.csv')
