import gl
from ETL import json_to_db, csv_to_db, list_to_csv


def table_headings(table_name):
    """
    returns column headings of a table
    eg.  headings = table_headings('stations')

    :param table_name:
    :return: column headings as list
    """
    con = gl.DB_CONNECTION
    xx = con.execute('select * from ' + table_name + ' where 1=0;')
    return [description[0] for description in xx.description]


def execute_sql(query_text):
    """
    executes query
    :return: query result object
    """
    results = gl.DB_CONNECTION.execute(query_text)
    gl.DB_CONNECTION.commit()
    return results


def etl_systems():
    """
    drops table first, then remakes and reads in data from 'systems.csv'
    :return: none
    """
    tbl = 'SYSTEMS'

    print(tbl + ': dropping..')
    execute_sql('drop table if exists ' + tbl + ';')

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
    execute_sql(q)

    print(tbl + ': fill')
    csv_to_db(gl.EDDB_PATH,
              'systems.csv',
              gl.DB,
              'systems',
              [(0, 2, 3, 4, 5, 19),
               ('id', 'name', 'x', 'y', 'z', 'needs_permit'),
               ('int', 'str', 'float', 'float', 'float', 'bool')])

    print(tbl + ': indexing..')
    execute_sql('create unique index systems_index on systems (id);')


def etl_listings():
    """
    drops table first, then remakes and reads in data from 'listings.csv'
    :return: none
    """
    tbl = 'LISTINGS'

    print(tbl + ': dropping..')
    execute_sql('drop table if exists ' + tbl + ';')

    print(tbl + ': create')
    q = """
        CREATE TABLE """ + tbl + """ (
        id int,
        station_id int,
        commodity_id int,
        supply int,
        buy_price int,
        sell_price int,
        demand int,
        collected_at int
        );
        """
    execute_sql(q)

    print(tbl + ': fill')
    csv_to_db(gl.EDDB_PATH,
              'listings.csv',
              gl.DB,
              'listings',
              [(0, 1, 2, 3, 5, 6, 7, 9),
               ('id', 'station_id', 'commodity_id', 'supply', 'buy_price', 'sell_price', 'demand', 'collected_at'),
               ('int', 'int', 'int', 'int', 'int', 'int', 'int', 'int')])

    print(tbl + ': indexing..')
    execute_sql('create unique index listings_index on ' + tbl + ' (station_id, commodity_id);')


def etl_commodities():
    """
    drops table first, then remakes and reads in data from 'commodities.json'
    :return: none
    """
    tbl = 'COMMODITIES'

    print(tbl + ': dropping..')
    execute_sql('drop table if exists ' + tbl + ';')

    print(tbl + ': create')
    q = """
        CREATE TABLE """ + tbl + """ (
        id int,
        name text,
        is_rare int);
        """
    execute_sql(q)

    print(tbl + ': fill')
    json_to_db(gl.EDDB_PATH,
               'commodities.json',
               ',{',
               gl.DB,
               'commodities',
               [['id', 'name', 'is_rare'],
                ['int', 'str', 'bool']])

    print(tbl + ': indexing..')
    execute_sql('create unique index commodity_index on commodities (id);')


def etl_stations():
    """
    drops table first, then remakes and reads in data from 'stations.jsonl'
    :return: none
    """
    tbl = 'STATIONS'

    print(tbl + ': dropping..')
    execute_sql('drop table if exists ' + tbl + ';')

    print(tbl + ': create')
    q = """
        CREATE TABLE stations (
        id int,
        name text,
        system_id int,
        max_landing_pad_size text,
        distance_to_star int,
        type text,
        is_planetary int,
        market_updated_at int,
        ed_market_id int
        );
        """
    execute_sql(q)

    print(tbl + ': fill')
    json_to_db(gl.EDDB_PATH,
               'stations.jsonl',
               None,
               gl.DB,
               'stations',
               [['id', 'name', 'system_id', 'max_landing_pad_size', 'distance_to_star', 'type', 'is_planetary',
                 'market_updated_at', 'ed_market_id'],
                ['int', 'str', 'int', 'str', 'int', 'str', 'bool', 'int', 'int']])

    print(tbl + ': indexing..')
    execute_sql('create unique index stations_index on stations (id);')


def create_stations_full():
    """
     drops table first, then remakes using data from tables 'stations' and 'systems'
     providing a table of statiosn and their star data
     only stations with...
       max_landing_pad_size = 'L'
       is_planetary = 0
       type <> 'Fleet Carrier'

    :return: none
    """
    tbl = 'STATIONS_FULL'

    print(tbl + ': dropping..')
    execute_sql('drop table if exists ' + tbl + ';')

    print(tbl + ': create as..')
    q = """
        CREATE TABLE """ + tbl + """ as
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
    execute_sql(q)

    print(tbl + ': indexing..')
    execute_sql('create unique index stations_full_index on stations_full (id);')


def create_near_stations(jump_range):
    """
    makes 'near_stations' with 2 columns representing station (ids) within a single hop
    :param jump_range:
    :return:
    """
    tbl = 'NEAR_STATIONS'

    print(tbl + ': dropping..')
    execute_sql('drop table if exists ' + tbl + ';')

    print(tbl + ': create as..')
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
    execute_sql(q)

    print(tbl + ': adding inbound route')
    q = """
        insert into """ + tbl + """ (id_a, id_b)
        select
        id_b, id_a
        from
        """ + tbl
    execute_sql(q)

    print(tbl + ': indexing..')
    execute_sql('create unique index near_stations_index on near_stations (id_a, id_b);')


def create_profits(cargo_capacity):
    """
    makes a table of near stations, and profit by commodity
      parse 'near_stations' - make outbound table of profitable commodities, where
        supply > 400
        demand > 400
        profit >= 15000
      parse 'outbound' table and make a table of inbound journey profit per commodity, where
        supply > 400
        demand > 400
        profit > 0  (might as well bring something back)
      make a table of the round trip, together with system and station stats

    :return: none
    """
    tbl = 'OUTBOUND'

    print(tbl + ': dropping..')
    execute_sql('drop table if exists ' + tbl + ';')

    print(tbl + ': create as..')
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
    execute_sql(q)

    tbl = 'INBOUND'

    print(tbl + ': dropping..')
    execute_sql('drop table if exists ' + tbl + ';')

    print(tbl + ': create as..')
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
    execute_sql(q)

    tbl = 'PROFITS'

    print(tbl + ': dropping..')
    execute_sql('drop table if exists ' + tbl + ';')

    print(tbl + ': create as..')
    q = """
        CREATE TABLE """ + tbl + """ as
        select

        o.station_id_ao as station_id_a,
        o.station_id_bo as station_id_b,
        coalesce(o.profit_o, 0) + coalesce(i.profit_i, 0) as profit,
        
        (coalesce(o.profit_o, 0) + coalesce(i.profit_i, 0)) * """ + str(cargo_capacity) + """ as profit_cr,

        max(sta.distance_to_star, stb.distance_to_star) as max_distance_to_star,

                       
        --1.0 * coalesce(o.profit_o, 0) / (coalesce(o.profit_o, 0) + coalesce(i.profit_i, 0)) as profit_o_prop,

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
        null as SC_time_a,

        --station b
        stb.name as system_name_b,
        stb.x as x_b, stb.y as y_b, stb.z as z_b, 
        stb.station_name as station_name_b,
        stb.distance_to_star as distance_to_star_b,
        null as SC_time_b

        from
        outbound o
        left join inbound i on i.station_id_ao = o.station_id_ao and i.station_id_bo = o.station_id_bo
        left join stations_full sta on sta.id = o.station_id_ao 
        left join stations_full stb on stb.id = o.station_id_bo
        ;
        """
    execute_sql(q)


def nearest_stations(my_x, my_y, my_z):
    """
     generates 'nearest_station.csv' with nearest 20 stations to x, y, z passed
     only stations with...
       is_planetary = 0
       type <> 'Fleet Carrier'

    :return: none
    """
    tbl = 'nearest_station'

    print(tbl + ': dropping..')
    execute_sql('drop table if exists ' + tbl + ';')

    print(tbl + ': create as..')
    q = """
        CREATE TABLE """ + tbl + """ as
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
        sy.needs_permit,
        
          (sy.x - (""" + str(my_x) + """)) * (sy.x - (""" + str(my_x) + """))
        + (sy.y - (""" + str(my_y) + """)) * (sy.y - (""" + str(my_y) + """))
        + (sy.z - (""" + str(my_z) + """)) * (sy.z - (""" + str(my_z) + """)) as dis2

        from
        stations st join
        systems sy on sy.id = st.system_id

        where
        st.is_planetary = 0 and
        st.type <> 'Fleet Carrier'

        order by
          (sy.x - (""" + str(my_x) + """)) * (sy.x - (""" + str(my_x) + """))
        + (sy.y - (""" + str(my_y) + """)) * (sy.y - (""" + str(my_y) + """))
        + (sy.z - (""" + str(my_z) + """)) * (sy.z - (""" + str(my_z) + """))

        limit 20

        """
    execute_sql(q)

    out_csv = [table_headings(tbl)]
    q = 'select * from ' + tbl + ';'
    result = execute_sql(q)
    out_csv.extend(list(result))
    list_to_csv(out_csv, gl.DB_PATH + tbl + '.csv')
