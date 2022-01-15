import gl
from SQL import table_headings


def main():
    print(table_headings('stations'))


if __name__ == '__main__':
    main()




def prep_out():
    create_near_stations(30)


def out():
    create_outbound()
    create_inbound()
    create_profits()
    tbl = 'profits'
    out_csv = [table_headings(tbl)]
    q = 'select * from ' + tbl + ';'
    result = execute_sql(q)
    out_csv.extend(list(result))
    list_to_csv(out_csv, gl.DB_PATH + tbl + '.csv')


def load_new_eddb():
    create_and_fill_systems()
    create_and_fill_listings()
    create_and_fill_commodities()
    create_and_fill_stations()
    create_stations_full()


# load_new_eddb()
# prep_out()
# out()

# functions:
# table_headings(table_name)
# execute_sql(query_text)

# create_and_fill_systems()
# create_and_fill_listings()
# create_and_fill_commodities()
# create_and_fill_stations()
# create_stations_full()

# create_near_stations(jump_range)
# create_outbound()
# create_inbound()
# create_profits()


# tbl = 'xxx'
#
# print(tbl + ': dropping..')
# execute_sql('drop table if exists ' + tbl + ';')
#
# print(tbl + ': create as..')
# q = """
#     CREATE TABLE """ + tbl + """ as
#     select distinct
#
#     l.collected_at,
#     st.market_updated_at,
#     l.collected_at - st.market_updated_at as delta
#
#     from
#     listings l
#     join stations st on st.id = l.station_id
#
#     where
#     l.collected_at > st.market_updated_at + 60 or
#     l.collected_at < st.market_updated_at - 60
#
#     ;
#     """
# execute_sql(q)
# out_csv = [table_headings(tbl)]
# q = 'select * from ' + tbl + ';'
# result = execute_sql(q)
# out_csv.extend(list(result))
# list_to_csv(out_csv, gl.DB_PATH + tbl + '.csv')

# def market_field_names():
#     q = """
#         select * from stations
#         where
#     --    id = 67060
#         ed_market_id = 128780753  -- MarketID in market.json
#
#         ;
#         """
#     q = """
#         select * from listings where station_id = 67070
#         ;
#         """
#
q = """
select * from systems order by (x-(-7796.9044261143))*(x-(-7796.9044261143))    +(y-(-1769.95145690901))*(y-(-1769.95145690901))    +(z-(16201.9713029544))*(z-(16201.9713029544)) limit 1

     ;"""

#from math import sqrt, floor#

#def dis(system_2, system_1):
#    x1, y1, z1 = system_1
#    x2, y2, z2 = system_2
#    return sqrt((x2-x1)**2 + (y2-y1)**2 + (z2-z1)**2)


#start = (-7796.90, -1769.95, 16201.97)
#end = (84.56, -1012.88, 10.44)
#dis = dis(start, end)
#unit_back_vector = ((end[0]-start[0])/dis, (end[1]-start[1])/dis, (end[2]-start[2])/dis)

#print(dis, unit_back_delta)

#waypoints = []
#start_waypoint = floor(18067 / 2000) * 2000
#for waypoint in range(start_waypoint, -1, -2000):
#    xx = end[0] - waypoint * unit_back_vector[0]
#    yy = end[1] - waypoint * unit_back_vector[1]
#    zz = end[2] - waypoint * unit_back_vector[2]
#    print(waypoint, xx, yy, zz)
#    q = """
#        select * from systems order by
#         (x-(""" + str(xx) + """))*(x-(""" + str(xx) + """))
#        +(y-(""" + str(yy) + """))*(y-(""" + str(yy) + """))
#        +(z-(""" + str(zz) + """))*(z-(""" + str(zz) + """)) limit 1;
#        """
#    waypoints.append(list(execute_sql(q)))

#for waypoint in waypoints:
#    print(waypoint)
