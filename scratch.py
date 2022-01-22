import SQL
import gl


2 stations 2 hops apart

q = """
    select
    s1.id_a,
    sf1.x, sf1.y, sf1.z,
    s1.id_b,
    sf2.x, sf2.y, sf2.z,
    s2.id_b,
    sf3.x, sf3.y, sf3.z
    from
      NEAR_STATIONS s1
    join NEAR_STATIONS s2 on s2.id_a = s1.id_b
    join STATIONS_FULL sf1 on sf1.id = s1.id_a
    join STATIONS_FULL sf2 on sf2.id = s2.id_a
    join STATIONS_FULL sf3 on sf3.id = s2.id_b

    where
    s2.id_b <> s1.id_a
    ;
    """

xx = list(SQL.execute_sql(q))

for x in xx:
    print(x)

# print(SQL.table_headings('NEAR_STATIONS'))

