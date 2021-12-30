from time import time
from ETL import get_json
# from market import get_prices
# from allsystems import get_station_systems
from listings import get_listings_prices


def testing():
    g = get_json('commodities.json', 'C:\\!\\CODING\\ED\\EDDB_Data\\', '},{',
                 [['id', 'integer'], ['name', 'string'], ['is_rare', 'boolean']])
    print(g)

    g = get_json('systems_populated.jsonl', 'C:\\!\\CODING\\ED\\EDDB_Data\\', None,
                 [['id', 'integer'], ['name', 'string'], ['needs_permit', 'boolean'],
                  ['x', 'float'], ['y', 'float'], ['z', 'float']])
    print(g)

    g = get_json('stations.jsonl', 'C:\\!\\CODING\\ED\\EDDB_Data\\', None,
                 [['id', 'integer'], ['name', 'string'], ['system_id', 'integer'], ['updated_at', 'integer'],
                  ['max_landing_pad_size', 'string'], ['distance_to_star', 'integer'],
                  ['market_updated_at', 'string'], ['is_planetary', 'boolean']])
    print(g)

    # 'station_id', 'commodity_id', 'supply', 'buy_price', 'sell_price', 'demand', 'collected_at'
    prices = get_listings_prices('C:\\!\\CODING\\ED\\EDDB_Data\\', 'listings_test.csv')
    for price in prices:
        print(price)


def test2():
    # testing()
    # station, prices = get_prices('C:\\Users\\alanm\\Saved Games\\Frontier Developments\\Elite Dangerous\\', 'Market.json')

    commodities = get_json('commodities.json', 'C:\\!\\CODING\\ED\\EDDB_Data\\', '},{',
                           [['id', 'integer'], ['name', 'string'], ['is_rare', 'boolean']])
    systems = get_json('systems_populated.jsonl', 'C:\\!\\CODING\\ED\\EDDB_Data\\', None,
                       [['id', 'integer'], ['name', 'string'], ['needs_permit', 'boolean'],
                        ['x', 'float'], ['y', 'float'], ['z', 'float']])
    stations = get_json('stations.jsonl', 'C:\\!\\CODING\\ED\\EDDB_Data\\', None,
                        [['id', 'integer'], ['name', 'string'], ['system_id', 'integer'], ['updated_at', 'integer'],
                         ['max_landing_pad_size', 'string'], ['distance_to_star', 'integer'],
                         ['market_updated_at', 'string'], ['is_planetary', 'boolean']])
    # 'station_id', 'commodity_id', 'supply', 'buy_price', 'sell_price', 'demand', 'collected_at'
    prices = get_listings_prices('C:\\!\\CODING\\ED\\EDDB_Data\\', 'listings_test.csv')

    p = 300
    this_price = prices[p]
    print(f'intersting row {p}: {this_price}')

    commodity_id = this_price[1]
    this_commodity = commodities[commodity_id]

    station_id = this_price[0]
    this_station = stations[station_id]

    print(f'commodity_id {commodity_id}: {this_commodity}')
    print(f'station_id {station_id}: {this_station}')
    system_id = this_station[1]
    this_system = systems[system_id]
    print(f'system_id {system_id}: {this_system}')


def main2():
    start_seconds = time()
    # systems = get_json('systems_populated.jsonl', 'C:\\!\\CODING\\ED\\EDDB_Data\\', None,
    #                    [['id', 'integer'], ['name', 'string'], ['needs_permit', 'boolean'],
    #                     ['x', 'float'], ['y', 'float'], ['z', 'float']])
    systems = get_json('systems.jsonl', 'C:\\!\\CODING\\ED\\EDDB_Data\\', None,
                       [['id', 'integer'], ['name', 'string'], ['needs_permit', 'boolean'],
                        ['x', 'float'], ['y', 'float'], ['z', 'float']])
    stations = get_json('stations.jsonl', 'C:\\!\\CODING\\ED\\EDDB_Data\\', None,
                        [['id', 'integer'], ['name', 'string'], ['system_id', 'integer'], ['updated_at', 'integer'],
                         ['max_landing_pad_size', 'string'], ['distance_to_star', 'integer'],
                         ['market_updated_at', 'string'], ['is_planetary', 'boolean']])
    out_header = ['system_name', 'x', 'y', 'z',
                  'station_name', 'distance_to_star', 'pad_size']
    print(f'reading took: {time() - start_seconds:.1f}s')
    print(f'stations: {len(stations)}')
    print(type(stations))

    for station in stations:
        station_details = stations[station]
        if station == 290534:
            if station_details[3] == 'L' and station_details[4] < 100 and not station_details[6]:
                try:
                    system_details = systems[station_details[1]]
                except:
                    system_details = ['none']
                print(station,
                      station_details,
                      # [station_details[x] for x in (0, 4)],
                      system_details)

    print(systems[703])


def main():
    stations = get_json('stations.jsonl', 'C:\\!\\CODING\\ED\\EDDB_Data\\', 'notdictionary', None,
                        [['id', 'integer'], ['name', 'string'], ['system_id', 'integer'], ['updated_at', 'integer'],
                         ['max_landing_pad_size', 'string'], ['distance_to_star', 'integer'],
                         ['market_updated_at', 'string'], ['is_planetary', 'boolean']])
    for station in stations:
        # print(station, stations[station])
        print(station)

    # get_station_systems('C:\\!\\CODING\\ED\\EDDB_Data\\', 'systems.csv', stations)


if __name__ == '__main__':
    main()
