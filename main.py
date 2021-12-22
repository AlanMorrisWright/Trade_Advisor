from ETL import get_json
from market import get_prices
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


def main():
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


if __name__ == '__main__':
    main()
