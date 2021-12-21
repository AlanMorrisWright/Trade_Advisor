from market import get_prices
from listings import get_listings_prices
from systems_populated import get_systems


def get_prices_test(file_path, file_name):

    station, prices = get_prices(file_path, file_name)
    for f in ['timestamp', 'StationName', 'StarSystem']:
        print(f'{f}: {station[f]}')
    for item_price in prices:
        for f in ['Name_Localised', 'BuyPrice', 'SellPrice', 'Stock', 'Demand', 'Rare']:
            print(f'{f}: {item_price[f]}')


def main():
    # station, prices = get_prices('C:\\Users\\alanm\\Saved Games\\Frontier Developments\\Elite Dangerous\\', 'Market.json')
    # systems = get_systems('C:\\!\\CODING\\ED\\EDDB_Data\\', 'systems_populated.jsonl')
    headings, data = get_listings_prices('C:\\!\\CODING\\ED\\EDDB_Data\\', 'listings_test.csv')
    print(headings)
    print(data)


if __name__ == '__main__':
    main()
