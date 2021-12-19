from market import get_prices


def get_prices_test(file_path, file_name):

    station, prices = get_prices(file_path, file_name)
    for f in ['timestamp', 'StationName', 'StarSystem']:
        print(f'{f}: {station[f]}')
    for item_price in prices:
        for f in ['Name_Localised', 'BuyPrice', 'SellPrice', 'Stock', 'Demand', 'Rare']:
            print(f'{f}: {item_price[f]}')


def main():
    # get_prices_test('C:\\!\\CODING\\ED\\J\\', 'Market.json')
    get_prices_test('C:\\Users\\alanm\\Saved Games\\Frontier Developments\\Elite Dangerous\\', 'Market.json')


if __name__ == '__main__':
    main()
