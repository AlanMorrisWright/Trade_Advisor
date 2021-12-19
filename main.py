from market import get_prices


def main():
    station, prices = get_prices('C:\\!\\CODING\\ED\\J\\', 'Market.json')
    for f in ['timestamp', 'StationName', 'StarSystem']:
        print(f'{f}: {station[f]}')
    for item_price in prices:
        for f in ['Name_Localised', 'BuyPrice', 'SellPrice', 'Stock', 'Demand', 'Rare']:
            print(f'{f}: {item_price[f]}')


if __name__ == '__main__':
    main()
