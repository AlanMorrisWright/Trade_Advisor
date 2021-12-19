def get_prices():
    with open('C:\\!\\CODING\\ED\\J\\Market.json') as f:
        lines = f.readlines()

    station = {}
    prices = []
    i = 0
    for line in lines:
        if line:
            if i == 0:
                neat_line = line\
                    .replace('{ ', '')\
                    .replace(', "Items":[ ', '')\
                    .replace(chr(10), '')\
                    .replace('":', '"|')\
                    .replace('"', '')
                station = dict((k.strip(), v.strip()) for k, v in (item.split('|') for item in neat_line.split(',')))
                i = i + 1
            else:
                neat_line = line\
                    .replace('{ ', '') \
                    .replace(' },', '') \
                    .replace(' }', '') \
                    .replace(chr(10), '') \
                    .replace('":', '"|') \
                    .replace('"', '')
                if neat_line[:3] == 'id|':
                    item_prices = dict((k.strip(), v.strip()) for k, v in (item.split('|') for item in neat_line.split(',')))
                    prices.append(item_prices)
    return station, prices


def main():
    station, prices = get_prices()
    for f in ['timestamp', 'StationName', 'StarSystem']:
        print(f'{f}: {station[f]}')
    for item_price in prices:
        for f in ['Name_Localised', 'BuyPrice', 'SellPrice', 'Stock', 'Demand', 'Rare']:
            print(f'{f}: {item_price[f]}')


if __name__ == '__main__':
    main()
