def get_prices(file_path, file_name):
    """
    converts Elite Dangerous marketplace data from JSON to dict
    :param file_path: of the JSON
    :param file_name: of the JSOM
    :return: dict of station details, list of dicts of commodity details
    """
    with open(file_path + file_name) as f:
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
                    .replace('{ ', '')\
                    .replace(' },', '')\
                    .replace(' }', '')\
                    .replace(chr(10), '')\
                    .replace('":', '"|')\
                    .replace('"', '')
                if neat_line[:3] == 'id|':
                    item_prices = dict(
                        (k.strip(), v.strip()) for k, v in (item.split('|') for item in neat_line.split(','))
                    )
                    prices.append(item_prices)
    return station, prices
