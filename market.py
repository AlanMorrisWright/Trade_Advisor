from ETL import key_value


def get_prices(file_path, file_name):
    # 2 parts to EDs file - station details and prices
    #   read station details
    #   read commodity prices

    with open(file_path + file_name) as f:
        lines = f.readlines()

    station = []
    fields = (('timestamp', 'MarketID'),  # , 'StationName', 'StationType', 'StarSystem'),
              ('sec1970', 'int'))  # , 'str', 'str', 'str'))
    line = lines[0]
    if fields[0][0] in line:
        for j, field in enumerate(fields[0]):
            val = key_value(field, line, fields[1][j])
            station.append(val)

    market = []
    fields = (('Name_Localised', 'BuyPrice', 'SellPrice', 'Stock', 'Demand', 'Rare'),
              ('str', 'int',  'int',  'int',  'int', 'bool'))
    for line in lines[1:]:
        if fields[0][0] in line:
            this = []
            for j, field in enumerate(fields[0]):
                val = key_value(field, line, fields[1][j])
                this.append(val)
            market.append(this)

    for line in market:
        o = list(station)
        o.extend(line)
        print(o)

get_prices('C:\\Users\\alanm\\Saved Games\\Frontier Developments\\Elite Dangerous\\', 'Market.json')
