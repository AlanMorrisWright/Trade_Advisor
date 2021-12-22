import csv


def get_listings_prices(file_path, file_name):
    """
    listings.csv
    CSV file rows: 6_800_823 (2021 12 20)
    CSV file columns:
        # 0: 'id'
        # 1: 'station_id'
        # 2: 'commodity_id'
        # 3: 'supply'
        # 4: 'supply_bracket'
        # 5: 'buy_price'
        # 6: 'sell_price'
        # 7: 'demand'
        # 8: 'demand_bracket'
        # 9: 'collected_at'
    :param file_path:
    :param file_name:
    :return:
    """
    with open(file_path + file_name, newline='') as f:
        reader = csv.reader(f, delimiter=',', quotechar='"')
        all_headings = next(reader)
        headings = [all_headings[1], all_headings[2], all_headings[3],
                    all_headings[5], all_headings[6], all_headings[7],
                    all_headings[9]]
        reader = csv.reader(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        data = [[int(row[1]), int(row[2]), int(row[3]),
                 int(row[5]), int(row[6]), int(row[7]),
                 int(row[9])] for row in reader if row]
    return data
