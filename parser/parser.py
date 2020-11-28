import csv
import json
import argparse
from collections import OrderedDict
from typing import List, Dict

seen_characteristics = set()  # Collecting all characteristics in the dataset to use as csv columns
seen_ads = set()			  # Deduplicating ads based on the combination of title, location, and price


def parse_characteristics(json_data: List[Dict]) -> Dict:
    """ Remove unnecessary information from characteristics.

        Sample input:
            [
                {'key': 'm',
                'currency': '',
                'label': 'Powierzchnia',
                'value': '21',
                'value_translated': '21 m²'}
            ]
        Sample output:
                {'Powierzchnia': '21 m²'}
    """
    characteristics = {}
    for record in json_data:
        key = record['label']
        value = record['value_translated']
        characteristics[key] = value
    return characteristics


def process_input(file_path: str) -> List[OrderedDict]:
    with open(file_path, 'r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        output_rows = []

        for row in reader:
            json_data = json.loads(row['characteristics'].encode('utf8').decode())
            characteristics = parse_characteristics(json_data)
            seen_characteristics.update(characteristics)
            row['characteristics'] = characteristics

            dedup_key = ''.join((row['title'], row['location'], row['price']))
            if dedup_key not in seen_ads:
                output_rows.append(row)
                seen_ads.add(dedup_key)
    return output_rows


def write_output(file_path: str, rows_to_write: List[OrderedDict]):
    with open(file_path, 'w', encoding='utf-8', newline='') as outfile:
        writer = csv.writer(outfile, dialect='excel', delimiter=';')
        headers = ['title', 'location', 'price', *seen_characteristics, 'timestamp']
        writer.writerow(headers)

        for row in rows_to_write:
            characteristics = [row['characteristics'].get(key, '') for key in seen_characteristics]
            writer.writerow([row['title'], row['location'], row['price'], *characteristics, row['timestamp']])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='parser.py', usage='parser.py input_file -o output_file')
    parser.add_argument(
        'input_file', type=str, help='csv file with raw spider output')
    parser.add_argument(
        '-o', dest='output_file', metavar='output_file', type=str, help='csv file with processed output')
    args = parser.parse_args()

    if not args.output_file: 
        args.output_file = f'parsed_{args.input_file}'

    rows_to_write = process_input(args.input_file)
    write_output(args.output_file, rows_to_write)
