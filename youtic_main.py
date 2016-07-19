from __future__ import (absolute_import, division,
                        print_function, unicode_literals)

import json
import logging
from collections import defaultdict

from itertools import zip_longest, groupby
from tqdm import tqdm

import pandas

import re

import os
from csvdef import youtic_combinations_csvdef
from csvdef.youtic_combinations_csvdef import save_youtic_combinations_output_data
from csvdef.youtic_products_csvdef import read_youtic_product_input_data, C_VENDOR, C_URL, C_STATUS, \
    C_INVENTORY_TRACKING, C_PRODUCT_ID, C_LANGUAGE, C_OPTIONS, C_PRICE, C_QUANTITY
from operator import itemgetter
import argparse

log = logging.getLogger(__name__)


parser = argparse.ArgumentParser(description='Youtic')

parser.add_argument(
    '-i', '--input', required=True,
    help="Directory that contains input CSV files."
)
parser.add_argument(
    '-o', '--output', required=True,
    help="Directory to write results to."
)

PRICE_NORMALIZER_RE = re.compile("€?([0-9]+)[.,]([0-9]+)")
CLIENT_FILENAME_NORMALIZER_RE = re.compile("[_ ]+")

CLIENT_INPUT_DATA_CACHE = {}


def read_client_input_data(filename):
    input_data = CLIENT_INPUT_DATA_CACHE.get(filename)

    if input_data is None:
        input_data = _read_client_input_data(filename)
        log.info("Read {} client data in file {}.".format(len(input_data), filename))
        CLIENT_INPUT_DATA_CACHE[filename] = input_data
    else:
        log.info("Using cached client data for {}.".format(filename))
    return input_data


def _read_client_input_data(filename):
    with open(filename, 'r') as f:
        input_data_raw = json.load(f)

    def price_cleaners(txt):
        if txt is None:
            return None
        return re.sub(PRICE_NORMALIZER_RE, "\g<1>.\g<2>", txt)

    cleaners = {
        'Price': [price_cleaners],

    }

    def clean(kv):
        key, value = kv
        if isinstance(value, str):
            value = value.strip()
        c_list = cleaners.get(key)
        if c_list is not None:
            for c in c_list:
                value = c(value)
        return key, value

    headers = input_data_raw.get('headers')

    input_data =\
        [dict(map(clean, zip_longest(headers, row))) for row in input_data_raw.get('rows')]

    input_data_grouped = dict([(key, list(group)) for (key, group) in groupby(input_data, key=itemgetter('pageUrl'))])
    return input_data_grouped


def save_youtic_output_data(data_frame, output_filename):
    stream_filename = output_filename

    data_frame.to_csv(
        stream_filename,
        index=False,
        encoding="utf-8",
    )


def main():

    logging.basicConfig(
        filename='{}.log'.format(os.path.basename(__file__)),
        level=logging.DEBUG
    )

    args = parser.parse_args()
    input_directory = args.input
    output_directory = args.output

    youtic_products_input_filename = os.path.join(input_directory, "Youtic-products.csv")

    youtic_products_input = read_youtic_product_input_data(youtic_products_input_filename)
    log.info("Read {} products.".format(len(youtic_products_input)))

    youtic_products_output = pandas.DataFrame(columns=youtic_products_input.columns)
    youtic_combinations_output = pandas.DataFrame(columns=youtic_combinations_csvdef.CSV_COLUMN_LIST)

    for _, row in tqdm(list(youtic_products_input.iterrows())):
        client_id = row.loc[C_VENDOR]
        client_input_filename = os.path.join(
            input_directory,
            "a-{}.json".format(re.sub(CLIENT_FILENAME_NORMALIZER_RE, "_", client_id.lower()))
        )

        client_input = read_client_input_data(client_input_filename)

        page_url = row.loc[C_URL]

        client_data_list = client_input.get(page_url)

        youtic_products_output.append(row)

        if not client_data_list:
            log.info("Product ID {} not found in client data.".format(row.loc[C_PRODUCT_ID]))
            # Not found
            row.loc[C_STATUS] = "D"
            row.loc[C_INVENTORY_TRACKING] = "B"
            row.loc[C_OPTIONS] = ""

        else:
            assert len(client_data_list) > 0

            client_data = client_data_list[0]
            if bool(client_data.get('error')) or bool(client_data.get('Availability')):
                # Found but value in availability or error
                log.info("Product ID {} found but no availability or in error.".format(row.loc[C_PRODUCT_ID]))
                row.loc[C_STATUS] = "D"
                row.loc[C_INVENTORY_TRACKING] = "B"
                row.loc[C_OPTIONS] = ""
            else:
                log.info("Product ID {} found in client data...".format(row.loc[C_PRODUCT_ID]))
                row.loc[C_STATUS] = "A"
                row.loc[C_PRICE] = client_data.get('Price')
                row.loc[C_LANGUAGE] = "fr"
                row.loc[C_QUANTITY] = "1"
                row.loc[C_INVENTORY_TRACKING] = "B"

                options = defaultdict(lambda: list())
                for client_data in client_data_list:
                    for key, value in client_data.items():
                        if value is None:
                            continue
                        if key not in ["Availability", 'error', 'pageUrl', 'Price']:
                            row.loc[C_INVENTORY_TRACKING] = "O"
                            value = value.replace(",", ".")
                            options[key].append(value)

                            combination_row = {
                                youtic_combinations_csvdef.C_PRODUCT_ID: row.loc[C_PRODUCT_ID],
                                youtic_combinations_csvdef.C_COMBINATION_CODE: "",
                                youtic_combinations_csvdef.C_COMBINATION: "{}: {}".format(key, value),
                                youtic_combinations_csvdef.C_AMOUNT: "1",
                                youtic_combinations_csvdef.C_LANGUAGE: row.loc[C_LANGUAGE],
                            }
                            youtic_combinations_output = youtic_combinations_output.append(combination_row, ignore_index=True, verify_integrity=True)
                # end for
                log.info("Found {} options.".format(len(options)))
                combination = "; ".join(
                    ["{}: S[{}]".format(option_key, ", ".join(option_set)) for option_key, option_set in options.items()])
                row.loc[C_OPTIONS] = combination
            # end if
        # end if not client_data_list

        youtic_products_output = youtic_products_output.append(row, ignore_index=True, verify_integrity=True)
    # end for

    log.info("Writing files...")
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    youtic_output_filename = os.path.join(output_directory, "Youtic-products.csv")
    save_youtic_output_data(youtic_products_output, youtic_output_filename)

    youtic_combinations_output_filename = os.path.join(output_directory, "Youtic-combinations.csv")
    save_youtic_combinations_output_data(youtic_combinations_output, youtic_combinations_output_filename)
    log.info("Done")

if __name__ == "__main__":
    main()
