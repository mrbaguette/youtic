from __future__ import (absolute_import, division,
                        print_function, unicode_literals)

import pandas

C_PRODUCT_CODE = "Product code"
C_LANGUAGE = "Language"
C_PRODUCT_ID = "Product id"
C_VENDOR = "Vendor"
C_STATUS = "Status"
C_QUANTITY = "Quantity"
C_PRICE = "Price"
C_OPTIONS = "Options"
C_INVENTORY_TRACKING = "Inventory tracking"
C_URL = "URL"

CSV_COLUMN_LIST = [
    C_PRODUCT_CODE,
    C_LANGUAGE,
    C_PRODUCT_ID,
    C_VENDOR,
    C_STATUS,
    C_QUANTITY,
    C_PRICE,
    C_OPTIONS,
    C_INVENTORY_TRACKING,
    C_URL
]


def read_youtic_product_input_data(input_filename):
    stream_filename = input_filename

    data = pandas.read_csv(
        stream_filename,
        encoding='utf-8',
        names=CSV_COLUMN_LIST,
        header=0,
        dtype=object,
        na_filter=False,
        parse_dates=False,
        error_bad_lines=True,
        converters={
        }
    )
    return data
