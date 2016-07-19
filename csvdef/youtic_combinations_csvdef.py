from __future__ import (absolute_import, division,
                        print_function, unicode_literals)

import pandas

C_PRODUCT_ID = "Product id"
C_COMBINATION = "Combination"
C_AMOUNT = "Amount"
C_LANGUAGE = "Language"

CSV_COLUMN_LIST = [
    C_PRODUCT_ID,
    C_COMBINATION,
    C_AMOUNT,
    C_LANGUAGE,
]


def save_youtic_combinations_output_data(data_frame, output_filename):
    stream_filename = output_filename

    data_frame.to_csv(
        stream_filename,
        index=False,
        encoding="utf-8",
    )
