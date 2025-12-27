"""[summary]

Returns:
    [type]: [description]
"""
import os
import pandas as pd
import numpy as np


# GLOBAL VARIABLES
DBPATH = r'C:\database\dev'
NTPATH = r'\\Dayorg1\ORGSHARE\TEAMS\ERP Shared Folder\Global S&OP\Daily Order Cover - Global\dolphin\wrk'
GLOBAL_SOP = r"\\Dayorg1\ORGSHARE\TEAMS\ERP Shared Folder\Global S&OP" \
    "\Daily Order Cover - Global\order_cover_output"


def main():
    """[summary]
    """
    print("Started...")
    dolphin_atm = "dolphin_atm.xlsx"
    dolphin_sco = "dolphin_ssco.xlsx"
    dolphin_sheetname = "Estimated Ship Dates"

    output_orders_all = "output_orders.csv"
    output_orders_atm = "output_orders_atm.csv"

    print("Reading dolphin files...")
    _dolphin_atm = read_files(NTPATH, dolphin_atm, dolphin_sheetname, 'XLS')
    _dolphin_sco = read_files(NTPATH, dolphin_sco, dolphin_sheetname, 'XLS')

    print("Modify Column Name for PO Receipt Date")
    _dolphin_sco.rename(columns={'SO CREATE': 'PO Receipt Date'}, inplace=True)

    print("Reading output files...")
    _output_all = read_files(DBPATH, output_orders_all, None, 'CSV')
    _output_atm = read_files(DBPATH, output_orders_atm, None, 'CSV')

    print("Formatting columns...")
    _dolphin_atm.columns = format_column_names(_dolphin_atm)
    _dolphin_sco.columns = format_column_names(_dolphin_sco)

    print("Concatenating dolphin data...")
    dolphin_cols = ['PO', 'Po Receipt Date']
    dolphin_combined = pd.concat(
        [_dolphin_atm[dolphin_cols], _dolphin_sco[dolphin_cols]])

    print("Merging output and dolphin data...")
    _output_all = merge_output_with_dolphin(
        _output_all, dolphin_combined.drop_duplicates(subset=['PO']))
    _output_atm = merge_output_with_dolphin(
        _output_atm, dolphin_combined.drop_duplicates(subset=['PO']))

    print("Updating booked dt column...")
    _output_all = update_booked_dt_column(_output_all)
    _output_atm = update_booked_dt_column(_output_atm)

    print(f"Posting test_output_orders.csv to {DBPATH}...")
    post_final_output(DBPATH, _output_all, "test_output_orders.csv")
    print(f"Posting test_output_orders_atm.csv to {DBPATH}...")
    post_final_output(DBPATH, _output_atm, "test_output_orders_atm.csv")

    print(f"Posting test_output_orders.csv to {GLOBAL_SOP}...")
    post_final_output(GLOBAL_SOP, _output_all, "output_orders.csv")
    print(f"Posting test_output_orders_atm.csv to {GLOBAL_SOP}...")
    post_final_output(GLOBAL_SOP, _output_atm, "output_orders_atm.csv")

    print("Finished.")


def read_files(path: str, filename: str, sheetname: str, filetype: str):
    """[summary]

    Args:
        filename (str): [description]
        sheetname (str): [description]
        filetype (str): [description]

    Returns:
        [type]: [description]
    """
    if filetype == 'XLS':
        _xls = pd.ExcelFile(os.path.join(path, filename))
        output_data = _xls.parse(sheetname)
    elif filetype == 'CSV':
        _csv = pd.read_csv(os.path.join(path, filename))
        output_data = _csv
    else:
        output_data = None
    return output_data


def format_column_names(dframe: pd.DataFrame):
    """[summary]

    Args:
        dframe (pd.DataFrame): [description]

    Returns:
        [type]: [description]
    """
    cols = [str(c).title() if len(str(c)) > 4 else str(c).upper()
            for c in dframe.columns]
    return cols


def merge_output_with_dolphin(output, dolphin):
    """[summary]

    Args:
        output ([type]): [description]
        dolphin ([type]): [description]

    Returns:
        [type]: [description]
    """
    output['Supply Detail'] = output['Supply Detail'].astype(str)
    dolphin['PO'] = dolphin['PO'].astype(str)
    merged_data = output.merge(dolphin, how='left', left_on='Supply Detail',
                               right_on='PO', validate='m:1')
    return merged_data


def update_booked_dt_column(output):
    """[summary]

    Args:
        output ([type]): [description]
    """
    book_dt = output['Booked Dt']
    rcpt_dt = output['Po Receipt Date']
    book_dt = pd.to_datetime(book_dt, dayfirst=True)

    filt_jabil_rcpt_dt = ~output['Po Receipt Date'].isnull()

    new_book_dt = np.where(filt_jabil_rcpt_dt, rcpt_dt, book_dt)
    output['Booked Dt'] = new_book_dt

    return output


def post_final_output(path: str, output, output_filename: str):
    """[summary]

    Args:
        output ([type]): [description]
        output_filename ([type]): [description]
    """
    try:
        output.drop(columns='Po Receipt Date', inplace=True)
    except KeyError:
        print("Column \'Po Receipt Date\' does not exist")
    finally:
        output.to_csv(os.path.join(path, output_filename), index=False)


if __name__ == '__main__':
    main()
