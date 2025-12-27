"""[summary]

Returns:
    [type]: [description]
"""

import glob
import os
import shutil
import datetime
from win32com import client
import pandas as pd


pd.options.display.max_columns = None
pd.options.display.max_rows = 100



def convert_xlsb_to_new_format(xlsb_file_path: str = None, file_format: int = 51,
                               out_file_path: str = None, active_ws: str = None):
    """
    Take an excel binary file (xlsb) and convert to file_format passed into function

    :params: REQUIRED xlsb_file_path     -> path to the location of excel binary file
    :params: REQUIRED out_file_path      -> path location to save enumerated file

    :params: OPTIONAL file_format        -> integer value of acceptable XLFileFormat enumeration
    :params: OPTIONAL file_ext           -> If you pass an unsupported file_format then you must
                                            pass the required file extension

    :default: If no file_format is passed the default conversion is to xlsx format

    :return: converted file saved to out_file_path

    XLFileFormat Enumeration
    ----------------------------------------------------------------------------------
    Name                Value	Description         Extension
    ----------------------------------------------------------------------------------
    xlCSVWindows        23      Windows CSV         *.csv
    xlCSVUTF8           62      UTF8 CSV            *.csv
    xlOpenXMLWorkbook   51      Open XML Workbook   *.xlsx
    xlUnicodeText       42      Unicode Text        No file extension; *.txt
    ----------------------------------------------------------------------------------

    Takes an xlsb file and returns a converted file based on XLFileFormat enumerations
    """

    try:
        excel_app = client.gencache.EnsureDispatch('Excel.Application')
    except AttributeError:
        # Corner case dependencies.
        import re
        import sys
        # Remove cache and try again.
        module_list = [m.__name__ for m in sys.modules.values()]
        for module in module_list:
            if re.match(r'win32com\.gen_py\..+', module):
                del sys.modules[module]
        shutil.rmtree(os.path.join(os.environ.get('LOCALAPPDATA'), 'Temp', 'gen_py'))
        excel_app = client.gencache.EnsureDispatch('Excel.Application')
    finally:
        excel_app.DisplayAlerts = False
        excel_app.Visible = True

    try:
        workbook = excel_app.Workbooks.Open(xlsb_file_path)
        excel_app.DisplayAlerts = False
        workbook = excel_app.ActiveWorkbook

        if active_ws:
            for sheet in excel_app.Sheets:
                if sheet.Name == active_ws:
                    excel_app.Sheets(active_ws).Select()

        workbook.SaveAs(Filename=out_file_path, FileFormat=file_format)
        workbook.Close()

        file_created_time = datetime.datetime. \
                            strftime(datetime.datetime.now(), '%Y%m%d%H%M')
        print(f'File created at {file_created_time} and saved to {out_file_path}')
    except AttributeError:
        print('Error occurred')
    finally:
        # warning -- this will close ALL running Excel processes
        excel_app.Quit()
        print("Finished conversion... Excel App still running")


def convert_to_txt(_path: str, active_ws: str = 'Data'):
    """[summary]

    Args:
        path (str): [description]
        file_ext_in (str, optional): [description]. Defaults to 'xlsb'.
        active_ws (str, optional): [description]. Defaults to 'Data'.

    Returns:
        [type]: [description]
    """
    if _path:
        file_ext_out = 'txt'

        xlsb_file_path = f'{_path}'
        outt_file_path = f'{_path.split(".")[0]}.{file_ext_out}'
        outt_file_path_snd = outt_file_path.replace(r'\rcv', r'\out')
        outt_file_frmt = 42

        convert_xlsb_to_new_format(xlsb_file_path=xlsb_file_path,
                                   out_file_path=outt_file_path,
                                   file_format=outt_file_frmt,
                                   active_ws=active_ws)

        shutil.copyfile(outt_file_path, outt_file_path_snd)
    else:
        print('No file path to process, path must not be null')


def remove_all_files_from_directory(path: str, file_type: str = '*.txt') -> None:
    """simple function to remove all files with same file_type

    Args:
        path (str): [description]
        file_type (str, optional): [description]. Defaults to '*.txt'.
    """
    _paths = glob.glob(os.path.join(path, file_type))
    for _file_path in _paths:
        os.remove(_file_path)


if __name__ == '__main__':

    PATH = (r"C:\Users\CP185176\OneDrive - NCR Corporation\project\dolphin\rcv\*.xlsx")
    PATHS = glob.glob(PATH)
    for file_path in PATHS:
        convert_to_txt(file_path, active_ws='Estimated Ship Dates')


    # det_path = r'C:\Users\CP185176\Documents\python\plan_det\python\plan_det\det\rcv\*.xlsb'
    # doc_path = r'C:\Users\CP185176\Documents\python\plan_det\python\plan_det\doc\rcv\*.xlsb'

    # paths = glob.glob(det_path)
    # for p in paths:
    #     print(p)
    #     convert_to_txt(p, 'xlsb', active_ws='Pivot')

    # paths = glob.glob(doc_path)
    # for p in paths:
    #     print(p)
    #     convert_to_txt(p, 'xlsb', active_ws='EX Data')
