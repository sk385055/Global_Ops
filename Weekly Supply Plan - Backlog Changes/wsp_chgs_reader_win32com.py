#!/usr/bin/python3
"""get conn to wincom application (excel, access, etc)
"""
import os
import glob
import subprocess
import sys
import win32com.client
import win32process

import pandas as pd

_DELAY = 0.90000000000000 # seconds
_TIMEOUT = 9000000000000000000000  # seconds

################################################################################
# Globals
################################################################################

PATH_READ = r"E:\_Projects\wsp\backlog_compare\wsp_xlsb"
PATH_WRITE = r"E:\_Projects\wsp\backlog_compare\wsp_csv"
PATH_CSV = r"E:\_Projects\wsp\backlog_compare\wsp_csv2"

################################################################################
# Getters
################################################################################


def get_conn_to_wincom_app(app: str = 'Excel'):
    """get conn to wincom application (excel, access, etc)
    """
    try:
        wincom_app = win32com.client.DispatchEx(f"{app}.Application")
    except AttributeError:
        # Corner case dependencies.
        import re
        import shutil
        # Remove cache and try again.
        module_list = [m.__name__ for m in sys.modules.values()]
        for module in module_list:
            if re.match(r'win32com\.gen_py\..+', module):
                del sys.modules[module]
        shutil.rmtree(os.path.join(os.environ.get(
            'LOCALAPPDATA'), 'Temp', 'gen_py'))
        wincom_app = win32com.client.DispatchEx('Excel.Application')
    finally:
        wincom_app.AutomationSecurity = 1  # 1 = msoAutomationSecurityLow
        wincom_app.Visible = True
        if app == 'Excel':
            wincom_app.DisplayAlerts = False
    return wincom_app


def close_conn_to_wincom_app(app: str, wincom_app):
    """close conn to wincom application (excel, access, etc)
    """
    print(f"Closing {wincom_app}")
    if app == 'Excel':
        wincom_app.Quit()
    elif app == 'Access':
        wincom_app.CloseCurrentDatabase()
    return f'{app} closed'


def close_workbook(workbook):
    """close workbook
    """
    print(f"Closing {workbook.Name}")
    workbook.Close()


def save_workbook(workbook):
    """save workbook
    """
    print(f"Saving {workbook.Name}")
    workbook.Save()


def save_as_workbook(workbook, new_workbook: str):
    """save workbook
    """
    print(f"Saving {workbook} as {new_workbook}")
    workbook.SaveAs(new_workbook)


def open_workbook(workbook: str, wincom_app):
    """open workbook
    """
    print(f"Opening {workbook}")
    return wincom_app.Workbooks.Open(workbook)


def get_pid(wincom_app):
    """get the thread and pid of the runinng app
    """
    thread, pid = win32process.GetWindowThreadProcessId(wincom_app.Hwnd)
    return thread, pid


def refresh_workbook(workbook):
    """refresh workbook
    print(f"Refreshing {workbook.Name}")
    workbook.RefreshAll()
       """

def kill_process_by_pid(pid):
    """kill running process by pid
    """
    print(f"Killing process {pid}")
    _p = subprocess.Popen(["powershell.exe",
                           os.path.abspath(r'E:\_Projects\wsp\backlog_compare\python\kill_process.ps1') + " " + str(pid)],
                          stdout=sys.stdout)
    _p.communicate()


def get_wsp_xlsbs():
    return glob.glob(os.path.join(PATH_READ, '*.xlsb'))


def get_wsp_csv():
    return glob.glob(os.path.join(PATH_WRITE, '*.csv'))


def get_wsp_data(file_read: str, file_write: str):
    """get the gop data workbook
    """
    # set-up excel instance
    wincom_app = get_conn_to_wincom_app('Excel')
    pid = get_pid(wincom_app)[1]

    # open gop workbook
    gop_xlsb = os.path.join(file_read)
    gop_workbook = open_workbook(gop_xlsb, wincom_app)
    gop_workbook.Worksheets("data").Activate()

    # format numeric columns as 4 decimal place numbers
    #gop_workbook.Worksheets("data").Columns("V:X").NumberFormat = "0.0000" jose commented
    gop_workbook.Worksheets("data").Columns("AC:AD").NumberFormat = "0.0000"
    gop_workbook.Worksheets("data").Columns("BH:BO").NumberFormat = "0.0000"
    #gop_workbook.Worksheets("data").Columns("CE:CH").NumberFormat = "0.0000"
    #gop_workbook.Worksheets("data").Columns("CM").NumberFormat = "0.0000"
    #gop_workbook.Worksheets("data").Columns("CY:DJ").NumberFormat = "0.0000"
    #gop_workbook.Worksheets("data").Columns("DN:DR").NumberFormat = "0.0000"
    #gop_workbook.Worksheets("data").Columns("DT").NumberFormat = "0.0000"

    # Save & close the Workbook as CSV UTF-8.
    gop_workbook.SaveAs(file_write, 62)
    close_workbook(gop_workbook)
    #kill_process_by_pid(pid)


def reader_xlsb():
    """read gop and mcid tagging data and generate csv
    """
    wsp_as_xlsbs = get_wsp_xlsbs()
    for wsp_as_xlsb in wsp_as_xlsbs:
        file_read = wsp_as_xlsb
        file_write = wsp_as_xlsb.replace('xlsb', 'csv')
        get_wsp_data(file_read, file_write)


def strip_columns_headers(headers: list) -> list:
    return [header.strip() for header in headers]


def reader_csv():
    """read gop and mcid tagging data and generate csv
    """
    wsp_as_csvs = get_wsp_csv()
    for wsp_as_csv in wsp_as_csvs:
        file_read = pd.read_csv(wsp_as_csv, low_memory=False)
        file_read.columns = strip_columns_headers(file_read.columns)
        columns = ['Excl', 'Excl2', 'Prod_LOB', 'LOB', 'LOB_']
        for column in file_read.columns:
            if column in columns:
                file_read.loc[:, column] = file_read[column].str.strip()

        file_read = file_read.dropna(subset=[
            "Order Category", "Order Category2", "Order Category3", "Order Category4", "OrderCat"], how='all').copy()
        filename = wsp_as_csv.split('\\')[-1]
        file_read.to_csv(os.path.join(PATH_CSV, filename), index=False)


if __name__ == '__main__':
    reader_xlsb()
    reader_csv()
