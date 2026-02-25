import win32com.client
import time


def refersh_pivot():
    xl = win32com.client.DispatchEx("Excel.Application")
    xl.Visible = True
    xl.DisplayAlerts = False
    wb = xl.workbooks.open(r'E:\_Projects\completions\reports\Completion.xlsx')
    #refersh all pivots

    wb.RefreshAll()
    #wait for refersh complete
    xl.CalculateUntilAsyncQueriesDone()
    wb.Save()
    wb.Close()
    xl.Quit()




