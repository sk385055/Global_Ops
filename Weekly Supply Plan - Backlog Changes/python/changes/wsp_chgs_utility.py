"""
Create portable serialized representations of Python objects.

See module wsp_chgs_lookups for ... .
See module wsp_chgs_mapping for ... .
See module wsp_chgs_filters for ... .
See module wsp_chgs_datacls for ... .

Classes:

    Compiler
    Unpickler

Functions:

    dump(object, file)
    dumps(object) -> string
    load(file) -> object
    loads(string) -> object

Misc variables:

    __version__
    format_version
    compatible_formats
"""

import os
import glob
import re

import pandas as pd

__version__ = "0.0.1"


class UserInterface:
    """ gui to get access to selecting the current and prior files """

    def __init__(self):
        # self.path = "../../wsps_final/*.csv"
        self.path = "../../wsps/*.csv"
        # self.path = "../../wsps_historic_out/*.csv"
        self.wsps = None
        self.priorfile = None
        self.currfile = None
        self.priorfile_idx = None
        self.currfile_idx = None

    @staticmethod
    def gui():
        print("=" * 70)
        print("WSP Changes Tool")
        print("=" * 70)

    def search_wsps(self):
        self.wsps = glob.glob(self.path)

    def list_wsps(self):
        if self.wsps:
            for i, wsp in enumerate(self.wsps):
                print(str(i + 1).zfill(2), "-->", wsp)

    def write_to_csv(self):
        with open("./list_of_wsp.csv", 'a+') as f:
            for wsp in self.wsps:
                f.write(wsp+'\n')

    def get_input(self):
        while True:
            self.priorfile_idx = input(
                "Enter the number for the prior file (eg 1 for the first file or 'X' to quit): "
            )
            if any([self.priorfile_idx == "X", self.currfile_idx == "X"]):
                break
            self.currfile_idx = input(
                "Enter the number for the curr file (eg 1 for the first file or 'X' to quit): "
            )
            if any([self.priorfile_idx == "X", self.currfile_idx == "X"]):
                break
            try:
                self.priorfile_idx = int(self.priorfile_idx)
                self.currfile_idx = int(self.currfile_idx)
                break
            except ValueError:
                print("Must enter an integer or ('X' to quit), try again")

    def set_files(self):
        if all([self.priorfile_idx, self.currfile_idx]):
            self.priorfile = self.wsps[self.priorfile_idx - 1]
            self.currfile = self.wsps[self.currfile_idx - 1]

    def show_compare(self):
        print("WSP Changes compare on:")
        print(self.priorfile, " --> ", self.currfile)


class ReadFile:
    def __init__(self, path: str = None):
        self.path = path
        self.data = None

    def read_csv(self):
        try:
            self.data = pd.read_csv(self.path, low_memory=False)
            self.data.columns = [c.strip() for c in self.data.columns]
        except ValueError:
            self.data = pd.DataFrame()
            print(f"Error reading file from {self.path}")

    def __repr__(self):
        if not self.data.empty:
            display_string = f"{'='*70}\n"
            display_string += f"filename:    {self.path}\n"
            display_string += f"num of rows: {self.data.shape[0]}\nnum of cols: {self.data.shape[1]}\n"
            display_string += f"{'='*70}\n"
            display_string += str(self.data.head())
        else:
            display_string = f"{'='*70}\n"
            display_string += f"filename:    {self.path}\n"
            display_string += f"{'='*70}\n"
        return display_string


class ChangesFile(ReadFile):
    def __init__(self, path: str):
        super().__init__(path=path)

    def get_week_num(self):
        pattern = re.compile("Wk[0-9]+")
        if re.search(pattern=pattern, string=self.path):
            self.week = re.search(pattern=pattern, string=self.path).group(0)
        return self.week

    def set_formats(self, fin_mth: pd.DataFrame = None):
        self.__check_excl_column()
        self.__set_column_names()
        self.__set_ssd()
        self.__set_fin_mth(fin_mth)
        self.__set_ssd_format()
        self.__fill_blanks()

    def __check_excl_column(self):
        if "Excl2" not in self.data.columns:
            self.data["Excl2"] = self.data["Excl"]

    def __set_column_names(self):
        self.data.rename(
            columns={
                "Value USD": "Value-USD",
                " Value-USD ": "Value-USD",
                " Value USD ": "Value-USD",
                " Quantity ": "Quantity",
                " MCC-USD ": "MCC USD",
                " Avg Cost ": "Avg Cost",
                " Total USD ": "Total USD",
                " Avg MCC ": "Avg MCC",
                " Total MCC ": "Total MCC",
            },
            inplace=True,
        )

    def __set_ssd(self):
        self.data["SSD"] = lookup(self.data["SSD"], dfirst=True)

    def __set_fin_mth(self, fin_mth):
        self.data = self.data.merge(
            fin_mth[["Date", "WeekNum"]], how="left", left_on="SSD", right_on="Date"
        )
        self.data.rename(columns={"WeekNum": "Plant_Ship_Wk"}, inplace=True)

    def __set_ssd_format(self):
        self.data["SSD"] = self.data["SSD"].dt.strftime("%Y-%m-%d")

    def __fill_blanks(self):
        self.data.fillna(0, inplace=True)


class MappingFile(ReadFile):
    def __init__(self, path: str):
        super().__init__(path=path)


def lookup(s, dfirst):
    """
    This is an extremely fast approach to datetime parsing.
    For large data, the same dates are often repeated. Rather than
    re-parse these, we store all unique dates, parse them, and
    use a lookup to convert all dates.
    """
    dates = {
        date: pd.to_datetime(date, dayfirst=dfirst, errors="coerce")
        for date in s.unique()
    }
    return s.map(dates)


if __name__ == "__main__":
    gui = UserInterface()
    gui.gui()
    gui.search_wsps()
    gui.list_wsps()
    gui.get_input()
    gui.set_files()
    gui.show_compare()

    prior_file = ChangesFile(gui.priorfile)
    curr_file = ChangesFile(gui.currfile)

    print(prior_file.get_week_num())
    print(curr_file.get_week_num())

    prior_file.read_csv()
    curr_file.read_csv()

    print(prior_file)
    print(curr_file)
