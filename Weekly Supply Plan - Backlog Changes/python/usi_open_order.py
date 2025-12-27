import os
import glob
from datetime import datetime, time, timedelta

import pandas as pd


class MultiColumns(object):

    def __init__(self, columns):
        self.columns = columns

    def collapse(self):
        pass

    def expand(self):
        pass

    def __repr__(self):
        pass


class USIOpenOrder(object):

    def __init__(self):
        self.path = r"C:\Users\CP185176\OneDrive - NCR Corporation\project\usi_open_order"

    def __current_time(self):
        self.current_time = datetime.now()

    def __start_time(self):
        self.start_time = datetime.now() + timedelta(days=-100)

    def __search_files(self):
        self.files = glob.glob(os.path.join(self.path, "*.xlsx"))

    @staticmethod
    def __get_timestamp(file):
        return datetime.fromtimestamp(os.path.getmtime(file))

    def __find_latest_file(self):
        max_delta = self.current_time - self.start_time
        for i, file in enumerate(self.files):
            timestamp = self.__get_timestamp(file)
            if max_delta > self.current_time - timestamp:
                self.file_idx, self.max_delta, self.current_file = i, self.current_time - timestamp, file

    def get_latest_file(self):
        self.__current_time()
        self.__start_time()
        self.__search_files()
        self.__find_latest_file()
        self.__display_latest_file()

    def __display_latest_file(self):
        print("=" * 25 + "The latest available file is" + "=" * 25)
        print(self.file_idx, self.current_file, self.max_delta)
        print("=" * 25 + "=" * len("The latest available file is") + "=" * 25)

    def read_data(self):
        self.data = pd.read_excel(self.current_file, header=[0, 1], skiprows=1)

    def __repr__(self):
        return str(self.data.head())


if __name__ == '__main__':
    usi_open_order = USIOpenOrder()
    usi_open_order.get_latest_file()
    usi_open_order.read_data()
    print(usi_open_order)

    for c in usi_open_order.data.columns:
        print(c)
