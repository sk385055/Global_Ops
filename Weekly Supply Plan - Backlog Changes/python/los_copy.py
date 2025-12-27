import shutil
import os
import glob

from subprocess import call
from datetime import datetime, timedelta


def copy_with_subprocess(cmd):
    """
    fast file transfer -- using standard python file transfer
    can be sloowwwww
    """
    proc = call(cmd)
    return proc


def fast_file_transfer(from_path: str, to_path: str):
    """
    fast file transfer -- using standard python file transfer
    can be sloowwwww
    """
    try:
        cmd = ['xcopy', from_path, to_path, '/i/y/j/f']
        copy_with_subprocess(cmd)
    except OSError as error:
        print(
            f"Could not transfer \nfrom:{from_path} \nto:{to_path} \nerror:{error}")


def generate_los_name():
    los_date = (datetime.now() - timedelta(days=1)).strftime("%d-%b-%Y")
    los_name = f"LOS_Final_{los_date}.zip"
    return los_name


def copy_los(los_name: str) -> None:
    path_from = f"\\\\susday014\\cdunix\\EDLRACE\\rcv\\{los_name}"
    path_to = f"C:\\database\\dev\\Inputs\\zip\\{los_name}*"
    fast_file_transfer(path_from, path_to)
    return los_name


def del_los() -> None:
    for file in glob.glob("C:\\database\\dev\\Inputs\\zip\\*.zip"):
        print("Deleting:", file)
        os.remove(file)


if __name__ == '__main__':
    del_los()
    copy_los(generate_los_name())
