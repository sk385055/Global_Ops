 #!/usr/bin/python3
"""
Create portable serialized representations of Python objects.

See module wsp_chgs_lookup for ... .
See module wsp_chgs_map for ... .
See module wsp_chgs_filters for ... .
See module wsp_chgs_data_cls for ... .
See module wsp_chgs_alogrithm for ... .
See module wsp_chgs_utility for ... .

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

__version__ = "0.0.1"


from datetime import datetime, timedelta

import pandas as pd

import wsp_chgs_utility
import wsp_chgs_algorithm
import wsp_chgs_data_cls
import wsp_chgs_lookup
import wsp_chgs_map
import wsp_chgs_filters


def datafields_and_files(priorwk: str = None, currwk: str = None) -> tuple:
    """ return datafields and file paths """
    datafields = wsp_chgs_data_cls.FieldValues(priorwk, currwk)
    filepaths = wsp_chgs_data_cls.FileValues()
    filepaths.dict_of_paths()
    datafields.dict_of_fields()
    return datafields, filepaths


def no_interface(priorfile: str = None, currfile: str = None):
    """reads wsp files without use of interface

    Args:
        priorfile (str, optional): [description]. Defaults to None.
        currfile (str, optional): [description]. Defaults to None.

    Returns:
        [type]: [description]
    """
    prior_file = wsp_chgs_utility.ChangesFile(priorfile)
    curr_file = wsp_chgs_utility.ChangesFile(currfile)

    if prior_file.get_week_num() == curr_file.get_week_num():
        prior_file.week = prior_file.week + ".1"
        curr_file.week = curr_file.week + ".2"

    print(prior_file.week, "-->", curr_file.week)
    datafields, filepaths = datafields_and_files(
        prior_file.week, curr_file.week
    )
    prior_file.read_csv()
    curr_file.read_csv()

    fin_mth = pd.read_csv(filepaths.paths.get("fmth"), parse_dates=["Date"])
    prior_file.set_formats(fin_mth)
    curr_file.set_formats(fin_mth)

    print(prior_file)
    print(curr_file)
    return (prior_file, curr_file, datafields, filepaths)


def interface():
    """ gui main display for getting files """
    gui = wsp_chgs_utility.UserInterface()
    gui.gui()
    gui.search_wsps()
    # gui.write_to_csv() # uncomment to append list of wsps to list_of_wsp.csv
    gui.list_wsps()
    gui.get_input()
    gui.set_files()
    gui.show_compare()

    prior_file = wsp_chgs_utility.ChangesFile(gui.priorfile)
    curr_file = wsp_chgs_utility.ChangesFile(gui.currfile)

    print(prior_file.get_week_num(), "-->", curr_file.get_week_num())
    datafields, filepaths = datafields_and_files(
        prior_file.get_week_num(), curr_file.get_week_num()
    )
    prior_file.read_csv()
    curr_file.read_csv()

    fin_mth = pd.read_csv(filepaths.paths.get("fmth"), parse_dates=["Date"])
    prior_file.set_formats(fin_mth)
    curr_file.set_formats(fin_mth)

    print(prior_file)
    print(curr_file)
    return (prior_file, curr_file, datafields, filepaths)


def get_monday():
    """ returns monday of the current week """
    weekday_offset = datetime.now().weekday()
    return (datetime.today() - timedelta(days=weekday_offset)).date()


def get_chgs_filename(
    bklg_changes: bool = True, x_file: str = None, y_file: str = None
) -> str:
    """ returns string formatted for changes filename """
    chgs_file_date = datetime.now().strftime("%Y%m%d")
    wsps_file_date = get_monday().strftime("%Y_%m_%d")
    if bklg_changes:
        chgs_filename = (
            f"{wsps_file_date} {x_file}_to_{y_file} Backlog Compare_{chgs_file_date}.csv")
    else:
        chgs_filename = (
            f"{wsps_file_date} {x_file}_to_{y_file} Compare_{chgs_file_date}.csv"
        )
    return chgs_filename


def generate_changes_file(
    **kwargs
):
    """ builds changes """
    chgs_filename = get_chgs_filename(
        bklg_changes=kwargs["bklg_changes"],
        x_file=kwargs["prior_wsp"].week,
        y_file=kwargs["curr_wsp"].week,
    )
    chgs_file = wsp_chgs_algorithm.create_wsp_changes_file(
        backlog_only=kwargs["bklg_changes"],
        create_csv=True,
        prior_data=kwargs["prior_wsp"].data,
        curr_data=kwargs["curr_wsp"].data,
        datafields=kwargs["datafields"],
        offerpf=kwargs["offerpf"],
        chgs_filename=chgs_filename,
        filepaths=kwargs["filepaths"],
    )
    return chgs_file


def get_chgs():
    """ gets changes data """
    prior_wsp, curr_wsp, datafields, filepaths = interface()
    offerpf = pd.read_csv(filepaths.paths.get("offerpf"))
    chgs = []
    for is_backlog_changes in [True, False]:
        kwargs = {"prior_wsp": prior_wsp,
                  "curr_wsp": curr_wsp,
                  "datafields": datafields,
                  "filepaths": filepaths,
                  "offerpf": offerpf,
                  "bklg_changes": is_backlog_changes, }
        chgs.append(generate_changes_file(**kwargs))
    return chgs, prior_wsp, curr_wsp


def get_chgs_no_interface(prior_file: str = None, curr_file: str = None):
    """ gets changes data """
    prior_wsp, curr_wsp, datafields, filepaths = no_interface(
        prior_file, curr_file)
    offerpf = pd.read_csv(filepaths.paths.get("offerpf"))
    chgs = []
    for is_backlog_changes in [True, False]:
        kwargs = {"prior_wsp": prior_wsp,
                  "curr_wsp": curr_wsp,
                  "datafields": datafields,
                  "filepaths": filepaths,
                  "offerpf": offerpf,
                  "bklg_changes": is_backlog_changes, }
        chgs.append(generate_changes_file(**kwargs))
    return chgs, prior_wsp, curr_wsp


def main():
    """ gets, sets and writes changes """
    try:
        chgs, prior_wsp, curr_wsp = get_chgs()

        chgs_filename = get_chgs_filename(
            bklg_changes=True,
            x_file=prior_wsp.week,
            y_file=curr_wsp.week,
        )
        chgs = wsp_chgs_lookup.backlog_lookup(
            chgs, chgs_filename.replace(".csv", ".1.csv"))
        chgs = wsp_chgs_map.backlog_map(
            chgs, chgs_filename.replace(".csv", ".2.csv"))
        chgs = wsp_chgs_filters.backlog_filters(
            chgs, chgs_filename.replace(".csv", ".3.csv"),
            prior_week=prior_wsp.week,
            curr_week=curr_wsp.week,
        )
    except TypeError:
        print("Nothing to do...")


def get_list_of_wsps():
    """ gets wsp list to generate chgs files """
    return pd.read_csv('../backlog_compare/python/chgs/__current.csv') 


def main_no_interface():
    """ gets, sets and writes changes """
    for row in get_list_of_wsps().itertuples(index=False, name=None):
        metric, creation_date, prior_wk, curr_wk, prior_file, curr_file = row
        # try:
        print(metric, creation_date, prior_wk, "-->", curr_wk)
        chgs, prior_wsp, curr_wsp = get_chgs_no_interface(
            prior_file, curr_file)

        chgs_filename = get_chgs_filename(
            bklg_changes=True,
            x_file=prior_wsp.week,
            y_file=curr_wsp.week,
        )
        chgs = wsp_chgs_lookup.backlog_lookup(
            chgs, chgs_filename.replace(".csv", ".1.csv"))
        chgs = wsp_chgs_map.backlog_map(
            chgs, chgs_filename.replace(".csv", ".2.csv"))
        chgs = wsp_chgs_filters.backlog_filters(
            chgs, chgs_filename.replace(".csv", ".3.csv"),
            prior_week=prior_wsp.week,
            curr_week=curr_wsp.week,
            creation_date=creation_date,
            metric=metric
        )
        # except TypeError:
        #     print("Nothing to do...")


if __name__ == "__main__":
    #main()
    main_no_interface()
