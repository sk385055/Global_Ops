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


import os

import wsp_chgs_data_cls


class LookupFields:
    mapping_columns = [
        "Region",
        "Country",
        "KeyAccount",
        "Master Customer Name",
        "Order Category4",
        "Offer PF",
        "offer_pf",
        "Range",
        "LOB",
        "Item",
        "Item Type",
        "Unit",
    ]


def get_filepaths():
    filepaths = wsp_chgs_data_cls.FileValues()
    filepaths.dict_of_paths()
    return filepaths


def backlog_lookup(chgs: list = None, chgs_filename: str = None) -> list:
    backlog = chgs[0]
    lookup = chgs[1]
    mapping_columns = LookupFields.mapping_columns
    filepaths = get_filepaths()
    lookup_bklg = lookup[mapping_columns][
        ~(lookup["Item"] == 0)
        & ~(lookup["Item"] == "0")
        & (lookup["Order Category4"].isin(["Backlog", "Comp/Bklg", "PBO"]))
    ]
    lookup_final = lookup_bklg.drop_duplicates(
        subset=["Item", "Unit"], keep="last")
    bklg_only = backlog[
        backlog["Order Category4"].isin(["Backlog", "Comp/Bklg", "PBO"])
    ]  # had to add Backlog as Wk04 WSP order category mappings are not consistent

    bklg_only = bklg_only.merge(
        lookup_final, how="left", on=["Item", "Unit"], validate="m:1"
    )

    bklg_only.to_csv(
        os.path.join(filepaths.paths.get("chgs"), chgs_filename), index=False
    )
    return [bklg_only, lookup]
