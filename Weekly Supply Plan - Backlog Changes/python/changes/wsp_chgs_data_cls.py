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


class FileValues:
    def __init__(self, inpath: str = None):
        self.inpath = r"../backlog_compare/wsp_final"
        self.finmth = r"../backlog_compare/python/maps/fin_mth.csv"
        self.offerpf = r"../backlog_compare/python/maps/offer_pf_map_active.csv"
        self.maps = r"../backlog_compare/python/maps"
        self.sndpath = r"../backlog_compare/wsp_chgs"
        self.paths = None

    def dict_of_paths(self):
        self.paths = {
            "wsps": self.inpath,
            "fmth": self.finmth,
            "offerpf": self.offerpf,
            "chgs": self.sndpath,
            "maps": self.maps,
        }
        return self.paths


class FieldValues:
    def __init__(self, x_file, y_file):
        self.x_file = x_file
        self.y_file = y_file

    @property
    def gop_cols(self):
        self.gop_fields = [
            "Source",
            "Scenario",
            "Sheetname",
            "Ref Org-Ord",
            "LOB_",
            "Product Group",
            "Offer PF",
            "Range",
            "Class",
            "TLS Class",
            "SSD",
            "Date",
            "SSD Mth",
            "SSD_Qtr",
            "Year",
            "MDAYS",
            "TLS_Qty",
            "Exclude",
            "ExcludeWSP",
        ]
        return self.gop_fields

    @property
    def group_key_all(self):
        self.group_fields_all = [
            "Region",
            "Country",
            "Source",
            "Ref Org-Ord",
            "LOB_",
            "KeyAccount",
            "Master Customer Name",
            "Order Type",
            "Order Category",
            "Order Category2",
            "Order Category3",
            "Order Category4",
            "Offer PF",
            "Range",
            "Class",
            "Item",
            "Item Type",
            "Unit",
            "SSD Mth",
            "SSD_Qtr",
            "PRM (Final)",
            "PRQ (Final)",
            "Excl",
            "Excl2",
            "LOB",
        ]
        return self.group_fields_all

    @property
    def group_key_bklg(self):
        self.group_fields_bklg = [
            "Order Category4",
            "Ref Org-Ord",
            "LOB_",
            "Source",
            "Class",
            "Item",
            "Item Type",
            "Unit",
            "SSD",
            "SSD Mth",
            "SSD_Qtr",
            "Excl",
            "Excl2",
        ]
        return self.group_fields_bklg

    @property
    def agg_key(self):
        self.agg_fields = {
            "Quantity": "sum",
            "Total Rev": "sum",
            "Total MCC": "sum",
            "Std Cost": "sum",
        }
        return self.agg_fields

    @property
    def table_cols(self):
        self.table_fields = [
            "Region",
            "Country",
            "Source",
            "Ref Org-Ord",
            "LOB_",
            "KeyAccount",
            "Master Customer Name",
            "Order Type",
            "Offer PF",
            "Range",
            "Class",
            "Item",
            "Item Type",
            "Unit",
            "SSD Mth",
            "SSD_Qtr",
            "Order Category",
            "Order Category2",
            "Order Category3",
            "Order Category4",
            "PRM (Final)",
            "PRQ (Final)",
            "Quantity",
            "Total Rev",
            "Total MCC",
            "Std Cost",
            "Excl",
            "Excl2",
            "LOB",
        ]
        return self.table_fields

    @property
    def table_cols_bklg(self):
        self.table_fields_bklg = [
            "Order Category4",
            "Ref Org-Ord",
            "LOB_",
            "Source",
            "Class",
            "Item",
            "Item Type",
            "Unit",
            "SSD",
            "SSD Mth",
            "SSD_Qtr",
            "Excl",
            "Excl2",
            "Quantity",
            "Total Rev",
            "Total MCC",
            "Std Cost",
        ]
        return self.table_fields_bklg

    @property
    def chg_cols(self):
        self.chg_fields = [
            "Region",
            "Country",
            "Source",
            "Ref Org-Ord",
            "LOB_",
            "KeyAccount",
            "Master Customer Name",
            "Order Type",
            "Order Category",
            "Order Category2",
            "Order Category3",
            "Order Category4",
            "Offer PF",
            "Range",
            "Class",
            "Item",
            "Item Type",
            "Unit",
            "SSD Mth",
            "SSD_Qtr",
            "PRM (Final)",
            "PRQ (Final)",
            "Excl",
            "Excl2",
            "LOB",
            f"{self.x_file}_Qty",
            f"{self.x_file}_Value-USD",
            f"{self.x_file}_MCC-USD",
            f"{self.x_file}_Std_Cost",
            f"{self.y_file}_Qty",
            f"{self.y_file}_Value-USD",
            f"{self.y_file}_MCC-USD",
            f"{self.y_file}_Std_Cost",
        ]
        return self.chg_fields

    @property
    def chg_cols_bklg(self):
        self.chg_fields_bklg = [
            "Order Category4",
            "Ref Org-Ord",
            "LOB_",
            "Source",
            "Class",
            "Item",
            "Item Type",
            "Unit",
            "SSD",
            "SSD Mth",
            "SSD_Qtr",
            "Excl",
            "Excl2",
            f"{self.x_file}_Qty",
            f"{self.x_file}_Value-USD",
            f"{self.x_file}_MCC-USD",
            f"{self.x_file}_Std_Cost",
            f"{self.y_file}_Qty",
            f"{self.y_file}_Value-USD",
            f"{self.y_file}_MCC-USD",
            f"{self.y_file}_Std_Cost",
        ]
        return self.chg_fields_bklg

    @property
    def out_cols(self):
        self.out_fields = [
            "Region",
            "Country",
            "Item",
            "Item Type",
            "Ref Org-Ord",
            "LOB_",
            "KeyAccount",
            "MDAYS",
            "Master Customer Name",
            "Order Category",
            "Order Category2",
            "Order Category3",
            "Order Category4",
            "Offer PF",
            "offer_pf",
            "Order Type",
            "PRM (Final)",
            "PRQ (Final)",
            "Range",
            "Class",
            "TLS Class",
            "SSD",
            "SSD Mth",
            "SSD_Qtr",
            "Year",
            "Scenario",
            "Sheetname",
            "Source",
            "Exclude",
            "Unit",
            "Excl",
            "Excl2",
            "LOB",
            f"{self.x_file}_Qty",
            f"{self.x_file}_Value-USD",
            f"{self.x_file}_MCC-USD",
            f"{self.x_file}_Std_Cost",
            f"{self.y_file}_Qty",
            f"{self.y_file}_Value-USD",
            f"{self.y_file}_MCC-USD",
            f"{self.y_file}_Std_Cost",
            "TLS_Prior_Qty",
            "TLS_Curr_Qty",
            "TLS_Qty",
            "Wk_Chg",
            "TLS_Chg",
        ]
        return self.out_fields

    @property
    def out_cols_bklg(self):
        self.out_fields_bklg = [
            "Order Category4",
            "Ref Org-Ord",
            "LOB_",
            "Source",
            "Class",
            "Item",
            "Item Type",
            "Unit",
            "SSD",
            "SSD Mth",
            "SSD_Qtr",
            "Excl",
            "Excl2",
            f"{self.x_file}_Qty",
            f"{self.x_file}_Value-USD",
            f"{self.x_file}_MCC-USD",
            f"{self.x_file}_Std_Cost",
            f"{self.y_file}_Qty",
            f"{self.y_file}_Value-USD",
            f"{self.y_file}_MCC-USD",
            f"{self.y_file}_Std_Cost",
            "Wk_Chg",
        ]
        return self.out_fields_bklg

    def dict_of_fields(self):
        self.fields = {
            "all": {
                "chg_cols": self.chg_cols,
                "out_cols": self.out_cols,
                "gop_cols": self.gop_cols,
                "table_cols": self.table_cols,
                "group_key": self.group_key_all,
                "agg_key": self.agg_key,
            },
            "bklg": {
                "chg_cols": self.chg_cols_bklg,
                "out_cols": self.out_cols_bklg,
                "gop_cols": self.gop_cols,
                "table_cols": self.table_cols_bklg,
                "group_key": self.group_key_bklg,
                "agg_key": self.agg_key,
            },
        }
        return self.fields


if __name__ == "__main__":
    datafields = FieldValues("Wk01", "Wk08")
    datafields.chg_cols
    datafields.out_cols
    datafields.gop_cols
    datafields.table_cols
    datafields.group_key_all
    datafields.group_key_bklg
    datafields.agg_key
    print(datafields.dict_of_fields)
    filepaths = FileValues()
    print(filepaths.dict_of_paths)
