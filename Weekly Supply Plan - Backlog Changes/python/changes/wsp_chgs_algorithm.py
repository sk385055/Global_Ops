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
import pandas as pd


__version__ = "0.0.1"


class Compiler:
    def __init__(self):
        pass


def add_tls_customer(data: pd.DataFrame = None):
    new_df = []
    for row in data.iterrows():
        if row[1]["Scenario"] != 0 and row[1]["Master Customer Name"] == 0:
            row[1]["Master Customer Name"] = "OTHER"
        new_df.append(row[1])
    df = pd.DataFrame(new_df, columns=data.columns)
    return df


def add_gop():
    """ this needs to be updated """
    # prior gop
    gop_prior = gop_prior.merge(
        fm[["Accounting Period", "WeekNum", "Count"]],
        how="left",
        left_on="SSD Mth",
        right_on="Accounting Period",
    )
    gop_prior.rename(columns={"WeekNum": "Plant_Ship_Wk"}, inplace=True)

    gop_prior["TLS_Qty"] = gop_prior["TLS_Qty"].fillna(0) / gop_prior["Count"]
    gop_prior["TLS_Prior_Qty"] = gop_prior["TLS_Qty"]

    # current gop
    gop_curr = gop_curr.merge(
        fm[["Accounting Period", "WeekNum", "Count"]],
        how="left",
        left_on="SSD Mth",
        right_on="Accounting Period",
    )
    gop_curr.rename(columns={"WeekNum": "Plant_Ship_Wk"}, inplace=True)

    gop_curr["TLS_Qty"] = gop_curr["TLS_Qty"].fillna(0) / gop_curr["Count"]
    gop_curr["TLS_Curr_Qty"] = gop_curr["TLS_Qty"]


def add_gop_fields(data: pd.DataFrame = pd.DataFrame()):
    """ temporary gop solution to add empty fields """
    if not data.empty:
        data["Scenario"] = ""
        data["Sheetname"] = ""
        data["Exclude"] = ""
        data["MDAYS"] = ""
        data["TLS Class"] = ""
        data["SSD"] = ""
        data["Year"] = ""
        data["TLS_Prior_Qty"] = 0
        data["TLS_Curr_Qty"] = 0
        data["TLS_Qty"] = 0
        data["TLS_Chg"] = 0
    return data


def create_wsp_changes_file(
    backlog_only: bool = False,
    create_csv: bool = False,
    prior_data: pd.DataFrame = None,
    curr_data: pd.DataFrame = None,
    prior_gop: pd.DataFrame = None,
    curr_gop: pd.DataFrame = None,
    datafields: object = None,
    offerpf: pd.DataFrame = None,
    chgs_filename: str = None,
    filepaths: object = None,
) -> pd.DataFrame:
    """[summary]

    Args:
        wsp (bool, optional): [description]. Defaults to False.
        prior_data (pd.DataFrame, optional): [description]. Defaults to None.
        curr_data (pd.DataFrame, optional): [description]. Defaults to None.
        prior_gop (pd.DataFrame, optional): [description]. Defaults to None.
        curr_gop (pd.DataFrame, optional): [description]. Defaults to None.
        datafields (object, optional): [description]. Defaults to None.
        offerpf (pd.DataFrame, optional): [description]. Defaults to None.
        chgs_filename (str, optional): [description]. Defaults to None.

    Returns:
        pd.DataFrame: [description]
    """
    # set-up variables
    fields = datafields.dict_of_fields()
    x_file = datafields.x_file
    y_file = datafields.y_file

    print("In changes:", x_file, "-->", y_file)

    # fields values
    if backlog_only:
        fields_dict = fields.get("bklg")
    else:
        fields_dict = fields.get("all")
    group_key = fields_dict.get("group_key")
    table_cols = fields_dict.get("table_cols")
    agg_key = fields_dict.get("agg_key")
    chg_cols = fields_dict.get("chg_cols")
    out_cols = fields_dict.get("out_cols")
    # perform grouping and get changes
    prior_grouped_data = (
        prior_data[table_cols].groupby(by=group_key).agg(agg_key).reset_index()
    )
    curr_grouped_data = (
        curr_data[table_cols].groupby(by=group_key).agg(agg_key).reset_index()
    )

    prior_grouped_data["Item Type"] = prior_grouped_data["Item Type"].astype(str)
    prior_grouped_data["Unit"] = prior_grouped_data["Unit"].astype(str)
    prior_grouped_data["Class"] = prior_grouped_data["Class"].astype(str)

    curr_grouped_data["Item Type"] = curr_grouped_data["Item Type"].astype(str)
    curr_grouped_data["Unit"] = curr_grouped_data["Unit"].astype(str)
    curr_grouped_data["Class"] = curr_grouped_data["Class"].astype(str)

    prior_grouped_data["Excl"] = prior_grouped_data["Excl"].fillna('Show').replace(0, 'Show')
    curr_grouped_data["Excl"] = curr_grouped_data["Excl"].fillna('Show').replace(0, 'Show')

    prior_grouped_data["Excl2"] = prior_grouped_data["Excl2"].fillna('Show').replace(0, 'Show')
    curr_grouped_data["Excl2"] = curr_grouped_data["Excl2"].fillna('Show').replace(0, 'Show')

    object_cols = ['Order Category4', 'Source', 'Class', 'Item', 'Item Type', 'Unit', 'LOB',
                   'SSD', 'SSD Mth', 'SSD_Qtr', 'Excl', 'Excl2', 'Exclude', 'PRM (Final)']

    for col in object_cols:
        try:
            prior_grouped_data[col] = prior_grouped_data[col].astype(str)
        except KeyError:
            pass
        try:
            curr_grouped_data[col] = curr_grouped_data[col].astype(str)
        except KeyError:
            pass

    prior_grouped_data.to_csv('./prior_grouped_data.csv', index=False)
    curr_grouped_data.to_csv('./curr_grouped_data.csv', index=False)

    print(prior_grouped_data.dtypes, curr_grouped_data.dtypes)
    bchg = pd.merge(
        prior_grouped_data,
        curr_grouped_data,
        how="outer",
        on=group_key,
        sort=False,
    )

    bchg.columns = chg_cols
    bchg["Exclude"] = "No"

    if all([prior_gop, curr_gop]):
        bout = pd.concat([bchg, prior_gop, curr_gop], sort=False)
    else:
        bout = bchg.copy()
    bout.fillna(0, inplace=True)

    print(bout.columns)
    if not backlog_only:
        bout = add_gop_fields(bout)
        bout = add_tls_customer(bout)
        bout["Wk_Chg"] = bout[f"{y_file}_Qty"] - bout[f"{x_file}_Qty"]
        bout["TLS_Chg"] = bout[f"{y_file}_Qty"] - bout["TLS_Qty"]
    else:
        bout["Wk_Chg"] = bout[f"{y_file}_Qty"] - bout[f"{x_file}_Qty"]

    bout["Class"] = bout["Class"].apply(lambda x: str(x)[:4])
    bout["Class"] = bout["Class"].astype(str)
    offerpf["Class"] = offerpf["Class"].astype(str)
    bout = bout.merge(offerpf, how="left", on="Class", validate="m:1")

    bout.rename(
        columns={
            "Range_x": "Range",
            "offer_pf_y": "offer_pf",
            "Offer PF_y": "Offer PF",
        },
        inplace=True,
    )

    bout.index.name = "Index"
    if not create_csv:
        print(f"Failed to generate: {chgs_filename}")
    else:
        print(f"Generating: {chgs_filename}")
        bout[out_cols].to_csv(os.path.join(
            filepaths.paths.get("chgs"), chgs_filename))

    return bout[out_cols]


if __name__ == "__main__":
    pass
