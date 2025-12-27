import datetime
import pandas as pd
import os
import numpy as np

import wsp_chgs_data_cls


def get_filepaths():
    filepaths = wsp_chgs_data_cls.FileValues()
    filepaths.dict_of_paths()
    return filepaths


def get_filters(wsp: pd.DataFrame = None):

    filt_raw = wsp["Class"].str.endswith("-").fillna(False)
    filt_sco = (
        (wsp["LOB"].str.upper() == "COMMERCE") & (
            wsp["offer_pf"].str.upper() == "SCO")
    ).fillna(False)
    filt_pos = (
        (wsp["LOB"].str.upper() == "COMMERCE") & (
            wsp["offer_pf"].str.upper() == "POS")
    ).fillna(False)
    filt_scanners = (
        (wsp["LOB"].str.upper() == "COMMERCE")
        & (wsp["offer_pf"].str.upper() == "SCANNERS")
    ).fillna(False)
    filt_other_commerce = (wsp["LOB"].str.upper() == "COMMERCE").fillna(False)

    filt_atm = (
        (wsp["LOB"].str.upper() == "BANKING") & (
            wsp["offer_pf"].str.upper() == "ATM")
    ).fillna(False)
    filt_kit = (
        (wsp["LOB"].str.upper() == "BANKING") & (
            wsp["offer_pf"].str.upper() == "KITS")
    ).fillna(False)
    filt_other_banking = (wsp["LOB"].str.upper() == "BANKING").fillna(False)

    conds = [
        filt_raw,
        filt_sco,
        filt_pos,
        filt_scanners,
        filt_other_commerce,
        filt_atm,
        filt_kit,
        filt_other_banking,
    ]
    choices = [
        "RAW Material",
        "SCO",
        "POS",
        "Scanners",
        "Commerce-Other",
        "ATM",
        "Upgrade Kits",
        "Banking-Other",
    ]
    return conds, choices


def get_qtrs(_prior: bool = True, _current: bool = True, _next: bool = True):
    current_year = datetime.datetime.now().year
    qtrs = []
    for year in range(current_year - 1, current_year + 2):
        for qtr in range(1, 5):
            if all([year == current_year - 1, _prior]):
                qtrs.append(f'{year}Q{qtr}')
            if all([year == current_year, _current]):
                qtrs.append(f'{year}Q{qtr}')
            if all([year == current_year+1, _next]):
                qtrs.append(f'{year}Q{qtr}')
    return qtrs


def backlog_filters(chgs: list = None, chgs_filename: str = None,
                    prior_week: str = None, curr_week: str = None,
                    creation_date: str = None, metric: str = None) -> list:
    bklg = chgs[0]
    wsp = chgs[1]
    filepaths = get_filepaths()
    datafields = wsp_chgs_data_cls.FieldValues(prior_week, curr_week)
    file_mb_bri = "./makebuy_bri.csv"
    file_mb_all = "./makebuy_plants.csv"
    mb_bri = pd.read_csv(
        os.path.join(filepaths.paths.get("maps"), file_mb_bri), encoding="utf-8"
    )
    mb_all = pd.read_csv(
        os.path.join(filepaths.paths.get("maps"), file_mb_all), encoding="utf-8"
    )

    wsp["Customer Groom"] = wsp["Master Customer Name"].apply(
        lambda x: str(x)[:15] + "..." if len(str(x)) > 15 else x
    )
    bklg["Customer Groom"] = bklg["Master Customer Name"].apply(
        lambda x: str(x)[:15] + "..." if len(str(x)) > 15 else x
    )

    conds, choices = get_filters(wsp)
    wsp["Products"] = np.select(condlist=conds, choicelist=choices)

    conds, choices = get_filters(bklg)
    bklg["Products"] = np.select(condlist=conds, choicelist=choices)

    wsp["LOB"] = np.where(
        wsp["Class"].str.endswith("-").fillna(False), "RAW", wsp["LOB"]
    )

    wsp = wsp.merge(
        mb_all[["Source", "make/buy"]].drop_duplicates(),
        how="left",
        on="Source",
        validate="m:1",
    )
    bklg = bklg.merge(
        mb_all[["Source", "make/buy"]].drop_duplicates(),
        how="left",
        on="Source",
        validate="m:1",
    )

    wsp["Class"] = wsp["Class"].astype(str)
    bklg["Class"] = bklg["Class"].astype(str)
    mb_bri["Class"] = mb_bri["Class"].astype(str)

    wsp = wsp.merge(
        mb_bri[["Class", "make/buy"]], how="left", on="Class", validate="m:1"
    )
    bklg = bklg.merge(
        mb_bri[["Class", "make/buy"]], how="left", on="Class", validate="m:1"
    )

    wsp["make/buy"] = np.where(
        wsp["Source"] == "BRI", wsp["make/buy_y"], wsp["make/buy_x"]
    )
    wsp["make/buy"] = np.where(
        (wsp["Source"].isin(["LG", "MAY", "ZEB", "BRI"])) & (
            wsp["make/buy"].isna()),
        "buy",
        wsp["make/buy"],
    )

    bklg["make/buy"] = np.where(
        bklg["Source"] == "BRI", bklg["make/buy_y"], bklg["make/buy_x"]
    )
    bklg["make/buy"] = np.where(
        (bklg["Source"].isin(["LG", "MAY", "ZEB", "BRI"])) & (
            bklg["make/buy"].isna()),
        "buy",
        bklg["make/buy"],
    )

    for c in ["make/buy_x", "make/buy_y"]:
        try:
            wsp.drop(columns=c, inplace=True)
            bklg.drop(columns=c, inplace=True)
            print(f"Dropped {c}")
        except KeyError as errvalue:
            print("The column doesn't exist", errvalue)

    wsp_name = os.path.join(
        filepaths.paths.get("chgs"), chgs_filename.replace("Backlog ", "")
    )
    bklg_name = os.path.join(filepaths.paths.get("chgs"), chgs_filename)
    print(wsp.shape, bklg.shape)
    print(wsp_name, bklg_name)

    col_addendum = ['Customer Groom', 'Products',
                    'make/buy', 'creation_date', 'metric', 'wk_to_wk']
    wsp['creation_date'] = creation_date
    wsp['metric'] = metric
    wsp['wk_to_wk'] = prior_week + ' --> ' + curr_week

    bklg['creation_date'] = creation_date
    bklg['metric'] = metric
    bklg['wk_to_wk'] = prior_week + ' --> ' + curr_week

    final_cols = datafields.out_cols + col_addendum
    wsp.index.rename("Index", inplace=True)
    bklg.index.rename("Index", inplace=True)
    wsp[wsp["SSD_Qtr"].isin(get_qtrs())][final_cols].to_csv(
        wsp_name, index=True, )
    bklg[final_cols].to_csv(bklg_name, index=True)
    return [bklg, wsp]


if __name__ == '__main__':
    print(get_qtrs())
