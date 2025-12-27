import pandas as pd
import math
from collections import Counter
import numpy as np
import os

import wsp_chgs_data_cls


def get_filepaths():
    filepaths = wsp_chgs_data_cls.FileValues()
    filepaths.dict_of_paths()
    return filepaths


def backlog_map(chgs: list = None, chgs_filename: str = None) -> list:
    backlog = chgs[0]
    lookup = chgs[1]
    filepaths = get_filepaths()
    backlog.rename(
        columns={"Order Category4_x": "Order Category4",
                 "Item Type_x": "Item Type"},
        inplace=True,
    )

    changes_columns = lookup.columns.to_list()

    backlog["fileref"] = "backlog"
    lookup["fileref"] = "lookup"

    new_bklg = pd.concat([backlog, lookup])
    new_bklg = new_bklg[new_bklg["fileref"] == "backlog"]

    new_bklg.shape

    new_bklg["TLS Class"] = new_bklg["Class"]

    new_bklg[changes_columns].to_csv(
        os.path.join(filepaths.paths.get("chgs"), chgs_filename), index=False
    )
    return [new_bklg, lookup]
