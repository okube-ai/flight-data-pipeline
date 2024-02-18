import os

import numpy as np
import pandas as pd
from scipy.io import loadmat
import datetime

# --------------------------------------------------------------------------- #
# Read Data                                                                   #
# --------------------------------------------------------------------------- #

i = -1
dirpath = "../data/tail_687_1"
for filename in os.listdir(dirpath):
    i += 1
    if i > 2:
        continue

    mat_filepath = os.path.join(dirpath, filename)
    pq_filepath = mat_filepath.replace(".mat", ".parquet")

    # if os.path.exists(pq_filepath):
    #     continue

    try:
        data = loadmat(mat_filepath, squeeze_me=True, simplify_cells=True)
    except ValueError:
        continue
    try:
        t0 = datetime.datetime(
            year=data["DATE_YEAR"]["data"][0],
            month=data["DATE_MONTH"]["data"][0],
            day=data["DATE_DAY"]["data"][0],
            hour=data["GMT_HOUR"]["data"][0],
            minute=data["GMT_MINUTE"]["data"][0],
            second=data["GMT_SEC"]["data"][0],
        )
    except:
        continue

    print(f"Processing {filename} - {t0} ")

    series = {}

    for k in data.keys():
        if k.startswith("_"):
            continue

        _data = data[k]["data"]
        _rate = data[k]["Rate"]

        print(k, data[k]["Description"])

        if _rate > 4:
            continue

        s = pd.Series(
            data=_data,
            name=k,
            index=t0 + pd.to_timedelta(np.arange(0, len(_data)) / _rate * 1e9)
        )
        series[k] = s

    df = pd.DataFrame(series)
    df["tstamp"] = df.index.astype("str").str.replace(" ", "T")
    df = df.reset_index(drop=True)

    # df.to_parquet(pq_filepath.replace(".mat", ".parquet"))
