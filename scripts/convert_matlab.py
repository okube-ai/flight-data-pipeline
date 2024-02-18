import os

import numpy as np
import pandas as pd
from scipy.io import loadmat
import datetime

# --------------------------------------------------------------------------- #
# Read Data                                                                   #
# --------------------------------------------------------------------------- #

tail = "687_1"

i = -1
dirpath = f"../data/tail_{tail}"
for filename in os.listdir(dirpath):
    i += 1
    # if i > 2:
    #     continue

    mat_filepath = os.path.join(dirpath, filename)
    pq_filepath = mat_filepath.replace(f"/tail_{tail}/", f"/tail_{tail}_pq/")
    pq_filepath = pq_filepath.replace(".mat", ".parquet")
    json_filepath = mat_filepath.replace(f"/tail_{tail}/", f"/tail_{tail}_json/")
    json_filepath = json_filepath.replace(".mat", ".json")

    # if os.path.exists(pq_filepath):
    #     print("Skipping", mat_filepath)
        # continue

    try:
        data = loadmat(mat_filepath, squeeze_me=True, simplify_cells=True)
    except:
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

        # print(k, data[k]["Description"])

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
    df["file_id"] = filename
    df = df.reset_index(drop=True)

    df.to_parquet(pq_filepath)

    df_meta = pd.DataFrame({
        "file_id": [filename],
        "aircraft_manufacturer": ["Airbus"],
        "aircraft_model": ["A320"],
    })
    df_meta.to_json(json_filepath, orient="records")
