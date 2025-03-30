import glob
from gc import collect
from pathlib import Path

import pandas as pd

# from azure.ai.ml import command, Input, Output


def read_table(file: Path, kwargs=None) -> pd.DataFrame:
    """reads [csv, parquet, json, excel] table from file"""
    assert file.exists()

    readers = {
        "csv": pd.read_csv,
        "parquet": pd.read_parquet,
        "json": pd.read_json,
        "xls": pd.read_excel,
        "xlsx": pd.read_excel,
    }
    if file.suffix[1:] not in readers.keys():
        raise ValueError(f"Unsupported file type: {file.suffix[1:]}")

    return readers[file.suffix[1:]](file, **(kwargs or {}))


def merge_all(
    dfs: list, on: str = None, how: str = "inner", silent_mode: bool = False, drop_dups: bool = True
) -> pd.DataFrame:
    """
    Merges all pandas dataframes.
    :param dfs: a list of pandas dataframes for merging.
    :param on: list of column names or a column that will be used for merging. By default, it is
    None, in this case will be found all the inner columns and used as `on`.
    :param silent_mode: logs will be shown if True
    :param drop_dups: clean duplicates for decrease memory usage, will be executed `.drop_duplicates(keep='first')`
    """
    # 1. validation
    assert len(dfs) > 1, "Required at least 2 dfs for merging"
    # py type
    assert all([isinstance(df, pd.DataFrame) for df in dfs]), "all items in `dfs` must be instance of pandas.DataFrame"
    # columns
    if isinstance(on, str):
        assert all([on in df.columns.to_list() for df in dfs]), f"not all columns has on=`{on}` column"

    elif isinstance(on, (list, tuple)):
        assert all([set(on).issubset(set(df.columns.to_list())) for df in dfs]), "not all columns exists in dfs."

    # if on = for all inner cols
    if on is None:
        on = [set(df.columns.to_list()) for df in dfs]
        on = list(set.intersection(*on))
        if not silent_mode:
            print(f"Found columns for merging:\n\t{on}")
    # do
    if not silent_mode:
        print(f"Starting for {len(dfs)} dfs:")
    left = dfs[0].copy()
    for i in range(1, len(dfs)):
        right = dfs[i].copy()
        if not silent_mode:
            print(f"\tMerging df {i}")
        if drop_dups is True:
            left = left.merge(right, on=on, how=how).drop_duplicates(keep="first")
        else:
            left = left.merge(right, on=on, how=how)
        collect()
    del right, dfs
    collect()
    return left


def get_last_n_dir(path: Path, n: int, ext="csv") -> list:
    """
    returns last n files from structure like `dir/year/month/day/file.ext`
    """
    files = list(path.rglob(f"*.{ext}"))
    files_processed = [list(reversed(str(p).split("/")[-2:-5:-1])) for p in files]
    weights = []
    for pth in files_processed:
        weights.append(sum([int(pth[0] * 100), int(pth[1] * 10), int(pth[2] * 1)]))

    mapping = {file: weight for file, weight in zip(files, weights)}

    files_sorted = list(sorted(files, reverse=True, key=lambda f: mapping[f]))
    return files_sorted[:n]


def read_concat_all(folder: Path, ext: str = "csv") -> pd.DataFrame:
    """
    read all files in `folder` with `ext` extension and concat them into one dataframe
    supported extensions: ['csv', 'parquet', 'json', 'xls', 'xlsx']
    """
    assert folder.exists(), f"Folder not found: {folder}"
    assert ext in ["csv", "parquet", "json", "xls", "xlsx"], f"Unsupported ext: {ext}"
    files = list(folder.rglob(f"*.{ext}"))
    dfs = [read_table(f) for f in files]
    return pd.concat(dfs)


def recursive_glob_list(folders: list, file_ext: str = "parquet"):
    """Takes a list of folders and returns a list of files recursively"""
    files = []
    for f in folders:
        files += glob.glob(f"{f}/**/*.{file_ext}", recursive=True)
    return files


def get_last_n_days(path: Path, days=90, freq=2, folder_name="folder"):
    """
    beta
    """

    def get_file_mark_mapping(files: list, dir_name: str):
        res = {}
        for file in files:
            tmp_name = file.split(f"/{dir_name}/")[-1].split("/")[:-1]
            tmp_name = list(reversed([int(t) for t in tmp_name]))
            tmp_name = sum([t * (10**i) for i, t in enumerate(tmp_name, start=0)])
            res[file] = tmp_name
        return res

    get_last = days // freq
    files = recursive_glob_list([path])
    mapping = get_file_mark_mapping(files, dir_name=folder_name)
    sorted_files = sorted(files, key=lambda file: mapping[file], reverse=True)
    last_files = sorted_files[:get_last]
    dfs = [pd.read_parquet(df) for df in last_files]
    return pd.concat(dfs)
