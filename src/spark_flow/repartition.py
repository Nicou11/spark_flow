import pandas as pd
import os

def re_partition(load_dt, from_path='/tmp/sparkdata'):
    home_dir = os.path.expanduser("~")
    read_path = f'{home_dir}/{from_patth}/load_dt={load_dt}'
    write_base = f'{home_dir}/data/movie/repartition'
    write_path = f'{write_dir}/load_dt={load_dt}'

    df = pd.read_parquet(read_path)
    df['load_df'] = load_dt
    rm_dir(write_path)
    df.to_parquet(
            write_path,
            partition_cols=['load_dt','multiMovieYn','repNationCd'])
    return len(df), read_path, f'{write_path}/load_dt={load_dt}'

def rm_dir(dir_path):
    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)
