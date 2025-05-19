import pandas as pd
import os


def walk_folder(path: str, type: str):
    paths = []
    for dirpath, dirnames, filenames in os.walk(path):
        if type in dirpath:  # used to distinguish from comments and articles
            for filename in filenames:
                full_path = os.path.join(dirpath, filename)
                paths.append(full_path)

    return paths


def get_raw_data(all_paths: list):
    all_dfs = []
    if all_paths:
        for filepath in all_paths:
            filename = os.path.splitext(filepath)[0].split('\\')[-1]
            print(f'reading file: {filename}')
            df = pd.read_csv(filepath, low_memory=False)
            df['file_name'] = filename
            all_dfs.append(df)

    return pd.concat(all_dfs)


if __name__ == "__main__":
    types = ['articles', 'comments']
    for type in types:
        paths = walk_folder('raw_data', type=type)
        data = get_raw_data(paths)
        data.to_csv(f'pre_data/all_{type}.csv')
