### Start solution here

import pandas as pd
import os

def read_data(path, delimiter=','):    
    return pd.read_csv(path, sep=delimiter)

def create_data_soruce(df, name):
    df['source'] = name
    return df

def consolidate_data(dfs):
    return pd.concat(dfs, ignore_index= True)

def save_output(df, out_dir, file_name):
    check_dir = os.path.isdir(out_dir)
    if not check_dir:
        os.makedirs(out_dir)
    df.to_csv(out_dir+file_name)


if __name__ == '__main__':
    df1 = read_data('input/data_source_1/sample_data.2.dat', '|')
    df1 = create_data_soruce(df1, 'sample_data.2.dat')
    df2 = read_data('input/data_source_1/sample_data.1.csv')
    df2 = create_data_soruce(df1, 'sample_data.1.csv')
    
    material_ref = read_data('input/data_source_2/material_reference.txt')
    df3 = read_data('input/data_source_2/sample_data.3.dat')
    df3 = create_data_soruce(df1, 'sample_data.3.dat')
    
    final_df = consolidate_data([df1, df2, df3])
    
    final_df = final_df.merge(material_ref, left_on='material_id', right_on='id')
    save_output(final_df, 'output/', 'consolidated_output.1.csv')