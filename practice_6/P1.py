
import os
import json
import pandas as pd
import numpy as np 
import seaborn as sns
import matplotlib.pyplot as plt 
from aux_func import *

pd.set_option("display.max_rows", 20, "display.max_columns", 60)


out_dir = "d1"
datafile = "data/game_logs.csv"

datafiles_out = "data/{}"

outfile = "out/" + out_dir + "/{}.json"

if __name__ == "__main__":
    df = pd.read_csv(datafile)

    evaluate_memory(df, datafile, outfile)
    
    df_optimized = df.copy()

    df_obj = convert_object_datatypes(df, outfile)
    df_int = int_downcast(df, outfile)
    df_float = float_downcast(df, outfile)

    df_optimized[df_int.columns] = df_int
    df_optimized[df_float.columns] = df_float
    df_optimized[df_obj.columns] = df_obj

    print(mem_usage(df))
    print(mem_usage(df_optimized))

    evaluate_memory(df_optimized, datafile, outfile, 'optimized')

    # select columns to load
    sc = ['date', 'h_line_score', 'v_line_score', 'winning_pitcher_name', 'winning_pitcher_id', 'losing_pitcher_name', 
          'losing_pitcher_id', 'h_homeruns', 'h_passed_balls', 'saving_pitcher_id', 'h_manager_name']
    
    opt_dtypes = df_optimized.dtypes
    nc = {}

    for key in sc:
        nc[key] = opt_dtypes[key]
        # print(f"key {key}:{opt_dtypes[key]}")

    read_by_columns = pd.read_csv(datafile, usecols = lambda x : x in sc, dtype=nc, parse_dates=['date'], infer_datetime_format=True)

    print(read_by_columns.shape)
    print("By colums size = ", mem_usage(read_by_columns))

    read_by_columns.to_csv(datafiles_out.format("game_dataset_optcols.csv"))

    # read by chunk

    chunks_read(datafile, sc, nc, datafiles_out, "game_dataset_df_chunk.csv")



    



