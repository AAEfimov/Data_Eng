
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

outfig = "out/" + out_dir + "/{}.png"

types_file = outfile.format("df_types")
opt_datafile_name = datafiles_out.format("game_dataset_optcols.csv")
chunk_filename = "game_dataset_df_chunk.csv"

selected_columns = ['date', 'h_game_number', 'v_score', 'h_score', 'v_at_bats', 'v_hits', 
           'h_homeruns', 'h_passed_balls', 'saving_pitcher_id', 'h_manager_name']

def get_stat_and_optimize(datafile):
    df = pd.read_csv(datafile)

    md = evaluate_memory(df, datafile)
    write_data_to_json(md, outfile.format(f"memusage_noopt"))
    
    df_optimized = df.copy()

    df_obj, conv_d_obj = convert_object_datatypes(df)
    write_data_to_json(conv_d_obj, outfile.format("objects_memopt"))

    df_int, conv_d_int = int_downcast(df)
    write_data_to_json(conv_d_int, outfile.format("int_downcast"))

    df_float, conv_d_float = float_downcast(df)
    write_data_to_json(conv_d_float, outfile.format("float_downcast"))

    df_optimized[df_int.columns] = df_int
    df_optimized[df_float.columns] = df_float
    df_optimized[df_obj.columns] = df_obj

    print(mem_usage(df))
    print(mem_usage(df_optimized))

    md_opt = evaluate_memory(df_optimized, datafile)
    write_data_to_json(md_opt, outfile.format(f"memusage_optimized"))

    # select columns to load
    
    opt_dtypes = df_optimized.dtypes
    nc = {}

    for key in selected_columns:
         nc[key] = opt_dtypes[key]
         # print(f"key {key}:{opt_dtypes[key]}")

    write_data_to_json(nc, types_file)
    # parse_dates=['date'], infer_datetime_format=True
    read_by_columns = pd.read_csv(datafile, usecols = lambda x : x in selected_columns, dtype=nc)

    print(read_by_columns.shape)
    print("By colums size = ", mem_usage(read_by_columns))

    read_by_columns.to_csv(opt_datafile_name)

    # read by chunk
    chunks_read(datafile, selected_columns, nc, datafiles_out, chunk_filename)

if __name__ == "__main__":

    ## Evaluate and optimization
    
    # get_stat_and_optimize(datafile)

    ## PLOTTING

    # Read types
    need_dtypes = read_pandas_types(types_file)

    print(need_dtypes)
    # , parse_dates=['date'], infer_datetime_format=True

    df_plot = pd.read_csv(opt_datafile_name, usecols = lambda x : x in need_dtypes.keys(), dtype = need_dtypes)

    df_plot.info(memory_usage='deep')

    df_plot['date'] = pd.to_datetime(df_plot['date'], format="%Y%m%d")
    # 1) plot homeruns

    df_g = df_plot.groupby(['date'])['h_homeruns'].count()
 
    plot =  df_g.plot(title="homeruns per year")
    plot.get_figure().savefig(outfig.format("homeruns"))

    # 2) plot score by manager
    df_m = df_plot.groupby(['h_manager_name'])[['v_score', 'h_score']].agg( {'v_score' : ['min', 'mean', 'max'],
                                                                             'h_score' : ['min', 'mean', 'max']})

    plot2 = df_m.plot(title="score by manager", rot=90, figsize=(30,15))
    plot2.get_figure().savefig(outfig.format("score"))

    # 3) pairplot
    sns.pairplot(df_plot).savefig(outfig.format("pairplot"))



    



