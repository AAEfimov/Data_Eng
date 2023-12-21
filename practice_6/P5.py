import os
import json
import pandas as pd
import numpy as np 
import seaborn as sns
import matplotlib.pyplot as plt 
from aux_func import *

pd.set_option("display.max_rows", 20, "display.max_columns", 200)

out_dir = "d5"
datafile = "data/asteroid.zip"

datafiles_out = "data/{}"

outfile = "out/" + out_dir + "/{}.json"
outfig = "out/" + out_dir + "/{}.png"

types_file = outfile.format("df_types")
opt_datafile_name = datafiles_out.format("asteroid_optcols.csv")
chunk_filename = "asteroid_df_chunk.csv"

selected_columns = ['spkid', 'full_name', 'diameter', 'albedo', 'epoch', 'equinox', 
           'tp', 'per', 'moid', 'class', 'rms']

def get_stat_and_optimize(datafile, chzs=None, compr='infer', nr=None):

    md = {}
    md_opt = {}
    conv_d_obj = {}
    conv_d_int = {}
    conv_d_float = {}
    #for df_chunk in pd.read_csv(datafile, compression=compr, chunksize = chzs, nrows=nr):
    try:
        df_chunk = pd.read_csv(datafile, compression=compr, chunksize = chzs, nrows=nr)

        md = evaluate_memory(df_chunk, datafile, md)
 
        df_optimized = df_chunk.copy()

        df_obj, conv_d_obj = convert_object_datatypes(df_chunk, conv_d_obj)
        df_int, conv_d_int = int_downcast(df_chunk, conv_d_int)
        df_float, conv_d_float = float_downcast(df_chunk, conv_d_float)
        
        df_optimized[df_int.columns] = df_int
        df_optimized[df_float.columns] = df_float
        df_optimized[df_obj.columns] = df_obj

        print(mem_usage(df_chunk))
        print(mem_usage(df_optimized))

        md_opt = evaluate_memory(df_optimized, datafile, md_opt)

    except:
        print("Exception!")

    write_data_to_json(conv_d_obj, outfile.format("objects_memopt"))
    write_data_to_json(conv_d_int, outfile.format("int_downcast"))
    write_data_to_json(conv_d_float, outfile.format("float_downcast"))

    write_data_to_json(md, outfile.format(f"memusage_noopt"))
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
    # # , parse_dates=['date'], infer_datetime_format=True

    df_plot = pd.read_csv(opt_datafile_name, usecols = lambda x : x in need_dtypes.keys(), dtype = need_dtypes)
    df_plot.info(memory_usage='deep')

    # 1) pairplot 
    # sns.pairplot(df_plot.select_dtypes(include=['float32'])).savefig(outfig.format("pairplot"))

    # 2) diam_by_class

    #df_g = df_plot.groupby(['class'])[['diameter']].agg( {'diameter' : ['min', 'max', 'mean']} )
 
    #plot =  df_g.plot(title="class by diametr")
    #plot.get_figure().savefig(outfig.format("diam_by_class"))

    # 3) 

    # fig, ax = plt.subplots()

    # sns.scatterplot(data=df_plot, x='epoch', y='class')

    # plt.savefig(outfig.format("Total"))

    # 4)  

    # pf_g = df_plot.groupby(['diameter'])[['albedo']].agg( {'albedo' : ['max', 'mean'],} )


    # fig, ax = plt.subplots()
    # sns.boxplot(data=pf_g.head(5000), x='diameter')
    # plt.savefig(outfig.format("Discribe_albedo"))

    # 5)

    fig, ax = plt.subplots()

    sns.scatterplot(data=df_plot, x='diameter', y='moid')

    plt.savefig(outfig.format("dm"))
    