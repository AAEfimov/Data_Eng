import os
import json
import pandas as pd
import numpy as np 
import seaborn as sns
import matplotlib.pyplot as plt 
from aux_func import *

pd.set_option("display.max_rows", 20, "display.max_columns", 60)

out_dir = "d2"
datafile = "data/flights.csv"

datafiles_out = "data/{}"

outfile = "out/" + out_dir + "/{}.json"
outfig = "out/" + out_dir + "/{}.png"

types_file = outfile.format("df_types")
opt_datafile_name = datafiles_out.format("flights_optcols.csv")
chunk_filename = "flights_df_chunk.csv"

selected_columns = ['YEAR', 'MONTH', 'DAY', 'ORIGIN_AIRPORT', 'DESTINATION_AIRPORT', 'AIR_TIME', 
           'DISTANCE', 'WHEELS_ON', 'AIRLINE_DELAY', 'WEATHER_DELAY']

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

    # 1) pairplot
    # sns.pairplot(df_plot).savefig(outfig.format("pairplot"))

    # 2) DELAY BY MONTH

    # df_del = df_plot.groupby(['MONTH'])[['AIRLINE_DELAY', 'WEATHER_DELAY']].agg({ 'AIRLINE_DELAY' : ['min', 'mean', 'max'],
    #                                                                                'WEATHER_DELAY' : ['min', 'mean', 'max']})
    # plot =  df_del.plot(title="delay per month")
    # plot.get_figure().savefig(outfig.format("delay"))

    # 3) Air time by distance
    # fig, ax = plt.subplots()

    # sns.boxplot(data=df_plot, x='DISTANCE', y='AIR_TIME')
    # plt.savefig(outfig.format("airtime_by_dist"))

    # 4) pie destiantion

    plot2 = df_plot['DESTINATION_AIRPORT'].value_counts().plot(kind='pie', title='Destination airport')
    plot2.get_figure().savefig(outfig.format("pie_destination"))

