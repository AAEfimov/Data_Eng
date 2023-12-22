import os
import json
import pandas as pd
import numpy as np 
import seaborn as sns
import matplotlib.pyplot as plt 
from aux_func import *

pd.set_option("display.max_rows", 20, "display.max_columns", 200)

out_dir = "d4"
datafile = "data/automotive.csv.zip"

datafiles_out = "data/{}"

outfile = "out/" + out_dir + "/{}.json"
outfig = "out/" + out_dir + "/{}.png"

types_file = outfile.format("df_types")
opt_datafile_name = datafiles_out.format("automotive_optcols.csv")
chunk_filename = "automotive_df_chunk.csv"

selected_columns = ['firstSeen', 'brandName', 'modelName', 'vf_TransmissionStyle', 'askPrice', 
           'isNew', 'vf_Series', 'vf_WheelSizeRear', 'vf_Seats']

# DtypeWarning: Columns (17,53,65,67,69,91,93,107,111,120,122) have mixed types. Specify dtype option on import or set low_memory=False.


def get_stat_and_optimize(datafile, chzs = None, compr='infer', nr=None):

    md = {}
    md_opt = {}
    conv_d_obj = {}
    conv_d_int = {}
    conv_d_float = {}

    total_mem_usage = 0
    total_optimal_memusage = 0

    for df_chunk in pd.read_csv(datafile, compression=compr, chunksize = chzs, nrows=nr, low_memory=False):
        print("read chunk")
        md = evaluate_memory(df_chunk, datafile, md)
        df_chunk.info()

        df_optimized = df_chunk.copy()

        df_obj, conv_d_obj = convert_object_datatypes(df_chunk, conv_d_obj)
        df_int, conv_d_int = int_downcast(df_chunk, conv_d_int)
        df_float, conv_d_float = float_downcast(df_chunk, conv_d_float)
        
        df_optimized[df_int.columns] = df_int
        df_optimized[df_float.columns] = df_float
        df_optimized[df_obj.columns] = df_obj

        total_mem_usage += mem_usage(df_chunk)
        total_optimal_memusage += mem_usage(df_optimized)

        md_opt = evaluate_memory(df_optimized, datafile, md_opt)

    print(f"MEM USAGE {total_mem_usage} OPT {total_optimal_memusage}")

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
    # get_stat_and_optimize(datafile, chzs=100_000)

    ## PLOTTING

    # Read types
    need_dtypes = read_pandas_types(types_file)
    print(need_dtypes)

    # # , parse_dates=['date'], infer_datetime_format=True

    df_plot = pd.read_csv(opt_datafile_name, usecols = lambda x : x in need_dtypes.keys(), dtype = need_dtypes)
    df_plot.info(memory_usage='deep')

    # 1) pairplot
    # sns.pairplot(df_plot).savefig(outfig.format("pairplot"))

    # 2) brands

    #df_n = df_plot.groupby(['brandName'])[['isNew']].count()

    #df_n = df_n[(df_n['isNew'] > 100000)]

    # plot2 = df_n.plot(title="brands car", rot=90, figsize=(30,15), kind='bar')
    # plot2.get_figure().savefig(outfig.format("brands"))

    # 3) pie_brandName

    # plot2 = df_plot['brandName'].head(10).value_counts().plot(kind='pie', title='Brands', autopct='%1.0f%%')    
    # plot2.get_figure().savefig(outfig.format("pie_brandName"))

    # 4) 

    # df_trans = df_plot.groupby(['vf_TransmissionStyle'])[['vf_WheelSizeRear']].agg({'vf_WheelSizeRear' : ['max', 'mean', 'min']})

    # plt.figure(figsize=(30,5))
    # plt.title('WheelSize per Transmission')
    # plt.plot(df_trans)
    # plt.savefig(outfig.format("askPrice"))

    # 5) 

    # new_ch = df_plot[(df_plot['brandName'] == 'NISSAN')].reset_index()

    # new_ch.loc[:, 'modelName'] = new_ch['modelName'].astype('str') 
    # new_ch.loc[:, 'modelName'] = new_ch['modelName'].astype('category') 

    # new_ch = new_ch[new_ch['askPrice'] < 3000000]

    # df_group = new_ch.groupby(['modelName', 'isNew'], as_index=False)['askPrice'].mean()

    # fig, ax = plt.subplots()
    # plt.title("NISSAN model prices")
    # sns.barplot(data=df_group, x="askPrice", y="modelName", hue="isNew") 
    # plt.savefig(outfig.format("model_price"))


    # 6)

    # df_plot['firstSeen'] = pd.to_datetime(df_plot['firstSeen'], format="%Y-%m-%d")
    # df_plot['year'] = df_plot['firstSeen'].dt.year
    # df_plot['Month'] = df_plot['firstSeen'].dt.month
    # df_plot['Day'] = df_plot['firstSeen'].dt.day


    # fig, ax = plt.subplots()

    # plt.figure(figsize=(16,6))
    # plt.title("Day/Month new counts")
    # sns.heatmap(df_plot.pivot_table(values='isNew', index=['Month'], columns=['Day'], aggfunc=np.sum), annot=False, cmap="YlGnBu", cbar=True);
    # plt.savefig(outfig.format("hitmap3"))