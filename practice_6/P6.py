
import os
import json
import pandas as pd
import numpy as np 
import seaborn as sns
import matplotlib.pyplot as plt 
from aux_func import *

pd.set_option("display.max_rows", 20, "display.max_columns", 60)

out_dir = "d6"
datafile = "data/Crime_Data_from_2010_to_2019.csv"

datafiles_out = "data/{}"

outfile = "out/" + out_dir + "/{}.json"
outfig = "out/" + out_dir + "/{}.png"

types_file = outfile.format("df_types")
opt_datafile_name = datafiles_out.format("Crime_dataset_optcols.csv")
chunk_filename = "Crime_dataset_df_chunk.csv"

selected_columns = ['DR_NO', 'DATE OCC', 'TIME OCC', 'AREA ', 'AREA NAME', 'Rpt Dist No', 'Vict Age', 'Vict Sex', 
           'Vict Descent', 'Weapon Used Cd', 'Weapon Desc']

def get_stat_and_optimize(datafile):

    df = pd.read_csv(datafile)

    print(df.info())

    md = evaluate_memory(df, datafile)
    
    df_optimized = df.copy()

    df_obj, conv_d_obj = convert_object_datatypes(df)
    df_int, conv_d_int = int_downcast(df)
    df_float, conv_d_float = float_downcast(df)


    df_optimized[df_int.columns] = df_int
    df_optimized[df_float.columns] = df_float
    df_optimized[df_obj.columns] = df_obj

    print(mem_usage(df))
    print(mem_usage(df_optimized))

    md_opt = evaluate_memory(df_optimized, datafile)

    write_data_to_json(md, outfile.format(f"memusage_noopt"))
    write_data_to_json(conv_d_obj, outfile.format("objects_memopt"))
    write_data_to_json(conv_d_int, outfile.format("int_downcast"))
    write_data_to_json(conv_d_float, outfile.format("float_downcast"))
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

    df_plot = pd.read_csv(opt_datafile_name, usecols = lambda x : x in need_dtypes.keys(), dtype = need_dtypes, parse_dates=['DATE OCC'])

    df_plot.info(memory_usage='deep')

    
    # 1) pairplot
    # sns.pairplot(df_plot).savefig(outfig.format("pairplot"))

    # 2)

    # plt.figure(figsize=(30,5))
    # gr_obj = df_plot.groupby(["DATE OCC", "Vict Sex"], as_index=False)['Vict Age'].mean()
    # sns.set_style("ticks",{'axes.grid' : True})
    # sns.lineplot(data=gr_obj, x="DATE OCC", y='Vict Age', hue="Vict Sex").set(title='Avg Age')
    # plt.savefig(outfig.format("avg_age"))

    # 3)

    # df_gr_date = df_plot.groupby(["AREA "])['Weapon Used Cd'].count()

    # plot1 = df_gr_date.plot(kind='bar', title='Weapon Used Cd')
    # plot1.get_figure().savefig(outfig.format("bar_Weapon_Used"))

    # 4)

    # df_g = df_plot.groupby(pd.Grouper(key = 'DATE OCC', freq='Y'))[['Vict Age']].agg({'Vict Age' : ['min' , 'max', 'mean']})
    # plot =  df_g.plot(title="Vict Age per year")
    # plot.get_figure().savefig(outfig.format("time"))

    # 5) 

    date_from = pd.to_datetime("20000101", format = "%Y%m%d")
    date_to = pd.to_datetime("20160101", format = "%Y%m%d")

    df_hitmap = df_plot[(df_plot['DATE OCC'] > date_from) & (df_plot['DATE OCC'] < date_to)] 

    df_hitmap['year'] = df_hitmap['DATE OCC'].dt.year
    df_hitmap['Month'] = df_hitmap['DATE OCC'].dt.month
    df_hitmap['Day'] = df_hitmap['DATE OCC'].dt.day

    # fig, ax = plt.subplots()

    # plt.figure(figsize=(16,6))
    # plt.title("Year/Month AREA Weapon Used Cd")
    # sns.heatmap(df_hitmap.pivot_table(values='Weapon Used Cd', index=['year'], columns=['Month'], aggfunc=np.sum), annot=True, cmap="YlGnBu", cbar=True);
    # plt.savefig(outfig.format("hitmap"))

    # 6)

    # fig, ax = plt.subplots()
    # plt.figure(figsize=(16,6))
    # plt.title("Year/Month AREA Rpt Dist No")

    # sns.heatmap(df_hitmap.pivot_table(values='Rpt Dist No', index=['year'], columns=['Month'], aggfunc=np.sum), annot=True, cmap="YlGnBu", cbar=True);
    # plt.savefig(outfig.format("hitmap2"))

    # # 7)


    # fig, ax = plt.subplots()

    # plt.figure(figsize=(16,6))
    # plt.title("Day/Month AREA Victim")
    # sns.heatmap(df_hitmap.pivot_table(values='AREA ', index=['Month'], columns=['Day'], aggfunc=np.sum), annot=False, cmap="YlGnBu", cbar=True);
    # plt.savefig(outfig.format("hitmap3"))

    # # 8)

    # df_gr_date = df_plot.groupby(["AREA "])['Weapon Used Cd'].count()

    # plot2 = df_gr_date.plot(kind='pie', title='AREA weapon used', autopct='%1.0f%%')
    # plot2.get_figure().savefig(outfig.format("pie_Weapon_Used"))
    



