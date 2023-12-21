import os
import json
import pandas as pd
import numpy as np 
import seaborn as sns
import matplotlib.pyplot as plt 
from aux_func import *

pd.set_option("display.max_rows", 20, "display.max_columns", 60)

out_dir = "d3"
datafile = "data/vacancies.csv.gz"

datafiles_out = "data/{}"

outfile = "out/" + out_dir + "/{}.json"
outfig = "out/" + out_dir + "/{}.png"

types_file = outfile.format("df_types")
opt_datafile_name = datafiles_out.format("vacancies_optcols.csv")
chunk_filename = "vacancies_df_chunk.csv"

selected_columns = ['id', 'schedule_id', 'premium', 'employer_id', 'type_id', 'type_name', 
           'salary_from', 'salary_to', 'code', 'employment_id']

def get_stat_and_optimize(datafile, compr='infer', chzs=None, nr=None):
    
    md = {}
    md_opt = {}
    conv_d_obj = {}
    conv_d_int = {}
    conv_d_float = {}
    total_mem_usage = 0
    total_optimal_memusage = 0

    for df_chunk in pd.read_csv(datafile, compression=compr, chunksize = chzs, nrows=nr):
        print("read chunk")
        md = evaluate_memory(df_chunk, datafile, md)
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

    # get_stat_and_optimize(datafile, chzs=200_000)

    ## PLOTTING

    # Read types
    need_dtypes = read_pandas_types(types_file)

    print(need_dtypes)
    # , parse_dates=['date'], infer_datetime_format=True

    df_plot = pd.read_csv(opt_datafile_name, usecols = lambda x : x in need_dtypes.keys(), dtype = need_dtypes)

    df_plot.info(memory_usage='deep')

    # 1) pairplot

    # sns.pairplot(df_plot).savefig(outfig.format("pairplot"))

    # 2) premium sale

    # df_m = df_plot.groupby(['premium'])[['salary_from', 'salary_to']].agg( {'salary_from' : ['min', 'mean', 'max'],
    #                                                                           'salary_to' : ['min', 'mean', 'max']})

    # plot2 = df_m.plot(kind='barh', title="premium sale", rot=90, figsize=(30,15))
    # plot2.get_figure().savefig(outfig.format("premium"))


    # 3)   empoyr_salary  
    
    # fig, ax = plt.subplots()
    # sns.jointplot(x="employer_id", y="salary_from", data=df_plot, hue="premium") #scatter
    # plt.savefig(outfig.format("empoyr_salary"))

    # 4)

    # plot2 = df_plot['schedule_id'].value_counts().plot(kind='pie', title='schedule', autopct='%1.0f%%')
    # plot2.get_figure().savefig(outfig.format("pie_schedule"))

    # 5)

    df_sched = df_plot.groupby(['schedule_id', 'premium'])[['salary_from']].agg({'salary_from' : ['mean'],
                                                                                })
    
    
    print(df_sched.head(10))

    plot2 = df_sched.plot(kind='barh', title="salary_by_sched", figsize=(30,15))
    plot2.get_figure().savefig(outfig.format("salary_by_sched"))
