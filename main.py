from parsing.awr_parser import AWRParser, get_tables_from_json
import os.path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.tools as plotly_tools
import plotly.graph_objs as go
from pythresh.thresholds.zscore import ZSCORE
from pythresh.thresholds.iqr import IQR
import datapane as dp

from reporting.plot_utils import *
from reporting.reporting_utils import *

from preprocessing.awr_preprocessing.load_profile_processing import LoadProcessor
from preprocessing.awr_preprocessing.sql_processing import SqlProcessor
from preprocessing.awr_preprocessing.wait_classes_processing import WaitClassProcessor
from preprocessing.awr_preprocessing.foreground_events_waits_processing import ForegroundEventWaitProcessor
from preprocessing.awr_preprocessing.tablespace_io_processing import TablespaceIoProcessor

from args_parser import args


### PARSE DATA

data = get_tables_from_json(args.tables)
p = AWRParser(data)

print("Parsing data...")
p.make_csv(mode=args.mode, 
           input_dir=args.inputDir, 
           output_dir=args.outputDir, 
           recursive=args.recursive)

print("Parsing complete!")



### GENERATE REPORT

print("Generating report...")
input_path = args.inputDir
l = LoadProcessor('load-prc', input_path=input_path)
df = l.grouped_dfs

melt_list = []
for i in range(l.tot_instances):
    df_melt = melt_df(df[i])
    df_melt['INST_NUM'] = np.full(len(df_melt), i+1)
    melt_list.append(df_melt)

df_melt = pd.concat(melt_list)


# df_melt = melt_df(df)
fig1 = generate_overview(df_melt)
fig2 = generate_big_plots(df_melt)

metric_list = ['DB Time(s)', 'Hard parses (SQL)', 'Read IO (MB)', 'Write IO (MB)']

peaks_list = []
figures_list = []
for i in range(l.tot_instances):
    peak_list_inst, figure_list_inst = generate_peaks_figures(l, i, df[i], metric_list)
    peaks_list.append(peak_list_inst)
    figures_list.append(figure_list_inst)


### SQL Processor
p = SqlProcessor('sql-proc', input_path=input_path)

# list of 3 tuples, one for each instance
top_list = []
for i in range(p.get_num_instances()):
    t1,t2,t3 = p.get_top_n(n=3, instance=i, start=str(peaks_list[i][0].index[0]), end=str(peaks_list[i][0].index[0]))
    top_list.append((t1,t2,t3))


wc = WaitClassProcessor('waitClasses-prc', input_path=input_path)
# fig3 = generate_wait_classes_plot(wc.grouped_dfs[1])


fw = ForegroundEventWaitProcessor('feWait-prc', input_path=input_path)

overview_list = generate_overiview_list(melt_list)
overview_list.insert(0, dp.Plot(
    fig1, label='All Instances'
))


big_plots_list = generate_big_plots_lis(melt_list)
big_plots_list.insert(0, dp.Plot(
    fig2, label='All Instances'
))


wait_class_list = generate_wait_class_list(wc)

tio = TablespaceIoProcessor('tbs-proc', input_path=input_path)


select_list = []

for i in range(len(figures_list)): # for each instance
    block_list = []
    for j in range(len(metric_list)):
        block = generate_critical_timestamp_block(p, wc, fw, tio, figures_list[i][j], peaks_list[i][j], metric_list[j],
                                                  instance=i)
        block_list.append(block)

    select_block = dp.Select(
        blocks=block_list,
        label=f'Instance {i+1}'
    )
    select_list.append(select_block)


### host info ###
df_host = pd.read_csv('data/parsed/centrico_2h/host_info.csv')
df_host = df_host.set_index('Host Name')
### db info ###
df_db = l.df[['DB_NAME', 'DB_ID', 'UNIQUE_NAME', 'ROLE', 'INSTANCE_NAME', 'INST_NUM']].drop_duplicates()
df_db = df_db.set_index('INSTANCE_NAME')

# set the number of instances
NUM_INSTANCES=l.get_num_instances(df)

#TODO reformat all "Critical Timestamps" tables
#TODO add log io stats

if NUM_INSTANCES > 1:

    datapane_app = dp.Blocks(
        dp.Page(
            dp.Group(
                dp.Text("## Database Info"),
                dp.Table(df_db),
                columns=1
            ),
            dp.HTML("""
                <html>
                <body style="background-color:white;">
                    <div>
                    <p> &nbsp&nbsp&nbsp  </p>
                    </div>
                </body>
                </html>
                """),
            dp.Group(
                dp.Text("## Host Info"),
                dp.Table(df_host),
                columns=1
            ),
            title='System Info'
        ),
        dp.Page(
            dp.Select(
                blocks=overview_list
            ),
            title='Overview'
        ),
        dp.Page(
            dp.Select(
                blocks=big_plots_list
            ),
            title="Load Profile"
        ),
        dp.Page(
            dp.Select(
                blocks=select_list
            ),
            title="Critical Timestamps"
        ),
        dp.Page(
            dp.Select(
                blocks=wait_class_list
            ),
            title="Wait Classes",
        ),
        dp.Page(
            dp.Text("#### Top 10 most used tablespace"),
            dp.Text("###### (ordered by IOs (Reads + Writes) desc)"),
            dp.Select(
                blocks=generate_tbs_io_list(tio),
            ),
            title="Tablespace IO"
        )
    )

else:

    datapane_app = dp.Blocks(
        dp.Page(
            dp.Group(
                dp.Text("## Database Info"),
                dp.Table(df_db),
                columns=1
            ),
            dp.HTML("""
                <html>
                <body style="background-color:white;">
                    <div>
                    <p> &nbsp&nbsp&nbsp  </p>
                    </div>
                </body>
                </html>
                """),
            dp.Group(
                dp.Text("## Host Info"),
                dp.Table(df_host),
                columns=1
            ),
            title='System Info'
        ),
        dp.Page(
            dp.Blocks(
                blocks=overview_list[1:]  # only the summary plot
            ),
            title='Overview'
        ),
        dp.Page(
            dp.Blocks(
                blocks=big_plots_list[1:]  # only the summary plot
            ),
            title="Load Profile"
        ),
        dp.Page(
            dp.Blocks(
                blocks=select_list
            ),
            title="Critical Timestamps"
        ),
        dp.Page(
            dp.Blocks(
                blocks=wait_class_list
            ),
            title="Wait Classes",
        ),
        dp.Page(
            dp.Text("#### Top 10 most used tablespace"),
            dp.Text("###### (ordered by IOs (Reads + Writes) desc)"),
            dp.Blocks(
                blocks=generate_tbs_io_list(tio),
            ),
            title="Tablespace IO"
        )
    )



dp.save_report(
    datapane_app,
    path="res/awr/awr_report.html",
    formatting=dp.Formatting(
        light_prose=False,
        accent_color="DarkSlateBlue",
        bg_color="#EEE",
        # text_alignment=dp.TextAlignment.RIGHT,
        font=dp.FontChoice.MONOSPACE,
        width=dp.Width.FULL,
    )
)

print("Report created!")