import datapane as dp

# from mmparser.reporting.plot_utils import *
from .plot_utils import *

def generate_timestamp_block_tables(t1, t2, t3, pie_df, fw_df, tbs_df, label):
    block = dp.Group(
        dp.Group(
            dp.Blocks(
                dp.Text("""## Wait Classes % DB time"""),
                dp.Plot(generate_wait_classes_pie(pie_df))
            ),
            dp.Blocks(
                dp.Text("""## Top 10 Foreground Events by Total Wait Time"""),
                dp.DataTable(fw_df)
            ),
            widths=[40, 60],
            columns=2
        ),
        dp.Text("""## Top CPU Time (s)"""),
        dp.DataTable(t1),
        dp.Text("""## Top Elapsed Time (s)"""),
        dp.DataTable(t2),
        dp.Text("""## Top User I/O Time (s)"""),
        dp.DataTable(t3),
        dp.Text("""## Tablespace IO"""),
        dp.DataTable(tbs_df),
        label=label,
        columns=1
    )

    return block


def generate_timestamps_block_list(sqlProcessor, waitClassesProcessor, foregroundEventWaitProcessor,
                                   tbsProcessor, peak_list, instance, n=3):
    block_list = []
    for ts in peak_list:
        ts = str(ts)
        t1, t2, t3 = sqlProcessor.get_top_n(n=n, instance=instance, start=ts, end=ts)
        pie_df = waitClassesProcessor.get_dbtime_percentages_instance(instance=instance, timestamp=ts)
        fw_df = foregroundEventWaitProcessor.filter_by_date(foregroundEventWaitProcessor.dfs[instance],
                                                            start=ts, end=ts)
        tbs_df = tbsProcessor.filter_by_date(tbsProcessor.dfs[instance],
                                                start=ts, end=ts)
        block = generate_timestamp_block_tables(t1, t2, t3, pie_df, fw_df, tbs_df, label=ts)
        block_list.append(block)

    return block_list


def generate_timestamps_dropdown(block_list):
    dropdown = dp.Select(
        blocks=block_list,
        type=dp.SelectType.DROPDOWN
    )

    return dropdown


def generate_critical_timestamp_block(sqlProcessor, waitClassesProcessor, foregroundEventWaitProcessor,
                                        tbsProcessor, figure, peak_df, label, instance):
    peak_list = list(peak_df.index)

    if len(peak_list) == 0:  # less than 2 peaks dp.Select DOES NOT work
        block = dp.Group(
            dp.Plot(figure),
            label=label,
        )

        return block
    
    if len(peak_list) == 1:
        peak_list.append(peak_list[0])
    
    block_list = generate_timestamps_block_list(sqlProcessor, waitClassesProcessor, foregroundEventWaitProcessor,
                                                tbsProcessor, peak_list, instance=instance, n=10)
    
    dropdown = generate_timestamps_dropdown(block_list)

    block = dp.Group(
        dp.Group(
            dp.Plot(figure),
            # TODO change this, for now showing only 10 values but anomalies should not be too much
            dp.Table(peak_df[[label]].head(10)),
            widths=[75, 25],
            columns=2
        ),
        dropdown,
        label=label,
        columns=1
    )

    return block


def generate_overiview_list(melt_list):
    block_list = []
    for i, df_melt in enumerate(melt_list):
        fig = generate_overview_instance(df_melt)
        page = dp.Plot(
            fig, label=f'Instance {i + 1}'
        )
        block_list.append(page)

    return block_list


def generate_big_plots_lis(melt_list):
    block_list = []
    for i, df_melt in enumerate(melt_list):
        fig = generate_big_plots_instace(df_melt)
        page = dp.Plot(
            fig, label=f'Instance {i + 1}'
        )
        block_list.append(page)

    return block_list


def generate_wait_class_list(waitClassProcessor):
    block_list = []
    for i, grouped_df in enumerate(waitClassProcessor.grouped_dfs):
        fig = generate_wait_classes_plot(grouped_df)
        page = dp.Plot(
            fig, label=f'Instance {i + 1}'
        )
        block_list.append(page)

    return block_list


def generate_tbs_io_list_instance(tbsProcessor, instance):
    tbs_list = tbsProcessor.get_most_used_tbs(instance=instance)
    block_list = []
    for tbs in tbs_list:
        fig = generate_tbs_io_plot(tbsProcessor, tablespace_name=tbs, instance=instance)
        block_list.append(dp.Plot(
            fig, label=tbs
        ))

    return block_list


def generate_tbs_io_list(tbsProcessor):
    select_list = []
    for i in range(tbsProcessor.get_num_instances(tbsProcessor.df)):
        sel = dp.Select(
            blocks=generate_tbs_io_list_instance(tbsProcessor, instance=i),
            type=dp.SelectType.DROPDOWN,
            label=f'Instance {i+1}'
        )
        select_list.append(sel)

    return select_list
