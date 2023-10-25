import plotly.express as px
import plotly.graph_objs as go
from plotly.subplots import make_subplots


def melt_df(df):
    df_melt = df.melt(value_vars=df.columns.tolist(), ignore_index=False)
    # df_melt['timestamp'] = df_melt.index
    df_melt = df_melt.reset_index()

    return df_melt


def generate_overview(df_melt):
    fig = px.line(df_melt, x='index', y='value', facet_col='variable',
                  facet_col_wrap=5,
                  color='INST_NUM',
                  facet_col_spacing=0.04, facet_row_spacing=0.04,
                  # width=1400,
                  height=600
                  )

    # fig.for_each_annotation(lambda x: x.update(text=x.text.split("=")[-1]))
    fig.update_xaxes(matches='x', tickformat='')
    # fig.update_yaxes(showticklabels=True, matches=None)

    fig.update_layout(autosize=True, template='plotly')

    fig.update_yaxes(matches=None, showticklabels=True, visible=True)
    # fig.update_annotations(font=dict(size=16))
    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
    # hide subplot y-axis titles and x-axis titles
    for axis in fig.layout:
        if type(fig.layout[axis]) == go.layout.YAxis:
            fig.layout[axis].title.text = ''
        if type(fig.layout[axis]) == go.layout.XAxis:
            fig.layout[axis].title.text = ''

    # fig.write_html(Path('res/awr/facet.html'))
    # fig.show()
    return fig


def generate_overview_instance(df_melt):
    fig = px.line(df_melt, x='index', y='value', facet_col='variable',
                  facet_col_wrap=5,
                  color='variable',
                  facet_col_spacing=0.04, facet_row_spacing=0.04,
                  # width=1400,
                  height=600
                  )

    # fig.for_each_annotation(lambda x: x.update(text=x.text.split("=")[-1]))
    fig.update_xaxes(matches='x', tickformat='')
    # fig.update_yaxes(showticklabels=True, matches=None)

    fig.update_layout(autosize=True, template='plotly')

    fig.update_yaxes(matches=None, showticklabels=True, visible=True)
    # fig.update_annotations(font=dict(size=16))
    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
    # hide subplot y-axis titles and x-axis titles
    for axis in fig.layout:
        if type(fig.layout[axis]) == go.layout.YAxis:
            fig.layout[axis].title.text = ''
        if type(fig.layout[axis]) == go.layout.XAxis:
            fig.layout[axis].title.text = ''

    # fig.write_html(Path('res/awr/facet.html'))
    # fig.show()
    return fig


def generate_big_plots(df_melt):
    fig = px.line(df_melt, x='index', y='value', facet_col='variable',
                  facet_col_wrap=2,
                  color='INST_NUM',
                  facet_col_spacing=0.05,
                  facet_row_spacing=0.02,
                  # width=1400,
                  height=5000
                  )

    # fig.for_each_annotation(lambda x: x.update(text=x.text.split("=")[-1]))
    fig.update_xaxes(matches='x', tickformat='')
    # fig.update_yaxes(showticklabels=True, matches=None)

    fig.update_layout(
        showlegend=True,
        # autosize=True
        template='plotly'
    )

    fig.update_yaxes(matches=None, showticklabels=True, visible=True)
    # fig.update_annotations(font=dict(size=16))
    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
    fig.for_each_xaxis(lambda xaxis: xaxis.update(showticklabels=True))
    # hide subplot y-axis titles and x-axis titles
    for axis in fig.layout:
        if type(fig.layout[axis]) == go.layout.YAxis:
            fig.layout[axis].title.text = ''
        # if type(fig.layout[axis]) == go.layout.XAxis:
        #     fig.layout[axis].title.text = ''

    # fig.write_html(Path('res/awr/plotlist.html'))
    # fig.show()
    return fig


def generate_big_plots_instace(df_melt):
    fig = px.line(df_melt, x='index', y='value', facet_col='variable',
                  facet_col_wrap=2,
                  color='variable',
                  facet_col_spacing=0.05,
                  facet_row_spacing=0.02,
                  # width=1400,
                  height=5000
                  )

    # fig.for_each_annotation(lambda x: x.update(text=x.text.split("=")[-1]))
    fig.update_xaxes(matches='x', tickformat='')
    # fig.update_yaxes(showticklabels=True, matches=None)

    fig.update_layout(
        showlegend=True,
        # autosize=True
        template='plotly'
    )

    fig.update_yaxes(matches=None, showticklabels=True, visible=True)
    # fig.update_annotations(font=dict(size=16))
    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
    fig.for_each_xaxis(lambda xaxis: xaxis.update(showticklabels=True))
    # hide subplot y-axis titles and x-axis titles
    for axis in fig.layout:
        if type(fig.layout[axis]) == go.layout.YAxis:
            fig.layout[axis].title.text = ''
        # if type(fig.layout[axis]) == go.layout.XAxis:
        #     fig.layout[axis].title.text = ''

    # fig.write_html(Path('res/awr/plotlist.html'))
    # fig.show()
    return fig


def generate_wait_classes_plot(df):
    df_melt = melt_df(df)

    fig = px.line(df_melt, x='index', y='value', color='variable',
                  # width=1400,
                  height=600
                  )

    fig.update_layout(
        autosize=True, template='plotly',
        xaxis_title="",
        yaxis_title="Total Wait Time (sec)"
    )

    # fig.write_html(Path('res/awr/facet.html'))
    # fig.show()
    return fig


def generate_peaks_figures(l, instance, df, metric_list):
    df = df[metric_list]
    figure_list = []
    peak_list = []
    for metric in metric_list:
        # thres = IQR()
        # labels = thres.eval(df[metric])
        # peak_timestamps = df[labels == 1][[metric]]

        peak_df = l.find_peaks_instance(instance, metric)
        # peak_timestamps = peak_df.index

        fig = px.line(df[[metric]])
        fig = px.scatter(peak_df[[metric]],
                         color_discrete_sequence=['#EF553B'],
                         labels={
                             "value": metric,
                             "index": "time"
                         },
                         title=metric
                         ).add_trace(fig.data[0])

        fig.update_layout(
            showlegend=False,
            autosize=True,
            template='plotly'
        )

        figure_list.append(fig)
        peak_list.append(peak_df)

    return peak_list, figure_list


def generate_wait_classes_pie(df):
    fig = px.pie(df, values='% DB time', names='Wait Class')
    fig.update_layout(autosize=True, template='plotly')
    # fig.show()
    return fig


def generate_tbs_io_plot(tbsProcessor, tablespace_name, instance):
    x, y1, y2 = tbsProcessor.get_metric_to_plot(instance=instance, tablespace_name=tablespace_name)

    # Create figure with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Add traces
    fig.add_trace(
        go.Scatter(x=x, y=y1, name='Av Rds/s'),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(x=x, y=y2, name='Writes avg/s'),
        secondary_y=True,
    )

    # Add figure title
    fig.update_layout(
        title_text=f'Tablespace [{tablespace_name}] I/O'
    )

    # Set x-axis title
    fig.update_xaxes(title_text="time")

    # Set y-axes titles
    fig.update_yaxes(title_text="<b>Av Rds/s</b>", secondary_y=False)
    fig.update_yaxes(title_text="<b>Writes avg/s</b>", secondary_y=True)

    return fig
