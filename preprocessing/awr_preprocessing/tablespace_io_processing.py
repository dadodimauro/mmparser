import pandas as pd
from .utils import AWRProcessor


def aggregate_df(df):
    """
    Aggregate the dataframe based on the 'Wait Class' column and for each value make a list of
    the 'Total Wait Time (sec)' metric

    Parameters
    ----------
    df : Dataframe

    Returns
    -------
    Dataframe with one row for each 'Wait Class' and the list of 'Total Wait Time (sec)' as values
    """

    grouped_df = df.groupby(['Tablespace']).apply(lambda x: [list(x['Av Rds/s']), list(x['Writes avg/s']), list
    (x['timestamp'])]) \
        .apply(pd.Series)
    grouped_df.reset_index(inplace=True)
    grouped_df.columns = ['Tablespace', 'Av Rds/s', 'Writes avg/s', 'timestamp']

    return grouped_df


class TablespaceIoProcessor(AWRProcessor):
    """
    Class for processing the 'Wait Classes by Total Wait Time' table of the AWR report
    """

    def __init__(self, name, input_path=None):
        super().__init__(name, input_path)
        self.df = None

        self.dfs = []
        self.grouped_dfs = []  # Total Wait Time (sec)

        self.set_df()

    def set_df(self):
        """
        Read the .csv file and create both a dataframe and a grouped dataframe
        """

        filepath = self.input_path / 'tablespace_io.csv'
        self.df = self.read_df(filepath)

        self.dfs = super().split_by_instance(self.df)  # split by instance the dataframe

        for i in range(self.tot_instances):
            self.dfs[i] = super().drop_system_info(self.dfs[i])
            grouped_df = aggregate_df(self.dfs[i])
            self.grouped_dfs.append(grouped_df)

    def get_metric_to_plot(self, instance, tablespace_name):
        line = self.grouped_dfs[instance][self.grouped_dfs[instance]['Tablespace'] == tablespace_name]
        y1 = line['Av Rds/s'].to_list()[0]
        y2 = line['Writes avg/s'].to_list()[0]
        x = line['timestamp'].to_list()[0]

        return x, y1, y2

    def get_most_used_tbs(self, instance, top=10):
        # ordered by IOs (Reads + Writes) desc
        df = self.dfs[instance]
        sorted_indices = (df["Reads"] + df["Writes"]).sort_values(ascending=False).index
        if top == -1:
            return df.loc[sorted_indices, :]['Tablespace'].unique()[:]

        return df.loc[sorted_indices, :]['Tablespace'].unique()[:top]
