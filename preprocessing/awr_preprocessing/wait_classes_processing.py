import numpy as np
import pandas as pd
from pathlib import Path
from pythresh.thresholds.iqr import IQR
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

    grouped_series = df.groupby('Wait Class')['Total Wait Time (sec)'].apply(list)
    grouped_df = pd.DataFrame(grouped_series)
    grouped_df.reset_index(inplace=True)
    grouped_df = grouped_df.rename(columns={'Wait Class': 'Metric', 'Total Wait Time (sec)': 'Values'})

    return grouped_df


class WaitClassProcessor(AWRProcessor):
    """
    Class for processing the 'Wait Classes by Total Wait Time' table of the AWR report
    """

    def __init__(self, name, input_path=None):
        super().__init__(name, input_path)
        self.df = None

        self.dfs = []
        self.grouped_dfs = []  # Total Wait Time (sec)

        self.set_df()

    def add_placeholders(self, df):
        # sometimes the number of classes changes, this adds placeholders to fix the problem
        wait_classes = ['Administrative', 'Application', 'Cluster', 'Commit', 'Concurrency', 'Configuration',
                        'DB CPU', 'Idle', 'Network', 'Other', 'Scheduler', 'System I/O', 'User I/O']
        tmp_df = pd.DataFrame(df.groupby('timestamp')['Wait Class'].apply(list))
        l = []
        for i in range(len(tmp_df)):
            # print(tmp_df.iloc[i].name, np.setdiff1d(wait_classes, tmp_df.iloc[i]['Wait Class']))
            columns = df.columns.to_list()
            diff = np.setdiff1d(wait_classes, tmp_df.iloc[i]['Wait Class'])
            for metric in diff:
                a = [np.nan] * len(columns)
                a[columns.index('Wait Class')] = metric
                a[columns.index('Total Wait Time (sec)')] = 0
                a[columns.index('timestamp')] = tmp_df.iloc[i].name
                l.append(a)

        tmp_df = pd.DataFrame(l, columns=df.columns.to_list())
        df = pd.concat([df, tmp_df])

        return df

    def set_df(self):
        """
        Read the .csv file and create both a dataframe and a grouped dataframe
        """

        filepath = self.input_path / 'wait_classes.csv'
        self.df = self.read_df(filepath)

        self.dfs = super().split_by_instance(self.df)  # split by instance the dataframe

        for i in range(self.tot_instances):
            self.dfs[i] = self.add_placeholders(self.dfs[i])

            timestamps = np.sort(self.dfs[i]['timestamp'].unique())

            grouped_df = aggregate_df(self.dfs[i])
            grouped_df = pd.DataFrame(np.array(grouped_df['Values'].tolist()).transpose(),
                                      columns=grouped_df['Metric'].tolist())
            grouped_df = grouped_df.set_index(timestamps)
            self.grouped_dfs.append(grouped_df)

    def write_df(self, path=None):
        if path is None:
            print('Error! outuput path not specified')
            # super().write_df(self.aggregated_path, self.grouped_df)
        else:
            # TODO is horrible - to be CHANGED
            for i in range(self.tot_instances):
                super().write_df(f'{path}_{i + 1}.csv', self.grouped_dfs[i])

    def find_peaks_instance(self, instance, metric=None):
        """
        Find outliers of the specified metric based on the Z-Score
            for A SPECIFIC instance

        Parameters
        ----------
        instance : int
            # of the instance
        metric : str
            metric of which find anomalies

        Returns
        -------
        Dataframes containing the anomalous values and the timestamps as index
        """

        if metric is None:
            print("Error! missing metric name")
            return
        if len(self.grouped_dfs) == 0:
            print("Error! 'grouped_dfs' not defined")

        # thres = ZSCORE()
        thres = IQR()

        labels = thres.eval(self.grouped_dfs[instance][metric])
        # peak_timestamps = self.grouped_df[labels == 1].index
        peaks_df = self.grouped_dfs[instance][labels == 1]

        return peaks_df

    def find_peaks(self, metric=None):
        """
        Find outliers of the specified metric based on the Z-Score
            for ALL instances

        Parameters
        ----------
        metric : str
                metric of which find anomalies

        Returns
        -------
        List of Dataframes containing the anomalous values and the timestamps as index for each instance
        """

        if metric is None:
            print("Error! missing metric name")
            return
        if len(self.grouped_dfs) == 0:
            print("Error! 'grouped_dfs' not defined")

        peaks_dfs = []
        for i in range(self.tot_instances):
            peaks_dfs.append(self.find_peaks_instance(instance=i, metric=metric))

        return peaks_dfs

    def get_dbtime_percentages_instance(self, instance, timestamp):
        """
        Get a dataframe containing the wait classes and the % of DB time used by each of them

        Parameters
        ----------
        instance : int
                    # of the instance
        timestamp : str
                    timestamp of the snapshot

        Returns
        -------
        Dataframe containing the  % of DB time used by each of the wait classes
        """

        df = super().filter_by_date(self.df[['Wait Class', '% DB time', 'timestamp']], start=timestamp, end=timestamp)
        return df[['Wait Class', '% DB time']]

    def get_dbtime_percentages(self, timestamp):
        """
        Get a list of dataframes containing the wait classes and the % of DB time used by each of them
            FOR EACH INSTANCE

        Parameters
        ----------
        timestamp : str
                    timestamp of the snapshot

        Returns
        -------
        List of dataframes containing the  % of DB time used by each of the wait classes
        """

        dbtime_list = []
        for i in range(self.tot_instances):
            dbtime_list.append(self.get_dbtime_percentages_instance(i, timestamp))

        return dbtime_list
