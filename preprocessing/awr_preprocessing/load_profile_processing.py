import numpy as np
import pandas as pd
from pathlib import Path
from pythresh.thresholds.zscore import ZSCORE
from pythresh.thresholds.iqr import IQR
from .utils import AWRProcessor



def aggregate_df(df):
    """
    Aggregate the dataframe based on the 'Name' column and for each value make a list of
    the 'Per Second' metric

    Parameters
    ----------
    df : Dataframe

    Returns
    -------
    Dataframe with one row for each 'Name' and the list of 'Per Second' as values
    """

    grouped_series = df.groupby('Name')['Per Second'].apply(list)
    grouped_df = pd.DataFrame(grouped_series)
    grouped_df.reset_index(inplace=True)
    grouped_df = grouped_df.rename(columns={'Name': 'Metric', 'Per Second': 'Values'})

    return grouped_df


class LoadProcessor_old(AWRProcessor):
    """
    Class for processing the Load Profile of the AWR report
    """

    def __init__(self, name, input_path=None):
        super().__init__(name, input_path)
        self.df = None

        self.grouped_df = None

        self.set_df()

    def set_df(self):
        """
        Read the .csv file and create both a dataframe and a grouped dataframe
        """

        filepath = self.input_path / 'load_profile.csv'
        self.df = self.read_df(filepath)

        timestamps = np.sort(self.df['timestamp'].unique())

        grouped_df = aggregate_df(self.df)
        grouped_df = pd.DataFrame(np.array(grouped_df['Values'].tolist()).transpose(),
                                  columns=grouped_df['Metric'].tolist())
        grouped_df = grouped_df.set_index(timestamps)
        self.grouped_df = grouped_df

    def write_df(self, df=None, path=None):
        if path is None:
            print('Error! outuput path not specified')
            # super().write_df(self.aggregated_path, self.grouped_df)
        else:
            super().write_df(path, self.grouped_df)

    def find_peaks(self, metric=None):
        """
        Find otliers of the specified metric based on the Z-Score

        Parameters
        ----------
        metric : str
                metric of which find anomalies

        Returns
        -------
        Dataframe containing the anomalous values and the timestamps as index
        """

        if metric is None:
            print("Error! missing metric name")
            return
        if self.grouped_df is None:
            print("Error! 'grouped_df' not defined")

        thres = ZSCORE()
        labels = thres.eval(self.grouped_df[metric])
        # peak_timestamps = self.grouped_df[labels == 1].index
        peaks_df = self.grouped_df[labels == 1]
        return peaks_df


class LoadProcessor(AWRProcessor):
    """
    Class for processing the Load Profile of the AWR report
    """

    def __init__(self, name, input_path=None):
        super().__init__(name, input_path)
        self.df = None

        self.dfs = []
        self.grouped_dfs = []

        self.set_df()

    def set_df(self):
        """
        Read the .csv file and create both a dataframe and a grouped dataframe
        """

        filepath = self.input_path / 'load_profile.csv'
        self.df = self.read_df(filepath)

        self.dfs = super().split_by_instance(self.df)  # split by instance the dataframe

        for i in range(self.tot_instances):
            timestamps = np.sort(self.dfs[i]['timestamp'].unique())

            grouped_df = aggregate_df(self.dfs[i])
            grouped_df = pd.DataFrame(np.array(grouped_df['Values'].tolist()).transpose(),
                                      columns=grouped_df['Metric'].tolist())
            grouped_df = grouped_df.set_index(timestamps)
            self.grouped_dfs.append(grouped_df)

    def write_df(self, df=None, path=None):
        if path is None:
            print('Error! outuput path not specified')
            # super().write_df(self.aggregated_path, self.grouped_df)
        else:
            #TODO is horrible - to be CHANGED
            for i in range(self.tot_instances):
                super().write_df(f'{path}_{i+1}.csv', self.grouped_dfs[i])

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