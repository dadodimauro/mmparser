import numpy as np
import pandas as pd
from pathlib import Path
from pythresh.thresholds.zscore import ZSCORE
from mmparser.preprocessing.awr_preprocessing.utils import AWRProcessor



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


class LoadProcessor(AWRProcessor):
    """
    Class for processing the Load Profile of the AWR report
    """

    def __init__(self, name):
        super().__init__(name)
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

    def write_df(self, path=None):
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
