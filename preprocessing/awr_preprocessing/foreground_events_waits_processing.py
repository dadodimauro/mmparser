import numpy as np
import pandas as pd
from pathlib import Path
from pythresh.thresholds.zscore import ZSCORE
from mmparser.preprocessing.awr_preprocessing.utils import AWRProcessor


class ForegroundEventWaitProcessor(AWRProcessor):
    """
    Class for processing the Top 10 Foreground Events by Total Wait Time of the AWR report
    """
    def __init__(self, name):
        super().__init__(name)
        self.df = None
        self.timestamps = None

        self.set_df()

    def set_df(self):
        """
        Read the .csv file and create both a dataframe and a grouped dataframe
        """

        filepath = self.input_path / 'foreground_events_wait.csv'
        self.df = self.read_df(filepath)
        self.df = super().drop_system_info(self.df)

        self.timestamps = np.sort(self.df['timestamp'].unique())

    def write_df(self, path=None):
        if path is None:
            print('Error! outuput path not specified')
            # super().write_df(self.aggregated_path, self.grouped_df)
        else:
            super().write_df(path, self.df)
