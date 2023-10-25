import numpy as np
import pandas as pd
from pathlib import Path
from pythresh.thresholds.zscore import ZSCORE
from .utils import AWRProcessor


class ForegroundEventWaitProcessor(AWRProcessor):
    """
    Class for processing the Top 10 Foreground Events by Total Wait Time of the AWR report
    """
    def __init__(self, name, input_path=None):
        super().__init__(name, input_path)
        self.df = None
        self.dfs = []
        self.timestamps = []

        self.set_df()

    def set_df(self):
        """
        Read the .csv file and create both a dataframe and a grouped dataframe
        """

        filepath = self.input_path / 'foreground_events_wait.csv'
        self.df = self.read_df(filepath)

        self.dfs = super().split_by_instance(self.df)  # split by instance the dataframe

        for i in range(self.tot_instances):
            timestamp = np.sort(self.dfs[i]['timestamp'].unique())
            self.timestamps.append(timestamp)
            self.dfs[i] = super().drop_system_info(self.dfs[i])

    def write_df(self, df=None, path=None):
        if path is None:
            print('Error! outuput path not specified')
            # super().write_df(self.aggregated_path, self.grouped_df)
        else:
            # TODO is horrible - to be CHANGED
            for i in range(self.tot_instances):
                super().write_df(f'{path}_{i + 1}.csv', self.dfs[i])

