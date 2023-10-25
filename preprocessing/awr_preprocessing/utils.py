import numpy as np
import pandas as pd
from pathlib import Path
from dateutil.parser import parse


class AWRProcessor:
    """
    Base AWRProcessor class, inherited by the other more specific Processor classes for the different AWR tables
    """

    def __init__(self, name, input_path='data/parsed/awr', aggregated_path=None):
        """

        Parameters
        ----------
        name : str
                name of the processor
        input_path : str or Path()
                        path of the input data folder
        aggregated_path : Deprecated
        """
        self.name = name
        if input_path is None:
            input_path = 'data/parsed/awr'
        self.input_path = Path(input_path)
        # self.aggregated_path = Path(aggregated_path)
        self.system_info = None
        self.tot_instances = None

    def read_df(self, filepath):
        """
        Read the csv file and extract the timestamp

        Parameters
        ----------
        filepath : path of the file

        Returns
        -------
        Returns a pandas Dataframe created from the .csv file
        """

        # df = pd.read_csv(filepath,
        #                  usecols=['DB_NAME', 'DB_ID', 'DB_NAME','DB_ID', 'UNIQUE_NAME', 'ROLE', 'INSTANCE_NAME', 'INST_NUM',
        #                           'B_Y', 'B_MO', 'B_D', 'B_H', 'B_MI', 'B_S',
        #                           'Name', 'Per Second'])
        df = pd.read_csv(filepath)
        df = df.rename(columns={'B_Y': 'year', 'B_MO': 'month', 'B_D': 'day',
                                'B_H': 'hour', 'B_MI': 'minute', 'B_S': 'second'})
        df['timestamp'] = pd.to_datetime(df[['month', 'day', 'year', 'hour', 'minute', 'second']])
        df = df.drop(['month', 'day', 'year', 'hour', 'minute', 'second',
                      'E_Y', 'E_MO', 'E_D', 'E_H', 'E_MI', 'E_S'], axis=1)
        return df

    def write_df(self, filepath, df):
        """

        Parameters
        ----------
        filepath : str or Path()
                    path where to store the output file
        df : Dataframe
                dataframe to be saved
        """

        df.to_csv(filepath, index=False)

    def filter_by_date(self, df, start, end):
        """
        Filter values of the dataframe between 'start' and 'end'

        Parameters
        ----------
        df : Dataframe
        start : str
                start timestamp
        end : str
                end timestamp

        Returns
        -------
        filtered values
        """

        try:
            ts_start = parse(start)
            ts_end = parse(end)
            return df[(df['timestamp'] >= ts_start) & (df['timestamp'] <= ts_end)]

        except ValueError:
            print('Error! timestamp format is not correct')
            return df

    def drop_system_info(self, df):
        """
        Drop dataframe columns containing system information
        The information about the instance are saved in the variable self.system_info

        Parameters
        ----------
        df : Dataframe

        Returns
        -------
        dataframe without the system information
        """

        self.system_info = df[['DB_NAME', 'DB_ID', 'UNIQUE_NAME', 'ROLE', 'INSTANCE_NAME']].head(1)
        tot_instaces = df['INST_NUM'].values.max()
        self.system_info['INST_NUM'] = tot_instaces

        df = df.drop(['DB_NAME', 'DB_ID', 'UNIQUE_NAME', 'ROLE', 'INSTANCE_NAME', 'INST_NUM'], axis=1)
        # df = df.drop(['DB_NAME', 'DB_ID', 'UNIQUE_NAME', 'ROLE', 'INSTANCE_NAME'], axis=1)
        return df

    def split_by_instance(self, df):
        """
        Divide the original dataframe into smaller ones, one for each instance

        Parameters
        ----------
        df : Dataframe

        Returns
        -------
        List of dataframes divided by instance
        """

        if self.system_info is None:
            tot_instances = df['INST_NUM'].values.max()
            self.tot_instances = tot_instances
        else:
            print('Error! system info already removed form the dataframe')
            return

        df_list = []
        for i in range(tot_instances):
            df_list.append(df[df['INST_NUM'] == i + 1])

        return df_list

    def filter_by_instance(self, df, instance):
        return df[df['INST_NUM'] == instance + 1]

    def get_num_instances(self, df):
        if self.tot_instances is not None:
            return self.tot_instances

        if self.system_info is None:
            tot_instances = df['INST_NUM'].values.max()
            self.tot_instances = tot_instances
        else:
            print('Error! system info already removed form the dataframe')
            return

        return self.tot_instances
