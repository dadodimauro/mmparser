import numpy as np
import pandas as pd
from pathlib import Path
from .utils import AWRProcessor


def aggregate_df(df, case):
    """
    Aggregate the dataframe based on the 'SQL Id' column and for each value make a list of
    the metric specified in the parameter 'case' (['cpu', 'elapsed', 'io'])

    Parameters
    ----------
    df : Dataframe
    case : str
            one of the following values: ['cpu', 'elapsed', 'io']
            the metric used for creating the list for each SQL Id

    Returns
    -------
    Dataframe with one row for each 'SQL Id' and the list of values of the 'case' metric as values
    """

    if case in ['cpu', 'elapsed', 'io']:
        grouped_series = df.groupby('SQL Id')['timestamp'].count().sort_values(ascending=False)
        grouped_df = pd.DataFrame(grouped_series)
        grouped_df.reset_index(inplace=True)
        grouped_df = grouped_df.rename(columns={'SQL Id': 'SQL Id', 'timestamp': f'Count ({case})'})
    else:
        print('Error! case parameter not specified or wrong')
        return df
    return grouped_df


def merge_dfs(df1, df2, df3=None, how='outer', on=None):
    """
    Merge the 2 or 3 dataframes

    Parameters
    ----------
    df1 : Dataframe
    df2 : Dataframe
    df3 : Dataframe
    how : str
            type of join, 'outer' by default
    on : (optional) str
            on what to merge

    Returns
    -------
    The merged dataframe
    """

    if df3 is None:
        df = pd.merge(df1, df2, how=how, on=on)
    else:
        df = pd.merge(df1, df2, how=how, on=on).merge(df3, how=how, on=on)
    return df


class SqlProcessor_old(AWRProcessor):
    """
    Class for processing the top10 SQL tables of the AWR report
    """

    def __init__(self, name, input_path=None):
        super().__init__(name, input_path)
        self.df1 = None  # cpu
        self.df2 = None  # elapsed time
        self.df3 = None  # user i/o
        self.merged_df = None

        self.set_dfs()
        # self.concatenate_dfs()

    def set_dfs(self):
        """
        Read the .csv file for the 3 tables and create a dataframe for each of them
        """

        filepath1 = self.input_path / 'sql_cpu.csv'
        filepath2 = self.input_path / 'sql_elapsed.csv'
        filepath3 = self.input_path / 'sql_user_io.csv'

        self.df1 = self.read_df(filepath1)
        self.df2 = self.read_df(filepath2)
        self.df3 = self.read_df(filepath3)

        # self.df1 = self.drop_system_info(self.df1)
        # self.df2 = self.drop_system_info(self.df2)
        # self.df3 = self.drop_system_info(self.df3)

    def concatenate_dfs(self):
        """
        Concatenate the 3 grouped dataframes

        Returns
        -------
        The Dataframe contatenation of the 3 grouped dataframes
        """

        if self.df1 is None or self.df2 is None or self.df3 is None:
            self.set_dfs()

        g_df1 = aggregate_df(self.df1, case='cpu')
        g_df2 = aggregate_df(self.df2, case='elapsed')
        g_df3 = aggregate_df(self.df3, case='io')

        self.merged_df = merge_dfs(g_df1, g_df2, g_df3, on='SQL Id')

    def get_top_n(self, n=5, start=None, end=None):
        """
        Get top n queries for each dataframe based respectively on:
        'CPU Time (s)', 'Elapsed Time (s)' and 'User I/O Time (s)'

        Parameters
        ----------
        n : int
            specify the top n rows desired
        start : (optional) str
        end : (optional) str

        Returns
        -------
        3 dataframes containing the top n entries
        """

        if start is not None and end is not None:
            top1 = self.filter_by_date(self.df1, start, end)
            top2 = self.filter_by_date(self.df2, start, end)
            top3 = self.filter_by_date(self.df3, start, end)

            # top1 = top1.sort_values('CPU Time (s)', ascending=False)[['timestamp', 'SQL Id', 'CPU Time (s)']].head(n)
            # top2 = top2.sort_values('Elapsed Time (s)', ascending=False)[['timestamp', 'SQL Id', 'Elapsed Time (s)']].head(n)
            # top3 = top3.sort_values('User I/O Time (s)', ascending=False)[['timestamp', 'SQL Id', 'User I/O Time (s)']].head(n)
            top1 = super().drop_system_info(top1.sort_values('CPU Time (s)', ascending=False).head(n))
            top2 = super().drop_system_info(top2.sort_values('Elapsed Time (s)', ascending=False).head(n))
            top3 = super().drop_system_info(top3.sort_values('User I/O Time (s)', ascending=False).head(n))

        else:
            top1 = super().drop_system_info(self.df1.sort_values('CPU Time (s)', ascending=False).head(n))
            top2 = super().drop_system_info(self.df2.sort_values('Elapsed Time (s)', ascending=False).head(n))
            top3 = super().drop_system_info(self.df3.sort_values('User I/O Time (s)', ascending=False).head(n))

        return top1, top2, top3

    def get_query_info(self, sql_id, start=None, end=None):
        """
        Retrieve all the statistic available in the 3 dataframes about the specified query

        Parameters
        ----------
        sql_id : str
                    SQL Id of the query
        start : (optional) str
        end : (optional) str

        Returns
        -------
        A merged dataframe containing all the statistic available in the 3 dataframes about the specified query
        """

        df1 = self.df1[self.df1['SQL Id'] == sql_id]
        df2 = self.df2[self.df2['SQL Id'] == sql_id]
        df3 = self.df3[self.df3['SQL Id'] == sql_id]

        if start is not None and end is not None:
            # df1 = self.filter_by_date(self.df1, start, end)
            # df2 = self.filter_by_date(self.df2, start, end)
            # df3 = self.filter_by_date(self.df3, start, end)
            df1 = self.filter_by_date(df1, start, end)
            df2 = self.filter_by_date(df2, start, end)
            df3 = self.filter_by_date(df3, start, end)
            # df1 = df1[df1['SQL Id'] == sql_id]
            # df2 = df2[df2['SQL Id'] == sql_id]
            # df3 = df3[df3['SQL Id'] == sql_id]
        # else:
        #     df1 = self.df1[self.df1['SQL Id'] == sql_id]
        #     df2 = self.df2[self.df2['SQL Id'] == sql_id]
        #     df3 = self.df3[self.df3['SQL Id'] == sql_id]

        # merged_df = pd.concat([df1, df2, df3], axis=0, ignore_index=True)
        merged_df = merge_dfs(df1, df2, df3)
        merged_df = self.drop_system_info(merged_df)
        merged_df = merged_df.set_index('timestamp')
        # merged_df = merged_df[['timestamp', 'CPU Time (s)', 'Executions', 'CPU per Exec (s)', '%Total',
        #                        'Elapsed Time', '%CPU', '%IO', 'SQL Module', 'PDB Name',
        #                        'SQL Text', 'Elapsed Time (s)', 'Elapsed Time Per Exec',
        #                        'User I/O Time (s)', 'UIO per Exec (s)']]
        return merged_df

    def get_query_cpu_time(self, sql_id, start=None, end=None):
        """
        Get cpu time over time for the specified query

        Parameters
        ----------
        sql_id : str
                    SQL Id of the query
        start : (optional) str
        end : (optional) str

        Returns
        -------
        Dataframe containing the data about the cpu time over time
        """

        df1 = self.df1[self.df1['SQL Id'] == sql_id]
        if start is not None and end is not None:
            df1 = self.filter_by_date(self.df1, start, end)

        df1 = df1[df1['SQL Id'] == sql_id][['CPU Time (s)', 'timestamp']]
        df1 = df1.set_index('timestamp')
        return df1

    def get_query_elapsed_time(self, sql_id, start=None, end=None):
        """
        Get elapsed time over time for the specified query

        Parameters
        ----------
        sql_id : str
                    SQL Id of the query
        start : (optional) str
        end : (optional) str

        Returns
        -------
        Dataframe containing the data about the elapsed time over time
        """

        df2 = self.df2[self.df2['SQL Id'] == sql_id]
        if start is not None and end is not None:
            df2 = self.filter_by_date(self.df2, start, end)

        df2 = df2[df2['SQL Id'] == sql_id][['Elapsed Time (s)', 'timestamp']]
        df2 = df2.set_index('timestamp')
        return df2

    def get_query_io_time(self, sql_id, start=None, end=None):
        """
        Get user i/o time over time for the specified query

        Parameters
        ----------
        sql_id : str
                    SQL Id of the query
        start : (optional) str
        end : (optional) str

        Returns
        -------
        Dataframe containing the data about the user i/o time over time
        """

        df3 = self.df3[self.df3['SQL Id'] == sql_id]
        if start is not None and end is not None:
            df3 = self.filter_by_date(self.df3, start, end)

        df3 = df3[df3['SQL Id'] == sql_id][['User I/O Time (s)', 'timestamp']]
        df3 = df3.set_index('timestamp')
        return df3


class SqlProcessor(AWRProcessor):
    """
    Class for processing the top10 SQL tables of the AWR report
    """

    def __init__(self, name, input_path=None):
        super().__init__(name, input_path)
        self.df1 = None  # cpu
        self.df2 = None  # elapsed time
        self.df3 = None  # user i/o
        self.merged_df = None

        self.set_dfs()
        # self.concatenate_dfs()

    def set_dfs(self):
        """
        Read the .csv file for the 3 tables and create a dataframe for each of them
        """

        filepath1 = self.input_path / 'sql_cpu.csv'
        filepath2 = self.input_path / 'sql_elapsed.csv'
        filepath3 = self.input_path / 'sql_user_io.csv'

        self.df1 = self.read_df(filepath1)
        self.df2 = self.read_df(filepath2)
        self.df3 = self.read_df(filepath3)

        # self.df1 = self.drop_system_info(self.df1)
        # self.df2 = self.drop_system_info(self.df2)
        # self.df3 = self.drop_system_info(self.df3)

    def concatenate_dfs(self):
        """
        Concatenate the 3 grouped dataframes

        Returns
        -------
        The Dataframe contatenation of the 3 grouped dataframes
        """

        if self.df1 is None or self.df2 is None or self.df3 is None:
            self.set_dfs()

        g_df1 = aggregate_df(self.df1, case='cpu')
        g_df2 = aggregate_df(self.df2, case='elapsed')
        g_df3 = aggregate_df(self.df3, case='io')

        self.merged_df = merge_dfs(g_df1, g_df2, g_df3, on='SQL Id')

    def get_top_n(self, instance, n=5, start=None, end=None):
        """
        Get top n queries for each dataframe based respectively on:
        'CPU Time (s)', 'Elapsed Time (s)' and 'User I/O Time (s)'

        Parameters
        ----------
        n : int
            specify the top n rows desired
        start : (optional) str
        end : (optional) str

        Returns
        -------
        3 dataframes containing the top n entries
        """

        if start is not None and end is not None:
            top1 = super().filter_by_instance(self.df1, instance)
            top2 = super().filter_by_instance(self.df2, instance)
            top3 = super().filter_by_instance(self.df3, instance)

            top1 = self.filter_by_date(top1, start, end)
            top2 = self.filter_by_date(top2, start, end)
            top3 = self.filter_by_date(top3, start, end)

            top1 = super().drop_system_info(top1.sort_values('CPU Time (s)', ascending=False).head(n))
            top2 = super().drop_system_info(top2.sort_values('Elapsed Time (s)', ascending=False).head(n))
            top3 = super().drop_system_info(top3.sort_values('User I/O Time (s)', ascending=False).head(n))

        else:
            #TODO: add filter by instance
            top1 = super().drop_system_info(self.df1.sort_values('CPU Time (s)', ascending=False).head(n))
            top2 = super().drop_system_info(self.df2.sort_values('Elapsed Time (s)', ascending=False).head(n))
            top3 = super().drop_system_info(self.df3.sort_values('User I/O Time (s)', ascending=False).head(n))

        return top1, top2, top3

    def get_query_info(self, sql_id, instance, start=None, end=None):
        """
        Retrieve all the statistic available in the 3 dataframes about the specified query

        Parameters
        ----------
        sql_id : str
                    SQL Id of the query
        start : (optional) str
        end : (optional) str

        Returns
        -------
        A merged dataframe containing all the statistic available in the 3 dataframes about the specified query
        """

        df1 = super().filter_by_instance(self.df1, instance)
        df2 = super().filter_by_instance(self.df2, instance)
        df3 = super().filter_by_instance(self.df3, instance)

        df1 = df1[df1['SQL Id'] == sql_id]
        df2 = df2[df2['SQL Id'] == sql_id]
        df3 = df3[df3['SQL Id'] == sql_id]

        if start is not None and end is not None:
            # df1 = self.filter_by_date(self.df1, start, end)
            # df2 = self.filter_by_date(self.df2, start, end)
            # df3 = self.filter_by_date(self.df3, start, end)
            df1 = self.filter_by_date(df1, start, end)
            df2 = self.filter_by_date(df2, start, end)
            df3 = self.filter_by_date(df3, start, end)
            # df1 = df1[df1['SQL Id'] == sql_id]
            # df2 = df2[df2['SQL Id'] == sql_id]
            # df3 = df3[df3['SQL Id'] == sql_id]
        # else:
        #     df1 = self.df1[self.df1['SQL Id'] == sql_id]
        #     df2 = self.df2[self.df2['SQL Id'] == sql_id]
        #     df3 = self.df3[self.df3['SQL Id'] == sql_id]

        # merged_df = pd.concat([df1, df2, df3], axis=0, ignore_index=True)
        merged_df = merge_dfs(df1, df2, df3)
        merged_df = self.drop_system_info(merged_df)
        merged_df = merged_df.set_index('timestamp')
        # merged_df = merged_df[['timestamp', 'CPU Time (s)', 'Executions', 'CPU per Exec (s)', '%Total',
        #                        'Elapsed Time', '%CPU', '%IO', 'SQL Module', 'PDB Name',
        #                        'SQL Text', 'Elapsed Time (s)', 'Elapsed Time Per Exec',
        #                        'User I/O Time (s)', 'UIO per Exec (s)']]
        return merged_df

    def get_query_cpu_time(self, sql_id, instance, start=None, end=None):
        """
        Get cpu time over time for the specified query

        Parameters
        ----------
        sql_id : str
                    SQL Id of the query
        start : (optional) str
        end : (optional) str

        Returns
        -------
        Dataframe containing the data about the cpu time over time
        """

        df1 = super().filter_by_instance(self.df1, instance)
        df1 = df1[df1['SQL Id'] == sql_id]
        if start is not None and end is not None:
            df1 = self.filter_by_date(df1, start, end)

        df1 = df1[df1['SQL Id'] == sql_id][['CPU Time (s)', 'timestamp']]
        df1 = df1.set_index('timestamp')
        return df1

    def get_query_elapsed_time(self, sql_id, instance, start=None, end=None):
        """
        Get elapsed time over time for the specified query

        Parameters
        ----------
        sql_id : str
                    SQL Id of the query
        start : (optional) str
        end : (optional) str

        Returns
        -------
        Dataframe containing the data about the elapsed time over time
        """

        df2 = super().filter_by_instance(self.df2, instance)
        df2 = df2[df2['SQL Id'] == sql_id]
        if start is not None and end is not None:
            df2 = self.filter_by_date(df2, start, end)

        df2 = df2[df2['SQL Id'] == sql_id][['Elapsed Time (s)', 'timestamp']]
        df2 = df2.set_index('timestamp')
        return df2

    def get_query_io_time(self, sql_id, instance, start=None, end=None):
        """
        Get user i/o time over time for the specified query

        Parameters
        ----------
        sql_id : str
                    SQL Id of the query
        start : (optional) str
        end : (optional) str

        Returns
        -------
        Dataframe containing the data about the user i/o time over time
        """

        df3 = super().filter_by_instance(self.df3, instance)
        df3 = df3[df3['SQL Id'] == sql_id]
        if start is not None and end is not None:
            df3 = self.filter_by_date(df3, start, end)

        df3 = df3[df3['SQL Id'] == sql_id][['User I/O Time (s)', 'timestamp']]
        df3 = df3.set_index('timestamp')
        return df3

    def get_num_instances(self, df=None):
        if df is None:
            df = self.df1

        return super().get_num_instances(df)


