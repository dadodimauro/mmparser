from bs4 import BeautifulSoup
import codecs
import os
import re
from time import time
from datetime import datetime
from pathlib import Path
from shutil import move
import json


def move_file(input_path, output_path):
    src = Path(input_path)
    dst = Path(output_path)
    move(src, dst)


def get_tables_from_json(file_path):
    """

    Parameters
    ----------
    file_path : str
                path of the json file containing the AWR tables information

    Returns
    -------
    data : dict
            the json file converted into a dictionary
    """
    file_path = Path(file_path)
    f = open(file_path, 'r')
    data = json.loads(f.read())
    f.close()
    return data


class AWRParser:
    """
    Parser class for the AWR miner

    Parameters
    ----------
    t : dict
        dictionary containing the name of the final .csv file as a key and the
        text of the 'summary' tag to find the AWR table of interest in the html report
    """

    def __init__(self, t):
        self.t = t

    ##### extract text from <th> or <td> tag
    def tfix(self, elem):
        # use the tag's own text, if it exists
        text = elem.string or ''

        # if <a> exists as a child, use its text instead (ie SQLID)
        for a in elem.find_all('a'):
            text = a.text

        # format
        text = text.strip()
        text = re.sub(r'\s+', ' ', text)
        for c in ['&#160;', ',', ':']:
            text = text.replace(c, '')
        return text

    # parse AWR in html format
    def parse(self, filelist):
        """
        parse the html AWR reports into a pandas Dataframe

        Parameters
        ----------
        filelist : list
                    list of html files to parse

        Returns
        -------
        output : dict
                    dictionary containing the name of the AWR table parsed as a key
                    and as a values the parsed data ready to be saved in .csv format
        """

        output = {}
        host_info = {}

        for filename in filelist:
            print('Processing {0}...'.format(filename))
            b_header = False  # begin header
            l_base = []  # report-specific info (list)
            d_base = ''  # report-specific info (string)
            flag = 0

            #### open file
            file = open(filename)
            soup = BeautifulSoup(file, features='lxml')

            for table in soup.find_all('table'):
                ##### extract <table summary=XXXXXX>
                section = ''
                summary = table.get('summary')
                # print(table.get('summary'))
                if summary is not None or summary != '':
                    section = " ".join(summary.split())
                # print(section)
                ##### extract DBName, etc from the 2nd row, columns1-4
                if section == 'This table displays database instance information':
                    if flag == 0:
                        l_base = [self.tfix(x) for x in table.find_all('td')][:4]

                    elif flag == 1:
                        l_base.extend([self.tfix(x) for x in table.find_all('td')][:2])

                    flag += 1

                ##### extract begin/end snap time from 2nd-3rd row, column3
                elif section == 'This table displays snapshot information':
                    for tr in list(table.find_all('tr'))[1:3]:
                        snap = list(tr.find_all('td'))[2]
                        st = datetime.strptime(snap.text, '%d-%b-%y %H:%M:%S')
                        l_base.extend(str(x) for x in (st.year, st.month, st.day, st.hour, st.minute, st.second))
                    d_base = ','.join(l_base) + ','

                ##### extract host informaion
                elif section == 'This table displays host information':
                    host_info['__header__'] = 'Host Name,Platform,CPUs,Cores,Sockets,Memory (GB)'
                    l_host = [self.tfix(x) for x in table.find_all('td')]
                    if l_host[0] not in host_info.keys():
                        host_info[l_host[0]] = ','.join(l_host)

                ##### for other sections, convert <th><td> structure into a CSV
                elif section in self.t:
                    # print(section)
                    (csvname, header) = self.t[section]

                    ##### create file entry on a first access
                    if csvname not in output:
                        output[csvname] = []
                        b_header = True

                    ##### iterate over <tr> tag
                    for tr in table.find_all('tr'):
                        ##### override header if specified by a grand table, otherwise use <th>
                        if b_header:
                            h_base = 'DB_NAME,DB_ID,UNIQUE_NAME,ROLE,INSTANCE_NAME,INST_NUM,B_Y,B_MO,B_D,B_H,B_MI,B_S,E_Y,E_MO,E_D,E_H,E_MI,E_S,'
                            h_data = header or ','.join(self.tfix(x) for x in tr.find_all('th'))
                            output[csvname].append(h_base + h_data)
                            b_header = False

                        ##### extract <td> data
                        l_td = [self.tfix(x) for x in tr.find_all('td')]
                        if len(l_td) > 0:
                            d_data = ','.join(l_td)
                            output[csvname].append(d_base + d_data)

        output['host_info.csv'] = []
        for v in host_info.values():
            output['host_info.csv'].append(v)

        ##### return output
        return output

    def make_csv(self, mode='append', input_dir='data/raw/awr', output_dir='data/parsed/awr',
                        recursive=False):
        """
        Parse the data and stores it into multiple .csv files, one for each AWR table

        Parameters
        ----------
        mode : str
                'append' to add the new AWR values to an already existing .csv file
                'new' to crete new .csv files
        input_dir : str
                    path of the input data (AWR reports in .html format)
        output_dir: str
                    path of the output data
        recursive: (optional) bool
                    if there are subfolders in the specified input directory to be used
                    (like the -r in unix)
        """

        if mode not in ['append', 'new']:
            print('Error! wrong mode specified, using default (append)')
            mode = 'append'

        filepath = Path(f'{input_dir}')
        if recursive:
            filelist = sorted(filepath.rglob('*.html'))
        else:
            filelist = sorted(filepath.glob('*.html'))

        # parse AWR
        output = self.parse(filelist)

        # write .csv file
        out_filepath = Path(output_dir)
        for csvname in output:
            flag = False  # if flag is true skip first line (skip intestation if append mode)
            if mode == 'append':
                out_filepath = Path(f'{output_dir}') / csvname
                if os.path.isfile(out_filepath):
                    f = open(out_filepath, 'a', encoding='utf-8')  # if file already exists append
                    flag = True
                    print('  Updated: ' + csvname)
                else:
                    f = open(out_filepath, 'w', encoding='utf-8')
                    print('  Created: ' + csvname)
            else:  # new
                # timestamp = int(time())
                # out_filepath = Path(f'{output_dir}') / str(timestamp)
                if not os.path.exists(out_filepath):
                    os.mkdir(out_filepath)
                out_filepath = Path(f'{output_dir}') / csvname
                f = open(out_filepath, 'w', encoding='utf-8')
                print('  Created: ' + csvname)

            # write/append on a different file for each table
            for line in output[csvname]:
                if flag is True:
                    flag = False
                    continue
                f.write(line + '\n')

            f.close()

            # TODO test if it works
            # move in obsolete folder all file used
            # for filename in filelist:
            #     if not os.path.exists(filepath / filename):
            #         move_file(filename, 'data/obsolete/')


# def main():
#     data = get_tables_from_json('tables.json')
#     p = Parser(data)
#     p.make_csv(mode='new')
#
#
# if __name__ == "__main__":
#     main()
