"""
MIT License

Copyright (c) 2017 Daniel N. R. da Silva

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import argparse
from bs4 import BeautifulSoup
from logger import *
from lxml import etree
import multiprocessing as mp
import os
import pandas as pd
import re
import sys
import time
import warnings

if sys.version_info[0] < 3:
    raise Exception("Python 3 or a more recent version is required.")

warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

data_frame_type_mapping = {
    'Users': {'int': ['AccountId', 'Age', 'DownVotes', 'Id', 'Reputation', 'UpVotes', 'Views'],
              'date': ['LastAccessDate', 'CreationDate'],
              'str': ['DisplayName', 'Location', 'ProfileImageUrl', 'WebsiteUrl'],
              'html': ['AboutMe']},

    'Comments': {'int': ['Id', 'PostId', 'Score', 'UserId'],
                 'date': ['CreationDate'],
                 'str': ['UserDisplayName'],
                 'html': ['Text']},

    'Badges': {'int': ['Class', 'Id', 'UserId'],
               'date': ['Date'],
               'str': ['Name'],
               'bool': ['TagBased']},

    'PostHistory': {'int': ['Id', 'PostHistoryTypeId', 'PostId', 'UserId'],
                    'date': ['CreationDate'],
                    'str': ['Comment', 'RevisionGUID', 'Text', 'UserDisplayName']},

    'PostLinks': {'int': ['Id', 'LinkTypeId', 'PostId', 'RelatedPostId'],
                  'date': ['CreationDate']},

    'Posts': {'int': ['AcceptedAnswerId', 'AnswerCount', 'CommentCount',
                      'FavoriteCount', 'Id', 'LastEditorUserId',
                      'OwnerUserId', 'ParentId', 'PostTypeId', 'Score', 'ViewCount'],
              'date': ['ClosedDate', 'CommunityOwnedDate', 'CreationDate',
                       'LastActivityDate', 'LastEditDate'],
              'str': ['Title', 'LastEditorDisplayName'],
              'html': ['Body', 'OwnerDisplayName'],
              'set': ['Tags']},

    'Tags': {'int': ['Count', 'ExcerptPostId', 'Id', 'WikiPostId'],
             'str': ['TagName']},

    'Votes': {'int': ['BountyAmount', 'Id', 'PostId', 'UserId', 'VoteTypeId'],
              'date': ['CreationDate']}
}


def read_xml_into_data_frame(xml_file_name: str) -> pd.DataFrame:
    """
    Parses a stack exchange xml and converts it to a data frame.

    :param xml_file_name: File name of XML to be parsed.
    :return: Returns a data frame created from parsed XML.
    """

    parser = etree.XMLParser(ns_clean=True, recover=True)
    xml_tree = etree.parse(xml_file_name, parser)
    xml_root = xml_tree.getroot()
    xml_df = pd.DataFrame.from_records([dict(child.attrib) for child in xml_root])

    return xml_df


def remove_tags_from_html_text(html_text: str) -> str:
    """
    Removes tags from an html text. 
    
    :param html_text: An html text string
    :return: Text with no html tags
    """

    return BeautifulSoup(str(html_text), 'lxml').get_text().replace('\n', '').strip() if 'nan' not in str(
        html_text).lower() else ''


def fix_data_frame_column_type(data_frame: pd.DataFrame, type_by_col_dict: dict):
    """
    Fix data frame columns type
    
    :param data_frame: A pandas data frame which is going to have column types fixed.
    :param type_by_col_dict: A dictionary {'column': 'type'} designating the columns type fixing.
    :return: None.
    """

    for type_by_col, cols in type_by_col_dict.items():
        if type_by_col == 'int':
            data_frame[cols] = data_frame[cols].fillna(-1 * sys.maxsize).astype(int)

        elif type_by_col == 'str':
            data_frame[cols] = data_frame[cols].fillna('').applymap(lambda x: str(x).strip())

        elif type_by_col == 'bool':
            data_frame[cols] = data_frame[cols].astype(bool)

        elif type_by_col == 'date':
            data_frame[cols] = data_frame[cols].apply(pd.to_datetime)

        elif type_by_col == 'html':
            data_frame[cols] = data_frame[cols].applymap(lambda x: remove_tags_from_html_text(x))

        elif type_by_col == 'set':
            data_frame[cols] = data_frame[cols].applymap(lambda x: {item.replace('<', '').replace('>', '')
                                                                    for item in re.findall('<.*?>', str(x))})
    data_frame.set_index('Id', inplace=True)


def write_data_frame_to_disk(data_frame: pd.DataFrame, output_fn: str):
    """
    Pickle serializes data frame and write it to disk.
    
    :param data_frame: Pandas data frame to be serialized.
    :param output_fn: Output file name.
    :return: None.
    """

    data_frame.to_pickle(output_fn)


def get_xml_file_names(xml_root_dir_name: str) -> list:
    """   
    Get xml file names from directory. 
        
    :param xml_root_dir_name: Directory name where stack exchange xmls are placed. 
    :return: A list containing those xmls names.
    """

    accepted_file_names = {'badges.xml', 'comments.xml', 'posthistory.xml', 'postlinks.xml',
                           'posts.xml', 'tags.xml', 'users.xml', 'votes.xml'}

    xml_file_names_list = [os.path.join(dir_name, f_name) for dir_name, _, file_names in os.walk(xml_root_dir_name)
                           for f_name in file_names if f_name.lower() in accepted_file_names]

    return xml_file_names_list


def xml_to_data_frame(xml_fn: str):
    """
    Converts and parses a xml file to a data frame file.
    
    :param xml_fn: Xml file name.
    :return: None.
    """

    process_name = mp.current_process().name

    try:
        parsing_logger.info('{} is processing {}.'.format(process_name, xml_fn))
        start_time = time.time()

        df_dir_name = os.path.dirname(xml_fn)
        df_name = os.path.basename(xml_fn).split('.')[0]
        output_fn = os.path.join(df_dir_name, df_name + '.pkl')

        df = read_xml_into_data_frame(xml_fn)
        fix_data_frame_column_type(df, data_frame_type_mapping[df_name])
        write_data_frame_to_disk(df, output_fn)

        finish_time = time.time()
        time_logger.info('{} took {:.2f}s to process {}.'.format(process_name, finish_time - start_time, xml_fn))

    except Exception as e:
        parsing_logger.warning('There was an error processing {}: {}'.format(xml_fn, e.__class__.__name__))
        raise


def xml_file_names_list_to_data_frames(xml_fn_list: list, nro_processes: int = 1):
    """
    Given a xml file name list, this function maps that list to a set of processes in order to they read, parse, and
    write those files to disk in a parallel fashion.
    
    :param xml_fn_list: List of xml file names to be read, parsed an written to disk.
    :param nro_processes: Number of processes to accomplish this task.
    :return: None.
    """

    pool = mp.Pool(processes=nro_processes)
    pool.map(xml_to_data_frame, xml_fn_list)


@timer_decorator('run this program')
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('xml_root_dir_name', type=str)
    parser.add_argument('nro_processes', type=int)
    args = parser.parse_args()

    xml_root_dir_name = args.xml_root_dir_name
    nro_processes = args.nro_processes

    xml_fn_list = get_xml_file_names(xml_root_dir_name)
    xml_file_names_list_to_data_frames(xml_fn_list, nro_processes)


if __name__ == '__main__':
    sys.exit(main())
