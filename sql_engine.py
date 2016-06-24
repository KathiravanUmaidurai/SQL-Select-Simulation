'''
            Simple SQL Select Engine Simulation with PYTHON code
                        Python Verson 2.6.5
'''

import sys
import re
import __builtin__

from utils import utils
from meta import meta

class SQL_Engine(object):
    
    def __init__(self):
        '''
            Along with vaiable initialaization check table availability  and load the data
        '''
        self.table = None
        self.query = None
        self.table_path = None
        self.file_obj = None
        self.aggregation  = None
        self.retry = False
        self.close = False
        self.condition_sql_str = ''
        self.condition_py_str = ''
        self.headers = []
        self.selected_columns = []
        self.table_data = []
        self.result_data = []
            
    def execute(self,query):
        '''
            Execute the given query and provide the result of it
        '''
        self.query = query
        self.query_analyze()
        try:
            self.check_and_load_data()
        except Exception,e:
            print e
            self.exit_retry_sql()
        self.get_select_column_list()
        self.get_condition_py_string()
        self.get_result()
        if self.result_data and self.aggregation:
            self.aggregation_functions()
        self.format_result_str()

    def query_analyze(self):
        '''
            Analyze the query and get the following information from it
            selected comluns
            agreegation function
            where conditions
        '''
        if self.query:
            self.table = utils.get_table_name(self.query)
            try:
                self.result_header = utils.get_result_header(self.query)
                aggregation_tuple = utils.get_aggregation(self.result_header)
                self.aggregation = aggregation_tuple
                self.condition_sql_str = utils.get_condition(self.query)
            except Exception,e:
                print e
                raise ValueError('Query string entered is not a valid to process... Please Check it !!!! ')
        
    def check_and_load_data(self):
        '''
            Load the headers and correspoding data of the given table
        '''
        try:
            self.table_path,self.data_type = meta.SQL_MetaData[self.table]
        except Exception,e:
            print e
            raise ValueError( 'Table Error... Table is not available in the SQL MetaData !!!! ')
        try:
            self.file_obj = open(self.table_path,'r')
            self.headers = self.file_obj.readline().replace('\n','').split(',')
            self.table_data = utils.load_data_into_dict(self.file_obj,self.headers,self.data_type)
        except Exception,e:
            print e
            raise ValueError('Dataset error ... Check the file path, values and data type of the values !!!! ')
        finally:
            self.file_obj.close()

    def get_select_column_list(self):
        '''
        '''
        try:
            self.selected_columns = utils.get_selected_columns(self.result_header,self.headers,self.aggregation)
        except Exception,e:
            print e
            raise ValueError("Query Error : Check the column names given in the select query")
        
    def get_condition_py_string(self):
        '''
            Creatring the condition string can runable in python from sql condition string
        '''
        if self.condition_sql_str:
            self.condition_py_str = self.condition_sql_str
            for each_header in self.headers:
                self.condition_py_str = self.condition_py_str.replace(each_header,"each_data['%s']"%each_header)
            for each_operator in meta.Operation_Metadata:
                pattern = re.compile(each_operator, re.IGNORECASE)
                self.condition_py_str = pattern.sub(meta.Operation_Metadata[each_operator],self.condition_py_str)
            
    def get_result(self):
        '''
            Getting the result row based on the where condition on  the given query
        '''
        try:
            for each_data in self.table_data:
                if not self.condition_py_str or eval(self.condition_py_str):
                    self.result_data.append(each_data)
        except Exception,e:
            print e
            raise ValueError('Query Error : Check the where condition in the query')

    def aggregation_functions(self):
        '''
            Applying the mapping python aggregation for sql aggreegation
            Filering the result data based on the aggregation function
        '''
        column_select = self.selected_columns[0]
        filter_result = [ each_result[column_select] for each_result in self.result_data]
        filter_result = getattr(__builtin__,meta.Aggregation_Py_Functions[self.aggregation])(filter_result)
        if not hasattr(filter_result,'__iter__'):
            self.result_data = [{column_select:filter_result}]
        else:
            self.result_data = [dict([(column_select,data)]) for data in filter_result]

    def format_result_str(self):
        '''
            Formatting the result data to display properly
        '''
        header_str = self.result_header if self.aggregation else ','.join(self.selected_columns)
        result_str = ''
        if self.result_data:
            for each_result in self.result_data:
                result_str += ','.join([str(each_result[header]) for header in  self.selected_columns if header in each_result])+'\n'
            result_str = '\n'+header_str+'\n'+result_str
        else:
            result_str = '----- No Data -----'
        print result_str
        
    def clear_data(self):
        '''
            Clearing perveious query related information to run the next query
        '''
        self.table = None
        self.query = None
        self.table_path = None
        self.file_obj = None
        self.aggregation  = None
        self.retry = False
        self.close = False
        self.condition_sql_str = ''
        self.condition_py_str = ''
        self.headers = []
        self.selected_columns = []
        self.table_data = []
        self.result_data = []
        
    def exit_retry_sql(self):
        '''
            Getting user option to close the engine or retry with other table since the current table is having as issue
        '''
        exit_choice = ''
        while exit_choice.lower() not in ['yes','no','y','n','exit','retry']:
            exit_choice = raw_input("\n Do you want to Exit(Yes) or Retry(No) with other table: ")
        if exit_choice.lower() in ['yes','y','exit']:
            self.close = True
        else:
            self.retry = True
        raise 


