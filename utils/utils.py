#-------------------- SQL Engile Util Modules-----------

import os
import re
from meta.meta import *

def get_table_name(query):
    '''
        Getting the table name from query
        query : string
        return : if match found table name (or) None
    '''
    find = re.search('%s(.*)' % (QUERY_METADATA['FROM']),query,re.IGNORECASE)
    if find:
        return find.group(1).split(' ')[0]
    else:
        return None
    
def get_result_header(query):
    '''
        Getting the string to display in result as header name from query
        query : string
        return : if match found result header (or) None
    '''    
    find = re.search('%s(.*)%s' % (QUERY_METADATA['SELECT'], QUERY_METADATA['FROM']),query,re.IGNORECASE)
    if find:
        return find.group(1)
    else:
        return None
    
def get_condition(query):
    '''
        Getting the where condition string from query
        query : string
        return : if match found condition string (or) None
    '''    
    find = re.search('%s(.*)' % (QUERY_METADATA['WHERE']), query,re.IGNORECASE)
    if find:
        return find.group(1)
    else:
        return None
    
def get_aggregation(result_header):
    '''
        Getting the Aggregation function from query
        result_header : string
        return : if match found Aggregation function (or) None
    '''    
    aggregation = None
    for each_aggr in AGGREGATION_METADATA:
        aggre_check = re.search('%s\((.*)\)'%(each_aggr),result_header,re.IGNORECASE)
        if aggre_check:
            aggregation = each_aggr
            break
    return aggregation

def get_selected_columns(result_header,headers,aggregation):
    '''
        Getting the columns that are used in select query
        result_header : string
        headers : [], list of all headers in the table
        aggregation : bool, Is aggregation function available in the query
        return : if match found Aggregation function (or) None
    '''  
    if not aggregation:
        selected_columns = headers if result_header.strip() =='*' else result_header.split(',')
    else:
        aggre_check = re.search('%s\((.*)\)'%(aggregation),result_header,re.IGNORECASE)
        selected_columns = [aggre_check.group(1)]
    for column in selected_columns:
        if column not in headers:
            raise Exception()
    return selected_columns

def check_dataset(base_path,table):
    '''
        To check the dataset for the perticiular table is availabe
        base_path : string , Current path of the base folder
        table : string, table name in the query
    '''
    dataset_file_path = base_path+DATASET_FOLDER+table+DATASET_EXTN
    if os.path.isfile(dataset_file_path):
        return dataset_file_path
    else:
        raise

def load_data_into_dict(file_obj,headers,datatype):
    '''
    '''
    table_data=[]
    schema = zip(headers,datatype)
    lines = file_obj.readlines()
    for line in lines:
        line = line.replace('\n','')
        line = re.sub('TRUE','True',line,re.IGNORECASE)
        line = re.sub('FALSE','False',line,re.IGNORECASE)
        data = dict(zip(headers,line.split(',')))
        for column_name,column_type in schema:
            if column_type != 'bool':
                data[column_name] = eval("%s('%s')"%(column_type,data[column_name]))
            else:
                data[column_name] = True if re.match('TRUE',data[column_name],re.IGNORECASE) else False 
        table_data.append(data)
    return table_data

    

