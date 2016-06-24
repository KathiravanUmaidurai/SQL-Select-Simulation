#-------------------- SQL Engile Util Modules-----------

import re
from .meta.meta import *

def get_table_name(query):
    find = re.search('%s(.*)' % (Query_MetaData['FROM']),query,re.IGNORECASE)
    if find:
        return find.group(1).split(' ')[0]
    else:
        return None
    
def get_result_header(query):
    find = re.search('%s(.*)%s' % (Query_MetaData['SELECT'], Query_MetaData['FROM']),query,re.IGNORECASE)
    if find:
        return find.group(1)
    else:
        return None
    
def get_condition(query):
    find = re.search('%s(.*)' % (Query_MetaData['WHERE']), query,re.IGNORECASE)
    if find:
        return find.group(1)
    else:
        return None
    
def get_aggregation(result_header):
    aggregation = None
    for each_aggr in Aggregation_Metadata:
        aggre_check = re.search('%s\((.*)\)'%(each_aggr),result_header,re.IGNORECASE)
        if aggre_check:
            aggregation = each_aggr
            break
    return aggregation

def get_selected_columns(result_header,headers,aggregation):
    '''
    '''
    if not aggregation:
        selected_columns = headers if result_header.strip() =='*' else result_header.split(',')
    else:
        aggre_check = re.search('%s\((.*)\)'%(aggregation),result_header,re.IGNORECASE)
        selected_columns = [aggre_check.group(1)]
    header_small_case = [header.lower() for header in headers]
    for columns in selected_columns:
        if columns.lower() not in header_small_case:
            raise 
    return selected_columns

def load_data_into_dict(file_obj,headers,datatype):
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

    

