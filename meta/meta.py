
#Global Meta Data Repositary for Analyze the query and split the condition and select columns

SQL_MetaData = {'products':['F:/DataStorage/dataset.txt',('str','int','int','float','bool')],
                'products_new':['F:/DataStorage/dataset_new.txt',('str','int','int','float','bool')]}
Query_MetaData = {'SELECT':'select ','FROM':' from ','WHERE':' where '}
Operation_Metadata = {'=':'==',' AND ':' and ',' OR ':' or ', 'TRUE':'True','FALSE':'False'}
Aggregation_Metadata = {'MAX':'max(','MIN':'min(','UNIQ':'uniq('}
Aggregation_Py_Functions = {'MAX':'max','MIN':'min','UNIQ':'set'}

