######################################################### SQL-Select-Simulation ########################################################################

Used Python version : 2.7.1

This is simulation of SQL engine for few types of select statements with limited aggregation functions using python


PRE CHECK TO RUN:
    
    1.Keep the dataset inside the dataset folder.
    
    2.Dataset tame should follow the name format(tablename.txt). tablename is nothing but what we are going to give in query.
        we can keep many number of datasets init. based on the select query it will fetch the correspoding table
        EX: query: "select * from products" will check for the file "products.txt" inside datase folder
        
    3.Provide the data type of the table information in the meta.py in the dataset as second line
        EX : First Line - title,brand,store,price,in_stock
             Second Line - str,int,int,float,bool


QUERY CONSTRAINTS:

    1.Only select statements can accept as input.

    2.Only MIN,MAX and UNIQ Aggreegation function are implemented

    3.Table name and column names are 'CASE SENSITIVE' and commands are 'Case Insensitive'. please be carefull while giving it.
        EX: SELECT brand from procutds WHERE in_stock = True    

    4. Where values conditions are should be 'CASE SENSITIVE'
 
   
FILE TO RUN:

    run.py 


COMMAND LINE GUIDANCE:

    1.Enter "Exit" at any time to exit the engine

    2.If there is the error in the table name or dataset values it will ask you for exit or retry with other query:
        EX : Do you want to Exit(Yes) or Retry(No) with other table:

    3.For other types query exception will print the error message and ask you enter the query again

    4. The none of the recodrs are matching with the condiftion result will print as "-------------- NO RESULTS FOUND -------------"
