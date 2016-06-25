'''
                    Basic PY file to start the SQL Engine

'''

from sql_engine import SQL_Engine

def start_engine():
    '''
        This function will get the table name and select query as user input
        At any case user provide exit as input will terminate the engine
    '''
    
    print "--------------------------------- SQL SELECT ENGINE IN PYTHON ----------------------------------------"
    print '\n\n\t\t\tEnter ""EXIT"" any time to terminate the engine\n\n'
    conn = SQL_Engine()
    query = raw_input("\nEnter Select Query : ")
    while(query.lower().strip()!='exit'):
        try:
            conn.execute(query)
        except Exception,e:
            if conn.close:
                return
            elif conn.retry:
                pass
            else:
                print e
        conn.clear_data()
        query = raw_input("\nEnter Select Query : ")
    return
            
if __name__ == '__main__':
    start_engine()
