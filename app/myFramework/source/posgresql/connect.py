# from sqlalchemy import create_engine

# def getConnection(dbname):

#     user='postgres'
#     password='postgres'
#     # host ='0.0.0.0'
#     host= '0.0.0.0'
#     port='9999'
#     # return engine
#     return create_engine(f'postgresql://{user}:{password}@postgre_db:{port}/{dbname}',pool_use_lifo=True, pool_pre_ping=True)  


import psycopg2

def getCursor(dbname):
    conn = psycopg2.connect(database = dbname, 
                            user = "postgres", 
                            host= '0.0.0.0',
                            password = "postgres",
                            port = 9999)
    cur = conn.cursor()
    return cur

# def getConnection(dbname):
#     conn = psycopg2.connect(database = dbname, 
#                             user = "postgres", 
#                             host= 'postgres_db',
#                             password = "postgres",
#                             port = 9999)
    
#     return conn


 