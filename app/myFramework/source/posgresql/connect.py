from sqlalchemy import create_engine
import psycopg2

# for getDF
def getCursor(dbname):
    user = "postgres"
    host = "postgres_db"
    passwprd = "postgres"
    conn = psycopg2.connect(database = dbname, 
                            user = user,
                            host= host,
                            password = passwprd,
                            port = 9999)
    cur = conn.cursor()
    return cur

def getEngine(dbname):

    user='postgres'
    password='postgres'
    host= 'postgres_db'
    port='9999'
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}')
   
    return engine






 