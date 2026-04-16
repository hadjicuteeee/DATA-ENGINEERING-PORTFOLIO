import os
import time
import logging
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
from extract import extract_shows

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

logging.basicConfig(filename="movies.log", level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

logging.info("Starting the process")

username = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_NAME")


def connecting_to_pg():

    engine = create_engine(f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}")

    try:
        with engine.connect() as conn:
            pass

        logging.info("Connected to postgreSQL")
        return engine
    
    except EnvironmentError as ee:
        logging.error(f"Error at environment ", {ee})
    except Exception as e:
        logging.warning(f"Error found in ", {e})
    
    return None

engine = connecting_to_pg()
if engine:
    clean = extract_shows()
    clean = pd.DataFrame(clean)
    if clean is not None and not clean.empty:
        try:
            try:
                ex_name = pd.read_sql("SELECT name FROM movies", engine)
            except:
                ex_name = pd.DataFrame(columns=['name'])
            
            clean = clean[~clean['name'].isin(ex_name['name'])]
            clean.to_sql("movies", engine, if_exists="append", index=False)

            logging.info(f"Successfully loaded to postgreSQL")
            print("Success")
        
        except Exception as e:
            logging.warning(f"Error found in ", {e})
            print(f"Error found in ", {e})
    else:
        print(f"Error fetching data")
else:
    print(f"Skipping loading to database")


    
    
