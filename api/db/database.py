from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os


MYSQL_CONN_URI = os.getenv('MYSQL_CONNECTION_URI')

engine = create_engine(MYSQL_CONN_URI)

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)

Base = declarative_base()