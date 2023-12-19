import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # PostgreSQL configurations
    DATABASE_USER = os.environ.get('DB_USER')
    DATABASE_PASSWORD = os.environ.get('DB_PASSWORD')
    DATABASE_HOST = os.environ.get('DB_HOST')
    DATABASE_PORT = os.environ.get('DB_PORT', 5432)
    DATABASE_NAME = os.environ.get('DB_NAME')
    
    #ElasticSearch configurations
    ELASTICSEARCH_URL = os.environ.get('ELASTICSEARCH_URL')
    ELASTICSEARCH_INDEX = os.environ.get('ELASTICSEARCH_INDEX')
    AMAZON_URL = os.environ.get('AMAZON_URL')