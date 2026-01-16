import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables from config/.env
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

def get_engine():
    """
    Creates and returns a SQLAlchemy engine for PostgreSQL
    """
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")

    connection_string = (
        f"postgresql+psycopg2://{db_user}:{db_password}"
        f"@{db_host}:{db_port}/{db_name}"
    )

    engine = create_engine(connection_string)
    return engine
