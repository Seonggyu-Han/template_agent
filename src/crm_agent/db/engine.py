from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from crm_agent.config import settings

def mysql_url() -> str:
    return (
        f"mysql+pymysql://{settings.mysql_user}:{settings.mysql_password}"
        f"@{settings.mysql_host}:{settings.mysql_port}/{settings.mysql_db}"
        f"?charset=utf8mb4"
    )

engine = create_engine(mysql_url(), pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
