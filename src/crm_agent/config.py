import os
from dataclasses import dataclass
from dotenv import load_dotenv

# .env는 프로젝트 루트에 있어야 함
load_dotenv()

@dataclass(frozen=True)
class Settings:
    # --- MySQL ---
    mysql_host: str = os.getenv("MYSQL_HOST", "127.0.0.1")
    mysql_port: int = int(os.getenv("MYSQL_PORT", "3307"))
    mysql_user: str = os.getenv("MYSQL_USER", "crm_user")
    mysql_password: str = os.getenv("MYSQL_PASSWORD", "crm_pass123!")
    mysql_db: str = os.getenv("MYSQL_DB", "crm")

    # --- OpenAI ---
    openai_api_key: str = os.getenv("OPENAI_API_KEY", "")

    # --- Pinecone ---
    pinecone_api_key: str = os.getenv("PINECONE_API_KEY", "")
    pinecone_index: str = os.getenv("PINECONE_INDEX", "")  # ✅ 반드시 "기존 인덱스명"으로 채워야 함
    pinecone_cloud: str = os.getenv("PINECONE_CLOUD", "aws")
    pinecone_region: str = os.getenv("PINECONE_REGION", "us-east-1")

    # ✅ namespace로 데이터 분리 (인덱스 개수 제한 회피)
    pinecone_namespace: str = os.getenv("PINECONE_NAMESPACE", "amore_crm_agent")

    # --- Models ---
    embed_model: str = os.getenv("EMBED_MODEL", "text-embedding-3-small")
    chat_model: str = os.getenv("CHAT_MODEL", "gpt-4.1-mini")

settings = Settings()
