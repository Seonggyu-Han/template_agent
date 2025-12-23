# tools/clear_default_namespace.py
from dotenv import load_dotenv
import os
from pinecone import Pinecone

load_dotenv(override=True)
pc = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
idx = pc.Index(os.getenv("PINECONE_INDEX", "pinecone-first"))

idx.delete(delete_all=True, namespace="")
print("✅ cleared default namespace ''")
