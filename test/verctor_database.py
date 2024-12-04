
from qdrant_client import QdrantClient
from qdrant_client.http.models import PointStruct
from qdrant_client.http.models import Distance, VectorParams

























import requests

url = "http://localhost:6333/collections/Vector_index_1f865210_08c8_49aa_87d5_9ad3e8b5f146_Node"

response = requests.request("GET", url)

print(response.text)














