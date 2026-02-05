# ----------------------------- core/query_ai.py (silent & minimal) -----------------------------
import os
import json
import numpy as np
from sentence_transformers import SentenceTransformer
import pickle

class QueryAI:
    """
    Query AI: παίρνει φυσική γλώσσα ερώτημα, κάνει similarity search στα chunks,
    και δημιουργεί SQL query χρησιμοποιώντας το preloaded LLM.
    """
    def __init__(self, base_path: str, db_name: str, top_k: int = 3, model_name: str = None, llm=None, embed_model="all-MiniLM-L6-v2"):
        self.base_path = base_path
        self.db_name = db_name
        self.db_folder = os.path.join(base_path, "databases", db_name)
        self.top_k = top_k
        self.llm = llm
        self.embed_model = SentenceTransformer(embed_model)

        if self.llm is None:
            raise RuntimeError("LLM instance not provided. Pass llm= preloaded Llama object.")

        # Load chunks & embeddings silently
        self.chunks_data = {}
        self.embeddings_data = {}

        for name in ["graph", "schema", "stats"]:
            chunks_file = os.path.join(self.db_folder, f"{name}_chunks.pkl")
            embeddings_file = os.path.join(self.db_folder, f"{name}_chunks_embeddings.npy")

            if os.path.exists(chunks_file) and os.path.exists(embeddings_file):
                with open(chunks_file, "rb") as f:
                    self.chunks_data[name] = pickle.load(f)
                self.embeddings_data[name] = np.load(embeddings_file)
            else:
                continue  # skip quietly

    def similarity_search(self, query: str):
        """
        Top-k chunks με cosine similarity.
        """
        query_vec = self.embed_model.encode(query, convert_to_numpy=True)
        top_chunks = []

        for name, embeddings in self.embeddings_data.items():
            if len(embeddings) == 0:
                continue
            sims = embeddings @ query_vec / (np.linalg.norm(embeddings, axis=1) * np.linalg.norm(query_vec) + 1e-10)
            top_indices = sims.argsort()[-self.top_k:][::-1]

            for idx in top_indices:
                chunk = self.chunks_data[name][idx]
                minimal_chunk = [{
                    "table": entry.get("table"),
                    "columns": entry.get("columns", []),
                    "primary_key": entry.get("primary_key", []),
                    "importance_score": entry.get("importance_score", 0)
                } for entry in chunk]
                top_chunks.append({
                    "source": name,
                    "score": float(sims[idx]),
                    "chunk": minimal_chunk
                })

        return sorted(top_chunks, key=lambda x: x["score"], reverse=True)[:self.top_k]

    def generate_sql(self, user_query: str):
        """
        Παίρνει φυσικό query, βρίσκει top chunks και ζητάει από το LLM να φτιάξει SQL query.
        """
        top_chunks = self.similarity_search(user_query)

        context_text = ""
        for c in top_chunks:
            context_text += f"Source: {c['source']}\n{json.dumps(c['chunk'], indent=2)}\n\n"

        prompt = f"""
You generate SQL queries only.

Database schema context:
{context_text}

User question:
{user_query}

Rules:
- Use only the tables that are necessary
- Use table aliases
- Do NOT explain anything
- Output SQL only

SQL:
"""

        result = self.llm(prompt, max_tokens=1024)
        sql_query = result["choices"][0]["text"].strip() if "choices" in result else str(result).strip()
        return sql_query
