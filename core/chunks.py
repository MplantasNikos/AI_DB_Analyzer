# ----------------------------- core/chunks.py (updated) -----------------------------
import os
import json
import pickle
import numpy as np
from sentence_transformers import SentenceTransformer
import faiss  # για fast similarity search

class MultiJSONChunker:
    """
    Σπάει JSON αρχεία (graph, schema, stats) σε chunks, φτιάχνει embeddings
    και φτιάχνει FAISS index για γρήγορη αναζήτηση.
    """
    def __init__(self, base_path: str, db_name: str, chunk_size: int = 50,
                 embed_model: str = "all-MiniLM-L6-v2", index_path=None):
        self.base_path = base_path
        self.db_name = db_name
        self.chunk_size = chunk_size
        self.db_folder = os.path.join(base_path, "databases", db_name)
        self.index_path = index_path or os.path.join(self.db_folder, "faiss_index.idx")

        self.files = {
            "graph": os.path.join(self.db_folder, f"{db_name}_graph.json"),
            "schema": os.path.join(self.db_folder, f"{db_name}.json"),
            "stats": os.path.join(self.db_folder, f"{db_name}_stats.json")
        }

        self.embed_model = SentenceTransformer(embed_model)
        os.makedirs(self.db_folder, exist_ok=True)

    def chunk_json(self, json_data, name):
        chunks = []
        if name in ("graph", "schema"):
            nodes = []
            if name == "graph":
                nodes = list(json_data["nodes"].items())
            else:
                for schema_name, tables in json_data.get("schemas", {}).items():
                    for table_name, table_info in tables.items():
                        nodes.append((f"{schema_name}.{table_name}", table_info))

            for i in range(0, len(nodes), self.chunk_size):
                chunk_nodes = nodes[i:i+self.chunk_size]
                chunk_summary = []
                for table_key, table_info in chunk_nodes:
                    chunk_summary.append({
                        "table": table_key,
                        "columns": [c["name"] for c in table_info.get("columns", [])],
                        "primary_key": table_info.get("primary_key", []),
                        "importance_score": table_info.get("importance_score", 0)
                    })
                chunks.append(chunk_summary)

        elif name == "stats":
            for i, (table_key, table_info) in enumerate(json_data.items()):
                if i % self.chunk_size == 0:
                    chunks.append([])
                chunks[-1].append({
                    "table": table_key,
                    "row_count": table_info.get("row_count"),
                    "column_count": table_info.get("column_count"),
                    "importance_score": table_info.get("importance_score", 0)
                })
        return chunks

    def embed_chunks(self, chunks):
        embeddings = []
        for chunk in chunks:
            text = json.dumps(chunk)
            vec = self.embed_model.encode(text, convert_to_numpy=True)
            embeddings.append(vec)
        return np.array(embeddings, dtype=np.float32)

    def save_chunks(self, name, chunks, embeddings):
        chunks_file = os.path.join(self.db_folder, f"{name}_chunks.pkl")
        embeddings_file = os.path.join(self.db_folder, f"{name}_chunks_embeddings.npy")

        with open(chunks_file, "wb") as f:
            pickle.dump(chunks, f)
        np.save(embeddings_file, embeddings)
        print(f"✅ {name} chunks and embeddings saved")

    def build_faiss_index(self, embeddings):
        dim = embeddings.shape[1]
        index = faiss.IndexFlatIP(dim)  # inner product ~ cosine similarity
        faiss.normalize_L2(embeddings)
        index.add(embeddings)
        faiss.write_index(index, self.index_path)
        print(f"✅ FAISS index saved to {self.index_path}")
        return index

    def run(self):
        all_embeddings = []
        for name, path in self.files.items():
            if not os.path.exists(path):
                if name == "stats":
                    print(f"⚠️ Stats file not found, skipping {name}")
                    continue
                else:
                    raise FileNotFoundError(f"{name} file not found: {path}")

            print(f"Processing {name} JSON...")
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)

            chunks = self.chunk_json(data, name)
            print(f"Created {len(chunks)} chunks for {name}")

            embeddings = self.embed_chunks(chunks)
            print(f"Created embeddings for {name}")

            self.save_chunks(name, chunks, embeddings)
            all_embeddings.append(embeddings)

        # Συνενώνουμε όλα τα embeddings για FAISS
        if all_embeddings:
            combined_embeddings = np.vstack(all_embeddings)
            self.build_faiss_index(combined_embeddings)

        print("✅ Multi-JSON chunking pipeline completed successfully!")
