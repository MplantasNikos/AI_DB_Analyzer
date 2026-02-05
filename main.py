import os
import sys
from dotenv import load_dotenv
from llama_cpp import Llama

from core.schema_loader import SchemaLoader
from core.graph_builder import GraphBuilder
from core.stats_collector import StatsCollector
from core.chunks import MultiJSONChunker
from core.query_ai import QueryAI

MODEL_NAME = "mistral-7b-instruct-v0.1.Q4_K_M.gguf"

def main():
    load_dotenv()

    # --- Base path ---
    try:
        base_path = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        base_path = os.getcwd()
    if getattr(sys, "frozen", False):
        base_path = os.path.dirname(sys.executable)

    db_name = os.getenv("DB_DATABASE")
    stats_mode = os.getenv("STATS_PARAMETER", "full").lower()
    if not db_name:
        raise RuntimeError(" DB_DATABASE not set in .env")

    db_folder = os.path.join(base_path, "databases", db_name)
    os.makedirs(db_folder, exist_ok=True)
    results_folder = os.path.join(base_path, "results")
    os.makedirs(results_folder, exist_ok=True)

    SchemaLoader(base_path, db_name).load_schema()

    GraphBuilder(base_path, db_name).build()

    StatsCollector(base_path, db_name, mode=stats_mode).collect_stats()


    MultiJSONChunker(base_path, db_name).run()

 
    llm = Llama(
        model_path=os.path.join(base_path, "models", MODEL_NAME),
        n_ctx=2048,
        n_gpu_layers=10,
        verbose=False
    )

    # -------------------------------
    #  Query AI
    query_ai = QueryAI(base_path=base_path, db_name=db_name, top_k=3, llm=llm)

    # -------------------------------
    #  Interactive loop
    while True:
        user_query = input("\n Query (or 'exit'): ").strip()
        if user_query.lower() in {"exit", "quit"}:
            break

        sql_query = query_ai.generate_sql(user_query)

        print("\n" + "="*60)
        print(sql_query)
        print("="*60 + "\n")

        # Save
        filename = os.path.join(results_folder, f"query_{len(os.listdir(results_folder)) + 1}.txt")
        with open(filename, "w", encoding="utf-8") as f:
            f.write(f"--- Query ---\n{user_query}\n\n--- SQL ---\n{sql_query}\n")

if __name__ == "__main__":
    main()
