import os
import json
from collections import defaultdict

class GraphBuilder:
    def __init__(self, base_path: str, db_name: str):
        self.base_path = base_path
        self.db_name = db_name

        # Φάκελος databases/<DB_NAME>
        self.db_folder = os.path.join(base_path, "databases", db_name)
        os.makedirs(self.db_folder, exist_ok=True)

        self.schema_file = os.path.join(self.db_folder, f"{db_name}.json")
        self.graph_file = os.path.join(self.db_folder, f"{db_name}_graph.json")

        if not os.path.exists(self.schema_file):
            raise FileNotFoundError(f"Schema JSON not found: {self.schema_file}")

        # --- Load schema ---
        with open(self.schema_file, "r", encoding="utf-8") as f:
            self.schema = json.load(f)

    def build(self) -> dict:
        # Αν υπάρχει ήδη graph.json, φορτώνει και skip
        if os.path.exists(self.graph_file):
            print(f"Graph JSON already exists at {self.graph_file}, loading from file...")
            with open(self.graph_file, "r", encoding="utf-8") as f:
                return json.load(f)

        graph = {
            "database": self.db_name,
            "nodes": {},
            "edges": [],
            "virtual_edges": [],
            "metadata": {
                "edge_types": ["foreign_key", "virtual_foreign_key"],
                "confidence_scale": "0.0 – 1.0"
            }
        }

        tables = self._collect_tables()

        for table_key, table in tables.items():
            graph["nodes"][table_key] = {
                "schema": table["schema"],
                "table": table["table"],
                "columns": table["columns"],
                "primary_key": table["primary_key"],
                "outgoing_edges": [],
                "incoming_edges": []
            }

        self._add_real_foreign_keys(graph, tables)
        self._add_virtual_foreign_keys(graph, tables)

        self._save(graph)
        return graph

    def _collect_tables(self):
        tables = {}

        for schema_name, schema in self.schema["schemas"].items():
            for table_name, table in schema.items():
                key = f"{schema_name}.{table_name}"
                tables[key] = {
                    "schema": schema_name,
                    "table": table_name,
                    "columns": table["columns"],
                    "primary_key": table.get("primary_key", []),
                    "foreign_keys": table.get("foreign_keys", [])
                }
        return tables

    def _add_real_foreign_keys(self, graph, tables):
        for source_key, table in tables.items():
            for fk in table["foreign_keys"]:
                target_key = f"{fk['ref_schema']}.{fk['ref_table']}"

                edge = {
                    "type": "foreign_key",
                    "from": source_key,
                    "to": target_key,
                    "column": fk["column"],
                    "ref_column": fk["ref_column"],
                    "confidence": 1.0,
                    "reason": "Declared foreign key constraint"
                }

                graph["edges"].append(edge)
                graph["nodes"][source_key]["outgoing_edges"].append(edge)
                graph["nodes"][target_key]["incoming_edges"].append(edge)

    def _add_virtual_foreign_keys(self, graph, tables):
        from collections import defaultdict
        column_index = defaultdict(list)

        # index columns by name
        for table_key, table in tables.items():
            for col in table["columns"]:
                column_index[col["name"].lower()].append((table_key, col))

        for source_key, table in tables.items():
            for col in table["columns"]:
                col_name = col["name"].lower()

                if not col_name.endswith("id"):
                    continue

                if col_name not in column_index:
                    continue

                for target_key, target_col in column_index[col_name]:
                    if target_key == source_key:
                        continue

                    edge = {
                        "type": "virtual_foreign_key",
                        "from": source_key,
                        "to": target_key,
                        "column": col["name"],
                        "ref_column": target_col["name"],
                        "confidence": 0.6,
                        "reason": "Column name match heuristic (*ID)"
                    }

                    graph["virtual_edges"].append(edge)
                    graph["nodes"][source_key]["outgoing_edges"].append(edge)
                    graph["nodes"][target_key]["incoming_edges"].append(edge)

    def _save(self, graph: dict):
        # Custom encoder για safety
        def default(o):
            from datetime import date, datetime
            if isinstance(o, (datetime, date)):
                return o.isoformat()
            return str(o)

        with open(self.graph_file, "w", encoding="utf-8") as f:
            json.dump(graph, f, indent=2, ensure_ascii=False, default=default)

        print(f"Graph JSON saved at: {self.graph_file}")
