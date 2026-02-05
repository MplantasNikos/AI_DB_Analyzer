import os
import json
import pyodbc
from datetime import datetime, date
from dotenv import load_dotenv

class SchemaLoader:
    def __init__(self, base_path: str, db_name: str):
        self.base_path = base_path
        self.db_name = db_name

        # Φάκελος databases/<DB_NAME>
        self.db_folder = os.path.join(base_path, "databases", db_name)
        os.makedirs(self.db_folder, exist_ok=True)

        load_dotenv(os.path.join(base_path, ".env"))

        self.driver = os.getenv("DB_DRIVER")
        self.server = os.getenv("DB_SERVER")
        self.database = os.getenv("DB_DATABASE")
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")

        if not all([self.driver, self.server, self.database, self.user, self.password]):
            raise RuntimeError("Λείπουν στοιχεία σύνδεσης από το .env")

        self.conn = self._connect()

    def _connect(self):
        conn_str = (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"UID={self.user};"
            f"PWD={self.password};"
            "TrustServerCertificate=yes;"
        )
        return pyodbc.connect(conn_str)

    def load_schema(self) -> dict:
        schema_file = os.path.join(self.db_folder, f"{self.db_name}.json")

        if os.path.exists(schema_file):
            print(f"Schema JSON already exists at {schema_file}, loading from file...")
            with open(schema_file, "r", encoding="utf-8") as f:
                return json.load(f)

        schema = {
            "database": self.database,
            "generated_at": datetime.utcnow().isoformat(),
            "dialect": "sqlserver",
            "schemas": {}
        }

        cursor = self.conn.cursor()

        # Tables & Columns
        cursor.execute("""
            SELECT
                s.name AS schema_name,
                t.name AS table_name,
                c.name AS column_name,
                ty.name AS data_type,
                c.max_length,
                c.is_nullable
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            JOIN sys.columns c ON t.object_id = c.object_id
            JOIN sys.types ty ON c.user_type_id = ty.user_type_id
            ORDER BY s.name, t.name, c.column_id
        """)

        for row in cursor.fetchall():
            sch = row.schema_name
            tbl = row.table_name

            schema["schemas"].setdefault(sch, {})
            schema["schemas"][sch].setdefault(tbl, {
                "columns": [],
                "primary_key": [],
                "foreign_keys": [],
                "indexes": [],
                "constraints": []
            })

            schema["schemas"][sch][tbl]["columns"].append({
                "name": row.column_name,
                "type": row.data_type,
                "max_length": row.max_length,
                "nullable": bool(row.is_nullable)
            })

        # Primary Keys
        cursor.execute("""
            SELECT
                SCHEMA_NAME(t.schema_id) AS schema_name,
                t.name AS table_name,
                c.name AS column_name
            FROM sys.indexes i
            JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            JOIN sys.tables t ON i.object_id = t.object_id
            WHERE i.is_primary_key = 1
        """)
        for row in cursor.fetchall():
            schema["schemas"][row.schema_name][row.table_name]["primary_key"].append(
                row.column_name
            )

        # Foreign Keys
        cursor.execute("""
            SELECT
                SCHEMA_NAME(tp.schema_id) AS parent_schema,
                tp.name AS parent_table,
                cp.name AS parent_column,
                SCHEMA_NAME(tr.schema_id) AS ref_schema,
                tr.name AS ref_table,
                cr.name AS ref_column
            FROM sys.foreign_keys fk
            JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
            JOIN sys.tables tp ON fkc.parent_object_id = tp.object_id
            JOIN sys.columns cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
            JOIN sys.tables tr ON fkc.referenced_object_id = tr.object_id
            JOIN sys.columns cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
        """)
        for row in cursor.fetchall():
            schema["schemas"][row.parent_schema][row.parent_table]["foreign_keys"].append({
                "column": row.parent_column,
                "ref_schema": row.ref_schema,
                "ref_table": row.ref_table,
                "ref_column": row.ref_column
            })

        # Indexes
        cursor.execute("""
            SELECT
                SCHEMA_NAME(t.schema_id) AS schema_name,
                t.name AS table_name,
                i.name AS index_name,
                i.is_unique
            FROM sys.indexes i
            JOIN sys.tables t ON i.object_id = t.object_id
            WHERE i.name IS NOT NULL AND i.is_primary_key = 0
        """)
        for row in cursor.fetchall():
            schema["schemas"][row.schema_name][row.table_name]["indexes"].append({
                "name": row.index_name,
                "unique": bool(row.is_unique)
            })

        # Save schema JSON
        self._save(schema)
        return schema

    def _save(self, schema: dict):
        path = os.path.join(self.db_folder, f"{self.db_name}.json")

        # Custom JSON encoder για datetime (αν τυχόν εμφανιστεί)
        def default(o):
            if isinstance(o, (datetime, date)):
                return o.isoformat()
            return str(o)

        with open(path, "w", encoding="utf-8") as f:
            json.dump(schema, f, indent=2, ensure_ascii=False, default=default)

    # Βοηθητική μέθοδος για αριθμό πινάκων
    def get_table_count(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM sys.tables")
        return cursor.fetchone()[0]
