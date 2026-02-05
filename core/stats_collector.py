import os
import json
import pyodbc
import datetime
from dotenv import load_dotenv

class StatsCollector:
    def __init__(self, base_path: str, db_name: str, mode="full"):
        """
        mode: "full", "light", "none"
        """
        self.base_path = base_path
        self.db_name = db_name
        self.mode = mode.lower()

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

    def collect_stats(self):
        if self.mode == "none":
            print("Stats collection skipped (mode=none).")
            return {}

        stats_file = os.path.join(self.db_folder, f"{self.db_name}_stats.json")

        if os.path.exists(stats_file):
            print(f"Stats JSON already exists at {stats_file}, loading from file...")
            with open(stats_file, "r", encoding="utf-8") as f:
                return json.load(f)

        cursor = self.conn.cursor()
        stats = {}

        # Get all tables
        cursor.execute("""
            SELECT s.name AS schema_name, t.name AS table_name
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
        """)
        tables = cursor.fetchall()

        for row in tables:
            schema, table = row.schema_name, row.table_name
            table_key = f"{schema}.{table}"
            table_stat = {}

            # Row count always
            cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
            table_stat["row_count"] = cursor.fetchone()[0]

            # Column info always
            cursor.execute(f"""
                SELECT c.name, t.name AS type_name
                FROM sys.columns c
                JOIN sys.types t ON c.user_type_id = t.user_type_id
                WHERE c.object_id = OBJECT_ID('{schema}.{table}')
            """)
            columns = cursor.fetchall()
            table_stat["column_count"] = len(columns)

            if self.mode == "full":
                table_stat["columns"] = {}
                for col_name, col_type in columns:
                    col_stat = {"type": col_type}
                    # Unique and null counts
                    try:
                        cursor.execute(f"SELECT COUNT(DISTINCT [{col_name}]), SUM(CASE WHEN [{col_name}] IS NULL THEN 1 ELSE 0 END) FROM [{schema}].[{table}]")
                        unique_count, null_count = cursor.fetchone()
                        col_stat["unique_count"] = unique_count
                        col_stat["null_count"] = null_count
                    except Exception:
                        col_stat["unique_count"] = col_stat["null_count"] = None

                    # Top 5 values
                    try:
                        cursor.execute(f"""
                            SELECT TOP 5 [{col_name}], COUNT(*) AS cnt
                            FROM [{schema}].[{table}]
                            GROUP BY [{col_name}]
                            ORDER BY cnt DESC
                        """)
                        top_vals = []
                        for v, c in cursor.fetchall():
                            if isinstance(v, (datetime.date, datetime.datetime)):
                                v = v.isoformat()
                            top_vals.append({"value": v, "count": c})
                        col_stat["top_values"] = top_vals
                    except Exception:
                        col_stat["top_values"] = []

                    # Min/max/avg for numeric and date
                    try:
                        cursor.execute(f"SELECT MIN([{col_name}]), MAX([{col_name}]), AVG([{col_name}]) FROM [{schema}].[{table}]")
                        min_val, max_val, avg_val = cursor.fetchone()

                        # Αν datetime ή date -> string
                        if isinstance(min_val, (datetime.date, datetime.datetime)):
                            min_val = min_val.isoformat()
                        if isinstance(max_val, (datetime.date, datetime.datetime)):
                            max_val = max_val.isoformat()
                        if isinstance(avg_val, (datetime.date, datetime.datetime)):
                            avg_val = avg_val.isoformat()

                        col_stat["min"] = min_val
                        col_stat["max"] = max_val
                        col_stat["avg"] = avg_val
                    except Exception:
                        col_stat["min"] = col_stat["max"] = col_stat["avg"] = None

                    table_stat["columns"][col_name] = col_stat

            # Importance score for light and full
            table_stat["importance_score"] = table_stat["row_count"] * table_stat["column_count"]

            stats[table_key] = table_stat

        # Save JSON στο φάκελο της βάσης
        with open(stats_file, "w", encoding="utf-8") as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)

        print(f"Stats JSON saved at: {stats_file}")
        return stats
