# AI_DB_Analyzer

**AI_DB_Analyzer** is a tool designed to analyze databases and generate **SQL queries** using a **large language model (LLM – Mistral 7B)**.  
Its goal is to understand the database structure, the relationships between tables, and suggest queries or draft SQL statements based on natural language questions from the user.

---

## 🔹 What the program does

1. **Schema Loader**  
   - Loads metadata and table schemas from the database.  
   - Understands which tables exist, what fields they contain, and how they are related.

2. **Graph Builder**  
   - Creates a graph representing the relationships between tables.  
   - Helps the AI determine how to join data from different tables.

3. **Stats Collector**  
   - Collects statistical information about the tables (e.g., row counts, unique values).  
   - These statistics are used to produce more accurate query suggestions.

4. **MultiJSON Chunker & Embeddings**  
   - Splits data into smaller chunks and generates embeddings for efficient AI processing.

5. **Load LLM (Mistral 7B)**  
   - Loads the language model that will analyze user questions and generate SQL queries.

6. **Query AI**  
   - Users provide natural language questions (e.g., "Which customers have more than 5 orders?").  
   - The program uses the LLM to produce **connected SQL queries** that answer the question.

7. **Interactive Loop & Results**  
   - Users can submit multiple queries until they choose to exit.  
   - Results are stored in the `results/` folder for future reference.

---

## 🔹 Why it was designed this way

- **Large databases**: Built to handle hundreds or even thousands of tables.  
- **Understanding relationships**: The program doesn’t just run SQL; it understands **how tables are joined**.  
- **Extensibility**: Can integrate different models or connect to a web interface later.

---

## 🔹 What we achieve

- Fast understanding and analysis of database structures.  
- Automatic generation of SQL queries from natural language.  
- Storage and tracking of results for future analysis.  
- Modular design for easy maintenance and extension.

---

This tool is ideal for developers, data analysts, or teams seeking **intelligent automation in database querying** without writing SQL manually every time.
