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
   - The model used is **`mistral-7b-instruct-v0.1.Q4_K_M.gguf`**, obtained from [Mistral AI](https://mistral.ai/) official releases.  
   - We chose this model because it is **open-weight, instruction-tuned, and optimized for inference**, allowing fast responses even on GPU.

6. **Query AI**  
   - Users provide natural language questions (e.g., "Which customers have more than 5 orders?").  
   - The program uses the LLM to produce **connected SQL queries** that answer the question.

7. **Interactive Loop & Results**  
   - Users can submit multiple queries until they choose to exit.  
   - All results are saved in the `results/` folder for future reference.

---

## 🔹 Project Folder Structure

```text
AI_DB_Analyzer/
│
├── run.bat                # Run the project with one click (Windows)
├── main.py                # Main program script
├── core/                  # Core modules: schema, graph, stats, chunks, query AI
├── databases/             # Database folders with metadata and schema files
├── models/                # LLM models (e.g., mistral-7b-instruct)
├── results/               # Generated SQL queries and outputs
├── requirements.txt       # Python dependencies
├── .env                   # Environment variables (DB name, stats mode)
└── venv/                  # Virtual environment (excluded from GitHub)
