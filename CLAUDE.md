I want to create a database system in Golang that combines the principles of SQLite with specialized support for both traditional row-based data and vector data. The implementation should follow the structure and design of SQLite, without reinventing anything from scratch. The goal is to adapt SQLite's architecture to efficiently store and manipulate both types of data—rows and vectors.

Please refer to the SQLite source code located in `./.claude/sqlite` to understand the existing structure and approach. Your task is to:

1. Create a database framework that can handle both row-based data and vector data.
2. Focus on implementing the core functionality, including:

    * Creating a new database that supports both row-based tables and vector storage.
    * Inserting rows and vectors into the database.
    * Storing, indexing, and efficiently querying both row-based and vector data.
    * Querying vectors using similarity search (e.g., cosine similarity, Euclidean distance).
3. Ensure that the database is lightweight, fast, and integrates seamlessly with Go applications.
4. Maintain the SQLite-like file-based storage approach, but adapt it to handle both row-based data and vector data operations.
5. Do not reinvent the wheel—follow the SQLite codebase for storage and query structure, and focus on adding the necessary logic for vector data handling.

Key implementation areas to address:

* **Data format** for storing both row-based data and vectors.
* **Indexing mechanism** for fast retrieval of both row-based data (using traditional indexing methods) and vectors (using approximate nearest neighbor search or other vector-specific indexing techniques).
* **Efficient serialization and deserialization** of both row and vector data.
* **Vector-based query functions**, including similarity search for the nearest vectors and traditional SQL-style queries for row-based data.

This system should mimic the structure and philosophy of SQLite but be capable of managing both row-based data and vector data in an integrated, efficient manner.
