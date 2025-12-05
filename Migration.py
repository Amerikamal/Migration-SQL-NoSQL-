import tkinter as tk
from tkinter import ttk, messagebox
from connection import Sql_db, NOSql_db

# ----------------------- Global Variables -----------------------
sqlite_cache = {}
mongo_cache = {}
mongo_display_mode = "columns"  # "columns" or "dict"

# ----------------------- Database Schema -----------------------
create_table_sqls = [
    """
    CREATE TABLE IF NOT EXISTS Users (
        UserID INTEGER PRIMARY KEY AUTOINCREMENT,
        Name TEXT NOT NULL,
        Email TEXT UNIQUE,
        Age INTEGER
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS Products (
        ProductID INTEGER PRIMARY KEY,
        ProductName TEXT NOT NULL,
        Price REAL NOT NULL,
        StockQuantity INTEGER DEFAULT 0
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS Orders (
        OrderID INTEGER PRIMARY KEY,
        UserID INTEGER NOT NULL,
        OrderDate TEXT NOT NULL,
        TotalAmount REAL,
        FOREIGN KEY (UserID) REFERENCES Users(UserID)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS Departments (
        DeptID INTEGER PRIMARY KEY,
        DeptName TEXT NOT NULL UNIQUE
    );
    """
]

# ----------------------- SQL Operations -----------------------
def initialize_database():
    """Initialize SQLite database with tables if they don't exist."""
    try:
        with Sql_db('test.db') as cur:
            for sql in create_table_sqls:
                cur.execute(sql)
        return True
    except Exception as e:
        messagebox.showerror("Initialization Error", f"Failed to initialize database:\n{e}")
        return False

def get_sql_tables():
    """Return all table names from SQLite."""
    try:
        with Sql_db('test.db') as cur:
            tables = cur.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
            return [t[0] for t in tables if t[0] != "sqlite_sequence"]
    except:
        return []

def load_sqlite_table(table_name):
    """Load SQLite table into TreeView."""
    sqlite_tree.delete(*sqlite_tree.get_children())
    if not table_name:
        return

    try:
        with Sql_db('test.db') as cur:
            cur.execute(f"PRAGMA table_info({table_name})")
            columns = [col[1] for col in cur.fetchall()]
            
            if not columns:
                messagebox.showwarning("Warning", f"Table '{table_name}' has no columns!")
                return
                
            sqlite_tree["columns"] = columns
            sqlite_tree["show"] = "headings"

            for col in columns:
                sqlite_tree.heading(col, text=col)
                sqlite_tree.column(col, width=120)

            rows = cur.execute(f"SELECT * FROM {table_name}").fetchall()
            sqlite_cache[table_name] = rows

            for row in rows:
                sqlite_tree.insert("", "end", values=row)

        sqlite_label.config(text=f"SQLite Table: {table_name} ({len(rows)} rows)")
    except Exception as e:
        messagebox.showerror("SQLite Error", str(e))

# ----------------------- MongoDB Operations -----------------------
def get_mongo_collections():
    """Return all collection names from MongoDB."""
    try:
        with NOSql_db("NOSQL_test") as db:
            return db.list_collection_names()
    except:
        return []

def load_mongo_table(collection_name):
    """Load MongoDB collection into TreeView."""
    mongo_tree.delete(*mongo_tree.get_children())
    if not collection_name:
        return

    try:
        with NOSql_db("NOSQL_test") as db:
            collection = db[collection_name]
            mongo_rows = list(collection.find({}, {"_id": 0}))
            mongo_cache[collection_name] = mongo_rows

            if mongo_display_mode == "dict":
                mongo_tree["columns"] = ("Document",)
                mongo_tree["show"] = "headings"
                mongo_tree.heading("Document", text="MongoDB Document")
                mongo_tree.column("Document", width=1000)
                for doc in mongo_rows:
                    mongo_tree.insert("", "end", values=(str(doc),))
            else:
                if mongo_rows:
                    columns = list(mongo_rows[0].keys())
                    mongo_tree["columns"] = columns
                    mongo_tree["show"] = "headings"
                    for col in columns:
                        mongo_tree.heading(col, text=col)
                        mongo_tree.column(col, width=120)
                    for row in mongo_rows:
                        mongo_tree.insert("", "end", values=tuple(row.values()))
                else:
                    mongo_tree["columns"] = []
                    mongo_tree["show"] = "headings"

        mongo_label.config(text=f"MongoDB Collection ({mongo_display_mode} view): {collection_name} ({len(mongo_rows)} docs)")
    except Exception as e:
        messagebox.showerror("MongoDB Error", str(e))

def toggle_mongo_display():
    """Toggle MongoDB display between columns and dict view."""
    global mongo_display_mode
    mongo_display_mode = "dict" if mongo_display_mode == "columns" else "columns"
    collection = mongo_combo.get()
    if collection:
        load_mongo_table(collection)

# ----------------------- Transfer Operations -----------------------
def transfer_sql_to_mongo(table_name=None):
    """Transfer SQLite table(s) to MongoDB."""
    tables_to_transfer = [table_name] if table_name else get_sql_tables()
    
    if not tables_to_transfer:
        messagebox.showwarning("Warning", "No tables found! Initialize database first.")
        return

    try:
        with Sql_db('test.db') as cur:
            transferred_count = 0
            for tbl in tables_to_transfer:
                cur.execute(f"PRAGMA table_info({tbl})")
                columns = [col[1] for col in cur.fetchall()]
                rows = cur.execute(f"SELECT * FROM {tbl}").fetchall()

                documents = [dict(zip(columns, row)) for row in rows]

                if documents:
                    with NOSql_db("NOSQL_test") as db:
                        collection = db[tbl]
                        collection.delete_many({})
                        collection.insert_many(documents)
                    transferred_count += 1

        # Refresh MongoDB combo
        mongo_combo["values"] = get_mongo_collections()
        
        if table_name:
            messagebox.showinfo("Success", f"Table '{table_name}' transferred to MongoDB!")
            load_mongo_table(table_name)
        else:
            messagebox.showinfo("Success", f"{transferred_count} table(s) transferred to MongoDB!")

    except Exception as e:
        messagebox.showerror("Error", str(e))

def transfer_mongo_to_sql(collection_name=None):
    """Transfer MongoDB collection(s) to SQLite."""
    collections = [collection_name] if collection_name else get_mongo_collections()
    
    if not collections or (collection_name and not collection_name):
        messagebox.showwarning("Warning", "No collection selected!")
        return
    
    # Check if tables exist, if not initialize
    existing_tables = get_sql_tables()
    if not existing_tables:
        response = messagebox.askyesno("Initialize Database", 
                                       "SQLite database has no tables.\nDo you want to initialize it now?")
        if response:
            if not initialize_database():
                return
        else:
            return

    try:
        with NOSql_db("NOSQL_test") as db:
            with Sql_db('test.db') as cur:
                transferred_count = 0
                for coll_name in collections:
                    collection = db[coll_name]
                    documents = list(collection.find({}, {"_id": 0}))
                    
                    if not documents:
                        continue
                    
                    transferred_count += 1
                    
                    # Handle Users table
                    if coll_name == "Users":
                        inserted = 0
                        skipped = 0
                        for doc in documents:
                            try:
                                existing = cur.execute(
                                    "SELECT UserID FROM Users WHERE Email = ?",
                                    (doc.get('Email'),)
                                ).fetchone()
                                
                                if not existing:
                                    cur.execute(
                                        "INSERT INTO Users (Name, Email, Age) VALUES (?, ?, ?)",
                                        (doc.get('Name'), doc.get('Email'), doc.get('Age'))
                                    )
                                    inserted += 1
                                else:
                                    skipped += 1
                            except:
                                skipped += 1
                        
                        msg = f"Users: {inserted} added, {skipped} skipped"
                    
                    # Handle Products table
                    elif coll_name == "Products":
                        for doc in documents:
                            cur.execute(
                                "INSERT OR REPLACE INTO Products (ProductID, ProductName, Price, StockQuantity) VALUES (?, ?, ?, ?)",
                                (doc.get('ProductID'), doc.get('ProductName'), doc.get('Price'), doc.get('StockQuantity', 0))
                            )
                        msg = f"Products: {len(documents)} transferred"
                    
                    # Handle Orders table
                    elif coll_name == "Orders":
                        for doc in documents:
                            cur.execute(
                                "INSERT OR REPLACE INTO Orders (OrderID, UserID, OrderDate, TotalAmount) VALUES (?, ?, ?, ?)",
                                (doc.get('OrderID'), doc.get('UserID'), doc.get('OrderDate'), doc.get('TotalAmount'))
                            )
                        msg = f"Orders: {len(documents)} transferred"
                    
                    # Handle Departments table
                    elif coll_name == "Departments":
                        for doc in documents:
                            cur.execute(
                                "INSERT OR IGNORE INTO Departments (DeptID, DeptName) VALUES (?, ?)",
                                (doc.get('DeptID'), doc.get('DeptName'))
                            )
                        msg = f"Departments: {len(documents)} transferred"
        
        # Refresh SQL combo
        table_combo["values"] = get_sql_tables()
        
        if collection_name:
            messagebox.showinfo("Success", f"Collection '{collection_name}' transferred to SQLite!\n{msg}")
            load_sqlite_table(collection_name)
        else:
            messagebox.showinfo("Success", f"{transferred_count} collection(s) transferred to SQLite!")

    except Exception as e:
        messagebox.showerror("Error", str(e))

# ----------------------- Tkinter GUI -----------------------
root = tk.Tk()
root.title("SQLite ↔ MongoDB Bidirectional Transfer")
root.geometry("1400x700")

# ===== SQLite Section =====
sqlite_label = tk.Label(root, text="SQLite Table", font=("Arial", 12, "bold"), fg="blue")
sqlite_label.pack(pady=5)

sqlite_tree = ttk.Treeview(root, height=10)
sqlite_tree.pack(expand=True, fill="both", padx=10, pady=5)

# ===== MongoDB Section =====
mongo_label = tk.Label(root, text="MongoDB Collection", font=("Arial", 12, "bold"), fg="green")
mongo_label.pack(pady=5)

mongo_tree = ttk.Treeview(root, height=10)
mongo_tree.pack(expand=True, fill="both", padx=10, pady=5)

# ===== Control Panel =====
control_frame = tk.Frame(root, bg="#f0f0f0", pady=15)
control_frame.pack(fill="x", padx=10)

# Row 1: SQLite Controls
row1 = tk.Frame(control_frame, bg="#f0f0f0")
row1.pack(fill="x", pady=5)

tk.Label(row1, text="SQLite Table:", bg="#f0f0f0", font=("Arial", 10, "bold")).pack(side="left", padx=5)
table_combo = ttk.Combobox(row1, values=get_sql_tables(), state="readonly", width=25)
table_combo.pack(side="left", padx=5)

tk.Button(row1, text="📋 Load SQLite", width=15, bg="#3498db", fg="white", 
          command=lambda: load_sqlite_table(table_combo.get())).pack(side="left", padx=3)

tk.Button(row1, text="➡️ Transfer to MongoDB", width=20, bg="#27ae60", fg="white",
          command=lambda: transfer_sql_to_mongo(table_combo.get())).pack(side="left", padx=3)

tk.Button(row1, text="➡️➡️ Transfer All to MongoDB", width=22, bg="#16a085", fg="white",
          command=lambda: transfer_sql_to_mongo()).pack(side="left", padx=3)

# Row 2: MongoDB Controls
row2 = tk.Frame(control_frame, bg="#f0f0f0")
row2.pack(fill="x", pady=5)

tk.Label(row2, text="MongoDB Collection:", bg="#f0f0f0", font=("Arial", 10, "bold")).pack(side="left", padx=5)
mongo_combo = ttk.Combobox(row2, values=get_mongo_collections(), state="readonly", width=25)
mongo_combo.pack(side="left", padx=5)

tk.Button(row2, text="📋 Load MongoDB", width=15, bg="#2ecc71", fg="white",
          command=lambda: load_mongo_table(mongo_combo.get())).pack(side="left", padx=3)

tk.Button(row2, text="⬅️ Transfer to SQLite", width=20, bg="#e67e22", fg="white",
          command=lambda: transfer_mongo_to_sql(mongo_combo.get())).pack(side="left", padx=3)

tk.Button(row2, text="⬅️⬅️ Transfer All to SQLite", width=22, bg="#d35400", fg="white",
          command=lambda: transfer_mongo_to_sql()).pack(side="left", padx=3)

tk.Button(row2, text="🔄 Toggle View", width=15, bg="#9b59b6", fg="white",
          command=toggle_mongo_display).pack(side="left", padx=3)

# Row 3: Refresh & Initialize Buttons
row3 = tk.Frame(control_frame, bg="#f0f0f0")
row3.pack(fill="x", pady=5)

tk.Button(row3, text="🔃 Refresh Lists", width=20, bg="#34495e", fg="white",
          command=lambda: [table_combo.config(values=get_sql_tables()), 
                          mongo_combo.config(values=get_mongo_collections())]).pack(side="left", padx=5)

tk.Button(row3, text="🔧 Initialize SQLite DB", width=20, bg="#c0392b", fg="white",
          command=lambda: initialize_database() and messagebox.showinfo("Success", "Database initialized!") 
                         and table_combo.config(values=get_sql_tables())).pack(side="left", padx=5)

# Initialize database on startup
initialize_database()

# Load first table by default
tables = get_sql_tables()
if tables:
    table_combo["values"] = tables
    table_combo.current(0)
    load_sqlite_table(table_combo.get())
else:
    sqlite_label.config(text="SQLite: No tables found - Click 'Initialize SQLite DB'")

collections = get_mongo_collections()
if collections:
    mongo_combo["values"] = collections
    mongo_combo.current(0)
else:
    mongo_label.config(text="MongoDB: No collections found")

root.mainloop()