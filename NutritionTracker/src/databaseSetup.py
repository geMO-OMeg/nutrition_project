import os
import pandas as pd
import mysql.connector
import ssl
#from sqlalchemy import create_engine, text

class CreateDatabase:

    def __init__(self):

        self.data_dir = "/home/bago/NutritionProj/NutritionTracker/cnf-fcen-csv"
        self.schema_file = f"{self.data_dir}/table_schema.csv"
        self.keys_file = f"{self.data_dir}/table_keys.csv"

        self.dbName = "nutrition_db" 
        self.user = ""
        self.password = ""
        self.host = "localhost"

        self.conn = None
        self.cursor = None

        self.schema_df = None
        self.keys_df = None
        self.readSchemaKeyFiles()

        # Mapping shorthand to MySQL data types
        self.type_mapping = {
            "INT": "INT",
            "D": "DATE",
            "T": "TEXT",
            "FLOAT": "FLOAT"
        }

    def connectToDB(self):
        try:
            # Connect to MYSQL
            self.conn = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password
            )
            self.cursor = self.conn.cursor()

        except mysql.connector.Error as err:
            print(f"MySQL connection error: {err}")
            self.closeConnection()  # To avoid using an invalid connection

        if self.conn:
            self.createDB()
                

    def createDB(self):
        # Create db 
        try:
            self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.dbName}")
            self.cursor.execute(f"USE {self.dbName}")
            print("db created successfully.")
            self.createTables()
        except Exception as err:
            print(f"db creation failed: {err}")
            self.closeConnection()
        


    def closeConnection(self):
        if self.cursor:
            self.cursor.close()
            self.cursor = None  
        if self.conn:
            self.conn.close()
            self.conn = None


    def readSchemaKeyFiles(self):
        # Read schema
        try:
            self.schema_df = pd.read_csv(self.schema_file)
        except Exception as ex:
            print(f"Error reading table_schema.csv because: {ex}")
            
        # Read keys
        try:
            self.keys_df = pd.read_csv(self.keys_file)
        except Exception as ex:
            print(f"Error reading table_keys.csv because: {ex}")


    def createTables(self):

        if self.schema_df is not None and self.keys_df is not None:
            tables = self.schema_df["TableName"].unique()

            # Step1: Build CREATE TABLE statements
            for table in tables:
                columns_sql = []
                for _, row in group.iterrows():
                    dataType = self.type_mapping.get(row['DataType'].strip().upper())
                    if not dataType:
                        raise(ValueError(f"unknown data type for {row['TableName']}, {row['ColumnName']}, {row['DataType']}"))
                    
                    isNull = "NOT NULL" if row['IsNullable'].strip().upper() == "F" else ""
                    print(f"appending: `{row['ColumnName']}` {dataType} {isNull}")
                    columns_sql.append(f"`{row['ColumnName']}` {dataType} {isNull}") 
                    
                if columns_sql:
                    create_sql = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(columns_sql)});"
                    try:
                        self.cursor.execute(create_sql)
                        print(f"Created table `{table_name}` (if not existed).")
                    except mysql.connector.Error as err:
                        print(f"Error creating table `{create_sql}`: {err}")
            
            # Step 2: Add Keys
            # Primary keys
            for table_name, group in self.keys_df.groupby("TableName"):
                pk_columns = group[(group["KeyType"].strip().upper() == "P")]["ColumnName"].tolist()
                if pk_columns:
                    cols_formatted = ", ".join(f"`{col}`" for col in pk_columns)
                    pk_sql = f"ALTER TABLE `{table_name}` ADD PRIMARY KEY ({cols_formatted});"
                    try:
                        self.cursor.execute(pk_sql)
                        print(f"Primary key added to `{table_name}`.")
                    except mysql.connector.Error as err:
                        print(f"Error altering primary keys`{pk_sql}`: {err}")

            # Foreign keys
            for _, row in self.keys_df.iterrows():
                if row["KeyType"].strip().upper() == "F":
                    fk_sql = (
                        f"ALTER TABLE `{row['TableName']}` "
                        f"ADD FOREIGN KEY (`{row['ColumnName']}`) "
                        f"REFERENCES `{row['ReferenceTable']}`(`{row['ReferenceColumn']}`);"
                    )
                    try:
                        self.cursor.execute(fk_sql)
                    except mysql.connector.Error as err:
                        print(f"Error altering foreign keys `{fk_sql}`: {err}")

            try:
                self.conn.commit()
                print("All keys committed successfully.")
            except mysql.connector.Error as err:
                print(f"Commit failed: {err}")

            # Step 3: Populate Tables
            for root, _, files in os.walk(self.data_dir):
                for file in files:
                    if file.endswith(".csv") and file not in [self.schema_file, self.keys_file]:
                        table_name = os.path.splitext(file)[0] 
                        file_path = os.path.join(root, file)
                        try:
                            df = pd.read_csv(file_path)
                            print(f"📥 Loading `{file}` into `{table_name}` ({len(df)} rows)...")

                            if df.empty:
                                print(f"⚠️ Skipping empty file: {file}")
                                continue

                            # Build INSERT statement
                            cols = ", ".join(f"`{col}`" for col in df.columns)
                            placeholders = ", ".join(["%s"] * len(df.columns))
                            insert_sql = f"INSERT INTO `{table_name}` ({cols}) VALUES ({placeholders})"

                            # Convert DataFrame rows to list of tuples
                            data = [tuple(row) for row in df.itertuples(index=False)]

                            self.cursor.executemany(insert_sql, data)
                            print(f"✅ Inserted {self.cursor.rowcount} rows into `{table_name}`.")

                        except Exception as e:
                            print(f"❌ Failed to load `{file}`: {e}")

        self.closeConnection()
        print("Database setup complete")

if __name__ == "__main__":
    app = CreateDatabase()
    app.connectToDB()