import os
import pandas as pd
from sqlalchemy import create_engine, text

class CreateDatabase:

    def __init__(self):

        self.data_dir = "/home/bago/NutritionProj/nutrition_project/NutritionTracker/files/cnf-fcen-csv"
        self.schema_file = f"{self.data_dir}/table_schema.csv"
        self.keys_file = f"{self.data_dir}/table_keys.csv"

        self.dbName = "" 
        self.user = ""
        self.password = ""
        self.host = ""
        self.port = ""

        self.temp_engine = None
        self.engine = None

        self.schema_df = None
        self.keys_df = None

        # Mapping shorthand to MySQL data types
        self.type_mapping = {
            "INT": "INT",
            "D": "DATE",
            "T": "TEXT",
            "FLOAT": "FLOAT"
        }

        self.readSchemaKeyFiles()

    def connectToDB(self):
        try:
            # Connect to MYSQL
            self.temp_engine = create_engine(
                f"mysql+mysqldb://{self.user}:{self.password}@{self.host}:{self.port}"
            )
            with self.temp_engine.connect() as conn: 
                conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {self.dbName}"))
                print(f"Database `{self.dbName}` created.")

            self.engine = create_engine(
            f"mysql+mysqldb://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbName}"
            )
        except Exception as err:
            print(f"MySQL connection error: {err}")
            self.temp_engine = None
            self.engine = None 
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

        if self.engine is None:
            print("No DB connecion")
            return
        
        if self.schema_df is not None and self.keys_df is not None:
            tables = self.schema_df["TableName"].unique()

            # Step1: Build CREATE TABLE statements
            for table in tables:
                columns_sql = []
                table_schema = self.schema_df[self.schema_df['TableName'] == table]

                for _, row in table_schema.iterrows():
                    dataType = self.type_mapping.get(row['DataType'])
                    if not dataType:
                        raise(ValueError(f"unknown data type for {row['TableName']}, {row['ColumnName']}, {row['DataType']}"))
                    
                    isNull = "NOT NULL" if row['IsNullable'] == "F" else ""
                    print(f"appending: `{row['ColumnName']}` {dataType} {isNull}")
                    columns_sql.append(f"`{row['ColumnName']}` {dataType} {isNull}") 
                    
                if columns_sql:
                    create_sql = f"CREATE TABLE IF NOT EXISTS `{table}` ({', '.join(columns_sql)});"
                    try:
                        with self.engine.begin() as conn:
                            conn.execute(text(create_sql))
                        print(f"Created table: {table}.")
                    except Exception as err:
                        print(f"Error creating table: {create_sql} because: {err}")

    def addKeys(self):

        if self.engine is None:
            print("No DB connecion")
            return
            
        
        with self.engine.begin() as conn:
            # Primary keys
            for table_name, group in self.keys_df.groupby("TableName"):
                pk_columns = group[(group["KeyType"] == "P")]["ColumnName"].tolist()
                if pk_columns:
                    cols_formatted = ", ".join(f"`{col}`" for col in pk_columns)
                
                    try:
                        conn.execute(text(f"ALTER TABLE `{table_name}` ADD PRIMARY KEY ({cols_formatted});"))
                        print(f"Primary key: {cols_formatted} added to: {table_name}.")
                    except Exception as err:
                        print(f"Error altering primary key {cols_formatted} for {table_name} because: {err}")

            # Foreign keys
            for _, row in self.keys_df.iterrows():
                if row["KeyType"] == "F":
                    fk_sql = f"""
                        ALTER TABLE `{row['TableName']}`
                        ADD FOREIGN KEY (`{row['ColumnName']}`)
                        REFERENCES `{row['ReferenceTable']}`(`{row['ReferenceColumn']}`);
                    """
                    try:
                        conn.execute(text(fk_sql))
                        print(f"Foreigh key: {fk_sql} added")
                    except Exception as err:
                        print(f"Error altering foreign keys {fk_sql} because: {err}")


    def populateTables(self):

        if self.engine is None:
            print("No DB connecion")
            return

        # Step 3: Populate Tables
        for root, _, files in os.walk(self.data_dir):
            for file in files:
                if file.endswith(".csv") and file not in [os.path.basename(self.schema_file), os.path.basename(self.keys_file)]:
                    table_name = os.path.splitext(file)[0] 
                    file_path = os.path.join(root, file)
                    try:
                        df = pd.read_csv(file_path)
                        print(f"Loading `{file}` into `{table_name}` ({len(df)} rows)...")

                        if df.empty:
                            print(f"Skipping empty file: {file}")
                            continue

                        df.to_sql(table_name, con=self.engine, if_exists="append", index=False)
                        print(f"✅ Inserted {len(df)} rows into {table_name}.")

                    except Exception as err:
                        print(f"Failed {file} because: {err}")

    def run(self):
        self.connectToDB()
        self.createTables()
        self.populateTables()
        self.addKeys()
        print("Database setup complete")
       

if __name__ == "__main__":
    app = CreateDatabase()
    app.run()
    