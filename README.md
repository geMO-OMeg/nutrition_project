# nutrition_project

# Required Libraries (Ubuntu)
    - sudo apt install python3-pandas
    - sudo apt install python3-sqlalchemy
    - sudo apt install python3-mysqldb

# Description

- databaseSetup.py:
    - Sets up a MySQL database using schema and key definitions from .csv files and populates it with data downloaded from the Canadian Nutrient File database
    - Functionality:
        - Reads table_schema.csv to create tables with the correct column names, data types, and nullable constraints.
        - Reads table_keys.csv to apply primary keys, composite primary keys (KeyType = P) and foreign key constraints referencing other tables and their respective columns (KeyType = F).
        - Recursively scans the cnf-fcen-csv directory for .csv files and inserts the contents into their corresponding tables.

    - Libraries Used:
        - Pandas: To load and parse CSV data.
        - SQLAlchemy: To interface with the MySQL database using high-level operations.
        - mysqldb: establishes a connection to a MySQL database using the MySQLdb driver.

