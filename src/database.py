import mysql.connector


class Database:
    def __init__(self, host, user, password):
        self.host = host
        self.user = user
        self.password = password
        self.curs = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password
        )
        self.cursor = self.curs.cursor()

    def show_db(self):
        # Returns all databases in current mysql instance
        query_obj = f'SHOW DATABASES'
        self.cursor.execute(query_obj)
        dbs = []
        for res in self.cursor:
            dbs.append(res[0])
        return dbs

    def show_tb(self):
        # Returns all table in the database
        query_obj = f'SHOW TABLES'
        self.cursor.execute(query_obj)
        tbs = []
        for res in self.cursor:
            tbs.append(res[0])
        return tbs

    def desc_tb(self, table):
        # Describes a database table
        self.cursor.execute(f"DESC {table}")
        for col in self.cursor:
            print('='*len(col))
            for i in col:
                print(i, end='|')
            print('='*len(col))

    def create_db(self, dbname):
        # Create or Use specified database
        query_obj = f'CREATE DATABASE IF NOT EXISTS {dbname}'
        try:
            self.cursor.execute(query_obj)
        except mysql.connector.Error as err:
            print(f"An error occurred: {err}")
        finally:
            query = f'USE {dbname}'
            self.cursor.execute(query)
            print(f"Now using Database: {dbname}")

    def create_tb(self, table_name, *args):     # Creates a table in the database
        columns = ", ".join(args)
        create_query = f'CREATE TABLE {table_name} ({columns})'
        
        try:
            self.cursor.execute(create_query)
            print("Table created successfully")
        except mysql.connector.Error as err:
            print(f"Error: {err}")

    def insert(self, table_name, **kwargs):     # Insert data into the table
        columns = ', '.join(kwargs.keys())  # Uses keys from the kwargs as columns to insert data into
        values = ', '.join(['%s'] * len(kwargs))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"

        try:
            data = tuple(kwargs.values())  # Gets the kwargs values
            self.cursor.execute(insert_query, data)
    
            print("Data inserted successfully")
        except mysql.connector.Error as err:
            print(f"Error: {err}")

    def retrieve(self, table_name, *args, **kwargs):    # Retrieves an item from the database
        columns = ', '.join(args)
        col = ', '.join(kwargs.keys())
        value = ', '.join(kwargs.values())

        if kwargs:
            query = f"SELECT {columns} FROM {table_name} WHERE {col} = '{value}'"
        else:
            query = f"SELECT {columns} FROM {table_name}"

        self.cursor.execute(query)
        obj = [i for i in self.cursor]
        return obj

    def search(self, search_query):     # Searches the database for words and return the documents that contains them
        query = f'''SELECT Documents.path FROM Documents
        JOIN InvertedIndex ON Documents.id=InvertedIndex.doc_id
        JOIN Words ON InvertedIndex.word_id=Words.id
        WHERE Words.term LIKE '%{search_query}%'
        '''
        self.cursor.execute(query)
        documents = [doc for doc in self.cursor]
        return search_query, documents

    def commit(self):
        self.curs.commit()
