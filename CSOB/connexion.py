import mysql.connector as msc

class Connexion:
    __user = "root"
    __password = "root"
    __host = "localhost"
    __port = "8081"
    __database = "vente_jeux" # None
    __cursor = None

    @classmethod
    def creation(cls, db_name, sql_file):
        if not(cls.__cursor):
            cls.__bdd = msc.connect(user = cls.__user, password = cls.__password, host = cls.__host, port = cls.__port, database = cls.__database)
            cls.__cursor = cls.__bdd.cursor()
            cls.__database = db_name
            # f = open(sql_file, 'r').read()
            # cls.__cursor.execute(f, multi=True)
        
            with open(sql_file, 'r') as sql_file:
                result_iterator = cls.__cursor.execute(sql_file.read(), multi=True)
                for res in result_iterator:
                    print("Running query: ", res)  # Will print out a short representation of the query
                    print(f"Affected {res.rowcount} rows" )

                cls.__bdd.commit()  # Remember to commit all your changes!
                cls.fermer()
        else:
            print(cls.__database)
            print()       
            

    @classmethod
    def ouvrir(cls):
        if cls.__cursor == None:
            cls.__bdd = msc.connect(user = cls.__user, password = cls.__password, host = cls.__host, port = cls.__port, database = cls.__database, allow_local_infile = True)
            cls.__cursor = cls.__bdd.cursor()


    @classmethod
    def execute(cls, query):
        cls.__cursor.execute(query)
        if query.split(" ")[0] != "SELECT":
            cls.__bdd.commit()
        
        else:
            return cls.__cursor.fetchall()

    @classmethod
    def execute_file(cls, sql_file):
        # f = open(file, 'r').read()
        # cls.__cursor.execute(f, multi=True)
        # cls.__bdd.commit()
        with open(sql_file, 'r') as sql_file:
            result_iterator = cls.__cursor.execute(sql_file.read(), multi=True)
            for res in result_iterator:
                print("Running query: ", res)  # Will print out a short representation of the query
                print(f"Affected {res.rowcount} rows" )

            cls.__bdd.commit()  # Remember to commit all your changes!

    @classmethod
    def fermer(cls):
        cls.__cursor.close()
        cls.__bdd.close()
        cls.__cursor = None
    
    @classmethod
    def drop_db(cls, database = None):
        if cls.__database:
            query = f"DROP DATABASE IF EXISTS {cls.__database}"
            cls.__cursor.execute(query)
            cls.__database = None
            cls.__bdd.commit()
        elif database:
            query = f"DROP DATABASE IF EXISTS {database}"
            cls.__cursor.execute(query)
            cls.__database = None
            cls.__bdd.commit()
        else:
            print("Erreur : Pas de nom de base de données précisé !")

    @classmethod
    def import_csv(cls, csv_name, table_cible):
        query = f"LOAD DATA LOCAL INFILE '{csv_name}' INTO TABLE {table_cible} FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\n' IGNORE 1 ROWS; "

        cls.__cursor.execute(query)
        cls.__bdd.commit()

    @classmethod
    def vente_EU(cls, jeu):
    
        cls.__cursor.callproc('vente_EU',[jeu,])

        for i in cls.__cursor.stored_results():
            print(i.fetchall())