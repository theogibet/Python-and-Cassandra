from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from datetime import date
import sys
import re

KEYSPACE = "testkeyspace"

############# Cassandra ##################
def init_cluster():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    return cluster
##########################################


############ Calcul âge / nombre d'hommes / femmes ###############
def parser_age(cluster, session):
    today = date.today()
    prepared = session.prepare("""INSERT INTO Indicateurs (Sexe, Nombre, AgeMoyen) VALUES (?, ?, ?)""")
    future = session.execute_async("SELECT Sexe, DateDeNaissance FROM Clients")
    a = b = c = d = 0
    try:
        rows = future.result()
        for row in rows:
            if (row[0] == "H"):
                a = a + 1
                dateofbirth = row[1].split("-")
                agehomme = (today.year - int(dateofbirth[0]) - ((today.month, today.day) < (int(dateofbirth[1]), int(dateofbirth[2]))))
                d = d + agehomme
            elif (row[0] == "F"):
                b = b + 1
                dateofbirth = row[1].split("-")
                agefemme = (today.year - int(dateofbirth[0]) - ((today.month, today.day) < (int(dateofbirth[1]), int(dateofbirth[2]))))
                c = c + agefemme
            else:
                print("error")
    except Exception:
        log.exeception()

    c = c / b
    d = d / a
    print("Nombre d'Hommes -> ", a, "\nAge moyen Hommes ->", c)
    print("Nombre de Femmes -> ", b, "\nAge moyen Femmes ->", d)
    session.execute(prepared.bind(("Femme", b, c)))
    session.execute(prepared.bind(("Homme", a, d)))

####################################################################

##### Pour afficher le contenu de la base de donnée Indicateurs #####

    # future = session.execute_async("SELECT * FROM Indicateurs")
    # try:
    #     rows = future.result()
    #     for row in rows:
    #         print(row)
    # except Exception:
    #     log.exeception()

##################################################################
    return True


def data_analysis(cluster):
    session = cluster.connect()
    session.execute("""DROP TABLE IF EXISTS testkeyspace.Indicateurs""")
    session.execute("""CREATE KEYSPACE IF NOT EXISTS %s WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }""" % KEYSPACE)
    session.set_keyspace(KEYSPACE)
    session.execute("""CREATE TABLE IF NOT EXISTS Indicateurs ( Sexe  text, Nombre int, AgeMoyen float, PRIMARY KEY (Sexe)  ) """)
    future = session.execute_async("SELECT * FROM Indicateurs")
    try:
        rows = future.result()
        for row in rows:
            print(row)
    except Exception:
        log.exeception()
    parser_age(cluster, session)
    return True
############################################

##### Pour afficher le contenu de la base de donnée clients #####

     # future = session.execute_async("SELECT * FROM Clients")
     # try:
     #     rows = future.result()
     #     for row in rows:
     #         print(row)
     # except Exception:
     #     log.exeception()

##################################################################
    return True



def main():
    cluster2 = init_cluster()
    data_analysis(cluster2)
    cluster2.shutdown()
    return True
if __name__ == "__main__":
    main()
