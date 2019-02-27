from flask import Flask
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

KEYSPACE = "testkeyspace"
VERSION = "0.0.1"

app = Flask(__name__)

@app.route('/')
def index():
    return 'Nothing to see here, try /kpis, /clients, /version'

@app.route('/version')
def version():
    return VERSION

@app.route('/kpis')
def kpis():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    session.execute("""CREATE KEYSPACE IF NOT EXISTS %s WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }""" % KEYSPACE)
    session.set_keyspace(KEYSPACE)
    future = session.execute_async("SELECT json * FROM Indicateurs")
    rows = []
    json = ""
    try:
        rows = future.result()
        for row in rows:
            json = json + row[0]
    except Exception:
        print("Error")
    return json

@app.route('/clients')
def clients():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    session.execute("""CREATE KEYSPACE IF NOT EXISTS %s WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }""" % KEYSPACE)
    session.set_keyspace(KEYSPACE)
    future = session.execute_async("SELECT json * FROM Clients LIMIT 500")
    rows = []
    json = ""
    try:
        rows = future.result()
        for row in rows:
            json = json + row[0]
    except Exception:
        print("Error")
    return json
