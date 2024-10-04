import os
import cx_Oracle
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import re
from dotenv import load_dotenv

load_dotenv()

# SQLAlchemy Base
Base = declarative_base()

# Oracle Instant Client location
lib_dir = os.getenv("ORACLE_CLIENT_LIB_DIR")

os.environ["LD_LIBRARY_PATH"] = f"{lib_dir}:{os.environ.get('LD_LIBRARY_PATH', '')}"

# Oracle Client start
if os.path.isdir(lib_dir):
    cx_Oracle.init_oracle_client(lib_dir=lib_dir)
    print("Oracle client started.")
else:
    raise Exception(f"Oracle client directory not found: {lib_dir}")

# User model
class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    email = Column(String(255))


# Abstract class
class DatabaseConnector:
    def connect(self, jdbc_string):
        raise NotImplementedError("Subclass must implement abstract method")


# Oracle Connector
class OracleConnector(DatabaseConnector):
    def connect(self, jdbc_string):
        pattern = r"oracle\+cx_oracle://(\w+):(\w+)@([\w.]+):(\d+)/(?:\?service_name=)?([\w]+)"
        match = re.match(pattern, jdbc_string)
        if match:
            username, password, host, port, service_name = match.groups()
            db_url = f"oracle+cx_oracle://{username}:{password}@{host}:{port}/?service_name={service_name}"
            print(f"Bağlantı dizesi: {db_url}")
            engine = create_engine(
                db_url,
                connect_args={"mode": cx_Oracle.SYSDBA}
            )
            Session = sessionmaker(bind=engine)
            return Session()
        else:
            raise ValueError("Invalid Oracle JDBC string format")


# MSSQL Connector
class MSSQLConnector(DatabaseConnector):
    def connect(self, jdbc_string):
        pattern = r"mssql://([\w\d]+):([^@]+)@([\w.]+):(\d+)/([\w\d]+)"
        match = re.match(pattern, jdbc_string)
        if match:
            username, password, host, port, dbname = match.groups()
            db_url = f"mssql+pyodbc://{username}:{password}@{host}:{port}/{dbname}?driver=ODBC+Driver+17+for+SQL+Server"
            print(f"Bağlantı dizesi: {db_url}")
            engine = create_engine(db_url)
            Session = sessionmaker(bind=engine)
            return Session()
        else:
            raise ValueError("Invalid MSSQL JDBC string format")


# PostgreSQL Connector
class PostgresConnector(DatabaseConnector):
    def connect(self, jdbc_string):
        pattern = r"postgresql://(\w+):(\w+)@([\w.]+):(\d+)/(\w+)"
        match = re.match(pattern, jdbc_string)
        if match:
            username, password, host, port, dbname = match.groups()
            db_url = f"postgresql://{username}:{password}@{host}:{port}/{dbname}"
            print(f"Bağlantı dizesi: {db_url}")
            engine = create_engine(db_url)
            Session = sessionmaker(bind=engine)
            return Session()
        else:
            raise ValueError("Invalid PostgreSQL JDBC string format")


# MySQL Connector
class MySQLConnector(DatabaseConnector):
    def connect(self, jdbc_string):
        pattern = r"mysql://(\w+):(\w+)@([\w.]+):(\d+)/(\w+)"
        match = re.match(pattern, jdbc_string)
        if match:
            username, password, host, port, dbname = match.groups()
            db_url = f"mysql://{username}:{password}@{host}:{port}/{dbname}"
            print(f"Bağlantı dizesi: {db_url}")
            engine = create_engine(db_url)
            Session = sessionmaker(bind=engine)
            return Session()
        else:
            raise ValueError("Invalid MySQL JDBC string format")


# ConnectionFactory sınıfı
class ConnectionFactory:
    @staticmethod
    def get_connector(db_type, jdbc_string):
        if db_type == "postgresql":
            return PostgresConnector().connect(jdbc_string)
        elif db_type == "mysql":
            return MySQLConnector().connect(jdbc_string)
        elif db_type == "mssql":
            return MSSQLConnector().connect(jdbc_string)
        elif db_type == "oracle":
            return OracleConnector().connect(jdbc_string)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")


# JDBC connection - .env
jdbc_postgres = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
jdbc_mysql = f"mysql://{os.getenv('MYSQL_USER')}:{os.getenv('MYSQL_PASSWORD')}@{os.getenv('MYSQL_HOST')}:{os.getenv('MYSQL_PORT')}/{os.getenv('MYSQL_DB')}"
jdbc_mssql = f"mssql://{os.getenv('MSSQL_USER')}:{os.getenv('MSSQL_PASSWORD')}@{os.getenv('MSSQL_HOST')}:{os.getenv('MSSQL_PORT')}/{os.getenv('MSSQL_DB')}"
jdbc_oracle = f"oracle+cx_oracle://{os.getenv('ORACLE_USER')}:{os.getenv('ORACLE_PASSWORD')}@{os.getenv('ORACLE_HOST')}:{os.getenv('ORACLE_PORT')}/?service_name={os.getenv('ORACLE_SERVICE_NAME')}"


# PostgreSQL connection
postgres_session = ConnectionFactory.get_connector("postgresql", jdbc_postgres)
print(f"PostgreSQL Connection: {postgres_session}")

# MySQL connection
mysql_session = ConnectionFactory.get_connector("mysql", jdbc_mysql)
print(f"MySQL Connection: {mysql_session}")

# MSSQL connection
mssql_session = ConnectionFactory.get_connector("mssql", jdbc_mssql)
print(f"MSSQL Connection: {mssql_session}")

# Oracle connection
oracle_session = ConnectionFactory.get_connector("oracle", jdbc_oracle)
print(f"Oracle Connection: {oracle_session}")

# Database tables
Base.metadata.create_all(postgres_session.get_bind())
Base.metadata.create_all(mysql_session.get_bind())
Base.metadata.create_all(mssql_session.get_bind())
Base.metadata.create_all(oracle_session.get_bind())

# Query
postgres_users = postgres_session.query(User).all()
for user in postgres_users:
    print(f"PostgreSQL user: {user.name} - {user.email}")

mysql_users = mysql_session.query(User).all()
for user in mysql_users:
    print(f"MySQL user: {user.name} - {user.email}")

mssql_users = mssql_session.query(User).all()
for user in mssql_users:
    print(f"MSSQL user: {user.name} - {user.email}")

oracle_users = oracle_session.query(User).all()
for user in oracle_users:
    print(f"Oracle user: {user.name} - {user.email}")

postgres_session.close()
mysql_session.close()
mssql_session.close()
oracle_session.close()
