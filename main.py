import os
import cx_Oracle

from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import re


# SQLAlchemy Base
Base = declarative_base()

lib_dir = "/System/Volumes/Data/Users/erolatik/Desktop/instantclient"

os.environ["LD_LIBRARY_PATH"] = f"{lib_dir}:{os.environ.get('LD_LIBRARY_PATH', '')}"

if os.path.isdir(lib_dir):
    cx_Oracle.init_oracle_client(lib_dir=lib_dir)
    print("Oracle client başarıyla başlatıldı.")
else:
    raise Exception(f"Oracle client directory not found: {lib_dir}")

class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    email = Column(String(255))


# Abstract
class DatabaseConnector:
    def connect(self, jdbc_string):
        raise NotImplementedError("Subclass must implement abstract method")

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


# JDBC bağlantı dizesi
jdbc_postgres = "postgresql://postgres:your_password@localhost:5432/mydatabase"
jdbc_mysql = "mysql://user:password@localhost:3306/mydatabase"
jdbc_mssql = "mssql://sa:YourStrongPassword!@localhost:1433/mydatabase"
jdbc_oracle = "oracle+cx_oracle://sys:MyPassword123@localhost:1521/?service_name=ORCLPDB1&mode=SYSDBA"


# PostgreSQL bağlantısı
postgres_session = ConnectionFactory.get_connector("postgresql", jdbc_postgres)
print(f"PostgreSQL Bağlantısı: {postgres_session}")

# MySQL bağlantısı
mysql_session = ConnectionFactory.get_connector("mysql", jdbc_mysql)
print(f"MySQL Bağlantısı: {mysql_session}")

mssql_session = ConnectionFactory.get_connector("mssql", jdbc_mssql)
print(f"MSSQL Bağlantısı: {mssql_session}")

oracle_session = ConnectionFactory.get_connector("oracle", jdbc_oracle)
print("Oracle Bağlantısı: {oracle_session}")

# PostgreSQL ve MySQL için tablolar
Base.metadata.create_all(postgres_session.get_bind())
Base.metadata.create_all(mysql_session.get_bind())
Base.metadata.create_all(mssql_session.get_bind())
Base.metadata.create_all(oracle_session.get_bind())


# PostgreSQL veritabanından verileri sorgulama
postgres_users = postgres_session.query(User).all()
for user in postgres_users:
    print(f"PostgreSQL Kullanıcı: {user.name} - {user.email}")

# MySQL veritabanından verileri sorgulama
mysql_users = mysql_session.query(User).all()
for user in mysql_users:
    print(f"MySQL Kullanıcı: {user.name} - {user.email}")

mssql_users = mssql_session.query(User).all()
for user in mssql_users:
    print(f"MSSQL Kullanıcı: {user.name} - {user.email}")

oracle_users = oracle_session.query(User).all()
for user in oracle_users:
    print(f"Oracle kullanıcı: {user.name} - {user.email}")

# Oturumları kapatma
postgres_session.close() # psql -U postgres -h localhost -d mydatabase

mysql_session.close() # mysql -u user -p -h localhost -D mydatabase
# password

mssql_session.close() # sqlcmd -S localhost -U sa -P YourStrongPassword! -d mydatabase

oracle_session.close()
