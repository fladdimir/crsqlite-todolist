from sqlite3 import Connection

from sqlalchemy import Engine, create_engine
from sqlalchemy.event import listen


def set_foreign_keys_pragma(dbapi_connection: Connection, connection_record):
    cursor = dbapi_connection.cursor()
    # cursor.execute("PRAGMA foreign_keys = ON")
    cursor.execute("PRAGMA foreign_keys = OFF")
    # crsqlite is not supporting foreign keys
    cursor.close()
    print("foreign_keys pragma set")


def load_crsqlite_extension(dbapi_connection: Connection, connection_record) -> None:
    dbapi_connection.enable_load_extension(True)
    dbapi_connection.load_extension("./crsqlite/crsqlite.so")
    dbapi_connection.enable_load_extension(False)
    print("crsqlite extension loaded")


def finalize_crsqlite(dbapi_connection: Connection, connection_record) -> None:
    cursor = dbapi_connection.cursor()
    cursor.execute("SELECT crsql_finalize();")
    cursor.close()


def get_engine(db_file: str = "./sqlite_test.db", echo=True) -> Engine:
    engine: Engine = create_engine("sqlite:///" + db_file, echo=echo)
    listen(engine, "connect", load_crsqlite_extension)
    listen(engine, "close", finalize_crsqlite)
    listen(engine, "close_detached", finalize_crsqlite)
    # listen(engine, "connect", set_foreign_keys_pragma)
    return engine
