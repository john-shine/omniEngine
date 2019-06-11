import getpass
import sys
import decimal
import psycopg2
import psycopg2.extras
from logger import get_logger

from config import DBPORT, DBUSER, DBPASS, DBHOST, DBNAME

logger = get_logger('sqltools')

con = dbc = None


def sql_connect(OUSER=None, OPASS=None):
    global con

    USER = getpass.getuser()
    try:
        if con is None:
            con = psycopg2.connect(
                database=DBNAME, 
                user=DBUSER, 
                password=DBPASS, 
                host=DBHOST, 
                port=DBPORT
            )

        return con
    except psycopg2.DatabaseError, e:
        logger.exception('Error %s', e)
        sys.exit(1)


def dbInit(ouser=None, opass=None):
    # Prime the DB Connection, it can be restarted in the select/execute statement if it gets closed prematurely.
    global dbc

    if dbc is None or dbc.closed:
        con = sql_connect()
        dbc = con.cursor(cursor_factory=psycopg2.extras.DictCursor)

    return dbc


def dbSelect(statement, values=None):
    dbInit()
    try:
        dbc.execute(statement, values)
        ROWS = dbc.fetchall()
        return ROWS
    except psycopg2.DatabaseError, e:
        logger.exception('Error: %s. Rollback returned: %s', e, dbRollback())
        sys.exit(1)


def dbExecute(statement, values=None):
    dbInit()
    try:
        dbc.execute(statement, values)
    except psycopg2.DatabaseError, e:
        logger.exception('Error: %s. Rollback returned: %s', e, dbRollback())
        sys.exit(1)


def dbUpgradeExecute(ouser, opass, statement, values=None):
    dbInit(ouser, opass)
    try:
        con.set_session(autocommit=True)
        dbc.execute(statement, values)
        con.set_session(autocommit=False)
    except psycopg2.DatabaseError, e:
        logger.exception('Error: %s. Rollback returned: %s', e, dbRollback())


def dbCommit():
    try:
        con.commit()
    except psycopg2.DatabaseError, e:
        logger.exception('Error: %s. Rollback returned: %s', e, dbRollback())
        sys.exit(1)


def dbRollback():
    if con:
        con.rollback()

        return True
    else:
        return False


def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)

    raise TypeError
