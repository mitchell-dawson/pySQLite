import sqlite3
from sqlite3 import Error
import pdb


def create_connection(db_file):
    """create a database connection to a SQLite database:
    @param db_file: the path to an SQLite .db file
    """
    try:
        conn = sqlite3.connect(db_file)
        print 'connected to SQLite v%s' %sqlite3.version
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    @param conn: Connection object
    @param create_table_sql: a CREATE TABLE statement
    - see example create_database.py script
    """

    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def insert_ignore(conn, table, column_tuple, value_tuple, id_table):

    """ perform an 'insert ignore' insertion into database
    @param conn: Connection object - see create_connection function above
    @param table: string, name of table to insert data into
    @param column_tuple: tuple of strings, names of columns to insert into
        @example column_tuple = ('dog_name', 'dog_breed')
        @example column_tuple = ('dog_name', )
    @param value_tuple: tuple, data to be inserted into database
        @example value_tuple = ('fido','doberman')
        @example value_tuple = ('fido', )
    @param id_table: string, primary key column name of table
    @return last_row_id: primary key id of row with this data
    """

    if len(column_tuple) == 1:

        sql = ("INSERT OR IGNORE INTO %s (%s) VALUES(" % (table, column_tuple[0]) +
               ','.join(['?'] * len(value_tuple)) + ");")

    else:
        sql = ("INSERT OR IGNORE INTO %s %s VALUES(" % (table, column_tuple) +
               ','.join(['?'] * len(value_tuple)) + ");")

    conn.execute(sql, value_tuple)

    # find the ID of the insertion
    where_clause = ' AND '.join(["%s=?"]*len(column_tuple)) % column_tuple

    sql = "SELECT %s FROM %s WHERE (" % (id_table, table) + where_clause + ");"

    last_row_id = int(conn.execute(sql, value_tuple).fetchone()[0])

    return last_row_id


def insert_ignore_sql(table, column_tuple, value_tuple, id_table):

    """ perform an 'insert ignore' insertion into database
    @param conn: Connection object - see create_connection function above
    @param table: string, name of table to insert data into
    @param column_tuple: tuple of strings, names of columns to insert into
        @example column_tuple = ('dog_name', 'dog_breed')
        @example column_tuple = ('dog_name', )
    @param value_tuple: tuple, data to be inserted into database
        @example value_tuple = ('fido','doberman')
        @example value_tuple = ('fido', )
    @param id_table: string, primary key column name of table
    @return last_row_id: primary key id of row with this data
    """

    if len(column_tuple) == 1:

        sql = ("INSERT OR IGNORE INTO %s (%s) VALUES(" % (table, column_tuple[0]) +
               ','.join(['?'] * len(value_tuple)) + ");")

    else:
        sql = ("INSERT OR IGNORE INTO %s %s VALUES(" % (table, column_tuple) +
               ','.join(['?'] * len(value_tuple)) + ");")

    return (sql, value_tuple)


def update(conn, table, column_tuple, value_tuple, id_table, id_num):

    """ perform an 'update' insertion on a single row into database
    @param conn: Connection object - see create_connection function above
    @param table: string, name of table to update data in
    @param column_tuple: tuple of strings, names of columns to update
        @example column_tuple = ('dog_name', 'dog_breed')  
        @example column_tuple = ('dog_name', )
    @param value_tuple: tuple, data to be updated in database
        @example value_tuple = ('fido','doberman')
        @example value_tuple = ('fido', )
    @param id_table: string, primary key column name of table   
    @param id_num: value of primary key in row we wish to update
    """

    if len(column_tuple) == 1:

        sql = "UPDATE %s SET %s=? WHERE %s = %s" % (
            table, column_tuple[0], id_table, id_num)

    else:

        sql = "UPDATE %s SET " % table

        for column in column_tuple:

            sql += (column + " = ?, ")

        sql = sql[:-2]

        sql += (" WHERE %s=%s" % (id_table, id_num))

    conn.execute(sql, value_tuple)


def update_sql(table, column_tuple, value_tuple, id_table, id_num):

    """ perform an 'update' insertion on a single row into database
    @param conn: Connection object - see create_connection function above
    @param table: string, name of table to update data in
    @param column_tuple: tuple of strings, names of columns to update
        @example column_tuple = ('dog_name', 'dog_breed')
        @example column_tuple = ('dog_name', )
    @param value_tuple: tuple, data to be updated in database
        @example value_tuple = ('fido','doberman')
        @example value_tuple = ('fido', )
    @param id_table: string, primary key column name of table
    @param id_num: value of primary key in row we wish to update
    """

    if len(column_tuple) == 1:

        sql = "UPDATE %s SET %s=? WHERE %s = %s" % (
            table, column_tuple[0], id_table, id_num)

    else:

        sql = "UPDATE %s SET " % table

        for column in column_tuple:

            sql += (column + " = ?, ")

        sql = sql[:-2]

        sql += (" WHERE %s=%s" % (id_table, id_num))

    return (sql, value_tuple)


def select(conn, sql):

    """ perform a SELECT query and return rows of results
    @param conn: connection object
    @param sql: SQL SELECT statement
        @example "SELECT dog_name from dogs
    """

    results = []

    for row in conn.execute(sql):
        results.append(row)

    return results


def insert_many_rows(conn, table, column_tuple, value_tuple):

    """
    @param conn: Connection object - see create_connection function above
    @param table: string, name of table to update data in
    @param column_tuple: tuple of strings, names of columns to update
        @example column_tuple = ('dog_name', 'dog_breed')
        @example column_tuple = ('dog_name', )
    @param value_tuple: tuple of tuples, data to be updated in database
        @example value_tuple = (('fido','doberman'), ('rover','pitbull'))
        @example value_tuple = (('fido',), ('rover',))
    @note: can only insert 1000 pieces of information at a time
    """

    if len(column_tuple) == 1:

        sql = "INSERT INTO %s (%s) VALUES " % (table, column_tuple[0])

        sql += len(value_tuple)*'(?), '

        sql = sql[:-2] + ';'

    else:

        sql = "INSERT INTO %s %s VALUES " % (table, column_tuple)

        sql += len(value_tuple) * (
                '(' + ((len(value_tuple[0])*'?, ')[:-2]) + '), ')

        sql = sql[:-2] + ';'

        # string out the valu_tuple
        value_tuple = [element for tupl in value_tuple for element in tupl]

    conn.execute(sql, value_tuple)


def insert_many_rows_sql(table, column_tuple, value_tuple):

    """
    @param conn: Connection object - see create_connection function above
    @param table: string, name of table to update data in
    @param column_tuple: tuple of strings, names of columns to update
        @example column_tuple = ('dog_name', 'dog_breed')
        @example column_tuple = ('dog_name', )
    @param value_tuple: tuple of tuples, data to be updated in database
        @example value_tuple = (('fido','doberman'), ('rover','pitbull'))
        @example value_tuple = (('fido',), ('rover',))
    @note: can only insert 1000 pieces of information at a time
    """

    if len(column_tuple) == 1:

        sql = "INSERT INTO %s (%s) VALUES " % (table, column_tuple[0])

        sql += len(value_tuple)*'(?), '

        sql = sql[:-2] + ';'

    else:

        sql = "INSERT INTO %s %s VALUES " % (table, column_tuple)

        sql += len(value_tuple) * (
                '(' + ((len(value_tuple[0])*'?, ')[:-2]) + '), ')

        sql = sql[:-2] + ';'

        # string out the valu_tuple
        value_tuple = [element for tupl in value_tuple for element in tupl]

    return (sql, value_tuple)


def add_column(conn, table_name, column_name, column_type):
    """
    @partasm
    """
    sql = "ALTER TABLE %s ADD COLUMN %s %s" % (
        table_name, column_name, column_type)

    conn.execute(sql)


def add_column_sql(table_name, column_name, column_type):
    """
    @partasm
    """
    sql = "ALTER TABLE %s ADD COLUMN %s %s" % (
        table_name, column_name, column_type)

    return sql
