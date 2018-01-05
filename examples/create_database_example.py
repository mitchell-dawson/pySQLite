import pySQLite_fun as SQL


#########################
# create the database ###
#########################

database = "./dog_example.db"

# create a database connection
conn = SQL.create_connection(database)

#######################################
# create the tables of the database ###
#######################################

create_table_sql = []

create_table_sql.append(""" CREATE TABLE IF NOT EXISTS dogs (
    id_dog              integer     PRIMARY KEY,
    dog_name            text        UNIQUE,
    id_dog_owner        integer,
    age_of_dog          integer     DEFAULT 0
); """)

create_table_sql.append(""" CREATE TABLE IF NOT EXISTS dog_owners (
    id_dog_owner    integer     PRIMARY KEY,
    owner_forename  text,
    owner_surname   text
); """)

# create database tables ###

if conn is not None:

    for sql in create_table_sql:

        SQL.create_table(conn, sql)

    print "%s has been created" % database

else:
    print("Error! cannot create the database connection.")
