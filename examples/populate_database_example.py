import pySQLite_fun as SQLite

#######################
# connect to database #
#######################
database = "./dog_example.db"

conn = SQLite.create_connection(database)

########################
# populate the table ###
########################

# add some owners to table
value_tuple = (('John', 'Smith'), ('James', 'Williams'),
               ('Christopher', 'Jones'))

column_tuple = ('owner_forename', 'owner_surname')

SQLite.insert_many_rows(conn, 'dog_owners', column_tuple, value_tuple)


# add some dogs to table
value_tuple = (('fido', 1, 3), ('rover', 2, 5), ('buddy', 2, 3),
               ('lucky', 3, 2))

column_tuple = ('dog_name', 'id_dog_owner', 'age_of_dog')

SQLite.insert_many_rows(conn, 'dogs', column_tuple, value_tuple)

conn.commit()


###############################
# change the owner of a dog ###
###############################

# Match rover with John Smith


# use a SELECT statament to find dog and owner primary key ids
sql = """SELECT id_dog_owner
FROM dog_owners
WHERE (owner_forename = 'John'
AND owner_surname = 'Smith');"""

results = SQLite.select(conn, sql)

id_dog_owner = int(results[0][0])

sql = """SELECT id_dog
FROM dogs
WHERE dog_name = 'rover';"""

results = SQLite.select(conn, sql)

id_dog = int(results[0][0])

# update the row in database
SQLite.update(conn, 'dogs', ('id_dog_owner',), (id_dog_owner,), 'id_dog', id_dog)

conn.commit()

#################################################
# add another column to database and populate ###
#################################################

# add owner ages to dog owners table

table_name = 'dog_owners'
column_name = 'owner_age'
column_type = 'integer'

# add new column
SQLite.add_column(conn, table_name, column_name, column_type)

# update each dog owners age
SQLite.update(conn, 'dog_owners', ('owner_age',), (20,), 'id_dog_owner', 1)
SQLite.update(conn, 'dog_owners', ('owner_age',), (25,), 'id_dog_owner', 2)
SQLite.update(conn, 'dog_owners', ('owner_age',), (42,), 'id_dog_owner', 3)

conn.commit()

# close the database ###
conn.close()
