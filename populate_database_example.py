import SQLite_fun as SQL

#######################
# connect to database #
#######################
database = "./dog_example.db"

conn = SQL.create_connection(database)



##########################
### populate the table ###
##########################

#add some owners to table
value_tuple = (('John', 'Smith'), ('James','Williams'), ('Christopher','Jones'))

column_tuple = ('owner_forename','owner_surname')

SQL.insert_many_rows(conn, 'dog_owners', column_tuple, value_tuple)



# add some dogs to table
value_tuple = ( ('fido',1,3), ('rover',2,5), ('buddy',2,3), ('lucky',3,2) )

column_tuple = ('dog_name', 'id_dog_owner', 'age_of_dog')

SQL.insert_many_rows(conn, 'dogs', column_tuple, value_tuple)

conn.commit()



#################################
### change the owner of a dog ###
#################################

# Match rover with John Smith


# use a SELECT statament to find dog and owner primary key ids
sql = """SELECT id_dog_owner 
FROM dog_owners 
WHERE (owner_forename = 'John' 
AND owner_surname = 'Smith');"""

results = SQL.select(conn, sql)

id_dog_owner = int(results[0][0])




sql = """SELECT id_dog 
FROM dogs 
WHERE dog_name = 'rover';"""

results = SQL.select(conn, sql)

id_dog = int(results[0][0])




# update the row in database
SQL.update(conn, 'dogs', ('id_dog_owner',), (id_dog_owner,), 'id_dog', id_dog)

conn.commit()


##########################
### close the database ###
##########################
conn.close()