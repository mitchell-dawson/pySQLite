#!/usr/bin/python
import pySQLite_fun as SQLite
import os
import cPickle as pickle
import pdb
import sys


def create_output_db_sql():

    #######################################
    # create the tables of the database ###
    #######################################

    create_table_sql = []

    create_table_sql.append((""" CREATE TABLE IF NOT EXISTS coffees (
        id              INTEGER     PRIMARY KEY,
        coffee_name     TEXT        NOT NULL,
        price           REAL        NOT NULL
    ); """,))

    create_table_sql.append((""" CREATE TABLE IF NOT EXISTS salespeople (
        id                  INTEGER     PRIMARY KEY,
        first_name          TEXT        NOT NULL,
        last_name           TEXT        NOT NULL,
        commission_rate     REAL        NOT NULL
    ); """,))

    create_table_sql.append((""" CREATE TABLE IF NOT EXISTS customers (
        id                  INTEGER     PRIMARY KEY,
        company_name        TEXT        NOT NULL,
        street_address      TEXT        NOT NULL,
        city                TEXT        NOT NULL,
        state               TEXT        NOT NULL,
        zip                 TEXT        NOT NULL
    ); """,))

    create_table_sql.append((""" CREATE TABLE IF NOT EXISTS orders (
        id                          INTEGER     PRIMARY KEY,
        customer_id                 INTEGER,
        salesperson_id              INTEGER,
        FOREIGN KEY(customer_id)    REFERENCES customers(id),
        FOREIGN KEY(salesperson_id) REFERENCES salespeople(id)
    ); """,))

    create_table_sql.append((""" CREATE TABLE IF NOT EXISTS order_items (
        id                      INTEGER     PRIMARY KEY,
        order_id                INTEGER,
        product_id              INTEGER,
        product_quantity        INTEGER,
        FOREIGN KEY(order_id)   REFERENCES orders(id),
        FOREIGN KEY(product_id) REFERENCES products(id)
    ); """,))

    return create_table_sql


def main():

    db_num = int(sys.argv[1])

    ROOT = "./"

    #########################
    # connect to database ###
    #########################

    #########################
    # read in information ###
    #########################

    ###############################
    # close database connection ###
    ###############################

    #######################################
    # make output file for SQL commands ###
    #######################################

    sql_output_file = "%s/output_sql_%d.pkl" % (ROOT, db_num)

    sql_list = []

    ######################
    # create table sql ###
    ######################

    sql_list += create_output_db_sql()

    ##########################
    # find sql for updates ###
    ##########################

    # coffees

    column_tuple = ('coffee_name', 'price')

    value_tuples = [
        ('Colombian', 7.99),
        ('French_Roast', 8.99),
        ('Espresso', 9.99),
        ('Colombian_Decaf', 8.99),
        ('French_Roast_Decaf', 9.99)]

    for value_tuple in value_tuples:

        # find sql
        sql_list.append(SQLite.insert_ignore_sql(
            'coffees', column_tuple, value_tuple, 'id'))

    # salespeople

    column_tuple = ('first_name', 'last_name', 'commission_rate')

    value_tuples = [
        ('Fred', 'Flinstone', 10.0),
        ('Barney', 'Rubble', 10.0)]

    for value_tuple in value_tuples:

        # find sql
        sql_list.append(SQLite.insert_ignore_sql(
            'salespeople', column_tuple, value_tuple, 'id'))

    ###################################
    # write sql list to output file ###
    ###################################

    with open(sql_output_file, 'wb') as sql_file:
        pickle.dump(sql_list, sql_file)

if __name__ == '__main__':
    main()
