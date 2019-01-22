#!/usr/bin/python
import pySQLite_fun as SQLite
import os
import pickle
import pdb


def main():

    db_nums = (1, 2, 3)

    ROOT = "./"

    # for each database
    for db_num in db_nums:

        #########################
        # connect to database ###
        #########################

        output_db = "%s/output_%d.db" % (ROOT, db_num)

        conn_output = SQLite.create_connection(output_db)
        curs = conn_output.cursor()

        ###########################################
        # open pickle file and read in sql list ###
        ###########################################

        sql_output_file = "%s/output_sql_%d.pkl" % (ROOT, db_num)

        with open(sql_output_file, 'rb') as sql_file:
            sql_list = pickle.load(sql_file)

        ##########################
        # execute sql commands ###
        ##########################

        commit_every = 100

        for sql_counter, sql_command in enumerate(sql_list):

            if len(sql_command) == 1:
                conn_output.execute(sql_command[0])

            elif len(sql_command) == 2:

                # check whether create table
                if sql_command[1] == 'curs':
                    curs.execute(sql_command[0])

                else:
                    conn_output.execute(sql_command[0], sql_command[1])

            if (sql_counter + 1) % commit_every == 0:
                conn_output.commit()

        conn_output.commit()
        conn_output.close()

if __name__ == '__main__':
    main()
