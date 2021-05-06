#!/bin/python

from argparse import ArgumentParser
from sqlite3 import connect


def main(database_file):
    cur = connect(database_file).cursor()
    tables = cur.execute("select name from sqlite_master where type = 'table'")

    for table in tables:
        print(table)
        for row in cur.execute(f"select * from {table[0]}"):
            print(row)

    cur.connection.commit()
    cur.close()


if __name__ == "__main__":
    arg_parser = ArgumentParser()
    arg_parser.add_argument(
        "database_file", help="The file from which the database is loaded"
    )
    args = arg_parser.parse_args()
    main(args.database_file)
