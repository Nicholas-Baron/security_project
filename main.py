#!/bin/python

from argparse import ArgumentParser, FileType
from pprint import pprint

from sql_helper import open_database, possible_values, read_whole_table


def main(database_file, diversity: int):

    cur, tables = open_database(database_file)
    assert len(tables) == 1
    table = tables[0]

    rows = read_whole_table(table, cur)
    assert len(rows) != 0

    print("Table name:", table)
    print(f"Read in {len(rows)} rows")
    print("Schema", [type(x) for x in rows[0]])

    for row in rows:
        print(row)

    values = possible_values(rows)
    pprint(values)
    print("Values per column", [len(x) for x in values])

    cur.connection.commit()
    cur.close()


if __name__ == "__main__":
    arg_parser = ArgumentParser()
    arg_parser.add_argument(
        "database_file",
        help="The file from which the database is loaded",
        type=FileType(),
    )
    arg_parser.add_argument(
        "-d",
        "--diversity",
        help="The l in l-diversity",
        default=3,
        type=int,
        required=False,
    )
    args = arg_parser.parse_args()
    main(args.database_file, args.diversity)
