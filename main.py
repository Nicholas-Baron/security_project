#!/bin/python

from argparse import ArgumentParser, FileType
from sqlite3 import connect
from pprint import pprint


def get_table_names(cursor) -> [str]:
    return [
        x[0]
        for x in cursor.execute("select name from sqlite_master where type = 'table'")
    ]


def possible_values(rows):
    values = [set() for _ in rows[0]]

    for row in rows:
        for i, col in enumerate(row):
            values[i].add(col)

    return values


def main(database_file, diversity: int):
    cur = connect(database_file.name).cursor()

    tables = get_table_names(cur)
    assert len(tables) == 1
    table = tables[0]

    rows = [row for row in cur.execute(f"select * from {table}")]
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
