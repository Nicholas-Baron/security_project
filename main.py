#!/bin/python

from argparse import ArgumentParser, FileType
from sqlite3 import connect


def get_table_names(cursor) -> [str]:
    tables = cursor.execute("select name from sqlite_master where type = 'table'")
    return [x[0] for x in tables]


def main(database_file, diversity: int):
    cur = connect(database_file.name).cursor()

    tables = get_table_names(cur)
    assert len(tables) == 1
    table = tables[0]

    print(table)
    for row in cur.execute(f"select * from {table}"):
        print(row)

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
