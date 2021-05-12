#!/bin/python

from argparse import ArgumentParser, FileType
from pprint import pprint

from sql_helper import open_database, possible_values, read_whole_table


def diversity_of(rows, sensitive_column_count: int) -> int:

    blocks_by_sensitive_data = {}
    for row in rows:
        sensitive_key = row[-sensitive_column_count:]
        qid = row[:-sensitive_column_count]
        print(sensitive_key, "has", qid)
        if sensitive_key in blocks_by_sensitive_data:
            blocks_by_sensitive_data[sensitive_key].append(qid)
        else:
            blocks_by_sensitive_data[sensitive_key] = [qid]

    return min(len(x) for x in blocks_by_sensitive_data.values())


def remove_column(col, rows):
    result = []
    for row in rows:
        result.append(tuple(x for i, x in enumerate(row) if i != col))

    return result


def main(database_file, diversity: int, sensitive_column_count: int, remove_list):

    cur, tables = open_database(database_file)
    assert len(tables) == 1
    table = tables[0]

    rows = read_whole_table(table, cur)
    assert len(rows) != 0

    print("Table name:", table)
    print(f"Read in {len(rows)} rows")
    print("Schema", [type(x) for x in rows[0]])

    for row in rows:
        print(tuple(row))

    values = possible_values(rows)
    pprint(values)
    print("Value counts: ", end="")
    pprint(
        {column_name: len(values) for column_name, values in values.items()},
    )

    cur.connection.commit()
    cur.close()

    processed = rows
    for col in sorted(remove_list, reverse=True):
        processed = remove_column(col, processed)

    print("Data after removing columns")
    for row in processed:
        print(tuple(row))

    print("Input diversity", diversity_of(processed, sensitive_column_count))


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
    arg_parser.add_argument(
        "-s",
        "--sensitive",
        help="The number of sensitive columns on the right side of the schema",
        default=1,
        type=int,
        required=False,
    )
    arg_parser.add_argument(
        "-r",
        "--remove_list",
        help="Which columns (0-based) to remove entirely as a comma-separated list",
        default=[],
        type=lambda string: [int(x) for x in string.split(",")],
        required=False,
    )
    args = arg_parser.parse_args()
    main(args.database_file, args.diversity, args.sensitive, args.remove_list)
