#!/bin/python

from argparse import ArgumentParser, FileType
from pprint import pprint
from math import log10

from sql_helper import (
    open_database,
    possible_values,
    read_whole_table,
    write_rows,
    col_names_and_types,
    type_list,
)


def quasi_id(row, sensitive_column_count):
    return row[:-sensitive_column_count]


def diversity_of(rows, sensitive_column_count):
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


# Entropy-based
def is_diverse(rows, sensitive_column_count, diversity):
    # QID tuples -> { sensitive -> count }
    q_star_blocks = {}

    for row in rows:
        qid = quasi_id(row, sensitive_column_count)
        sensitive = row[-sensitive_column_count:]
        if qid not in q_star_blocks:
            # This qid has not been seen
            q_star_blocks[qid] = {sensitive: 1}
        elif sensitive not in q_star_blocks[qid]:
            # This sensitive attribute for the current qid has not been seen
            q_star_blocks[qid][sensitive] = 1
        else:
            q_star_blocks[qid][sensitive] += 1

    pprint(q_star_blocks)

    for q_star_block in q_star_blocks.values():
        total = 0
        for sensitive, count in q_star_block.items():
            tuple_count = sum(q_star_block.values())
            frac_with_sensitive = count / tuple_count
            for other_sensitive in (x for x in q_star_block.keys() if x != sensitive):
                total += frac_with_sensitive * log10(
                    q_star_block[other_sensitive] / tuple_count
                )

        if -total < log10(diversity):
            return False

    return True


def remove_column(col, rows):
    result = []
    for row in rows:
        result.append(tuple(x for i, x in enumerate(row) if i != col))

    return result


def anonymized(value):
    if type(value) is int:
        if value <= 0:
            return value
        # Make the lowest non-zero digit a zero.
        val_str = str(value)
        for x in range(1, len(val_str) + 1):
            if val_str[-x] != "0":
                val_str = val_str[: -(x + 1)] + "0" + ("0" * x)
                break

        assert int(val_str) < value, {"val_str": val_str, "value": value}

        return int(val_str)
    else:
        assert False, ("Could not anonymize ", value)


def main(
    database_file, output_file, diversity: int, sensitive_column_count: int, remove_list
):

    cur, tables = open_database(database_file)
    assert len(tables) == 1
    table = tables[0]

    rows = read_whole_table(table, cur)
    assert len(rows) != 0
    print("Read in", len(rows), "rows")

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

    initial_diversity = diversity_of(processed, sensitive_column_count)
    print("Input diversity", initial_diversity)
    if is_diverse(processed, sensitive_column_count, diversity):
        print(f"Removing columns has resulted in a {initial_diversity}-diverse dataset")
    else:
        print("Need to diversify data")
        while not is_diverse(processed, sensitive_column_count, diversity):
            new_processed = []
            print("Diversifying...")
            for row in processed:
                print(row)
                new_processed.append(
                    tuple(
                        anonymized(col)
                        if i < len(row) - sensitive_column_count
                        else col
                        for i, col in enumerate(row)
                    )
                )

            processed = new_processed
            print("New diversity is", diversity_of(processed, sensitive_column_count))

    write_rows(
        output_file,
        processed,
        col_names_and_types(rows[0].keys(), type_list(rows[0]), remove_list),
    )


if __name__ == "__main__":
    arg_parser = ArgumentParser()
    arg_parser.add_argument(
        "database_file",
        help="The file from which the database is loaded",
        type=FileType(),
    )
    arg_parser.add_argument(
        "output_file",
        help="The file where the result database will be saved",
        type=FileType("w"),
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
    main(
        args.database_file,
        args.output_file,
        args.diversity,
        args.sensitive,
        args.remove_list,
    )
