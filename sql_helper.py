from sqlite3 import connect


def open_database(file):
    cursor = connect(file.name).cursor()
    return (cursor, get_table_names(cursor))


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


def read_whole_table(table: str, cursor) -> [str]:
    return [row for row in cursor.execute(f"select * from {table}")]
