from sqlite3 import connect, Row


def open_database(file):
    connection = connect(file.name)
    connection.row_factory = Row
    cursor = connection.cursor()
    return (cursor, get_table_names(cursor))


def get_table_names(cursor) -> [str]:
    return [
        x[0]
        for x in cursor.execute("select name from sqlite_master where type = 'table'")
    ]


def possible_values(rows):
    values = {column_name: set() for column_name in rows[0].keys()}

    for row in rows:
        for column_name, col in zip(row.keys(), row):
            values[column_name].add(col)

    return values


def read_whole_table(table: str, cursor) -> [str]:
    return [row for row in cursor.execute(f"select * from {table}")]
