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


def write_rows(file, rows, columns, table_name="result"):
    with connect(file.name) as connection:
        cursor = connection.cursor()

        cursor.execute(f'create table {table_name} ({", ".join(columns)})')

        cursor.executemany(f"insert into {table_name} values (?,?,?)", rows)

        connection.commit()


def col_names_and_types(names, types, removed):
    return [
        "[" + col_name + "] " + col_type
        for i, (col_name, col_type) in enumerate(zip(names, types))
        if i not in removed
    ]


def type_list(row) -> [str]:
    def sql_type_for(typ):
        if typ is int:
            return "INTEGER"
        elif typ is str:
            return "TEXT"
        else:
            assert False, ("Unknown Python to SQLite conversion for", typ)

    return [sql_type_for(type(x)) for x in row]
