import os
import sys
import argparse
import sqlite3


from dbfread import DBF
import csv

import logging


log = logging.getLogger()


typemap = {
    'F': 'FLOAT',
    'L': 'BOOLEAN',
    'I': 'INTEGER',
    'C': 'TEXT',
    'N': 'REAL',  # because it can be integer or float
    'M': 'TEXT',
    'D': 'DATE',
    'T': 'DATETIME',
    '0': 'INTEGER',
}


def add_table(cursor, table):
    """Add a dbase table to an open sqlite database."""

    cursor.execute('drop table if exists `%s`' % table.name)

    field_types = {}
    for f in table.fields:
        field_types[f.name] = typemap.get(f.type, 'TEXT')

    # Create the table
    defs = ', '.join(['"%s" %s' % (f, field_types[f]) for f in table.field_names])
    sql = 'create table "%s" (%s)' % (table.name, defs)
    log.info('Create table "%s"', table.name)
    cursor.execute(sql)

    # Create data rows
    refs = ', '.join([':' + f for f in table.field_names])
    sql = 'insert into "%s" values (%s)' % (table.name, refs)

    for rec in table:
        cursor.execute(sql, list(rec.values()))


def dbf2sqlite(conn, paths, encoding='utf-8'):
    cursor = conn.cursor()

    for table_file in paths:
        try:
            table = DBF(
                table_file,
                lowernames=True,
                encoding='cp1251',
                # char_decode_errors=char_decode_errors,
            )
            add_table(cursor, table)
        except UnicodeDecodeError as e:
            log.exception('Encoding error happened: %s', e)
            raise

    conn.commit()
    cursor.close()
    return conn


def write_to_csv(cursor, path):
    with open(path, 'w', newline='') as f:
        writer = csv.writer(f, dialect='excel')
        writer.writerow([d[0] for d in cursor.description])
        for row in cursor:
            writer.writerow(row)


def get_query(path):
    with open(path) as f:
        return f.read()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('tables', nargs='*')
    parser.add_argument('--query', default='query.sql')
    parser.add_argument('-o', '--output', default='output.csv')
    parser.add_argument('--log-level', default='DEBUG')
    parser.add_argument('--encoding', default='utf-8')
    return parser.parse_args()


def main():
    args = get_args()
    logging.basicConfig(level=args.log_level, format='%(asctime)-15s [%(levelname)s] %(message)s')
    tables = args.tables or ['dbase_83.dbf']
    conn = sqlite3.connect(':memory:')

    dbf2sqlite(conn, tables, encoding=args.encoding)

    cursor = conn.cursor()
    sql = get_query(args.query)
    log.debug('Executing query:\n%s', sql)
    cursor.execute(sql)

    log.info('Writing output to "%s"', args.output)
    write_to_csv(cursor, args.output)
    cursor.close()

    conn.close()


if __name__ == '__main__':
    main()
