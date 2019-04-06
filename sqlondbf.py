import os
import sys
import argparse
import sqlite3


from dbfread import DBF
import csv

import logging


log = logging.getLogger()


dbf_typemap = {
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


xl_typemap = {

}


def add_dbf_table(cursor, table):
    """Add a dbase table to an open sqlite database."""

    cursor.execute('drop table if exists `%s`' % table.name)

    field_types = {}
    for f in table.fields:
        field_types[f.name] = dbf_typemap.get(f.type, 'TEXT')

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

    names = []
    for table_file in paths:
        try:
            table = DBF(
                table_file,
                lowernames=True,
                encoding='cp1251',
                # char_decode_errors=char_decode_errors,
            )
            add_dbf_table(cursor, table)
            names.append(table.name)
        except UnicodeDecodeError as e:
            log.exception('Encoding error happened: %s', e)
            raise

    conn.commit()
    cursor.close()
    return names


def add_xl_table(cursor, sheet, table_name):
    header = [cell.value for cell in sheet.row(0)]

    # Create the table
    defs = ', '.join(['"%s" %s' % (f, 'TEXT') for f in header])
    sql = 'create table "%s" (%s)' % (table_name, defs)
    log.info('Create table "%s"', table_name)
    cursor.execute('drop table if exists `%s`' % table_name)
    cursor.execute(sql)

    refs = ', '.join([':' + f for f in header])
    sql = 'insert into "%s" values (%s)' % (table_name, refs)

    for row_idx in range(1, sheet.nrows):
        cursor.execute(sql, list(sheet.row_values(row_idx)))


def xl2sqlite(conn, paths,  encoding=None):
    import xlrd
    cursor = conn.cursor()
    names = []
    for path in paths:
        rb = xlrd.open_workbook(path, encoding_override=encoding)
        sheet = rb.sheet_by_index(0)   # ignore all sheets except first
        table_name = os.path.basename(path).split('.')[0]
        add_xl_table(cursor, sheet, table_name=table_name)
        names.append(table_name)
    return names


def write_to_csv(cursor, path):
    with open(path, 'w', newline='') as f:
        writer = csv.writer(f, dialect='excel')
        writer.writerow([d[0] for d in cursor.description])
        for row in cursor:
            writer.writerow(row)


def get_query(path, *args, **kwargs):
    with open(path) as f:
        raw_query = f.read()
    return raw_query.format(*args, **kwargs)


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('tables', nargs='*')
    parser.add_argument('-q', '--query', default='query.sql')
    parser.add_argument('-o', '--output', default='output.csv')
    parser.add_argument('--log-level', default='DEBUG')
    parser.add_argument('--encoding', default='utf-8')
    parser.add_argument('-f', '--format', default='dbf')
    return parser.parse_args()


fmt_map = {
    'dbf': dbf2sqlite,
    'xl': xl2sqlite,
}


def main():
    args = get_args()
    logging.basicConfig(level=args.log_level, format='%(asctime)-15s [%(levelname)s] %(message)s')
    tables = args.tables or ['dbase_83.dbf']
    conn = sqlite3.connect(':memory:')

    table_names = fmt_map.get(args.format)(conn, tables, encoding=args.encoding)

    cursor = conn.cursor()
    sql = get_query(args.query, *table_names)
    log.debug('Executing query:\n%s', sql)
    cursor.execute(sql)

    log.info('Writing output to "%s"', args.output)
    write_to_csv(cursor, args.output)
    cursor.close()

    conn.close()


if __name__ == '__main__':
    main()
