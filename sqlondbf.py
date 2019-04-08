# coding: utf-8
import os
import argparse
import sqlite3
import xlrd
import tkinter as tk
from tkinter import filedialog, messagebox
from datetime import date, datetime

import dbf
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

dbf_python_tipemap = {
    float: 'F(19,5)',
    bool: 'L',
    int: 'N(19,0)',
    str: 'C(255)',
    date: 'D',
    datetime: 'D',
}

from xlrd.sheet import ctype_text, XL_CELL_DATE


def xl_to_sql_type(val):
    return {
        'empty': 'TEXT',
        'text': 'TEXT',
        'number': 'REAL',
        'xldate': 'DATE',
        'bool': 'BOOLEAN',
        'error': 'TEXT',
        'blank': 'TEXT'
    }.get(ctype_text[val])


def add_dbf_table(cursor, table):
    """Add a dbase table to an open sqlite database."""

    cursor.execute('drop table if exists `%s`' % table.name)

    field_types = {}
    for f in table.fields:
        field_types[f.name] = dbf_typemap.get(f.type, 'TEXT')

    # Create the table
    defs = ', '.join(['"%s" %s' % (f, field_types[f]) for f in table.field_names])
    sql = 'create table "%s" (%s)' % (table.name, defs)
    log.debug('def: %s', defs)
    log.info('Создаю таблицу "%s"', table.name)
    cursor.execute(sql)

    # Create data rows
    refs = ', '.join([':' + f for f in table.field_names])
    sql = 'insert into "%s" values (%s)' % (table.name, refs)

    for rec in table:
        cursor.execute(sql, list(rec.values()))


def dbf2sqlite(conn, paths, encoding=None):
    cursor = conn.cursor()

    names = []
    for table_file in paths:
        try:
            table = DBF(
                table_file,
                lowernames=True,
                encoding=encoding,
                # char_decode_errors=char_decode_errors,
            )
            add_dbf_table(cursor, table)
            names.append(table.name)
        except UnicodeDecodeError as e:
            log.exception('Ошибка кодировки: %s', e)
            raise

    conn.commit()
    cursor.close()
    return names


def add_xl_table(cursor, sheet, table_name, date_mode):
    header = list(sheet.row_values(0))
    types = [xl_to_sql_type(t) for t in sheet.row_types(1)]

    defs = ', '.join(['"%s" %s' % (field, field_type) for field, field_type in zip(header, types)])
    log.debug('defs: %s', defs)
    sql = 'create table "%s" (%s)' % (table_name, defs)
    log.info('Создаю таблицу "%s"', table_name)
    cursor.execute('drop table if exists `%s`' % table_name)
    log.debug('execute: %s', sql)
    cursor.execute(sql)

    refs = ', '.join([':' + f for f in header])
    sql = 'insert into "%s" values (%s)' % (table_name, refs)
    for row_idx in range(1, sheet.nrows):
        values = []
        for col_idx in range(0, sheet.ncols):
            value = sheet.cell_value(row_idx, col_idx)
            if sheet.cell_type(row_idx, col_idx) == XL_CELL_DATE:
                value = xlrd.xldate.xldate_as_datetime(value, date_mode).date()
            values.append(value)
        cursor.execute(sql, values)


def xl2sqlite(conn, paths,  encoding=None):
    cursor = conn.cursor()
    names = []
    for path in paths:
        rb = xlrd.open_workbook(path, encoding_override=encoding)
        sheet = rb.sheet_by_index(0)   # ignore all sheets except first
        table_name = os.path.basename(path).split('.')[0]
        add_xl_table(cursor, sheet, table_name=table_name, date_mode=rb.datemode)
        names.append(table_name)
    return names


def write_to_csv(cursor, path):
    with open(path, 'w', newline='') as f:
        f.write('sep=,\n')
        writer = csv.writer(f, dialect='excel')
        writer.writerow([d[0] for d in cursor.description])
        for row in cursor:
            writer.writerow(row)


def get_query(path, *args, **kwargs):
    with open(path) as f:
        raw_query = f.read()
    return raw_query.format(*args, **kwargs)


def get_args():
    parser = argparse.ArgumentParser('Загружает dbf или xls таблицы, выполняет над ними запрос и пишет результат в сsv')
    parser.add_argument('tables', nargs='*', help='Файлы с таблицами')
    parser.add_argument('--cli', action='store_true', help='использовать консольную версию')
    parser.add_argument('-q', '--query', default='query.sql', help='Путь к файлу с sql запросом')
    parser.add_argument('-o', '--output', help='Путь к файлу, куда будет записан результат')
    parser.add_argument('-l', '--log-level', default='INFO')
    parser.add_argument('-e', '--encoding', default='cp866', help='Кодировка исходных таблиц')
    parser.add_argument('-f', '--file-format', default='dbf', help='Формат файла с таблицей', choices=fmt_map.keys())
    parser.add_argument('-s', '--sqlite', default=':memory:')
    parser.add_argument('--out-fmt', default='dbf', choices=out_fmt_map.keys())
    return parser.parse_args()


def write_to_dbf(cursor, path):
    headers = [d[0] for d in cursor.description]
    try:
        first_row = next(cursor)
    except StopIteration:
        raise ValueError('Что-то пошло не так: в результате 0 строк!')
    # FIXME:
    if os.path.isfile('dbf.schema'):
        defs = open('dbf.schema').read().strip()
    else:
        defs = '; '.join(
            '%s %s' % (
                name, dbf_python_tipemap.get(type(value), 'C'))
                for name, value in zip(headers, first_row)
        )
    log.debug('Dbf defs: %s', defs)
    log.debug(list(type(value) for value in first_row))

    table = dbf.Table(path, defs, codepage='cp866')  # FIXME: encoding hardcoded
    table.open(mode=dbf.READ_WRITE)
    table.append(first_row)
    for row in cursor:
        table.append(row)
    table.close()


fmt_map = {
    'dbf': dbf2sqlite,
    'xls': xl2sqlite,
}

out_fmt_map = {
    'csv': write_to_csv,
    'dbf': write_to_dbf,
}


def gui(args):
    master = tk.Tk()
    master.geometry("600x500")
    file_options = {
        'first_table': {
            'caption': 'Выберите файл первой таблицы',
            'value': args.tables[0] if args.tables and len(args.tables) > 0 else None,
        },
        'second_table': {
            'caption': 'Выберите файл второй таблицы',
            'value': args.tables[1] if args.tables and len(args.tables) > 1 else None,
        },
        'query_file': {
            'caption': 'Выберите файл запроса',
            'value': args.query,
        },
    }

    def ask_file_opt(opt):
        def func():
            value = filedialog.askopenfilename(title=file_options[opt]['caption'])
            file_options[opt]['value'] = value
            file_options[opt]['label']['text'] = value
        return func

    row = 0
    for opt in file_options:
        button = tk.Button(master, text=file_options[opt]['caption'], command=ask_file_opt(opt))
        button.grid(row=row, column=0, sticky='w')
        label = tk.Label(master, text=file_options[opt]['value'])
        label.grid(row=row, column=1, sticky='w')
        file_options[opt]['label'] = label
        row += 1

    encoding = tk.Entry(master)
    encoding.insert(tk.END, args.encoding or '')
    encoding.grid(row=row, column=1, sticky='w')
    tk.Label(master, text='Введите кодировку файлов таблиц').grid(row=row, column=0, sticky='w')
    row += 1

    out_fmt = tk.Entry(master)
    out_fmt.insert(tk.END, args.out_fmt or '')
    out_fmt.grid(row=row, column=1, sticky='w')
    tk.Label(master, text='Введите формат вывода').grid(row=row, column=0, sticky='w')
    row += 1

    output = tk.Entry(master)
    output.insert(tk.END, args.output or '')
    output.grid(row=row, column=1, sticky='w')
    tk.Label(master, text='Путь к файлу с результатом (не обязательно)').grid(row=row, column=0, sticky='w')
    row += 1

    def execute():
        try:
            result_output = do_processing(
                sqlite=':memory:',
                tables=[file_options['first_table']['value'], file_options['second_table']['value']],
                query_file=file_options['query_file']['value'],
                encoding=encoding.get(),
                output=output.get(),
                default_format=args.file_format,
                out_fmt=out_fmt.get(),
            )
        except Exception as e:
            log.exception('Неожиданная ошибка: %s', e)
        else:
            messagebox.showinfo(
                title='Завершено',
                message='Все получилось! Результат в файле "{}"'.format(os.path.realpath(result_output))
            )

    execute_button = tk.Button(master, text='Выполнить', command=execute)
    execute_button.grid(row=row, column=0, sticky='w')
    row += 1

    exit_button = tk.Button(master, text='Выйти', command=master.quit)
    exit_button.grid(row=row, column=0, sticky='w')
    row += 1

    txt_frm = tk.Frame(master, width=800, height=300)
    txt_frm.grid(row=row, column=0, columnspan=2)
    row += 1
    append_gui_logger(txt_frm, args.log_level)

    master.columnconfigure(0, weight=1)
    master.columnconfigure(1, weight=4)

    tk.mainloop()


class LoggingToGUI(logging.Handler):
    """ Used to redirect logging output to the widget passed in parameters """
    def __init__(self, level, console):
        super(LoggingToGUI, self).__init__(level)
        self.console = console
        self.sep = '\n'

    def emit(self, message):
        msg = self.format(message) + self.sep
        self.console.insert(tk.END, msg)
        self.console.see(tk.END)


def append_gui_logger(frame, log_level):
    # txt_frm.pack(fill="both", expand=True)
    frame.grid_propagate(False)
    frame.grid_rowconfigure(0, weight=1)
    frame.grid_columnconfigure(0, weight=1)

    log_box = tk.Text(frame, borderwidth=3, relief="sunken")
    log_box.grid(row=0, column=0, sticky="nsew", padx=2, pady=2)
    log_box.config(font=("consolas", 12), undo=True, wrap='word')
    scrollbar = tk.Scrollbar(frame, command=log_box.yview)
    scrollbar.grid(row=0, column=1, sticky='nsew')
    log_box['yscrollcommand'] = scrollbar.set

    lh = LoggingToGUI(log_level, log_box)
    lh.setFormatter(logging.Formatter(fmt='%(asctime)-15s [%(levelname)s] %(message)s'))
    log.addHandler(lh)
    log.info('Тут будут писаться сообщения о происходящем')


def cli(args):
    do_processing(
        sqlite=args.sqlite,
        tables=args.tables,
        query_file=args.query,
        encoding=args.encoding,
        output=args.output,
        default_format=args.file_format,
        out_fmt=args.out_fmt
    )


def setup_logging(log_level):
    logging.basicConfig(level=log_level, format='%(asctime)-15s [%(levelname)s] %(message)s')


def main():
    args = get_args()
    setup_logging(args.log_level)
    if args.cli:
        cli(args)
    else:
        gui(args)


def do_processing(sqlite, tables, query_file, encoding, output, out_fmt, default_format):
    log.debug('Выполняю запрос: sqlite=%s tables=%s query_file=%s encoding=%s output=%s default_format=%s',
             sqlite, tables, query_file, encoding, output, default_format)
    conn = sqlite3.connect(sqlite, detect_types=sqlite3.PARSE_DECLTYPES)
    table_names = []
    for table in tables:
        if not table:
            continue
        fmt = default_format
        if table.endswith('.dbf'):
            fmt = 'dbf'
        if table.endswith('.xls') or table.endswith('.xlsx'):
            fmt = 'xls'
        load_func = fmt_map.get(fmt)
        table_names.extend(load_func(conn, [table], encoding=encoding))
    log.info('Загруженные таблицы: %s', ', '.join('"{}"'.format(name) for name in table_names))
    sql = get_query(query_file, *table_names)
    cursor = conn.cursor()
    log.debug('Выполняем запрос:\n%s', sql)
    cursor.execute(sql)

    output = output or '_'.join(os.path.basename(t) for t in tables + [query_file]).replace(' ', '_') + '.{}'.format(out_fmt)
    log.info('Записываю результат в файл "%s"', output)
    out_fmt_map[out_fmt](cursor, output)
    cursor.close()
    conn.close()
    return output


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        log.exception('Случилась неожиданная ошибка: %s', e)
