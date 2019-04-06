# coding: utf-8
import os
import sys
import argparse
import sqlite3
import xlrd
import tkinter as tk
from tkinter import filedialog, messagebox

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


# TODO: support proper types?
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


def add_xl_table(cursor, sheet, table_name):
    header = [cell.value for cell in sheet.row(0)]

    # TODO: support proper types
    defs = ', '.join(['"%s" %s' % (f, 'TEXT') for f in header])
    sql = 'create table "%s" (%s)' % (table_name, defs)
    log.info('Создаю таблицу "%s"', table_name)
    cursor.execute('drop table if exists `%s`' % table_name)
    cursor.execute(sql)

    refs = ', '.join([':' + f for f in header])
    sql = 'insert into "%s" values (%s)' % (table_name, refs)

    for row_idx in range(1, sheet.nrows):
        cursor.execute(sql, list(sheet.row_values(row_idx)))


def xl2sqlite(conn, paths,  encoding=None):
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
    parser.add_argument('-e', '--encoding')
    parser.add_argument('-f', '--file-format', default='dbf', help='Формат файла с таблицей', choices=fmt_map.keys())
    parser.add_argument('-s', '--sqlite', default=':memory:')
    return parser.parse_args()


fmt_map = {
    'dbf': dbf2sqlite,
    'xls': xl2sqlite,
}


def gui(args):
    master = tk.Tk()
    master.geometry("600x400")
    file_options = {
        'first_table': {
            'caption': 'Выберите файл первой таблицы',
            'value': None,
        },
        'second_table': {
            'caption': 'Выберите файл второй таблицы',
            'value': None,
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

    encoding = tk.StringVar()
    encoding.set(args.encoding)
    entry = tk.Entry(textvariable=encoding, text='Выберите кодировку')
    entry.insert(tk.END, 'cp866')
    entry.grid(row=row, column=1, sticky='w')
    tk.Label(master, text='Введите кодировку файлов таблиц').grid(row=row, column=0, sticky='w')
    row += 1

    def execute():
        output = do_processing(
            sqlite=':memory:',
            tables=[file_options['first_table']['value'], file_options['second_table']['value']],
            query_file=file_options['query_file']['value'],
            encoding=encoding.get(),
            output=args.output,
            default_format=args.file_format,
        )
        messagebox.showinfo(
            title='Завершено',
            message='Все получилось! Результат в файле "{}"'.format(os.path.realpath(output))
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
        sqlite=args.sqlie,
        tables=args.tables,
        query_file=args.query,
        encoding=args.encoding,
        output=args.output,
        default_format=args.file_format
    )


def setup_logging(log_level):
    lh = logging.StreamHandler()
    lh.setFormatter(logging.Formatter(fmt='%(asctime)-15s [%(levelname)s] %(message)s'))
    lh.setLevel(log_level)
    log.setLevel(log_level)
    log.addHandler(lh)


def main():
    args = get_args()
    setup_logging(args.log_level)
    if args.cli:
        cli(args)
    else:
        gui(args)


def do_processing(sqlite, tables, query_file, encoding, output, default_format):
    log.debug('Execute with args: %s %s %s %s %s %s', sqlite, tables, query_file, encoding, output, default_format)
    conn = sqlite3.connect(sqlite)
    table_names = []
    for table in tables:
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

    output = output or '_'.join(os.path.basename(t) for t in tables + [query_file]).replace(' ', '_') + '.csv'
    log.info('Записываю результат в файл "%s"', output)
    write_to_csv(cursor, output)
    cursor.close()
    conn.close()
    return output


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        log.exception('Случилась неожиданная ошибка: %s', e)
