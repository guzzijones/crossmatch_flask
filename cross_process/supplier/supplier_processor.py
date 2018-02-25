from __future__ import print_function
import sys
import argparse
import os
import psycopg2
import importlib
import pdb
from .models import SupMaster, SupData

def str_to_class(string):
    return getattr(sys.modules[__name__], string)


class ColumnMissingError(Exception):
    def __init__(self, message):
        super(ColumnMissingError, self).__init__(message)

class DBSqlAlchemy(object):

    def __init__(self,dbmodel):
        self.model = dbmodel

    #todo(aj) sqlalchemy
    def upsert(self, dataframe, table, keys):

class DataBase(object):
    #todo(aj) use sqlalchemy here instead - pass in model
    def __init__(self, host="localhost", port='5432', database='crossmatch',
                 user='crossmatch', password='hinton50'):
        self.conn = psycopg2.connect(host=host, port=port,
                                     database=database,
                                     user=user,
                                     password=password)

    def select(self, statement):
        cur = self.conn.cursor()
        cur.execute(statement)
        while True:
            row = cur.fetchone()
            if row is None:
                break
            yield row

    @staticmethod
    #todo(aj) sqlalchemy
    def to_query_format(listin, columns):
        new_string = ""
        zipped = zip(listin, columns)
        for item, column in zipped:
            if column == "date":
                new_string += " to_date(\'" + str(item) + "\',\'YYYYMMDD\'), "
            elif isinstance(item, str):
                new_string += "'" + item.replace("'", "''") + "', "
            else:
                new_string += str(item).replace("'", "''") + ", "
        new_string = new_string[0:-2]
        return new_string

    #todo(aj) sqlalchemy
    def update_format(self, dfrow, columns):
        new_string = ""
        row = dfrow[columns]
        zipped = zip(row.values.tolist(), columns)
        for item, column in zipped:
            if column == "date":
                new_string += "date=to_date(\'" + str(item) \
                        + "\',\'YYYYMMDD\'), "
            elif isinstance(item, str):
                new_string += column + "='" + item.replace("'", "''") + "', "
            else:
                new_string += column + "=" + str(item) + ", "
        new_string = new_string[0:-2]
        return new_string

    #todo(aj) sqlalchemy
    def upsert(self, dataframe, table, keys):
        try:
            # update columns
            columns_update = []
            for col in dataframe.columns:
                if col not in keys:
                    columns_update.append(col)

            cur = self.conn.cursor()
            columns = ", ".join(dataframe.columns)
            # todo(aj) iterrows is slow, but who cares
            for i, row in dataframe.iterrows():
                values = DataBase.to_query_format(row.values.tolist(),
                                                  dataframe.columns)
                update_part = self.update_format(row, columns_update)
                sql = "INSERT into " + table + " (" + columns + ") values (" +\
                      values + ") on conflict (" + ", ".join(keys) + \
                      ") do update set " + update_part

                cur.execute(sql, row)
            self.conn.commit()
        except psycopg2.DatabaseError as e:
            if self.conn:
                self.conn.rollback()
            print("Error inserting " + str(e))


class Processor(object):
    """ virtual could be different processors"""
    KEYS = [
        "sup_id",
        "dist_id",
        "date",
        "prod_id"
    ]
    REQURIED_COLS = [
        u'sup_id',
        u"dist_id",
        u"dist_name",
        u"address",
        u"city",
        u"state",
        u"zip",
        u"prod_id",
        u"prod_desc",
        u"size",
        u"pack",
        u"proof",
        u"vintage",
        u"date",
        u"qty"
    ]

    CONFIG_FOLDER = u"config"
    HOME = u"home"
    INCOMING = u"incoming"
    ARCHIVED = u"archived"

    def __init__(self, supplier_id):
        self.supplier_id = supplier_id
        sys.path.append(os.path.join(os.sep,
                                     Processor.HOME, supplier_id,
                                     Processor.CONFIG_FOLDER))
        self.home_dir = os.path.join(os.sep, Processor.HOME, supplier_id)
        self.config_dir = os.path.join(self.home_dir, Processor.CONFIG_FOLDER)
        self.incoming_file_blob_pre = os.path.join(os.sep, Processor.HOME,
                                                   supplier_id,
                                                   Processor.INCOMING)

    def verify_cols(self, columns):
        for col in Processor.REQURIED_COLS:
            if col not in columns:
                raise ColumnMissingError("missing column: " + col)

    def process(self):
        parser = importlib.import_module("parser_cust")
        for filename in os.listdir(self.incoming_file_blob_pre):
            dataframe = None
            if os.path.isfile(os.path.join(
                              self.incoming_file_blob_pre, filename)):
                dataframe = parser.read(os.path.join(
                            self.incoming_file_blob_pre,
                            filename))
                self.verify_cols(dataframe.columns)
                # call read

                db = DataBase()
                db.upsert(dataframe, "sup_data", Processor.KEYS)
                dst_file_full = os.path.join(self.home_dir, Processor.ARCHIVED,
                                             os.path.basename(filename))

                os.rename(os.path.join(self.incoming_file_blob_pre, filename),
                          dst_file_full)
                # todo(aj) write info to history table


class Suppliers(object):

    def __init__(self):
        self.db = DataBase()
        self.suppliers = self.get_suppliers()

    def get_suppliers(self):
        sql = "select sup_id from sup_master"
        sups = []
        for i in self.db.select(sql):
            sups.append(i[0])
        return sups

    def process_action(self, action):
        for sup in self.suppliers:
            pdb.set_trace()
            action(sup)
            action.process()
            # todo(aj) return stacktrace for reporting to gui
            # todo(aj) catch pandas exception  return backtrace
            # todo(aj) catch db exception  return backtrace
            # todo(aj) file exception  return backtrace
            return


def old_main():
    argparser = argparse.ArgumentParser(prog="suppliers")
    argparser.add_argument(u'--supplier_id', help=u"supplier id",
                           required=True)
    args = argparser.parse_args()
    supplier_id = args.supplier_id
    processor = Processor(supplier_id)
    processor.process()


def main():
    sup = Suppliers()
    sup.process_action(Processor)


if __name__ == "__main__":
    main()
