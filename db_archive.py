import traceback

from db_manager import DatabaseManager
from dbconnection import ConnectionManager
from pandas.io import sql
import argparse

"""Run File using 

python db_archive.py -t transactions -fd 2020-06-10 -td 2020-06-12

"""

class DatabaseArchiver:
    retry = 0

    def get_retries(self):
        return self.retry

    def get_db_engine(self):
        db_manager = DatabaseManager()
        return db_manager.get_connection_engine()

    def get_db_connection(self):
        con_manager = ConnectionManager()
        return con_manager.get_connection()

    def archive_database(self, table_name, from_date, to_date, interval):
        sqlalchemy_db_con = None
        sql_db_con = None
        count = None
        try:
            sqlalchemy_db_con = self.get_db_engine()
            sql_db_con = self.get_db_connection()
            # validate to and from date
            if not self.is_valid_from_date(sql_db_con, table_name, from_date) or not self.is_valid_to_date(
                    sql_db_con, table_name,
                    to_date):
                return

            # get total record count
            count = self.get_number_of_records(sql_db_con, table_name, from_date, to_date)

            # validate interval
            if not self.is_valid_interval(count, interval):
                return

            self.start_archive_task(sqlalchemy_db_con, sql_db_con, table_name, from_date, to_date, count, interval)

            print("Number of rows is " + str(count))

        except Exception as ex:
            print("Error!! \n ❌", ex)
            try:
                self.start_archive_task(sqlalchemy_db_con, table_name, from_date, to_date, count, interval)
            except Exception as e:
                print("Error!! \n ❌", ex)
                traceback.print_exception(None, ex, ex.__traceback__)

        finally:
            if sqlalchemy_db_con.closed == False:
                sqlalchemy_db_con.close()
            if sql_db_con.is_connected():
                sql_db_con.disconnect()

    def start_archive_task(self, sqlalchemy_con, sql_con, table_name, from_date, to_date, count, interval):
        start_id = self.get_start_id(sql_con, table_name, from_date)
        end_id = self.get_end_id(sql_con, table_name, to_date)
        print("Archiving records from id " + str(start_id) + " to " + str(end_id))
        # Create loop
        iter = start_id
        reset_table = True
        while iter <= end_id:
            # select data_frame to copy
            selected_df = self.get_records_df(sqlalchemy_con, table_name, iter, end_id, interval)

            if selected_df is None:
                raise Exception('select failed')

            # copy dataframe to new table/ append dataframe
            success = self.copy_records_df(sqlalchemy_con, selected_df, table_name, reset_table)
            reset_table = False
            if success is False:
                raise Exception('copy failed')

            # Increment start_id
            iter += interval

        # if sqlalchemy_con.closed == False:
        #     sqlalchemy_con.close()
        # check if copying done
        archival_count = self.get_archival_data_count(sqlalchemy_con, table_name)

        if archival_count > count:
            print("Removing duplicate records")
            # remove duplicates
            self.remove_duplicate_entries(sqlalchemy_con, table_name)
            archival_count = self.get_archival_data_count(sqlalchemy_con, table_name)

        if archival_count == count:
            self.delete_table_data(sql_con, table_name, start_id, end_id)

    def is_valid_interval(self, count, interval):
        return count > interval

    def get_number_of_records(self, con, table_name, from_date, to_date):
        GET_COUNT_QUERY = "select count(*) as total from {} where (createdAt LIKE '%s' or createdAt > '%s') and (createdAt LIKE '%s' or createdAt < '%s')".format(
            table_name)
        cursor = con.cursor()
        cursor.execute(GET_COUNT_QUERY % (from_date + '%', from_date, to_date + "%", to_date))

        # count_df = sql.read_sql_query(GET_COUNT_QUERY.format(table_name), params=[from_date, to_date], con=con)
        return cursor.fetchone()[0]

    def is_valid_from_date(self, con, table_name, from_date):
        GET_RECORDS_BEFORE = "select count(*) as tot from {} where createdAt LIKE '%s' or createdAt < '%s'".format(
            table_name)
        cursor = con.cursor()
        cursor.execute(GET_RECORDS_BEFORE % (from_date + '%', from_date))
        # count_df = sql.read_sql_query(GET_RECORDS_BEFORE.format(table_name), params=[from_date + '%', from_date],
        #                               con=con)
        if cursor.fetchone()[0] == 0:
            print("Invalid From date!")
            return False
        else:
            return True

    def is_valid_to_date(self, con, table_name, to_date):
        if to_date is None:
            return False

        GET_RECORDS_AFTER = "select count(*) as tot from {} where createdAt LIKE '%s' or createdAt > '%s'".format(
            table_name)
        cursor = con.cursor()
        cursor.execute(GET_RECORDS_AFTER % (to_date + '%', to_date))

        # count_df = sql.read_sql_query(GET_RECORDS_AFTER.format(table_name), params=[to_date + '%', to_date],
        #                               con=con)
        if cursor.fetchone()[0] == 0:
            print("Invalid To date!")
            return False
        else:
            return True

    def get_start_id(self, con, table_name, date):
        GET_START_ID_QUERY = "select id from {} where createdAt LIKE '%s' or createdAt > '%s' order by createdAt asc limit 1 ".format(
            table_name)
        cursor = con.cursor()
        cursor.execute(GET_START_ID_QUERY % (date + '%', date))

        # start_id_df = sql.read_sql_query(GET_START_ID_QUERY.format(table_name), params=[date + '%', date], con=con)
        return cursor.fetchone()[0]

    def get_end_id(self, con, table_name, date):
        GET_END_ID_QUERY = "select id from {} where createdAt LIKE '%s' or createdAt < '%s' order by createdAt desc limit 1 ".format(
            table_name)
        cursor = con.cursor()
        cursor.execute(GET_END_ID_QUERY % (date + '%', date))

        # end_id_df = sql.read_sql_query(GET_START_ID_QUERY.format(table_name), params=[date + '%', date], con=con)
        return cursor.fetchone()[0]

    def get_records_df(self, con, table_name, start_id, end_id, interval):
        if start_id + interval < end_id:
            GET_RECORDS_BETWEEN = "select * from {} where id >= %s and id < %s "
        else:
            GET_RECORDS_BETWEEN = "select * from {} where id >= %s and id <= %s "
        records_df = sql.read_sql_query(GET_RECORDS_BETWEEN.format(table_name),
                                        params=[str(start_id), str(min(start_id + interval, end_id))],
                                        con=con)
        return records_df

    def copy_records_df(self, con, selected_df, table_name, reset):
        archival_table_name = table_name + '_archived'
        if reset:
            behaviour = 'replace'
        else:
            behaviour = 'append'
        try:
            sql.to_sql(frame=selected_df, con=con, name=archival_table_name, if_exists=behaviour, index=False)
            # cur = con.cursor()
            # cur.execute("INSERT IGNORE INTO " + archival_table_name + " SELECT * FROM " + table_name + "")
            # con.commit()
            return True
        except Exception as e:
            print("Error!! \n ❌", e)
            return False

    def get_archival_data_count(self, con, table_name):
        GET_TOTAL_DATA_COUNT_QUERY = "select count(id) as tot from " + table_name + "_archived "
        # cursor = con.cursor()
        # cursor.execute(GET_TOTAL_DATA_COUNT_QUERY)
        # return cursor.fetchone()[0]
        count_df = sql.read_sql_query(GET_TOTAL_DATA_COUNT_QUERY, con)
        return count_df['tot'][0]

    def remove_duplicate_entries(self, con, table_name):
        archived_table_name = table_name + "_archived"
        READ_RECORDS = "select * from {}"
        records_df = sql.read_sql_query(READ_RECORDS.format(archived_table_name), con)
        distinct_records_df = records_df.drop_duplicates()
        try:
            sql.to_sql(frame=distinct_records_df, con=con, name=archived_table_name, if_exists='replace', index=False)
            return True
        except Exception as e:
            print("Error!! \n", e)
            return False

    def delete_table_data(self, con, table_name, start_id, end_id):
        delete_data_query = "DELETE FROM {} WHERE id >=  %s and id <=  %s".format(table_name)
        cursor = con.cursor()
        cursor.execute(delete_data_query % (str(start_id), str(end_id)))
        con.commit()


if __name__ == '__main__':
    # Initialize parser
    parser = argparse.ArgumentParser()

    # Adding optional argument
    parser.add_argument("-t", "--table", help="table name")
    parser.add_argument("-fd", "--from_date", help="from date")
    parser.add_argument("-td", "--to_date", help="to date")
    parser.add_argument("-i", "--interval", help="archival interval")
    args = parser.parse_args()

    if args.interval is None:
        interval = 10
    else:
        interval = args.interval

    archiver = DatabaseArchiver()
    archiver.archive_database(args.table, args.from_date, args.to_date, interval)
