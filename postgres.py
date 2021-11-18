import psycopg2
import psycopg2 as pg
from XML import XML
from pprint import pprint

class psql:
    database = ""
    server = ""
    username = ""
    conn = pg
    cur = conn
    pw = None
    connected = False
    error = None

    def __init__(self, database=None, server=None, username=None, pw=None, xmlfile=None):
        """
        Creates a psql object

        :param database: databasename
        :param server: hostaddress
        :param username: username for the database
        """
        if xmlfile == None:
            self.database = database
            self.server = server
            self.username = username
            self.pw = pw

            self.connect(pw)
        else:
            xml = XML(xmlfile)

            self.database = xml.SearchForTag('database')
            self.server = xml.SearchForTag('host')
            self.username = xml.SearchForTag('user')
            self.pw = xml.SearchForTag('pw')

            self.connect()

            del xml


    def connect(self):
        """
        connects the psql object to a database and creates a cursor within the psql object

        :param pw: password
        :return:
        """
        try:
            self.conn = self.conn.connect("dbname = '{0}' user = '{1}' host = '{2}' password = '{3}'".format(
                self.database, self.username, self.server, self.pw))

            self.cur = self.conn.cursor()

            self.connected = True
        except psycopg2.OperationalError as e:
            print(e)


    def reconnect(self):
        try:
            self.conn = self.conn.connect("dbname = '{0}' user = '{1}' host = '{2}' password = '{3}'".format(
                self.database, self.username, self.server, self.pw))

            self.cur = self.conn.cursor()
            print("connected to {0}".format(self.database))
        except:
            print("Failed to connect")

    def disconnect(self):
        """
        disconnects from the database

        :return:
        """
        self.conn.close()
        print("disconnected from {}".format(self.database))


    def insert(self, query):
        self.cur.execute(query)

    def q_select(self, query, params=None):
        """
        Sends a query to the connected database and returns the data

        :param query: string, PostgreSQL query
        :return: object of rows
        """
        rows = []
        if params == None:
            try:
                self.cur.execute(query)
                rows = self.cur.fetchall()
            except pg.errors.InFailedSqlTransaction:
                rows = False

        else:
            try:
                self.cur.execute(query, params)
                rows = self.cur.fetchall()
            except pg.errors.InFailedSqlTransaction:
                rows = False

        return rows

    def send_q(self, query, data=None):
        try:
            self.cur.execute(query, data)
            self.conn.commit()
        except pg.errors.UniqueViolation as e:
            print(e)
            self.sql.conn.rollback()
        except pg.errors.SyntaxError as e:
            print(e)
        except pg.errors.InvalidTextRepresentation as e:
            print(e)




    def transpose_sql_query(self, rows):
        """
        Extracts the 0th and 1st columns from rows and transposes them

        :param rows: cursor object, data returned from a query
        :return: list, 0th and 1st column of rows
        """
        time = []
        val = []
        for row in rows:
            time.append(row[0])
            val.append(row[1])
        data = [time, val]
        return data



class Measurement:
    timestamp = None
    tag_id = None
    value = None

    sql = None

    def connect(self, sql_con):
        self.sql = sql_con

    def upload_values(self):
        self.sql.cur.execute("""Insert into measurement (tag_id, meas_timestamp, meas_value)
                             values(%s,%s,%s)""", (self.tag_id, self.timestamp, self.value))


class Tag:
    """
    Class to hold a process tags information
    """
    tagID = None
    connected_tag = None
    station_id = None
    unit_type = None
    tag = None
    tag_desc = None
    interval = None
    interval_unit = None
    pw = None
    timestamp = None
    measurements = None

    sql = None

    def __init__(self, tagname, database='dmf_trans_db', host='10.129.80.212'):
        """
        Object of a tag

        :param tagname: string, name of tag
        :param database: string, name of database
        :param host: string, name or ip adress of server
        """

        """Connects to database"""
        self.sql = self.connect()

        """Gets metainformation of the given tag"""
        self.get_tag(tagname)
        #print(self.tag + ' initialized')


    def connect(self, xml_file='./DBConnection.xml'):
        """
        Connects to a database using an xml file
        :return: connected psql object
        """

        xml = XML(xml_file)

        database = xml.SearchForTag('database')
        host = xml.SearchForTag('host')
        user = xml.SearchForTag('user')
        pw = xml.SearchForTag('pw')
        sql = psql(database, host, user)
        sql.connect(pw)
        return sql

    def prepare_inserts(self, data):
        """
        Transforms data so that it can be inserted and creates the query for inserting into aggregation

        :param data: 2D list [timestamp, tag_id, value]
        :return: query and data for insertion into aggregation
        """
        #print('timestamp, id, val')
        #print(len(data[0]), len(data[1]), len(data[2]))
        tup_list = []
        for i in range(len(data[0])):
            tup_list.append((data[0][i], data[1][i], data[2][i]))
        #print(len(tup_list))


        records_list_template = ','.join(['%s'] * (len(tup_list)))
        insert_query = 'insert into aggregations(meas_time, tag_id, agg_val) values {}'.format(records_list_template)
        #print(insert_query)
        #print(insert_query.count('(%s, %s, %s)'))
        return insert_query, tup_list




    def upload_meta1(self):
        """
        Uploads meta information (such as attributes of this class) to the connected database

        :return:
        """
        #print(self.tagID, self.connected_tag, self.station_id,
        #                                             self.unit_type, self.tag, self.tag_desc, self.interval)
        #query = "insert into tag(tag_id, station_id, unit_type, tag, tag_description, meas_interval) " \
        #        "values('{}','{}',{},'{}','{}','{}')".format(self.tagID, self.station_id,
        #                                             self.unit_type, self.tag, self.tag_desc, self.interval)
        #Sett til meas_interval
        print(self.tagID, self.tag_desc, self.tag)
        try:

            self.sql.cur.execute("""insert into tag(tag_id, station_id, unit_type, tag, tag_description, meas_interval,
            meas_interval_unit)
            values(%s,%s,%s,%s,%s,%s,%s)
            on conflict (tag_id)
            do update 
                set station_id=%s,
                tag_description=%s
            where tag.tag_id = %s;
            """,(self.tagID, self.station_id, self.unit_type, self.tag, self.tag_desc, self.interval,
                 self.interval_unit, self.station_id, self.tag_desc, self.tagID
                                                ))
        except pg.errors.UniqueViolation:
            #self.sql.cur.tr
            self.sql.conn.rollback()


        #self.sql.insert(query)

    def upload_meta2(self):
        """
        Uploads foreign keys to the database for this tag
        :return:
        """
        self.sql.cur.execute("""update tag set FK_tag_id = %s where tag_id = %s;""",
                             (self.connected_tag, self.tagID))
        print(self.connected_tag, self.tagID)

    def update_meta(self):
        """
        Updates a tags name, description and tag id
        :return:
        """
        self.sql.cur.execute("""update tag set tag = %s, tag_description = %s where tag_id = %s;""",
                             (self.tag, self.tag_desc, self.tagID))

    def get_tag(self, tagname):
        """
        Gets meta information of a tag from table tag in database and sets properties in the Tag object

        :param tagname: string, name of tag
        :return:
        """

        query = """select * from tag where tag = '{}'""".format(tagname)
        rows = self.sql.q_select(query)

        self.tagID = rows[0][0]
        self.connected_tag = rows[0][1]
        self.station_id = rows[0][2]
        self.unit_type = rows[0][3]
        self.tag = tagname
        self.tag_desc = rows[0][5]
        self.interval = rows[0][6]

    def get_measurement(self, time_from, time_to, table='measurement'):
        """
        Gets measurement for the given time period and tag

        :param time_from: string or datetime, Start time for dataset
        :param time_to: string or datetime, Stop time for dataset
        :return:
        """
        query = """select distinct meas_time, meas_value
                from {}
                where tag_id = '{}' and meas_time >= '{}' and meas_time < '{}'
                order by meas_time asc;""".format(table, self.tagID, time_from, time_to)

        rows = self.sql.q_select(query)
        self.timestamp = []
        self.measurements = []
        for row in rows:
            self.timestamp.append(row[0])
            self.measurements.append(float(row[1]))


    def append_measurement(self, time_from, time_to, table='measurement'):
        """
        Gets measurement for the given time period and tag

        :param time_from: string or datetime, Start time for dataset
        :param time_to: string or datetime, Stop time for dataset
        :return:
        """
        query = """select distinct meas_time, meas_value
                from {}
                where tag_id = '{}' and meas_time >= '{}' and meas_time < '{}'
                order by meas_time asc;""".format(table, self.tagID, time_from, time_to)

        rows = self.sql.q_select(query)

        for row in rows:
            self.timestamp.append(row[0])
            self.measurements.append(float(row[1]))

    def get_avg_measurement(self, time_from, time_to, agg_time, aggregation, table='measurement'):
        """
        Gets measurement for the given time period and tag

        :param time_from: string or datetime, Start time for dataset
        :param time_to: string or datetime, Stop time for dataset
        :return:
        """

        query = """
        SELECT time_bucket('{} minutes', meas_time) AS avg_min, {}(meas_value)
        FROM {}
        where tag_id = %s and meas_time between %s and %s
        GROUP BY avg_min
        ORDER BY avg_min asc;""".format(agg_time, aggregation, table)

        rows = self.sql.q_select(query, (self.tagID, time_from, time_to))
        self.timestamp = []
        self.measurements = []
        for row in rows:
            self.timestamp.append(row[0])
            self.measurements.append(float(row[1]))

if __name__ == '__main__':
    sql = psql(xmlfile="./DBconnection.xml")
    print(sql.q_select("""Select * from tag"""))