import sys
import logging
from opcua import Client, ua
from postgres import psql
sys.path.insert(0, "..")
import datetime as dt
from pytictoc import TicToc

#https://www.psycopg.org/docs/sql.html
class SubHandler(object):
    """
    Client to subscription. It will receive events from server
    """
    psql = psql(xmlfile='./DBconnection.xml')
    t = TicToc()
    print(type(psql))
    def __init__(self, id):
        self.id = id
    def datachange_notification(self, node, val, data):
        self.t.tic()
        self.psql.send_q("""insert into measurement values (%s, %s, %s)""", (self.id, dt.datetime.now(), val))
        self.t.toc()
    def event_notification(self, event):
        print("Python: New event", event)


class OPC:
    def __init__(self, url):
        logging.basicConfig(level=logging.WARN)
        self.client = Client(url)
        self.client.connect()

    def add_node(self, node):
        self.node = self.client.get_node(node)

    def send(self, value):
        dv = ua.DataValue(ua.Variant(value, ua.VariantType.Double))
        dv.ServerTimestamp = None
        dv.SourceTimestamp = None
        print(value)
        self.node.set_value(dv)

    def create_subscribtion(self, id):
        handler = SubHandler(id)
        self.sub = self.client.create_subscription(100, handler)
        self.handle = self.sub.subscribe_data_change(self.node)

    def delete_subscribtion(self, ):
        self.sub.unsubscribe(self.handle)
        self.sub.delete()

    def disconnect(self):
        self.client.disconnect()