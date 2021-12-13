from opc import OPC
from postgres import psql
try:
    opc_tt = OPC("opc.tcp://localhost:49320/OPCUA/SimulationServer/")
    opc_tt.add_node("ns=2;s=Channel1.Device1.TT1")
    opc_u = OPC("opc.tcp://localhost:49320/OPCUA/SimulationServer/")
    opc_u.add_node("ns=2;s=Channel1.Device1.H1_u")

    opc_tt.create_subscribtion(1)
    opc_u.create_subscribtion(2)
except KeyboardInterrupt:
    opc_tt.delete_subscribtion()
    opc_tt.disconnect()
    opc_u.delete_subscribtion()
    opc_u.disconnect()
    opc_tt.handler.sql.disconnect()
    opc_u.handler.sql.disconnect()