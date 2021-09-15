import time
import pyodbc
import pandas 
from pyarrow import flight

##############################################################################################
class DremioClientAuthMiddlewareFactory(flight.ClientMiddlewareFactory):
    """A factory that creates DremioClientAuthMiddleware(s)."""

    def __init__(self):
        self.call_credential = []

    def start_call(self, info):
        return DremioClientAuthMiddleware(self)

    def set_call_credential(self, call_credential):
        self.call_credential = call_credential


class DremioClientAuthMiddleware(flight.ClientMiddleware):
    """
    A ClientMiddleware that extracts the bearer token from 
    the authorization header returned by the Dremio 
    Flight Server Endpoint.
    Parameters
    ----------
    factory : ClientHeaderAuthMiddlewareFactory
        The factory to set call credentials if an
        authorization header with bearer token is
        returned by the Dremio server.
    """

    def __init__(self, factory):
        self.factory = factory

    def received_headers(self, headers):
        auth_header_key = 'authorization'
        authorization_header = []
        for key in headers:
          if key.lower() == auth_header_key:
            authorization_header = headers.get(auth_header_key)
        self.factory.set_call_credential([
            b'authorization', authorization_header[0].encode("utf-8")])

def query_flight(hostname, flightport, username, password, sqlquery,
  tls, certs):
    """
    Connects to Dremio Flight server endpoint with the provided credentials.
    It also runs the query and retrieves the result set.
    """

    try:
        # Default to use an unencrypted TCP connection.
        scheme = "grpc+tcp"
        connection_args = {}

        if tls:
            # Connect to the server endpoint with an encrypted TLS connection.
            print('[INFO] Enabling TLS connection')
            scheme = "grpc+tls"
            if certs:
                print('[INFO] Trusted certificates provided')
                # TLS certificates are provided in a list of connection arguments.
                with open(certs, "rb") as root_certs:
                    connection_args["tls_root_certs"] = root_certs.read()
            else:
                print('[ERROR] Trusted certificates must be provided to establish a TLS connection')
                sys.exit()
 
        # Two WLM settings can be provided upon initial authneitcation
        # with the Dremio Server Flight Endpoint:
        # - routing-tag
        # - routing queue
        initial_options = flight.FlightCallOptions(headers=[
            (b'routing-tag', b'test-routing-tag'),
            (b'routing-queue', b'Low Cost User Queries')
        ])
        client_auth_middleware = DremioClientAuthMiddlewareFactory()
        client = flight.FlightClient("{}://{}:{}".format(scheme, hostname, flightport),
          middleware=[client_auth_middleware], **connection_args)

        # Authenticate with the server endpoint.
        bearer_token = client.authenticate_basic_token(username, password, initial_options)
        print('[INFO] Authentication was successful')

        if sqlquery:
            # Construct FlightDescriptor for the query result set.
            flight_desc = flight.FlightDescriptor.for_command(sqlquery)
            print('[INFO] Query: ', sqlquery)

            # In addition to the bearer token, a query context can also
            # be provided as an entry of FlightCallOptions. 
            # options = flight.FlightCallOptions(headers=[
            #     bearer_token,
            #     (b'schema', b'test.schema')
            # ])

            # Retrieve the schema of the result set.
            options = flight.FlightCallOptions(headers=[bearer_token])
            schema = client.get_schema(flight_desc, options)
            print('[INFO] GetSchema was successful')
            print('[INFO] Schema: ', schema)

            # Get the FlightInfo message to retrieve the Ticket corresponding
            # to the query result set.
            flight_info = client.get_flight_info(flight.FlightDescriptor.for_command(sqlquery),
                options)
            print('[INFO] GetFlightInfo was successful')
            print('[INFO] Ticket: ', flight_info.endpoints[0].ticket)

            # Retrieve the result set as a stream of Arrow record batches.
            reader = client.do_get(flight_info.endpoints[0].ticket, options)
            print('[INFO] Reading query results from Dremio')
            return reader.read_pandas()

    except Exception as exception:
        print("[ERROR] Exception: {}".format(repr(exception)))
        raise
##############################################################################################

host = 'tcn851'#'0.0.0.0'#'localhost'
port = 31010
uid = 'tahmad'
pwd = 'Pakistan1*'
driver = "/opt/dremio-odbc/lib64/libdrillodbc_sb64.so"
#driver = "Dremio ODBC Driver 64-bit"
#ODBC
sql = '''SELECT * FROM Samples."samples.dremio.com"."NYC-taxi-trips" limit 64000000''' #5120000'''

####################################
start = time.time()
####################################

cnxn = pyodbc.connect("Driver={};ConnectionType=Direct;HOST={};PORT={};AuthenticationType=Plain;UID={};PWD={}".format(driver, host,port,uid,pwd),autocommit=True)
#cnxn=pyodbc.connect(dsn='Dremio ODBC 64-bit',autocommit=True)
odbc_cursor=cnxn.cursor()
odbc_cursor.execute(sql+ '-- odbc')
df=odbc_cursor.fetchall()
#out = pandas.read_sql(sql,cnxn)
#print(out)

####################################
stop = time.time()
print('ODBC took {} seconds.'
      .format(stop - start))
##################################################################################
#FLIGHT

####################################
start = time.time()
####################################

port = 32010
out=query_flight(host, port, uid, pwd, sql, False, False)
#print(out)

####################################
stop = time.time()
print('Flight took {} seconds.'
      .format(stop - start))
##################################################################################
#turbodbc

####################################
start = time.time()
####################################

import turbodbc
options = turbodbc.make_options(read_buffer_size=turbodbc.Megabytes(100),
                       parameter_sets_to_buffer=1000,
                       varchar_max_character_limit=10000,
                       use_async_io=True,
                       prefer_unicode=True,
                       autocommit=True,
                       large_decimals_as_64_bit_types=True,
                       limit_varchar_results_to_max=True)

user = 'tahmad'
password= 'Pakistan1*'


driver = "/opt/dremio-odbc/lib64/libdrillodbc_sb64.so"
server = 'localhost'
port = 31010

#print("driver={};server={};port={};database={};uid={};pwd={}".format(driver,host,port,"Dremio ODBC driver 64-bit",uid,pwd))
#turbocxn=turbodbc.connect(dsn='Dremio ODBC 64-bit', turbodbc_options=options)
turbocxn=turbodbc.connect(driver=driver,server=host,port=31010,database="Dremio ODBC 64-bit",uid=uid,pwd=pwd,turbodbc_options=options)#"driver={},server={},port={},database={},uid={},pwd={}".format(driver,host,port,"Dremio ODBC 64-bit",uid,pwd))
#print(turbocxn)

turbocursor=turbocxn.cursor()
turbocursor.execute(sql+ '-- turbodbc')
df=turbocursor.fetchall()
#print(pandas.DataFrame(df))

####################################
stop = time.time()
print('turbODBC took {} seconds.'
      .format(stop - start))
##################################################################################
