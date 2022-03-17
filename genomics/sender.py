# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""An example Flight CLI client."""

import multiprocessing
import threading
from multiprocessing import Pool
from multiprocessing.pool import ThreadPool

import collections
from datetime import datetime
import socket

import argparse
import sys
import glob
import time
import gc
import psutil

import pyarrow
import pyarrow.flight
import pyarrow.csv as csv

#################################################
CHRMS=['1', '110', '120', '130', '140', '150','160', '170', '180', '190', '2', '210', '220', '230', '240', '250', '260', '270', '280', '290', '3', '31', '32', '33', '34','35','36','37', '4', '41', '42', '43', '44','45','46','47', '5', '51', '52', '53', '54','55','56','57', '6', '61', '62', '63', '64','65','66', '7', '71', '72', '73','74','75','76', '8', '81', '82', '83','84','85', '9', '91', '92', '93','94','95', '10', '101', '102', '103','104','105', '11', '111', '112', '113','114','115', '12', '121', '122', '123','124','125', '13', '131', '132','133','134', '14', '141', '142','143', '15', '151', '152','153', '16', '161', '162','163', '17', '171', '172', '18', '181', '182', '19', '191', '20', '201','202', '21', '211', '22', '221', '23', '231', '232', '233','234','235', '24', '241', '25']
################################################
################################################
## getting the hostname by socket.gethostname() method
host = socket.gethostname()
#################################################

def print_mem_usage(event):

    mem_usage = "{:.2f}".format(psutil.virtual_memory().available / (1024 ** 3))
    log_msg = "["+ host +" - "+ datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "]: " + event + ", available memory: " + mem_usage + " GB"
    print(log_msg)
    with open(args.path+"mem_profiler.log", "a") as f:
        f.write(log_msg + "\n")
################################################
def list_flights(args, client, connection_args={}):
    print('Flights\n=======')
    for flight in client.list_flights():
        descriptor = flight.descriptor
        if descriptor.descriptor_type == pyarrow.flight.DescriptorType.PATH:
            print("Path:", descriptor.path)
        elif descriptor.descriptor_type == pyarrow.flight.DescriptorType.CMD:
            print("Command:", descriptor.command)
        else:
            print("Unknown descriptor type")

        #print("Total records:", end=" ")
        if flight.total_records >= 0:
            print(flight.total_records)
        else:
            print("Unknown")

        #print("Total bytes:", end=" ")
        if flight.total_bytes >= 0:
            print(flight.total_bytes)
        else:
            print("Unknown")

        print("Number of endpoints:", len(flight.endpoints))
        print("Schema:")
        print(flight.schema)
        print('---')

    print('\nActions\n=======')
    for action in client.list_actions():
        print("Type:", action.type)
        print("Description:", action.description)
        print('---')


def do_action(args, client, connection_args={}):
    try:
        buf = pyarrow.allocate_buffer(0)
        action = pyarrow.flight.Action(args.action_type, buf)
        print('Running action', args.action_type)
        for result in client.do_action(action):
            print("Got result", result.body.to_pybytes())
    except pyarrow.lib.ArrowIOError as e:
        print("Error calling action:", e)


def push_data(args, client, connection_args={}):
    print('File Name:', args.file, ' has been sent to ', args.host)
    with pyarrow.RecordBatchFileReader(args.file) as reader:
        my_table = reader.read_all()
    #my_table = csv.read_csv(args.file)
    #print('Table rows=', str(len(my_table)))
    #df = my_table.to_pandas()
    #print(df.head())
    writer, _ = client.do_put(
        pyarrow.flight.FlightDescriptor.for_path(args.file), my_table.schema)
    writer.write_table(my_table)
    writer.close()

##################################################################################
def flight_transfers(node_index):
    choromosomes=CHRMS[node_index*int(N):(node_index*int(N))+int(N)]
    print(choromosomes)
    for choromo in choromosomes:
        paths=sorted(glob.glob(args.path+'arrow/'+nodesdirs[INDEX-1]+'/'+choromo+"_chr" + '*.arrow')) #Single file always, <= change it
        for path in paths:
            push_data_to_server(path,node_index)

            #with open(log_file, "a") as f:
            #    f.write("[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "]: ")
            #    f.write("File: " + path + " is being sent to " + nodeslist[node_index+1] + 
            #            " : with id= "+choromo+" \n")

def push_data_to_server(path,node_index):
    #print('File Name:', path, ' has been sent.', nodeslist[node_index+1])
     
    with pyarrow.RecordBatchFileReader(path) as reader:
        table = reader.read_all()
    #my_table = csv.read_csv(args.file)
    #print('Table rows=', str(len(my_table)))
    #df = my_table.to_pandas()
    #print(df.head())
    writer, _ = get_client(node_index).do_put(
        pyarrow.flight.FlightDescriptor.for_path(path), table.schema)
    writer.write_table(table)
    writer.close()

    #with open(log_file, "a") as f:
    #    f.write("[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "]: ")
    #    f.write("File: " + path + " has been sent to " + nodeslist[node_index+1] +
    #            " : with id= "+choromo+" \n")

    print('File Name:', path, ' has been sent.', nodeslist[node_index+1])

def get_client(node_index):
    host=  nodeslist[node_index+1]
    port = 32108
    scheme = "grpc+tcp"
    connection_args = {}
    if args.tls:
        scheme = "grpc+tls"
        if args.tls_roots:
            with open(args.tls_roots, "rb") as root_certs:
                connection_args["tls_root_certs"] = root_certs.read()
    if args.mtls:
        with open(args.mtls[0], "rb") as cert_file:
            tls_cert_chain = cert_file.read()
        with open(args.mtls[1], "rb") as key_file:
            tls_private_key = key_file.read()
        connection_args["cert_chain"] = tls_cert_chain
        connection_args["private_key"] = tls_private_key
    return pyarrow.flight.FlightClient(f"{scheme}://{host}:{port}", **connection_args)

##################################################################################
def get_flight(args, client, connection_args={}):
    if args.path:
        descriptor = pyarrow.flight.FlightDescriptor.for_path(*args.path)
    else:
        descriptor = pyarrow.flight.FlightDescriptor.for_command(args.command)

    info = client.get_flight_info(descriptor)
    print(info)
    print(info.endpoints)
    for endpoint in info.endpoints:
        print('Ticket:', endpoint.ticket)
        for location in endpoint.locations:
            print(location)
            get_client = pyarrow.flight.FlightClient(location,
                                                     **connection_args)
            reader = get_client.do_get(endpoint.ticket)
            df = reader.read_pandas()
            print(df)


def _add_common_arguments(parser):
    parser.add_argument('--tls', action='store_true', help='Enable transport-level security')
    parser.add_argument('--tls-roots', default=None,  help='Path to trusted TLS certificate(s)')
    parser.add_argument("--mtls", nargs=2, default=None, metavar=('CERTFILE', 'KEYFILE'), help="Enable transport-level security")
    #parser.add_argument('host', type=str, help="Address or hostname to connect to")

parser = argparse.ArgumentParser()
requiredNamed = parser.add_argument_group('Required arguments')

requiredNamed.add_argument("-proc",  "--proc",  help="Process number", required=True)
requiredNamed.add_argument("-path",  "--path",  help="Input FASTQ path", required=True)
requiredNamed.add_argument("-nodes",  "--nodes",  help="Number of nodes", required=True)

_add_common_arguments(parser)
#subcommands = parser.add_subparsers()

#cmd_list = subcommands.add_parser('list')
#cmd_list.set_defaults(action='list')
#_add_common_arguments(cmd_list)
#cmd_list.add_argument('-l', '--list', action='store_true',
#                      help="Print more details.")

#cmd_do = subcommands.add_parser('do')
#cmd_do.set_defaults(action='do')
#_add_common_arguments(cmd_do)
#cmd_do.add_argument('action_type', type=str,
#                    help="The action type to run.")

#cmd_put = subcommands.add_parser('put')
#cmd_put.set_defaults(action='put')
#_add_common_arguments(cmd_put)
#cmd_put.add_argument('file', type=str,
#                     help="CSV file to upload.")

#cmd_get = subcommands.add_parser('get')
#cmd_get.set_defaults(action='get')
#_add_common_arguments(cmd_get)
#cmd_get_descriptor = cmd_get.add_mutually_exclusive_group(required=True)
#parser.add_argument('-p', '--path', type=str, action='append', help="The path for the descriptor.")
#parser.add_argument('-c', '--command', type=str, help="The command for the descriptor.")

#####################################
args = parser.parse_args()

#from args
INDEX=int(args.proc)

PARTS=128#16#128
NODES=int(args.nodes)
N=int(PARTS)/int(NODES)

total=0
####################################

nodes = open("/home/tahmad/tahmad/testing/pre/nodeslist.txt", "r")
nodeslist=nodes.read().split(',')
for k in range(len(nodeslist)):
    nodeslist[k] = nodeslist[k].rstrip('\n') #nodeslist[k].replace('.bullx','').rstrip('\n')

nodesdirs=[]
nodes=sorted(glob.glob(args.path+'arrow/*'))
for node in nodes:
    nodesdirs.append(node.split('/')[-1])

##################################

def main():
    '''
    if not hasattr(args, 'action'):
        parser.print_help()
        sys.exit(1)

    commands = {
        'list': list_flights,
        'do': do_action,
        'get': get_flight,
        'put': push_data,
    }
    host, port = args.host.split(':')
    port = int(port)
    scheme = "grpc+tcp"
    connection_args = {}
    if args.tls:
        scheme = "grpc+tls"
        if args.tls_roots:
            with open(args.tls_roots, "rb") as root_certs:
                connection_args["tls_root_certs"] = root_certs.read()
    if args.mtls:
        with open(args.mtls[0], "rb") as cert_file:
            tls_cert_chain = cert_file.read()
        with open(args.mtls[1], "rb") as key_file:
            tls_private_key = key_file.read()
        connection_args["cert_chain"] = tls_cert_chain
        connection_args["private_key"] = tls_private_key
    client = pyarrow.flight.FlightClient(f"{scheme}://{host}:{port}",
                                         **connection_args)
    '''
    #for node in nodeslist:
    #    if (hostname==node):
    #        print(nodeslist.index(hostname))

    p = ThreadPool(NODES) #Equals to number of nodes
    p.map(flight_transfers, range(NODES))
    #flight_transfers(0)
    '''
    while True:
        try:
            action = pyarrow.flight.Action("healthcheck", b"")
            options = pyarrow.flight.FlightCallOptions(timeout=1)
            list(client.do_action(action, options=options))
            break
        except pyarrow.ArrowIOError as e:
            if "Deadline" in str(e):
                print("Server is not ready, waiting...")
    '''
    #commands[args.action](args, client, connection_args)
    #flight_transfers(0)

if __name__ == '__main__':
    time.sleep(10)
    #host_name = socket.gethostname().strip(".bullx")
    #log_file = host_name + "_sender.log"
    print_mem_usage("CLIENT - Before starting Arrow Flight communication.")
    main()
    print_mem_usage("CLIENT - After finishing Arrow Flight communication.")
    #with open(log_file, "a") as f:
    #    f.write("[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "]: ")
    #    f.write(hostname  + " : finished transferes.\n") 
