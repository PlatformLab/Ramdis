#!/usr/bin/env python

from __future__ import division
import sys
sys.path.append('../RAMCloud/scripts')
import os
from os import makedirs
from os.path import join, exists
import cluster
from optparse import OptionParser
import re
    
def flatten_args(args):
    """
    Given a dictionary of arguments, produce a string suitable for inclusion
    in a command line, such as "--name1 value1 --name2 value2"
    """
    return " ".join(["%s %s" % (name, value)
            for name, value in args.iteritems()])

def runExperiment(options, servers, replicas, serverSpan, valueSize,
        keySpaceLen, test, clients):
    # Formulate a directory name based on the experiment parameters
    dataDir = "s%dr%d_ss%dvs%dksl%d" % (
            servers,
            replicas,
            serverSpan,
            valueSize,
            keySpaceLen)
    
    if not exists(join(options.output_dir, dataDir)):
        makedirs(join(options.output_dir, dataDir))

    cluster_args = {
        'num_servers': servers,
        'replicas':    replicas,
        'backup_disks_per_server': options.backup_disks_per_server,
        'disjunct':    options.disjunct,
        'transport':   options.transport,
        'timeout':     options.timeout,
        'debug':       options.debug,
        'log_dir':     options.log_dir,
        'log_level':   options.log_level,
        'share_hosts': True,
        'verbose':     options.verbose
    }

    if options.master_args != None:
        cluster_args['master_args'] = options.master_args

    client_args = {
        '--serverSpan': serverSpan,
        '--valueSize': valueSize,
        '--keyspacelen': keySpaceLen,
        '--tests': test,
        '--timeLimit': options.time_limit,
        '--outputDir': join(options.output_dir, dataDir)
    }

    if clients % 4 == 0:
        cluster_args['num_clients'] = int(clients / 4)
        client_args['--threads'] = 4
    elif clients % 3 == 0:
        cluster_args['num_clients'] = int(clients / 3)
        client_args['--threads'] = 3
    elif clients % 2 == 0:
        cluster_args['num_clients'] = int(clients / 2)
        client_args['--threads'] = 2
    else:
        cluster_args['num_clients'] = clients
        client_args['--threads'] = 1

    if not options.per_client_ops:
        client_args['--requests'] = int(options.total_ops / clients)
    else:
        client_args['--requests'] = options.per_client_ops

    totalOps = 0
    if not options.per_client_ops:
        totalOps = options.total_ops
    else:
        totalOps = options.per_client_ops * clients

    print "Running: s=%d, r=%d, ss=%d, vs=%d, ksl=%d, test=%s, c=%d (%dx%d), to=%d ..." % (servers, replicas, serverSpan, valueSize, keySpaceLen, test, clients, cluster_args['num_clients'], client_args['--threads'], totalOps),
    sys.stdout.flush()

    cluster.run(client="../ramdis-benchmark/ramdis-benchmark %s" % (flatten_args(client_args)), **cluster_args)

    print ""


if __name__ == '__main__':
    parser = OptionParser(description=
            'Run Ramdis performance benchmarks on a RAMCloud cluster.',
            usage='%prog [options]',
            conflict_handler='resolve')
    # Cluster options
    parser.add_option('--servers', default='4',
            metavar='N', dest='num_servers',
            help='Comma separated list of number of hosts on which to run '
                 'servers')
    parser.add_option('--replicas', default='3',
            metavar='N', dest='replicas',
            help='Comma separated list of number of disk backup copies for '
                 'each segment')
    parser.add_option('--numBackupDisks', type=int, default=2,
            metavar='N', dest='backup_disks_per_server',
            help='Number of backup disks to use on each server host '
                 '(0, 1, or 2)')
    parser.add_option('--disjunct', action='store_true', default=False,
            metavar='True/False',
            help='Do not colocate clients on a node (servers are never '
                  'colocated, regardless of this option)')
    parser.add_option('--masterArgs', metavar='mARGS',
            dest='master_args',
            help='Additional command-line arguments to pass to '
                 'each master')
    parser.add_option('--transport', default='basic+infud',
            dest='transport',
            help='Transport to use for communication with servers')
    parser.add_option('--timeout', type=int, default=30,
            metavar='SECS',
            help="Abort if the client application doesn't finish within "
                 'SECS seconds')
    parser.add_option('--debug', action='store_true', default=False,
            help='Pause after starting servers but before running '
                 'clients to enable debugging setup')
    parser.add_option('--logDir', default='logs', metavar='DIR',
            dest='log_dir',
            help='Top level directory for log files; the files for '
                 'each invocation will go in a subdirectory.')
    parser.add_option('--logLevel', default='NOTICE',
            choices=['DEBUG', 'NOTICE', 'WARNING', 'ERROR', 'SILENT'],
            metavar='L', dest='log_level',
            help='Controls degree of logging in servers')
    parser.add_option('--verbose', action='store_true', default=False,
            help='Print progress messages')

    # Ramdis benchmark options
    parser.add_option('--tests',
            default='get,set,incr,lpush,rpush,lpop,rpop,lrange',
            metavar='OPS', dest='tests',
            help='Comma separated list of operations to benchmark.')
    parser.add_option('--serverSpan', default='1',
            metavar='N', dest='server_span',
            help='Comma separated list of number of RAMCloud servers to use '
                 'for the workload.')
    parser.add_option('--valueSize', default='3',
            metavar='N', dest='value_size', 
            help='Comma separated list of size in bytes of value to '
                 'read/write in GET/SET/PUSH/POP/SADD/SPOP, etc.')
    parser.add_option('--lrangelen', default='100',
            metavar='N', dest='lrange_len', 
            help='Comma separated list of ranges [0,lrangelen] for LRANGE '
                 'command. Maximum value is 100000.')
    parser.add_option('--keyspacelen', default='1',
            metavar='N', dest='key_space_len', 
            help='Comma separated list of key space sizes. Will make '
                 'operations execute on a random set of keys in the space '
                 'from [0,keyspacelen).')
    parser.add_option('--clients', default='1',
            metavar='N', dest='clients',
            help='Comma seperated list of number of clients to benchmark '
                 'each operation with.')
    parser.add_option('--totalOps', 
            metavar='nOPS', dest='total_ops', type=int,
            help='Total number of operations to execute for each test by '
                 'all clients.')
    parser.add_option('--perClientOps', 
            metavar='cOPS', dest='per_client_ops', type=int,
            help='Total number of operations to execute for each test by '
                 'each clients.')
    parser.add_option('--timeLimit', default=20,
            metavar='L', dest='time_limit', type=int,
            help='Time limit, in seconds, for clients to run. Clients will '
                 'finish executing if they reach this time limit without '
                 'completing their request quota.')
    parser.add_option('--outputDir', 
            metavar='DIR', dest='output_dir',
            help='Directory for benchmark output. Benchmark will create a '
            'subdirectory named according to the experiment parameters where '
            'data files will be stored.')

    (options, args) = parser.parse_args()

    # Check options
    if not options.total_ops and not options.per_client_ops:
        print "ERROR: Must specify either --totalOps or --perClientOps"
        sys.exit()

    if options.total_ops and options.per_client_ops:
        print "ERROR: Must specify either --totalOps or --perClientOps, not "
        "both"

    # Create output directory if it doesn't exist
    if not exists(options.output_dir):
        makedirs(options.output_dir)

    # Convert parameter-sweep type options from csvs to lists
    serversList = [int(servers) for servers in options.num_servers.split(',')]
    replicasList = [int(replicas) for replicas in options.replicas.split(',')]
    serverSpanList = [int(serverSpan) for serverSpan in options.server_span.split(',')]
    valueSizeList = [int(valueSize) for valueSize in options.value_size.split(',')]
    lrangeLenList = [int(lrangeLen) for lrangeLen in options.lrange_len.split(',')]
    keySpaceLenList = [int(keySpaceLen) for keySpaceLen in options.key_space_len.split(',')]
    testList = options.tests.split(',')
    clientsList = [int(clients) for clients in options.clients.split(',')]

    for servers in serversList:
        for replicas in replicasList:
            # Sanity checks
            if servers < replicas + 1: 
                print "ERROR: Cannot execute (servers=%s,replicas=%s) "
                "parameter combination, servers must be at least 1 greater "
                "than replicas" % (num_servers, replicas)
                continue

            for serverSpan in serverSpanList:
                for valueSize in valueSizeList:
                    for keySpaceLen in keySpaceLenList:
                        for test in testList:
                            for clients in clientsList:
                                runExperiment(options, servers, replicas,
                                        serverSpan, valueSize, keySpaceLen,
                                        test, clients)

