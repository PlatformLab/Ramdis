#!/usr/bin/env python

from __future__ import division
import sys
sys.path.append('../RAMCloud/scripts')
import os
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

if __name__ == '__main__':
    parser = OptionParser(description=
            'Run Ramdis performance benchmarks on a RAMCloud cluster.',
            usage='%prog [options]',
            conflict_handler='resolve')
    # Cluster options
    parser.add_option('--servers', type=int, default=4,
            metavar='N', dest='num_servers',
            help='Number of hosts on which to run servers')
    parser.add_option('--replicas', type=int, default=3,
            metavar='N', dest='replicas',
            help='Number of disk backup copies for each segment')
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
    parser.add_option('--tests', metavar='OPS', dest='tests',
            help='Comma seperated list of operations to benchmark.')
    parser.add_option('--serverSpan', metavar='N', dest='server_span',
            default=1,
            help='Number of RAMCloud servers to use for the workload.')
    parser.add_option('--valueSize', metavar='N', dest='value_size', default=3,
            help='Size in bytes of value to read/write in '
            'GET/SET/PUSH/POP/SADD/SPOP, etc.')
    parser.add_option('--lrangelen', metavar='N', dest='lrange_len', default=100,
            help='Get elements [0,lrangelen] for LRANGE command. Maximum value '
            'is 100000.')
    parser.add_option('--keyspacelen', metavar='N', dest='key_space_len', 
            default=1,
            help='Execute operations on a random set of keys in the space '
            'from [0,keyspacelen).')
    parser.add_option('--clients', metavar='N', dest='clients',
            help='Comma seperated list of number of clients to benchmark '
            'each operation with.')
    parser.add_option('--totalOps', type=int,
            metavar='nOPS', dest='total_ops',
            help='Total number of operations to execute for each test by '
            'all clients.')
    parser.add_option('--perClientOps', type=int,
            metavar='cOPS', dest='per_client_ops',
            help='Total number of operations to execute for each test by '
            'each clients.')
    parser.add_option('--outputDir', metavar='DIR', dest='output_dir',
            help='Directory for benchmark output files.')

    (options, args) = parser.parse_args()

    if not options.tests:
        print "ERROR: Must specify tests with --test"
        sys.exit()

    if not options.clients:
        print "ERROR: Must specify number of clients to run with --clients"
        sys.exit()

    if not options.total_ops and not options.per_client_ops:
        print "ERROR: Must specify either --totalOps or --perClientOps"
        sys.exit()

    cluster_args = {
        'num_servers': options.num_servers,
        'replicas':    options.replicas,
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

#    theoutputdir = "s%dr%d_ss%svs%dlrl%dksl%d" % ()

    client_args = {
        '--valueSize':      options.value_size,
        '--lrangelen':      options.lrange_len,
        '--keyspacelen':    options.key_space_len,
        '--outputDir':      options.output_dir
    }

    # Create output directory if it doesn't exist
    if not os.path.exists(options.output_dir):
        os.makedirs(options.output_dir)

    # Run tests
    for test in options.tests.split(','):
        client_args['--tests'] = test

        for c in options.clients.split(','):
            if int(c) % 4 == 0:
                cluster_args['num_clients'] = int(int(c) / 4)
                client_args['--threads'] = 4
            elif int(c) % 3 == 0:
                cluster_args['num_clients'] = int(int(c) / 3)
                client_args['--threads'] = 3
            elif int(c) % 2 == 0:
                cluster_args['num_clients'] = int(int(c) / 2)
                client_args['--threads'] = 2
            else:
                cluster_args['num_clients'] = int(c)
                client_args['--threads'] = 1
           
            if not options.per_client_ops:
                client_args['--requests'] = int(options.total_ops / int(c))
            else:
                client_args['--requests'] = options.per_client_ops

            totalOps = 0
            if not options.per_client_ops:
                totalOps = options.total_ops
            else:
                totalOps = options.per_client_ops * int(c)

            print "===== TEST: %s CLIENTS: %d (%dx%d) OPS: %s =====" % (test, 
                    int(c), 
                    int(cluster_args['num_clients']), 
                    int(client_args['--threads']),
                    totalOps)

            cluster.run(client="../ramdis-benchmark/ramdis-benchmark %s" % (flatten_args(client_args)), **cluster_args)

            print ""
