#!/usr/bin/env python

from __future__ import division
import sys
sys.path.append('../RAMCloud/scripts')
import cluster
from optparse import OptionParser
import re

def read_csv_into_list(filenameList):
    """
    Read csv files of floats, concatenate them all into a flat list and return
    them.
    """
    numbers = []
    for filename in filenameList:
        for line in open(filename, 'r'):
            if not re.match('([0-9]+\.[0-9]+) ', line):
                for value in line.split(","):
                    numbers.append(float(value))
    return numbers

def gen_cdf(filenameList, outputFilename):
    """
    Read data values from fileis given by filenameList, and produces a cdf in
    text form.  Each line in the printed output will contain a fraction and a
    number, such that the given fraction of all numbers in the log file have
    values less than or equal to the given number.
    """
    # Read the file into an array of numbers.
    numbers = read_csv_into_list(filenameList)

    # Output to the current file + .cdf
    outfile = open(outputFilename, 'w')

    # Generate a CDF from the array.
    numbers.sort()
    result = []
    outfile.write("%8.4f    %8.3f\n" % (0.0, 0.0))
    outfile.write("%8.4f    %8.3f\n" % (numbers[0], 1/len(numbers)))
    for i in range(1, 100):
        outfile.write("%8.4f    %8.3f\n" % (numbers[int(len(numbers)*i/100)], 
            i/100))
    outfile.write("%8.4f    %8.3f\n" % (numbers[int(len(numbers)*999/1000)], 
        .999))
    outfile.write("%8.4f    %9.4f\n" % (numbers[int(len(numbers)*9999/10000)], 
        .9999))
    outfile.write("%8.4f    %8.3f\n" % (numbers[-1], 1.0))
    outfile.close()
    
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
    parser.add_option('--valueSize', metavar='N', dest='value_size', default=3,
            help='Size in bytes of value to read/write in '
            'GET/SET/PUSH/POP/SADD/SPOP, etc.')
    parser.add_option('--lrange', metavar='N', dest='lrange', default=100,
            help='Get elements [0,lrange] for LRANGE command. Maximum value '
            'is 100000.')
    parser.add_option('--keyspacelen', metavar='N', dest='keyspacelen', 
            default=1,
            help='Execute operations on a random set of keys in the space '
            'from [0,keyspacelen).')
    parser.add_option('--clients', metavar='N', dest='clients',
            help='Comma seperated list of number of clients to benchmark '
            'each operation with.')
    parser.add_option('--totalOps', type=int, default=100000,
            metavar='nOPS', dest='total_ops',
            help='Total number of operations to execute for each test by '
            'all clients.')
    parser.add_option('--outputDir', metavar='DIR', dest='output_dir',
            help='Directory for benchmark output files.')

    (options, args) = parser.parse_args()

    if not options.tests:
        print "ERROR: Must specify tests with --test"
        sys.exit()

    if not options.clients:
        print "ERROR: Must specify number of clients to run with --clients"
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

    client_args = {
        '--valueSize':      options.value_size,
        '--lrange':         options.lrange,
        '--keyspacelen':    options.keyspacelen,
        '--outputDir':      options.output_dir
    }

    # Run tests
    for test in options.tests.split(','):
        client_args['--tests'] = test

        for c in options.clients.split(','):
            if int(c) % 4 == 0:
                cluster_args['num_clients'] = (int(c) / 4)
                client_args['--threads'] = 4
            elif int(c) % 3 == 0:
                cluster_args['num_clients'] = (int(c) / 3)
                client_args['--threads'] = 3
            elif int(c) % 2 == 0:
                cluster_args['num_clients'] = (int(c) / 2)
                client_args['--threads'] = 2
            else:
                cluster_args['num_clients'] = int(c)
                client_args['--threads'] = 1

            client_args['--requests'] = options.total_ops / int(c)

            print "===== TEST: %s CLIENTS: %d (%dx%d) =====" % (test, int(c), 
                    int(cluster_args['num_clients']), 
                    int(client_args['--threads']))

#            cluster.run(client="../ramdis-benchmark/ramdis-benchmark %s" % (flatten_args(client_args)), **cluster_args)

            print ""

    # Parse output files and generate graphs
    for test in options.tests.split(','):
        tvcfile = open("%s/%s_throughput_v_clients.dat" 
                % (options.output_dir, test), 'w')
        for c in options.clients.split(','):
            reqLatFilenameList = []
            execSumFilenameList = []
            for j in range(1, int(c) + 1):
                reqLatFilenameList.append("%s/%s_client%d-%d_reqLatencies.dat" 
                        % (options.output_dir, test, int(c), j))
                execSumFilenameList.append("%s/%s_client%d-%d_execSummary.dat" 
                        % (options.output_dir, test, int(c), j))

            # Output latency CDF
            gen_cdf(reqLatFilenameList, "%s/%s_client%d-all_reqLatencies.cdf" 
                    % (options.output_dir, test, int(c)))

            # Output total throughput
            maxTotalTime = 0.0
            totalOps = 0
            for filename in execSumFilenameList:
                sumDict = {}
                for line in open(filename, 'r'):
                    cols = line.split(' ')
                    sumDict[cols[0]] = cols[1]
                
                if float(sumDict['totalTime']) > maxTotalTime:
                    maxTotalTime = float(sumDict['totalTime'])
                    
                totalOps += int(sumDict['totalOps'])

            tputfile = open("%s/%s_client%d-all_throughput.dat" 
                    % (options.output_dir, test, int(c)), 'w')
            tputfile.write("%.2f\n" % (float(totalOps) / maxTotalTime))
            tputfile.close()

            tvcfile.write("%d %9.2f\n" 
                    % (int(c), float(totalOps) / maxTotalTime))

        tvcfile.close()
