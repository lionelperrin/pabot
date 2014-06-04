#!/usr/bin/env python

#  Copyright 2014 Nokia Solutions and Networks Oyj
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import os, sys, time, datetime, math
import multiprocessing
from glob import glob
from StringIO import StringIO
import shutil
import subprocess
from robot import run, rebot
from robot import __version__ as ROBOT_VERSION
from robot.api import ExecutionResult
from robot.errors import Information
from robot.result.visitor import ResultVisitor
from multiprocessing.pool import ThreadPool
from robot.run import USAGE
from robot.utils import ArgumentParser
import signal
import random
from contextlib import contextmanager

CTRL_C_PRESSED = False

@contextmanager
def _acquire_resource(resources):
    if resources is not None:
        r = resources.get()
        yield r
        resources.put(r)
    else:
        yield None

def _quote_cmd(cmd):
    return [c if ' ' not in c else '"%s"' % c for c in cmd]

def execute_and_wait_with(args):
    global CTRL_C_PRESSED
    if CTRL_C_PRESSED:
        # Keyboard interrupt has happened!
        return
    time.sleep(0)
    datasources, outs_dir, options, suite_name_list, command, verbose, shared_resources = args
    datasources = [d.encode('utf-8') if isinstance(d, unicode) else d for d in datasources]
    for suite_name in suite_name_list:
        outs = os.path.join(outs_dir, suite_name)
        os.makedirs(outs)
        with open(os.path.join(outs, 'stdout.txt'), 'w') as stdout, \
             open(os.path.join(outs, 'stderr.txt'), 'w') as stderr, \
             _acquire_resource(shared_resources) as resource:
            cmd_resources = ["%s" % (resource)] if resource else []
            cmd = _quote_cmd (command ) \
                  + _quote_cmd( _options_for_custom_executor(options, outs, suite_name) ) \
                  + cmd_resources \
                  + _quote_cmd( datasources )
            process, rc = _run(cmd, stderr, stdout, suite_name, verbose)
        if rc != 0:
            print _execution_failed_message(suite_name, process, rc, verbose)
        else:
            print 'PASSED %s' % suite_name
   
def _run(cmd, stderr, stdout, suite_name, verbose):
    process = subprocess.Popen(' '.join(cmd),
                               shell=True,
                               stderr=stderr,
                               stdout=stdout)
    if verbose:
        print '[PID:%s] EXECUTING PARALLEL SUITE %s with command:\n%s' % (process.pid, suite_name, ' '.join(cmd))
    else:
        print '[PID:%s] EXECUTING %s' % (process.pid, suite_name)
    rc = None
    while rc is None:
        rc = process.poll()
        time.sleep(0.1)
    return process, rc

def _execution_failed_message(suite_name, process, rc, verbose):
    if not verbose:
        return 'FAILED %s' % suite_name
    return 'Execution failed in %s with %d failing test(s)' % (suite_name, rc)

def _options_for_custom_executor(*args):
    return _options_to_cli_arguments(_options_for_executor(*args))

def _options_for_executor(options, outs_dir, suite_name):
    options = options.copy()
    options['log'] = 'NONE'
    options['report'] = 'NONE'
    options['suite'] = suite_name
    options['outputdir'] = outs_dir
    options['monitorcolors'] = 'off'
    if ROBOT_VERSION >= '2.8':
        options['monitormarkers'] = 'off'
    return options

def _options_to_cli_arguments(opts):
    res = []
    for k, v in opts.items():
        if isinstance(v, str):
            res += ['--' + str(k), str(v)]
        elif isinstance(v, unicode):
            res += ['--' + str(k), v.encode('utf-8')]
        elif isinstance(v, bool) and (v is True):
            res += ['--' + str(k)]
        elif isinstance(v, list):
            for value in v:
                if isinstance(value, unicode):
                    res += ['--' + str(k), value.encode('utf-8')]
                else:
                    res += ['--' + str(k), str(value)]
    return res

class GatherSuiteNames(ResultVisitor):

      def __init__(self):
          self.result = []

      def end_suite(self, suite):
          if len(suite.tests):
             self.result.append(suite.longname)

def get_suite_names(output_file):
    if not os.path.isfile(output_file):
       return []
    try:
       e = ExecutionResult(output_file)
       gatherer = GatherSuiteNames()
       e.visit(gatherer)
       return gatherer.result
    except:
       return []

def _pre_compute_distrib( suite_names, processes ):
    lists = [ list() for i in range(processes) ]
    for i, s in enumerate(suite_names):
        lists[i%processes].append( s )
    return lists

def _read_resources( filename ):
    with open(filename, 'r') as f:
        return [i.strip() for i in f.readlines() if i.strip() != '' and not i.strip().startswith('#')]

def _parse_args(args):
    pabot_args = {'command':['pybot'],
                  'verbose':False,
                  'processes':max(multiprocessing.cpu_count(), 2),
                  'load_balancing':True,
                  'seed':None,
                  'resources':[],
                  }
    while args and args[0] in ['--command', '--processes', '--verbose', '--no_load_balancing', '--randomize_suites', '--resource_file']:
        if args[0] == '--command':
            end_index = args.index('--end-command')
            pabot_args['command'] = args[1:end_index]
            args = args[end_index+1:]
        if args[0] == '--processes':
            pabot_args['processes'] = int(args[1])
            args = args[2:]
        if args[0] == '--verbose':
            pabot_args['verbose'] = True
            args = args[1:]
        if args[0] == '--no_load_balancing':
            pabot_args['load_balancing'] = False
            args = args[1:]
        if args[0] == '--randomize_suites':
            try:
                #if next argument is a seed, read it
                pabot_args['seed'] = int(args[1])
                args = args[2:]
            except ValueError:
                pabot_args['seed'] = random.randint(0, sys.maxint)
                args = args[1:]
        if args[0] == '--resource_file':
            pabot_args['resources'] = _read_resources(args[1])
            args = args[2:]

    if pabot_args['resources'] and pabot_args['processes'] > len(pabot_args['resources']):
        pabot_args['processes'] = len(pabot_args['resources'])
        print 'reducing the number of processes to %d: the number of resources' % (pabot_args['processes'])
    options, datasources = ArgumentParser(USAGE, auto_pythonpath=False, auto_argumentfile=False).parse_args(args)
    keys = set()
    for k in options:
        if options[k] is None:
            keys.add(k)
    for k in keys:
        del options[k]
    return options, datasources, pabot_args

def solve_suite_names(outs_dir, datasources, options):
    opts = _options_for_dryrun(options, outs_dir)
    run(*datasources, **opts)
    output = os.path.join(outs_dir, opts['output'])
    suite_names = get_suite_names(output)
    if os.path.isfile(output):
        os.remove(output)
    return suite_names

def _options_for_dryrun(options, outs_dir):
    options = options.copy()
    options['log'] = 'NONE'
    options['report'] = 'NONE'
    if ROBOT_VERSION >= '2.8':
        options['dryrun'] = True
    else:
        options['runmode'] = 'DryRun'
    options['output'] = 'suite_names.xml'
    options['outputdir'] = outs_dir
    options['stdout'] = StringIO()
    options['stderr'] = StringIO()
    options['monitorcolors'] = 'off'
    if ROBOT_VERSION >= '2.8':
        options['monitormarkers'] = 'off'
    return options

def _options_for_rebot(options, datasources, start_time_string, end_time_string):
    rebot_options = options.copy()
    rebot_options['name'] = options.get('name', ', '.join(datasources))
    rebot_options['starttime'] = start_time_string
    rebot_options['endtime'] = end_time_string
    rebot_options['output'] = rebot_options.get('output', 'output.xml')
    rebot_options['monitorcolors'] = 'off'
    if ROBOT_VERSION >= '2.8':
        options['monitormarkers'] = 'off'
    return rebot_options

def _now():
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

def _print_elapsed(start, end):
    elapsed = end - start
    millis = int((elapsed * 1000) % 1000)
    seconds = int(elapsed) % 60
    elapsed_minutes = (int(elapsed)-seconds)/60
    minutes = elapsed_minutes % 60
    elapsed_hours = (elapsed_minutes-minutes)/60
    elapsed_string = ''
    if elapsed_hours > 0:
        elapsed_string += '%d hours ' % elapsed_hours
    elapsed_string += '%d minutes %d.%d seconds' % (minutes, seconds, millis) 
    print 'Elapsed time: '+elapsed_string

def keyboard_interrupt(*args):
    global CTRL_C_PRESSED
    CTRL_C_PRESSED = True
    
def _create_queue( resources ):
    q = multiprocessing.Queue( len(resources) )
    for e in resources:
        q.put( e )
    return (q, resources)

def _parallel_execute(datasources, options, outs_dir, pabot_args, suite_names):
    if suite_names:
        original_signal_handler = signal.signal(signal.SIGINT, keyboard_interrupt)
        pool = ThreadPool(pabot_args['processes'])
        if pabot_args['seed']:
            rand = random.Random( pabot_args['seed'] )
            rand.shuffle( suite_names )
        if pabot_args['load_balancing']:
            shared_resources = (None, None)
            if pabot_args['resources']:
                shared_resources = _create_queue( pabot_args['resources'] )
            suite_names_distrib = [([i], shared_resources) for i in suite_names]
        else:
            shared_resources = [ (None, None) ] * pabot_args['processes']
            if pabot_args['resources']:
                assert( pabot_args['processes'] == len(pabot_args['resources']) )
                shared_resources = [ _create_queue([e]) for e in pabot_args['resources'] ]
            suite_names_distrib = zip( _pre_compute_distrib( suite_names, pabot_args['processes'] ), shared_resources )

        if pabot_args['verbose']:
            print 'Parallel execution of suites: '
            for (suite, (resources_queue, resources_names)) in suite_names_distrib:
                print '- %s' % (str(suite) if len(suite) > 1 else suite[0]),
                if resources_names:
                    print "using resource %s" % ("from %s" % str(resources_names) if len(resources_names) > 1 else "'%s'" % resources_names[0]),
                print
        
        result = pool.map_async(execute_and_wait_with,
                   [(datasources,
                     outs_dir,
                     options,
                     suite,
                     pabot_args['command'],
                     pabot_args['verbose'],
                     resources_queue)
                    for (suite, (resources_queue, resources_names)) in suite_names_distrib])
        while not result.ready():
            # keyboard interrupt is executed in main thread and needs this loop to get time to get executed
            try:
                time.sleep(0.1)
            except IOError:
                keyboard_interrupt()
        pool.close()
        pool.join()
        result.get() #throw exception from workers if any
        signal.signal(signal.SIGINT, original_signal_handler)

def _output_dir(options):
    outputdir = '.'
    if 'outputdir' in options:
        outputdir = options['outputdir']
    outpath = os.path.join(outputdir, 'pabot_results')
    if os.path.isdir(outpath):
        shutil.rmtree(outpath)
    return outpath

def main(args):
    start_time = time.time()
    start_time_string = _now()
    #NOTE: timeout option
    try:
        options, datasources, pabot_args = _parse_args(args)
        outs_dir = _output_dir(options)
        suite_names = solve_suite_names(outs_dir, datasources, options)
        _parallel_execute(datasources, options, outs_dir, pabot_args, suite_names)
        print "Merging test results."
        sys.exit(rebot(*sorted(glob(os.path.join(outs_dir, '**/*.xml'))),
                       **_options_for_rebot(options, datasources, start_time_string, _now())))
    except Information, i:
        print """A parallel executor for Robot Framework test cases.

Supports all Robot Framework command line options and also following options (these must be before normal RF options):

--verbose
more output

--command [ACTUAL COMMANDS TO START ROBOT EXECUTOR] --end-command
RF script for situations where pybot is not used directly

--processes [NUMBER OF PROCESSES]
How many parallel executors to use (default max of 2 and cpu count)

--no_load_balancing 
pre-compute the distribution of the suites among the workers. 
This option gives a reproduceable distribution but the total execution time may increase in a significant way.

--randomize_suites [SEED]
randomize suites execution order using an optional seed argument

--resource_file [FILENAME]
use FILENAME to declare resources for the workers 
   for instance, if workers require a servername variable, FILENAME can be defined with this content:
   --variable servername:server1
   --variable servername:server2
   --variable servername:server3
"""
        print i.message
    finally:
        _print_elapsed(start_time, time.time())


if __name__ == '__main__':
    main(sys.argv[1:])
