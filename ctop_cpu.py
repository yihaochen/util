#! /usr/bin/env python
# Author: Yi-Hao Chen
# Email: ychen@astro.wisc.edu
# Date: 2014-04-24

import sys
import time
import curses
from subprocess import Popen, PIPE
import multiprocessing


################################################################
######## Variables #############################################

default_cluster = 'heinzs'
cluster = {}
cluster['lazarian'] = ['node%02i'%n for n in range( 1,16)]
cluster['zweibel']  = ['node%02i'%n for n in range(17,24)]
cluster['townsend'] = ['node%02i'%n for n in range(25,48)]
cluster['heinzs']   = ['node%02i'%n for n in range(49,73)]
cluster['all']      = ['node%02i'%n for n in range( 1,73)] 

# Command to be executed. Run top for 2 iterations to get the reliable cpu usage.
command = "ssh %s -o ConnectTimeout=2 -o ServerAliveInterval=2 'top -b -d 0.1 -n 2 || true'"

# Number of header lines in top output
nhead = 5
# Number of processes to be extracted in top output
nproc = 8
# Number of bars used for percentage display
nbars = 40


################################################################
######## Helper Functions ######################################

def print_help():
    msg = "Usage: ctop [ cluster definition | hostfile | node name ]\n"
    msg+= "       Default cluster: %s (%s~%s)" % (default_cluster, cluster[default_cluster][0] , cluster[default_cluster][-1])
    print(msg)

def remove_dup(list):
    new_list = []
    for item in list:
        if item not in new_list:
            new_list.append(item)
    return new_list

def read_host(argv):
    try:
        return cluster[argv]
    except KeyError:
        try:
            with open(argv) as f:
                nodes = []
                for l in f.readlines():
                    nodes.append(l[:6])
            nodes = remove_dup(nodes)
            return nodes
        except IOError:
            try:
                return ['node%02i' % int(argv)]
            except:
                return [argv]

def cut_last(lines):
    init_line = 0
    for j, line in enumerate(lines):
        if 'top - ' in line.decode('utf8'):
            init_line = j
    return [line.decode('utf8') for line in lines[init_line:]]

def get_bars(perc):
    bars =  "|" * int((float(perc) / 100 * nbars))
    empty_bars = " " * (nbars - len(bars))
    return bars, empty_bars

def cpu_usage_bar(lines):
    perc = lines[2][7:12]
    bars, empty_bars = get_bars(perc)
    return '[%s%s] CPU:%5s%%' %  (bars, empty_bars, perc)

def mem_usage(lines):
    total = lines[3].split()[1]
    used = lines[3].split()[3]
    return 'MEM: %6s / %6s' % (used, total)

def swap_usage(lines):
    total = lines[4].split()[1]
    used = lines[4].split()[3]
    return 'SWAP: %6s / %6s' % (used, total)

def get_n_tasks(lines):
    # 'top' is always running, but we don't count it
    n_tasks = int(lines[1].split()[3]) - 1
    return 'Tasks: %2i running' %  (n_tasks)

def trim_top(lines, ntasks):
    out = ''
    for l in lines[:nhead+2+ntasks]:
        out = out + l
    return out

def get_tasks_names(lines):
    n_tasks = int(lines[1].split()[3])
    tasks = {}
    for line in lines[nhead+2:nhead+2+n_tasks]:
        user = line.split()[1]
        running = line.split()[7]
        name = line.split()[11]
        if running == 'R' and name != 'top':
            if user not in tasks: tasks[user] = {}
            tasks[user][name] = tasks[user][name] + 1 if name in tasks[user] else 1
    if len(tasks) == 0:
        out = ''
    else:
        out = '('
        for user, names in tasks.items():
            out = out + user + ':'
            for name, n in names.items():
                s = name if n == 1 else name + '*' + str(n)
                out = out + s + ', '
        out = out.rstrip(', ') + ')'
    return out


class Ctop:

    def __init__(self, nodes):
        self.lock = multiprocessing.Lock()
        self.nodes = nodes
        self.window = curses.initscr()
        self.window.erase()
        self.window.refresh()

    def single_node_display(self, node, y):
        cmd = command % (node)
        p = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        self.lock.acquire()
        rtn = p.wait()
        # If the return code is not 0, some errors occured.
        if rtn != 0:
            self.window.addstr(y+1, 0, '%s: (%i)%s\n' % (node, rtn, p.stderr.read().strip() ), curses.A_BOLD)
        else:
            lines = cut_last(p.stdout.readlines())
            self.window.addstr(y+1, 0, '%s: %s %s %s %s %s\n' % (node, cpu_usage_bar(lines), mem_usage(lines), swap_usage(lines),
                              get_n_tasks(lines), get_tasks_names(lines)))
        self.window.refresh()
        self.window.move(0,0)
        self.window.refresh()
        self.lock.release()

    def run(self):
        self.jobs = []
        for i, node in enumerate(self.nodes):
            child = multiprocessing.Process(target=self.single_node_display, args=(node, i))
            self.jobs.append(child)
            child.start()
        for job in self.jobs:
            job.join()

        if len(self.nodes) == 1:
            cmd = command % (node)
            p = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE)
            rtn = p.wait()
            lines = cut_last(p.stdout.readlines())
            self.window.addstr(2, 0, trim_top(lines, nproc))
        self.window.refresh()

    def terminate(self):
        for job in self.jobs:
            job.terminate()


################################################################
######## Major Work ############################################


def main(argv):
    if len(argv) == 1:
        nodes = cluster[default_cluster]
    elif argv[1] == '-h' or argv[1] == '--help':
        print_help()
        exit()
    else:
        nodes = read_host(argv[1])

    ctop = Ctop(nodes)
    try:
        for i in range(10):
           ctop.run()
    except KeyboardInterrupt:
        ctop.terminate()

    finally:
        curses.endwin()

if __name__ == '__main__':
    main(sys.argv)
