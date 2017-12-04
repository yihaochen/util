#!/usr/bin/env python
import time
t0 = time.time()
import sys
from mpi4py import MPI

def enum(*sequential, **named):
    """Handy way to fake an enumerated type in Python
    http://stackoverflow.com/questions/36932/how-can-i-represent-an-enum-in-python
    """
    enums = dict(zip(sequential, range(len(sequential))), **named)
    return type('Enum', (), enums)

# Define MPI message tags
tags = enum('READY', 'DONE', 'EXIT', 'START')

# Initializations and preliminaries
comm = MPI.COMM_WORLD   # get MPI communicator object
size = comm.size        # total number of processes
rank = comm.rank        # rank of this process
status = MPI.Status()   # get MPI status object
name=MPI.Get_processor_name()

def taskpull(worker_fn, tasks, initialize=None, callback=None, print_result=False):
    """
    worker_fn: callable function that take arguments from items in tasks

    tasks: iterator that return arguments for worker_fn

    initialize: callable function that is only executed at the beginning of master process

    callback: callable function that is only executed at the end of master process
    """
    if rank == 0:
        if initialize: initialize()
        # Master process executes code below
        t1 = time.time()
        sys.stdout.write("Timer started\n")
        task_index = 0
        num_workers = size - 1
        closed_workers = 0
        results = {}
        sys.stdout.write("Master starting with %d workers\n" % num_workers)
        while closed_workers < num_workers:
            data = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
            source = status.Get_source()
            tag = status.Get_tag()
            if tag == tags.READY:
                # Worker is ready, so send it a task
                try:
                    task = next(tasks)
                    comm.send(task, dest=source, tag=tags.START)
                    #print("Sending task to worker %03d" % (source))
                except StopIteration:
                    comm.send(None, dest=source, tag=tags.EXIT)
                #if task_index < len(tasks):
                #    comm.send(tasks[task_index], dest=source, tag=tags.START)
                #    print("Sending task %d to worker %d" % (task_index, source))
                #    task_index += 1
                #else:
                #    comm.send(None, dest=source, tag=tags.EXIT)
            elif tag == tags.DONE:
                wname, task, workedtime, result = data
                results[task] = result
                if print_result: pr = result
                else: pr = task
                sys.stdout.write("Worker %03d on %s returned data in %6.1f s: %s\n" %
                        (source, wname, workedtime, pr))
            elif tag == tags.EXIT:
                wname = data
                sys.stdout.write("Worker %03d on %s exited. (%3d/%3d are still working)\n" %
                        (source, wname, num_workers-closed_workers-1, num_workers))
                closed_workers += 1

        sys.stdout.write("Master finishing\n")
        t2 = time.time()
        sys.stdout.write('Total time: %.2f s\n--\ninitialization: %.2f s\nparallel execution: %.2f\n'\
               %  (t2-t0, t1-t0, t2-t1))
        if callback: callback()
        return results

    else:
        # Worker processes execute code below
        wname = MPI.Get_processor_name()
        #sys.stdout.write("I am a worker with rank %03d on %s.\n" % (rank, wname))
        while True:
            comm.send(None, dest=0, tag=tags.READY)
            task = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
            tag = status.Get_tag()

            if tag == tags.START:
                #sys.stdout.write("Worker %03d on %s got a job: %s\n" % (rank, wname, task))
                tw0 = time.time()
                # Do the work here
                if isinstance(task, tuple):
                    result = worker_fn(*task)
                else:
                    result = worker_fn(task)
                workedtime = time.time() - tw0
                comm.send((wname, task, workedtime, result), dest=0, tag=tags.DONE)
            elif tag == tags.EXIT:
                break

        comm.send(wname, dest=0, tag=tags.EXIT)
        return None
