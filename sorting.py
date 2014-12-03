from Queue import Empty
from threading import Thread
from uuid import uuid4

__author__ = 'phillip'

import multiprocessing
from collections import namedtuple



JobSpec = namedtuple("JobSpec", ("id", "length", "segments", "lock", "finished"))
ListSpec = namedtuple("ListSpec", ("start", "list"))

manager = multiprocessing.Manager()
joblist = manager.dict()

currentjobids = set()
reqq = None

def merge(l1, l2):
    """Merges two sorted lists together
    :param l1: list - first list to sort
    :param l2: list - second list to sort
    :rtype : list
    :return: the merge list
    """

    #Reverse the lists
    l1 = list(reversed(l1))
    l2 = list(reversed(l2))

    ret = []

    while True:
        # If either list is empty, reverse the other one and append it to the end
        if not l1:
            ret.extend(reversed(l2))
            return ret
        if not l2:
            ret.extend(reversed(l1))
            return ret

        # Append the lowest last element of the two lists
        ret.append(l1.pop() if l1[-1] < l2[-1] else l2.pop())

def calculatestartend(ls):
    start = ls.start
    end = ls.start + len(ls.list) - 1
    start *= 10
    end *= 10
    start -= 5
    end += 5
    return start, end

def submitlist(jb, ls):
    """
    Submits a list to the job's segment dictionary
    :param jb: The JobSpec for the job
    :param ls: The ListSpec for the segment
    """
    segstart, segend = calculatestartend(ls)  # Get the segment id for the current segment
    seg = None
    opp = None
    with jb.lock:  # Lock the segments dictionary
        segments = jb.segments
        if segstart in segments:
            seg, opp = segments.pop(segstart, None)
        elif segend in segments:
            seg, opp = segments.pop(segend, None)
        if seg:
            segments.pop(opp)
        else:
            segments[segstart] = (ls, segend)
            segments[segend] = (ls, segstart)
    if seg:
        reqq.put(("merge", (ls, seg)), )

def mergesegements(jb, ls1, ls2):
    ret = merge(ls1.list, ls2.list)
    if len(ret) == jb.length:
        joblist[jb.id] = ret
        jb.finished.set()
    else:
        start = ls1.start
        start = start if start < ls2.start else ls2.start
        rls = ListSpec(start, ret)
        reqq.put(("submit", (rls, )), )

def jobdispatch(jb):
    event = jb.finished
    while not event.is_set():
        try:
            req, args = reqq.get(timeout=1)
        except Empty:

            continue
        if req == "merge":
            mergesegements(jb, *args)
        elif req == "submit":
            submitlist(jb, *args)



def upsidedownmerge(l, processes=multiprocessing.cpu_count()):
    global reqq
    id = uuid4()
    while id in currentjobids:
        id = uuid4()
    currentjobids.add(id)

    reqq = multiprocessing.Queue()

    event = manager.Event()
    jb = JobSpec(id, len(l), manager.dict(), manager.Lock(), event)
    pool = []
    for i in xrange(processes):
        pool.append(multiprocessing.Process(target=jobdispatch, args=(jb, )).start())

    i = 0
    for p in getsortedpairs(l):
        ls = ListSpec(i, p)
        reqq.put(("submit", (ls, )))
        i += 2
    jb.finished.wait()
    for proc in pool:
        proc.join()
    currentjobids.remove(id)
    return joblist.pop(id, None)

def getsortedpairs(itr):
    itr = iter(itr)
    while True:
        p1 = itr.next()
        try:
            p2 = itr.next()
        except StopIteration:
            yield [p1]
        if p1 < p2:
            yield [p1, p2]
        else:
            yield [p2, p1]

pool = multiprocessing.Pool()