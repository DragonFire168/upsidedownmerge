from random import randint
from sorting import upsidedownmerge

__author__ = 'phillip'

def main():
    print "Generating test data"
    data = [randint(0, 100) for i in xrange(50000)]
    print "Starting sort"
    sorteddata = upsidedownmerge(data)
    print "Sort finished"
    print "List was sorted: ", checksorted(sorteddata)

def checksorted(l):
    last = l[0]
    for i in l:
        if i < last:
            return False
    return True

if __name__ == "__main__":
    main()
