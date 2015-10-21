import os
import fnmatch
import glob
import itertools as itr
import pprint
import numpy as np
import logging
import operator
from subprocess import call

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s [%(module)-8s|%(processName)-13s] %(levelname)-5s%(message)s")
log = logging.getLogger(__name__)



# =============================================================================
# objects
# =============================================================================

class fileBase(object):
    def __init__(self, pathname, filename):
        self.filename   = filename
        self.pathname   = pathname
        self.fullpath   = os.path.join(pathname,filename)
        try:
            self.SN         = findSN(filename)
            self.SNs        = findSNs(filename)
            self.R          = findR(filename)
            self.Run        = findRun(filename)
        except:
            pass

    def __str__(self):
        return self.filename


# =============================================================================
# reading from disk
# =============================================================================

def scan_files(path, regex='*', walk=False, exclude=None, printlist=False,\
               reverse=False):
    '''
    Walk through all the sub-directories in 'path' and find out the files
    matching 'regex.'  Return a list of 'virusFitsFile' object.
    '''
    files = []
    log.debug('-Scanning for %s in %s' % (regex, path))

    # Walk through all sub-directories
    if walk:
        for pathname, dirnames, filenames in os.walk(path):
            regex = regex if isinstance(regex, list) else [regex]
            for reg in regex:
                for filename in fnmatch.filter(filenames, reg):
                    if os.path.getsize(os.path.join(pathname,filename))>0:
                        files.append(fileBase(pathname, filename))
    # List only files in the current directory
    else:
        regex = regex if isinstance(regex, list) else [regex]
        for reg in regex:
            for filename in fnmatch.filter(os.listdir(path), reg):
                if os.path.getsize(os.path.join(path,filename))>0:
                    files.append(fileBase(path, filename))

    files = sorted(files, key=lambda f:f.fullpath, reverse=reverse)
    log.debug('-Total %i files found' % len(files))

    if printlist:
        if len(files) < 10:
            for f in files:
                print files.index(f), f
        else:
            print 0, files[0]
            print 1, files[1]
            print '.\n.\n.'
            print files.index(files[-3]), files[-3]
            print files.index(files[-2]), files[-2]
            print files.index(files[-1]), files[-1]

    return files


def listfile(datapath, pattern):
    outfiles = []
    for filename in fnmatch.filter(os.listdir(datapath), pattern):
        outfiles.append( (findSN(filename), findR(filename), os.path.join(datapath, filename)) )
    return sorted(outfiles)


#def listfile(datapath, pattern):
#    """
#    List the file matching pattern in datapath,
#    return a list of tuple (SN, R, filename).
#    """
#    outfiles = []
#    for filename in fnmatch.filter(os.listdir(datapath), pattern):
#        if os.path.getsize(filename)>0:
#            outfiles.append( (findSN(filename), findR(filename), os.path.join(datapath, filename)) )
#    return sorted(outfiles)


def read_data(data_path, x_column, y_column):
    table = np.genfromtxt(data_path)
    xx = table[:,x_column]    # first column is true e
    yy = table[:,y_column]    # second column is annz e

    return xx,yy


def openfile(save_path):
    from socket import gethostname
    if 'bellatrix' in gethostname():
        if save_path.split('.')[-1] == 'ps': call(['gv', save_path])
        elif save_path.split('.')[-1] == 'pdf': call(['okular', save_path])
        elif save_path.split('.')[-1] == 'png': call(['gwenview', save_path])


def iter_Attr(files, attr):
    dic = {}
    for f in files:
        if getattr(f,attr) in dic:
            dic[getattr(f,attr)].append(f)
        else:
            dic[getattr(f,attr)] = [f]
    return [(key,dic[key]) for key in sorted(dic.iterkeys())]
    return dic.items()


# =============================================================================
# misc tools
# =============================================================================

def linear(xx, a, b):
        return [ a*x+b for x in xx]


def findSN(filename):
    fnsplit = filename[:-4].split('_')
    for seq in fnsplit:
        if 'SN' in seq:
            return int(seq.lstrip('SN'))


def findSNs(filename):
    base = filename[filename.rfind('/')+1:filename.find('R')]
    SNs = base.split('_')[:-1]
    SNs = [int(SN.lstrip('SN')) for SN in SNs if SN.strip('SN').isdigit()]
    return SNs


def findR(filename):
    ind = filename.rfind('R')
    R = filename[ind+1:-4]
    try:
        return float(R)
    except:
        # R could be "dist"
        return R
    return None


def findRun(filename):
    ind = filename.rfind('Run')
    if ind > 0:
        Run = filename[ind+3:].split('_')[0]
    else:
        return None
    try:
        return int(Run)
    except:
        return None


def split(s, seps):
    res = [s]
    for sep in seps:
        s, res = res, []
        for seq in s:
            res += seq.split(sep)
    return res


def pr(obj):
    """Pretty print data 'obj'."""
    pprint.pprint(obj)


def calc_mc_rms(xx1, yy1, xx2, yy2, yy1e=None, yy2e=None):
    import scipy.optimize as optimize
    par1, cov1 = optimize.curve_fit(linear, xx1, yy1, p0=[1.0,0.001], sigma=yy1e)
    par2, cov2 = optimize.curve_fit(linear, xx2, yy2, p0=[1.0,0.001], sigma=yy2e)
    #print '# m1, m1e, m2, m2e, c1, c1e, c2, c2e, rms1, rms2'
    return par1[0]-1, np.sqrt(cov1[0,0]), par2[0]-1, np.sqrt(cov2[0,0]), par1[1], np.sqrt(cov1[1,1]), par2[1], np.sqrt(cov2[1,1]), np.sqrt(np.mean((yy1-xx1)**2)), np.sqrt(np.mean((yy2-xx2)**2))


