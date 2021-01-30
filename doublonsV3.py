#!/usr/bin/python3
 
#########################################################
# DuplicatesFinder : a simple script to find duplicates #
#########################################################

# 1. FilesCrawler = generate a hash (based on a limited number of bytes) for each file in rootDirectory
# 2. HashsHandler = stores hashes and detect potential duplicates
# 3. DuplicateChecker = calculate a complete hash for each file received and export a list of duplicates

__author__ = 'clsergent'
__version__ = '2.2 - 14DEC2020'
__licence__ = 'EUPL1.2'

import os
import argparse
import multiprocessing
import hashlib
import timeit

QUEUE_END = '\x03\x04'      # signal transmitted to close a queue
HASH_BYTES = 15000          # default number of bytes to read for a partial hash (-1 = EOF)
HASH_BLOCK_SIZE = 65536     # max length to feed the hash function in a row
HASH_FUNCTIONS = hashlib.algorithms_guaranteed
REDUCED_LENGTH = 4          # number of bytes taken from hash to build a faster dictionnary
SPLIT_SYMBOL = "; "         # default symbol to separate data in the csv file
PREFIX_PATH = ''            # default prefix added to each path in the csv file (used for relative paths)


class Process(multiprocessing.Process):
    """standard process class"""
    def __init__(self, *args, hashFunction='md5', **kwds):
        multiprocessing.Process.__init__(self, *args, **kwds)
        
        if hasattr(hashlib, hashFunction):
            self._hashFunction = getattr(hashlib, hashFunction)
        else:
            raise ValueError("invalid hash function supplied")

    @property
    def hashFunction(self):
        return self._hashFunction

    def log(self, *logs, verbose=False):
        """simple log method"""
        if not verbose:
            print("{}:".format(self.name), *logs)
    

class FilesCrawler(Process):
    """generate a hash based on the first bytes (hashBytes) for each file in the root directory"""
    def __init__(self, rootDirectory, hashFunction, hashBytes=-1, **kwds):
        Process.__init__(self, hashFunction= hashFunction)
        
        self._rootDirectory = rootDirectory
        self._hashBytes = hashBytes # hash is processed only on the first bytes (-1 -> EOF)
        self._outQueue = multiprocessing.Queue() # queue to export hashs/paths

    @property
    def queue(self):
        """return the queue"""
        return self._outQueue
    
    def run(self):
        """start the process"""
        self.log('pid is {0}'.format(self.pid))
        self.walk()
    
    def walk(self):
        """generate hash for files in rootDirectory"""
        totalFiles=0
        for root, dirs, files in os.walk(self._rootDirectory):
            for fileName in files:
                path = os.path.join(root,fileName)

                # avoid non regular files
                if not os.path.isfile(path): continue

                # avoid empty files (which can't be accurately compared)
                try:
                    if os.lstat(path).st_size == 0: continue
                except:
                    continue

                # generate a hash sent to outQueue
                try:
                    hasher = self.hashFunction()
                    with open(path, 'rb') as file:
                        hasher.update(file.read(self._hashBytes))
                    self._outQueue.put((hasher.digest(), path))
                except:
                    self.log('an error occurred while reading {0}'.format(path))
                else:
                   self.log('{0} -> {1}'.format(path, hash), verbose=True)
                                
            totalFiles += len(files)
            self.log('{0} controlled : {1:8}'.format('\x08'*21, totalFiles))
            
        self._outQueue.put(QUEUE_END)           #close the queue


class HashHandler(Process):
    """Process in charge of collecting and filtering hashs"""
    def __init__(self, inQueue, outQueue):
        Process.__init__(self)
        
        self._inQueue = inQueue    	#queue from FilesCrawler
        self._outQueue = outQueue  	#queue to the CopyChecker
        self._hashes= dict()         #dict of hash:[path|True]
        self._reduced = list()      #reduced list of hashs
    
    def run(self):
        """start the process"""
        self.log('pid is {0}'.format(self.pid))
        self.getHashs()
    
    def getHashs(self):
        """retrieve hashes from queue"""
        value = self._inQueue.get()
        while value != QUEUE_END:
            if type(value) is tuple and len(value) == 2:
                hash, path = value
                if hash[:REDUCED_LENGTH] in self._reduced:      #seach in reduced (fast)
                    if self._hashes.get(hash, False):            #search full hash (slow/accurate)
                        if self._hashes[hash]:                   #if a path is found, there is no duplicate yet
                            self._outQueue.put((hash,self._hashes[hash]))    #send the first path
                            self._hashes[hash]= True             #True means that there are doubles already
                            
                        self._outQueue.put((hash,path))         #send the double
                        
                else:
                    self._hashes[hash] = path                    #add a new value
                    self._reduced.append(hash[:REDUCED_LENGTH])
                    
            else:
                self.log('data received is invalid {0}'.format(value))
                
            value = self._inQueue.get()
            
        self._inQueue.close()
        self._outQueue.put(QUEUE_END)


class CopyChecker(Process):
    """process in charge of verifying and exporting duplicates"""
    def __init__(self, exportFile, splitSymbol, hashFunction, hashBytes= -1, prefixPath= '', encoding= None, separator= None, **kwds):
        Process.__init__(self, hashFunction= hashFunction)
        
        self._exportFile = exportFile
        self._splitSymbol = splitSymbol
        self._hashBytes = hashBytes
        self._prefixPath = prefixPath
        self._encoding = encoding
        self._separator = separator
        
        self._copies = dict()                       #copies to export
        self._inQueue = multiprocessing.Queue()     #queue to the CopyChecker

    @property
    def queue(self):
        """return the queue"""
        return self._inQueue

    def run(self):
        self.log('pid is {0}'.format(self.pid))
        self.checkCopies()
        self.export()
    
    def checkCopies(self):
        """execute an complete check over potential duplicates"""
        value = self._inQueue.get()
        while value != QUEUE_END:
            if type(value) is tuple and len(value) == 2:
                hash, path = value

                # avoid invalid path (likely deleted file)
                if not os.path.isfile(path):
                    value = self._inQueue.get()
                    continue

                # get full hash if file length exceed hashBytes
                if os.lstat(path).st_size > self._hashBytes:
                    hasher = self.hashFunction()
                    with open(path, 'rb', buffering= False) as file:
                        while data := file.read(HASH_BLOCK_SIZE):
                            hasher.update(data)
                        hash = hasher.digest()

                # append the file or create a new entry
                if self._copies.get(hash, False):
                    self._copies[hash].append(path)
                else:
                    self._copies[hash] = [path]
                    
            else:
                self.log('data received is invalid {0}'.format(value))
                
            value = self._inQueue.get()
            
        self._inQueue.close()
    
    def export(self):
        """export duplicates"""
        with open(self._exportFile, 'w', encoding=self._encoding) as f:
            for value in self._copies.values():

                # only write copies (at least two files)
                if len(value) >= 2:
                    # add the prefix
                    line = self._splitSymbol.join([self._prefixPath + v for v in value])

                    # if requested, change the separator (for cross-platform purposes (e.g. SMB)
                    if self._separator:
                        line = line.replace(os.path.sep, self._separator)
                        
                    f.write('{0}{1}{2}\n'.format(os.lstat(value[0]).st_size, self._splitSymbol, line))

    
def getArgs():
    """parse script arguments"""
    parser = argparse.ArgumentParser(description='Script looking for doubles')
    
    # arguments for FilesCrawler process
    parser.add_argument('rootDirectory', type=str, help='root directory to search for duplicates')
    parser.add_argument('-f', '--hashFunction', type=str, help="hash function to use from list {0}".format(HASH_FUNCTIONS))
    parser.add_argument('-b', '--hashBytes', type=int, help="number of bytes used for the first hash")
    
    # arguments for CopyChecker
    parser.add_argument('exportFile', type=str, help="csv file filled with duplicates info")
    parser.add_argument('-p', '--prefixPath', type=str, help="a prefix added to the paths in exportFile")
    parser.add_argument('-e', '--encoding', type=str, help="encoding used to encode exportFile (utf8, latin1)")
    parser.add_argument('-s', '--separator', type=str, help="specific pathname separator for data in exportFile")
    parser.add_argument('-S', '--splitSymbol', type=str, help="specific symbol to separate data in exportFile")
    
    # general arguments
    # parser.add_argument('-g', '--logFile', type=str, help="log file")
    parser.add_argument('-d', '--daemon', action='store_true', help="run as daemon")
    
    return parser.parse_args()


def checkArgs(args):
    """check args from argparse"""
    
    # rootDirectory
    if not os.path.isdir(args.rootDirectory):
        raise ValueError("rootDirectory is invalid")
    
    # exportFile
    if not os.path.isdir(os.path.dirname(args.exportFile)):
        raise ValueError("exportFile is invalid")
    
    if not args.splitSymbol:
        args.splitSymbol = SPLIT_SYMBOL
    
    if not args.hashBytes:
        args.hashBytes = HASH_BYTES
    
    if not args.prefixPath:
        args.prefixPath = PREFIX_PATH
    
    if not args.hashFunction:
        args.hashFunction = 'md5'
    
    return args

        
def run():
    """run the script"""
    args = checkArgs(getArgs())
    
    crawler = FilesCrawler(**args.__dict__)
    checker = CopyChecker(**args.__dict__)
    handler = HashHandler(crawler.queue, checker.queue)
    
    crawler.start()
    handler.start()
    
    if args.daemon:
        checker.start()
    else:
        checker.run()


if __name__ == '__main__':
    timer = timeit.Timer('run()', 'from __main__ import run')
    print('\nTime elapsed: {0}'.format(timer.timeit(1)))

