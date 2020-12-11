#!/usr/bin/python3
#-*- coding: utf-8 -*-
 
#################################################
# Copy Finder : a simple script to find doubles #
#################################################

# 1. FilesCrawler = generate a hash (based on a limited number of bytes) for each file in rootDirectory
# 2. HashsHandler = stores hashs and detect potential doubles
# 3. CopyChecker = calculate a complete hash for each file received and export a list of doubles

__author__ = 'clsergent'
__version__ = '2.1 - 05DEC2020'
__licence__ = 'GPLv3'

import os
import argparse
import multiprocessing
import timeit

QUEUE_END = '\x03\x04'      #signal transmitted to close a queue
HASH_BYTES = 15000      #default number of bytes to read for a partial hash (-1 = EOF)
HASH_FUNCTIONS = ('md5', 'sha', 'default')
REDUCED_LENGTH = 4
SPLIT_SYMBOL = "; "         #default symbol to separate data in the export file
PREFIX_PATH = ''

class Process(multiprocessing.Process):
    """standard process class"""
    def __init__(self, *args, hashFunction=None, **kwds):
        multiprocessing.Process.__init__(self, *args, **kwds)
        
        if hashFunction == 'md5':
            self._hashFunction = self.md5Hash
        
        elif hashFunction == 'sha':
            self._hashFunction = self.shaHash
        
        else:
            self._hashFunction = self.defaultHash
    
    def log(self, *logs, verbose=False):
        """simple log method"""
        if not verbose:
            print("{}:".format(self.name), *logs)
    
    def getHashFunction(self):
        return self._hashFunction
    
    @staticmethod
    def md5Hash(arg):
        importlib.import_module('md5')
        return md5.md5(arg).digest()
    
    @staticmethod
    def shaHash(arg):
        importlib.import_module('sha')
        return sha.sha(arg).digest()
    
    #@staticmethod
    def defaultHash(self, arg):
        return str(hash(arg))
    
    hashFunction = property(getHashFunction)
    

class FilesCrawler(Process):
    """generate a hash based on the first bytes (hashBytes) for each file in rootDirectory"""
    def __init__(self, rootDirectory, hashFunction, hashBytes=-1, **kwds):
        Process.__init__(self, hashFunction= hashFunction)
        
        self._rootDirectory = rootDirectory
        #self._hashFunction =
        self._hashBytes = hashBytes #hash is processed only on the first bytes (-1 -> EOF)
        self._outQueue = multiprocessing.Queue() # queue to export hashs/paths
    
    def getQueue(self):
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
                
                if not os.path.isfile(path):    #avoid non regular files
                    continue
                    
                if fileName[0] == '~':          #remove temporary files (starting by ~)
                    try:
                        os.unlink(path)
                        self.log('removed temporary file {0}'.format(path), verbose=True)
                        continue
                    except:
                        continue
                
                try:                            #avoid empty files (which can't be accurately compared)
                    if os.lstat(path).st_size == 0:
                        continue
                except:
                    continue
                
                try:                            #generate a hash sent to outQueue
                    with open(path, 'rb') as file:
                        hash = self.hashFunction(file.read(self._hashBytes))
                    self._outQueue.put((hash, path))
                except:
                    self.log('an error occured while reading {0}'.format(path))
                else:
                   self.log('{0} -> {1}'.format(path, hash), verbose=True)
                                
            totalFiles += len(files)
            self.log('{0}Controled : {1:8}'.format('\x08'*21, totalFiles))
            
        self._outQueue.put(QUEUE_END)           #close the queue
    
    queue = property(getQueue)

class HashHandler(Process):
    """Process in charge of collecting and filtering hashs"""
    def __init__(self, inQueue, outQueue):
        Process.__init__(self)
        
        self._inQueue = inQueue    	#queue from FilesCrawler
        self._outQueue = outQueue  	#queue to the CopyChecker
        self._hashs= dict()         #dict of hash:[path|True]
        self._reduced = list()      #reduced list of hashs
    
    def run(self):
        """start the process"""
        self.log('pid is {0}'.format(self.pid))
        self.getHashs()
    
    def getHashs(self):
        """retrieve hashs from queue"""
        value = self._inQueue.get()
        while value != QUEUE_END:
            if type(value) is tuple and len(value) == 2:
                hash, path = value
                if hash[:REDUCED_LENGTH] in self._reduced:      #seach in reduced (fast)
                    if self._hashs.get(hash, False):            #search full hash (slow/accurate)
                        if self._hashs[hash]:                   #if a path is found, there is no double yet
                            self._outQueue.put((hash,self._hashs[hash]))    #send the first path
                            self._hashs[hash]= True             #True means that there are doubles already
                            
                        self._outQueue.put((hash,path))         #send the double
                        
                else:
                    self._hashs[hash] = path                    #add a new value
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
    
    def run(self):
        self.log('pid is {0}'.format(self.pid))
        self.checkCopies()
        self.export()
    
    def checkCopies(self):
        value = self._inQueue.get()
        while value != QUEUE_END:
            if type(value) is tuple and len(value) == 2:
                hash, path = value
                
                if not os.path.isfile(path):                    #avoid invalid path (likely deleted file)
                    value = self._inQueue.get()
                    continue
                    
                if os.lstat(path).st_size > self._hashBytes:    #get full hash if file length exceed hashBytes
                    with open(path, 'rb') as file:
                        hash = self._hashFunction(file.read())
                        
                if self._copies.get(hash, False):
                    self._copies[hash].append(path)
                else:                                           #add a new entry if necessary
                    self._copies[hash] = [path]
                    
            else:
                self.log('data received is invalid {0}'.format(value))
                
            value = self._inQueue.get()
            
        self._inQueue.close()
    
    def export(self):
        """export duplicates"""
        with open(self._exportFile, 'w', encoding=self._encoding) as f:
            for value in self._copies.values():
                if len(value) >= 2:     #only write copies (at least two files)
                    line = self._splitSymbol.join([self._prefixPath + v for v in value])
                        
                    if self._separator: #change the separator
                        line = line.replace(os.path.sep, self._separator)
                        
                    f.write('{0}{1}{2}\n'.format(os.lstat(value[0]).st_size, self._splitSymbol, line))
    
    def getQueue(self):
        """return the queue"""
        return self._inQueue
        
    queue = property(getQueue)
    
def getArgs():
    """parse script arguments"""
    parser = argparse.ArgumentParser(description='Script looking for doubles')
    
    #arguments for FilesCrawler process
    parser.add_argument('rootDirectory', type=str, help='Root directory to seach for doubles')
    parser.add_argument('-f', '--hashFunction', type=str, help="hash function to use from list {0}".format(HASH_FUNCTIONS))
    parser.add_argument('-b', '--hashBytes', type=int, help="first hash is performed only on first HASHBYTES")
    
    #arguments for CopyChecker
    parser.add_argument('exportFile', type=str, help="File containing hahs")
    parser.add_argument('-p', '--prefixPath', type=str, help="add a prefix to the paths in exportFile")
    parser.add_argument('-e', '--encoding', type=str, help="encode exportFile using encoding (utf8, latin1)")
    parser.add_argument('-s', '--separator', type=str, help="set a specific pathname separator for data in exportFile")
    parser.add_argument('-S', '--splitSymbol', type=str, help="set a specific symbol to separate data in exportFile")
    
    #general arguments
    parser.add_argument('-g', '--logFile', type=str, help="write log to logFile")
    parser.add_argument('-d', '--daemon', action='store_true', help="run as daemon")
    
    return parser.parse_args()

def checkArgs(args):
    """check args"""
    
    #rootDirectory
    if not os.path.isdir(args.rootDirectory):
        raise ValueError("rootDirectory is invalid")
    
    #exportFile
    if not os.path.isdir(os.path.dirname(args.exportFile)):
        raise ValueError("exportFile is invalid")
    
    if not args.splitSymbol:
        args.splitSymbol = SPLIT_SYMBOL
    
    if not args.hashBytes:
        args.hashBytes = HASH_BYTES
    
    if not args.prefixPath:
        args.prefixPath = PREFIX_PATH
    
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

