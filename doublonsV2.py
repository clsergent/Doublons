#!/usr/bin/python
#-*- coding: utf-8 -*-
 
import os
import argparse
import md5
import multiprocessing
import timeit

QUEUE_END = '\x03\x04'
QUEUE_SEP = '\x29'

CHECKSUM_BYTES = 15000

SPLIT_SYMBOL = "; "

class Logger(object):
    def __init__(self, logFile=None):
        self._logFile = logFile
    
    def log(self, log, enabled=True):
        if self._logFile and enabled:
            if type(self._logFile) != str:
                print type(self._logFile), self._logFile
                return
            try:
                with open(self._logFile, 'a') as f:
                    f.write('{0}: {1}\n'.format(self.name, log))
            except:
                print 'failed to log: {0}'.format(log)

class ChecksumBot(multiprocessing.Process, Logger):
    """process in charge of retrieving checksum of files in rootDirectory (and subdirectories)"""
    def __init__(self, rootDirectory, logFile=None, checksumBytes=-1):
        multiprocessing.Process.__init__(self)
        Logger.__init__(self, logFile)
        if rootDirectory and os.path.isdir(rootDirectory):
            self._rootDirectory = rootDirectory
        else:
            self.log("Root path provided is invalid")
            raise ValueError, "Root path provided is invalid"

        self._checksumBytes = checksumBytes #checksum only on the x bytes (-1 -> until EOF)
        self._outQueue = multiprocessing.Queue() # queue to export checksums/paths
    
    def getQueue(self):
        """return the queue"""
        return self._outQueue
    
    def run(self):
        """start the process"""
        self.log(str(self.pid))
        self.walk()
    
    def walk(self):
        total=0
        for root, dirs, files in os.walk(self._rootDirectory):
            for f in files:
                path = os.path.join(root,f)
                if not os.path.isfile(path):
                    continue
                
                try:    # supprimer les fichiers temporaires
                    if os.path.basename(path)[0] == '~':
                        os.unlink(path)
                except:
                    pass
                
                try:
                    if os.lstat(path).st_size == 0:   # ne pas traiter les fichiers vides
                        continue
                except:
                    pass
                
                try:
                    with open(path, 'rb') as ff:
                        checksum = md5.md5(ff.read(self._checksumBytes)).digest()
                    self._outQueue.put((checksum, path))
                except:
                    self.log('an error occured while parsing {0}'.format(root))
                else:
                   self.log('{0} -> {1}'.format(path, checksum), False)
                                
            total+= len(files)
            out = '\x08'*21
            out += 'Controled : {0:8}'.format(total)
            print out,
        self._outQueue.put(QUEUE_END) #close the queue
    
    queue = property(getQueue)

class ChecksumsHandler(multiprocessing.Process, Logger):
    """Process in charge of collecting and filtering checksums"""
    def __init__(self, inQueue, outQueue, logFile=None):
        multiprocessing.Process.__init__(self)
        Logger.__init__(self, logFile)
        
        self._inQueue = inQueue    	#queue from ChecksumBot
        self._outQueue = outQueue  	#queue to the CopyChecker
        self._checksums=dict()      #dict of checksum:[path|None]
        self._reduced = list()      #reduced list of checksums
    
    def run(self):
        """start the process"""
        self.log(str(self.pid))
        self.getChecksums()
    
    def getChecksums(self):
        """retrieve checksums from queue"""
        value = self._inQueue.get()
        while value != QUEUE_END:
            try:
                checksum, path = value
                if checksum[:4] in self._reduced:
                    if self._checksums.has_key(checksum):#checksum in self._reduced:
                        if self._checksums[checksum]:
                            self._outQueue.put((checksum,self._checksums[checksum]))
                            self._checksums[checksum]= None
                        self._outQueue.put((checksum,path))
                else:
                    self._checksums[checksum] = path
                    self._reduced.append(checksum[:4])
                    
            except ValueError:
                self.log('an error occured searching {0}'.format(value))
                
            value = self._inQueue.get()
        self._outQueue.put(QUEUE_END)
        self._inQueue.close()
        del self._checksums
        del self._reduced

class CopyChecker(multiprocessing.Process, Logger):
    """process in charge of verifying and exporting duplicates"""
    def __init__(self, exportFile, splitSymbol, logFile= None, checksumBytes= -1, prefixPath= None, encode=None, separator= None):
        multiprocessing.Process.__init__(self)
        Logger.__init__(self, logFile)
        
        if exportFile:
            self._exportFile = exportFile
        else:
            self.log("Provided export file is invalid")
            raise ValueError, "No valid export file path has been provided"
        
        self._exportFile = exportFile
        self._splitSymbol = splitSymbol
        self._inQueue = multiprocessing.Queue()  #queue to the CopyChecker
        self._copies=dict()
        self._checksumBytes = checksumBytes
        if not prefixPath:
            self._prefixPath = ''
        else:
            self._prefixPath = prefixPath
        self._encode= encode
        self._separator= separator
    
    def run(self):
        self.log(str(self.pid))
        self.checkCopies()
        self.export()
    
    def checkCopies(self):
        value = self._inQueue.get()
        while value != QUEUE_END:
            try:
                checksum, path = value
                if not os.path.isfile(path):
                    value = self._inQueue.get()
                    continue
                if os.lstat(path).st_size > self._checksumBytes: #get full checksum if necessary
                    with open(path, 'r') as f:
                        checksum = md5.md5(f.read()).digest()
                if self._copies.has_key(checksum):
                    self._copies[checksum].append(path)
                else:
                    self._copies[checksum] = [path]
                    
            except:# ValueError:
                self.log('an error occured while checking {0}'.format(value))
            else:
                self.log('{0} has been added to the list of potential doubles'.format(path), False)
                
            value = self._inQueue.get()
        self._inQueue.close()
    
    def export(self):
        """export duplicates"""
        with open(self._exportFile, 'w') as f:
            for value in self._copies.values():
                if len(value) >= 2:
                    out = self._splitSymbol.join([self._prefixPath + v for v in value])+'\n'
                    if self._encode:
                        out= out.decode('utf-8').encode(self._encode, 'ignore')
                    if self._separator:
                        out= out.replace(os.path.sep, self._separator)
                    f.write('{0}{1}{2}'.format(os.lstat(value[0]).st_size, self._splitSymbol, out))
    
    def getQueue(self):
        """return the queue"""
        return self._inQueue
        
    queue = property(getQueue)


def initParser():
    """initialization of the parser"""
    parser = argparse.ArgumentParser(description='Script looking for duplicates')
    
    parser.add_argument('rootDirectory', type=str, help='Root directory to seach for duplicates')
    parser.add_argument('exportFile', type=str, help="File containing md5 sums")
    parser.add_argument('-g', '--logFile', type=str, help="write log to logFile")
    parser.add_argument('-b', '--checksumBytes', type=int, help="first checksum is performed only on first CHECKSUMBYTES")
    parser.add_argument('-p', '--prefixPath', type=str, help="add a prefix to the paths in exportFile")
    parser.add_argument('-e', '--encode', type=str, help="encode results (ex: Latin1)")
    parser.add_argument('-s', '--separator', type=str, help="allows to set a specific separator")
    parser.add_argument('-S', '--splitSymbol', type=str, help="allows to set a specific symbol to separate data")
    parser.add_argument('-d', '--daemon', action='store_true', help="run as daemon")
    
    return parser

def run():
    """run the script"""
    parser = initParser()
    args = parser.parse_args()
    
    if not args.checksumBytes:
        args.checksumBytes = CHECKSUM_BYTES
    
    if not args.splitSymbol:
        args.splitSymbol = SPLIT_SYMBOL
    
    bot = ChecksumBot(args.rootDirectory, args.logFile, args.checksumBytes)
    checker = CopyChecker(args.exportFile, args.splitSymbol, args.logFile, args.checksumBytes, args.prefixPath, args.encode, args.separator)
    handler = ChecksumsHandler(bot.queue, checker.queue, args.logFile)
    
    bot.start()
    handler.start()
    
    if args.daemon:
        checker.start()
    else:
        checker.run()

if __name__ == '__main__':
    timer = timeit.Timer('run()', 'from __main__ import run')
    print '\nTime elapsed: {0}'.format(timer.timeit(1))

