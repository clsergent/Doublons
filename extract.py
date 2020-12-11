#!/usr/bin/python3
#-*- coding: utf-8 -*-
 
import os
import argparse
import timeit
import re

SPLIT_SYMBOL = "; "

class Extractor(object):
    """ """
    def __init__(self, target, inputFile, export, splitSymbol, separator=None, **kwds):
        self._target = target
        self._inputFile = inputFile
        self._export = export
        self._splitSymbol = splitSymbol
        
        if separator:
            self._separator = separator
        else:
            self._separator = os.path.sep
        
    def compare(self):
        self.exportData('', 'w')
        with open(self._inputFile, 'r') as f:
            line = f.readline()
            while line != '':
                if self._target in line:
                    splitLine = line.replace('\n','').split(self._splitSymbol)
                    items = splitLine[:1]    #size is first in line
                    items += [i for i in splitLine[1:] if self._target in i]
                    items += [i for i in splitLine[1:] if not self._target in i]
                    self.exportData(self._splitSymbol.join(items)+'\n', mode='a')
                line = f.readline()
    
    def exportData(self, data, mode='a'):
        try:
            with open(self._export, mode) as f:
                f.write(data)
        except:
            print('failed to write data to {0} ({1})'.format(self._export, data))

def initParser():
    """initialization of the parser"""
    parser = argparse.ArgumentParser(description='Script extrayant les doublons générés')
    
    parser.add_argument('target', type=str, help="Dossier cible pour l'extraction des données")
    parser.add_argument('inputFile', type=str, help="Fichier contenant les données à extraire")
    parser.add_argument('export', type=str, help="Fichier de sortie")
    parser.add_argument('-g', '--logFile', type=str, help="write log to logFile")
    parser.add_argument('-e', '--encode', type=str, help="encode results (ex: Latin1)")
    parser.add_argument('-S', '--splitSymbol', type=str, help="séparateur de données dans inputFile")
    parser.add_argument('-s', '--separator', type=str, help="séparateur dans les chemins d'accès")
    parser.add_argument('-p', '--prefix', type=str, help="permet de retirer un préfixe à target lors des comparaisons")
    parser.add_argument('-d', '--directory', action='store_true', help="use target and export as directories")
    
    return parser

def runMultiple(args):
    if not os.path.isdir(args.target):
        print("target is not a valid directory")
        return
    
    if not os.path.isdir(args.export):
        print("export is not a valid directory")
        return
    
    root, dirs, files = next(os.walk(args.target))
    for dir in dirs:
        print("extraction from {0}".format(os.path.join(root,dir).replace(args.prefix,'')))
        Extractor(os.path.join(root,dir).replace(args.prefix,''),
                          args.inputFile,
                          os.path.join(args.export,'{0}.csv'.format(dir)),
                          args.splitSymbol,
                          args.separator).compare()
    
def run():
    """run the script"""
    parser = initParser()
    args = parser.parse_args()
    
    if not args.splitSymbol:
        args.splitSymbol = SPLIT_SYMBOL
    
    if not args.prefix:
        args.prefix=''
    
    if not args.directory:
        extract = Extractor(**args.__dict__)
        extract.compare()
    else:
        runMultiple(args)
        
            
        

if __name__ == '__main__':
    timer = timeit.Timer('run()', 'from __main__ import run')
    print('\nTime elapsed: {0}'.format(timer.timeit(1)))
    