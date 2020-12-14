# Doublons
Duplicate files search and removal

# Purpose
DoublonsV3.py is a script intended to find duplicates in a directory using a hash function. It outputs a customizable csv file containing the duplicates. It has been intensively used on a high volume of files managed by a debian OS running on a bare metal server.

DoublonsV2.py is provided for python 2.7 as a stopgap only.

# Requirements
Python 3.8 or above is required to use DoublonsV3.py. If an earlier version of python 3 is supplied, a minor modification in the script must be made as it uses the "walrus operator" in file read operation. Check https://www.python.org/dev/peps/pep-0572/ to learn more about it. 

If no Python 3 is useable, DoublonsV2.py may be used insead. It is not safe to use this script as there is NO BOUNDARY on memory consumption.

# Commandline
type *python3 DoublonsV3.py -h* to get help

# License
This repository and its content are licensed under the EUPL-1.2-or-later.

Check https://joinup.ec.europa.eu/collection/eupl/eupl-text-eupl-12 to read it in one of the 23 langages of the UE.

