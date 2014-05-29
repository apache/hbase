#!/usr/bin/env python2.7

import argparse
import datetime
from subprocess import check_output
from xml.dom.minidom import parse

parser = argparse.ArgumentParser(
    description='Get current version of a pom and generate a new one')
parser.add_argument('--pom',
                    metavar='path/to/pom.xml',
                    help='Pom to get base version from')
parser.add_argument('--date',
                    help='Use the current date rather than SCM version',
                    action='store_true')
parser.add_argument('--git_svn',
                    help='Get the last svn revision from git-svn',
                    action='store_true')
parser.add_argument('--svn',
                    help='Get the last svn revision from svn log',
                    action='store_true')


def get_version_from_git_log(git_log):
    svn_line = [line.strip() for line in git_log.splitlines(True)
                if line.strip().startswith("git-svn-id")][0]
    url = svn_line.split()[1]
    rev = url.split('@')[-1]
    return rev

def get_git_version():
    """
    This will retrieve the svn revision from the git log.
    Should be used if the local repository was created by
    git svn clone.
    """
    version = None
    depth = 1
    while not version:
        try:
            g_args = ["git", "log", "-n", str(depth), "HEAD"]
            git_log = check_output(g_args)
            version = get_version_from_git_log(git_log)
        except:
            version = None
            depth += 1

    if depth > 1:
        version = int(version) + 1
        version = str(version) + "-SNAPSHOT"

    return version


def get_svn_version():
    """
    retrieves revision from:

    elliott@dev2086:dingo {}$ svn info
        Path: .
        Working Copy Root Path: /tmp/dingo
        URL: svn+ssh://tubbs/svnhive/hadoop/branches/hadoop-hdfs-dingo-20131121-rc
        Repository Root: svn+ssh://tubbs/svnhive
        Repository UUID: e7acf4d4-3532-417f-9e73-7a9ae25a1f51
        Revision: 42511
        Node Kind: directory
        Schedule: normal
        Last Changed Author: pritam
        Last Changed Rev: 42506
        Last Changed Date: 2014-05-27 14:03:56 -0700 (Tue, 27 May 2014)
    """
    svn_info = check_output(["svn", "info"])
    rev_line = [line.strip() for line in svn_info.splitlines(True)
                if line.strip.startswith("Last Changed Rev")][0]
    rev = rev_line.split()[-1]
    return rev

args = parser.parse_args()

root = parse(args.pom).childNodes[0]
version = [c.firstChild.nodeValue for c in root.childNodes
           if c.nodeType == 1 and c.tagName == 'version'][0]
version_parts = version.split(".")
if args.date:
    version_parts[-1] = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
elif args.git_svn:
    version_parts[-1] = get_git_version()
elif args.svn:
    version_parts[-1] = get_svn_version()

print ".".join(version_parts)
