#!/usr/bin/python
'''
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
'''

import sys
import time
import os

# Modify this import path to point to the correct location to thrift.
thrift_path = os.path.abspath('/Users/sergey/Downloads/thrift/lib/py/build/lib.macosx-10.8-intel-2.7')
sys.path.append(thrift_path)
gen_py_path = os.path.abspath('gen-py')
sys.path.append(gen_py_path)

from thrift import Thrift
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from hbase.ttypes import TThriftServerType
from hbase.Hbase import Client, ColumnDescriptor, Mutation

def printVersions(row, versions):
  print "row: " + row + ", values: ",
  for cell in versions:
    print cell.value + "; ",
  print

def printRow(entry):
  print "row: " + entry.row + ", cols:",
  for k in sorted(entry.columns):
    print k + " => " + entry.columns[k].value,
  print


def demo_client(host, port, is_framed_transport):

  # Make socket
  socket = TSocket.TSocket(host, port)

  # Make transport
  if is_framed_transport:
    transport = TTransport.TFramedTransport(socket)
  else:
    transport = TTransport.TBufferedTransport(socket)

  # Wrap in a protocol
  protocol = TBinaryProtocol.TBinaryProtocol(transport)

  # Create a client to use the protocol encoder
  client = Client(protocol)

  # Connect!
  transport.open()

  # Check Thrift Server Type
  serverType = client.getThriftServerType()
  if serverType != TThriftServerType.ONE:
    raise Exception("Mismatch between client and server, server type is %s" % serverType)

  t = "demo_table"

  #
  # Scan all tables, look for the demo table and delete it.
  #
  print "scanning tables..."
  for table in client.getTableNames():
    print "  found: %s" %(table)
    if table == t:
      if client.isTableEnabled(table):
        print "    disabling table: %s"  %(t)
        client.disableTable(table)
      print "    deleting table: %s"  %(t)
      client.deleteTable(table)

  columns = []
  col = ColumnDescriptor()
  col.name = 'entry:'
  col.maxVersions = 10
  columns.append(col)
  col = ColumnDescriptor()
  col.name = 'unused:'
  columns.append(col)

  try:
    print "creating table: %s" %(t)
    client.createTable(t, columns)
  except AlreadyExists, ae:
    print "WARN: " + ae.message

  cols = client.getColumnDescriptors(t)
  print "column families in %s" %(t)
  for col_name in cols.keys():
    col = cols[col_name]
    print "  column: %s, maxVer: %d" % (col.name, col.maxVersions)

  dummy_attributes = {}
  #
  # Test UTF-8 handling
  #
  invalid = "foo-\xfc\xa1\xa1\xa1\xa1\xa1"
  valid = "foo-\xE7\x94\x9F\xE3\x83\x93\xE3\x83\xBC\xE3\x83\xAB";

  # non-utf8 is fine for data
  mutations = [Mutation(column="entry:foo",value=invalid)]
  print str(mutations)
  client.mutateRow(t, "foo", mutations, dummy_attributes)

  # try empty strings
  mutations = [Mutation(column="entry:", value="")]
  client.mutateRow(t, "", mutations, dummy_attributes)

  # this row name is valid utf8
  mutations = [Mutation(column="entry:foo", value=valid)]
  client.mutateRow(t, valid, mutations, dummy_attributes)

  # non-utf8 is not allowed in row names
  try:
    mutations = [Mutation(column="entry:foo", value=invalid)]
    client.mutateRow(t, invalid, mutations, dummy_attributes)
  except ttypes.IOError, e:
    print 'expected exception: %s' %(e.message)

  # Run a scanner on the rows we just created
  print "Starting scanner..."
  scanner = client.scannerOpen(t, "", ["entry:"], dummy_attributes)

  r = client.scannerGet(scanner)
  while r:
    printRow(r[0])
    r = client.scannerGet(scanner)
  print "Scanner finished"

  #
  # Run some operations on a bunch of rows.
  #
  for e in range(100, 0, -1):
    # format row keys as "00000" to "00100"
    row = "%0.5d" % (e)

    mutations = [Mutation(column="unused:", value="DELETE_ME")]
    client.mutateRow(t, row, mutations, dummy_attributes)
    printRow(client.getRow(t, row, dummy_attributes)[0])
    client.deleteAllRow(t, row, dummy_attributes)

    mutations = [Mutation(column="entry:num", value="0"),
                 Mutation(column="entry:foo", value="FOO")]
    client.mutateRow(t, row, mutations, dummy_attributes)
    printRow(client.getRow(t, row, dummy_attributes)[0]);

    mutations = [Mutation(column="entry:foo",isDelete=True),
                 Mutation(column="entry:num",value="-1")]
    client.mutateRow(t, row, mutations, dummy_attributes)
    printRow(client.getRow(t, row, dummy_attributes)[0])

    mutations = [Mutation(column="entry:num", value=str(e)),
                 Mutation(column="entry:sqr", value=str(e*e))]
    client.mutateRow(t, row, mutations, dummy_attributes)
    printRow(client.getRow(t, row, dummy_attributes)[0])

    time.sleep(0.05)

    mutations = [Mutation(column="entry:num",value="-999"),
                 Mutation(column="entry:sqr",isDelete=True)]
    client.mutateRowTs(t, row, mutations, 1, dummy_attributes) # shouldn't override latest
    printRow(client.getRow(t, row, dummy_attributes)[0])

    versions = client.getVer(t, row, "entry:num", 10, dummy_attributes)
    printVersions(row, versions)
    if len(versions) != 3:
      print("FATAL: wrong # of versions")
      sys.exit(-1)

    r = client.get(t, row, "entry:foo", dummy_attributes)
    # just to be explicit, we get lists back, if it's empty there was no matching row.
    if len(r) > 0:
      raise "shouldn't get here!"

  columnNames = []
  for (col, desc) in client.getColumnDescriptors(t).items():
    print "column with name: "+desc.name
    print desc
    columnNames.append(desc.name+":")

  print "Starting scanner..."
  scanner = client.scannerOpenWithStop(t, "00020", "00040", columnNames, dummy_attributes)

  r = client.scannerGet(scanner)
  while r:
    printRow(r[0])
    r = client.scannerGet(scanner)

  client.scannerClose(scanner)
  print "Scanner finished"

  transport.close()


if __name__ == '__main__':

  import sys
  if len(sys.argv) < 3:
    print 'usage: %s <host> <port>' % __file__
    sys.exit(1)

  host = sys.argv[1]
  port = sys.argv[2]
  is_framed_transport = False
  demo_client(host, port, is_framed_transport)
