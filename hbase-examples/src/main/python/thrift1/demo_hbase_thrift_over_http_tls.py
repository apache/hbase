#!/usr/bin/env python
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
#
# test thrift server over HTTP with SSL in place
#
# presumes thrift python lib is installed
# presumes you have access to hbase thrift binding (i.e. you add gen_py to PYTHONPATH)
# presumes thrift proxy is running on port 9090
# presumes thrift proxy is running over https
# presumes access to create and use tables in a namespace 'test'
#
# usage:
# ./demo_hbase_thrift_over_http_tls.py host-running-thrift1.example.com
import sys

from thrift.transport import THttpClient
from thrift.protocol import TBinaryProtocol
from gen_py.hbase import Hbase
from gen_py.hbase.ttypes import ColumnDescriptor
from gen_py.hbase.ttypes import Mutation

print("[INFO] setup connection")
transport = THttpClient.THttpClient(f"https://{sys.argv[1]}:9090")
client = Hbase.Client(TBinaryProtocol.TBinaryProtocol(transport))

table='test:thrift_proxy_demo'

print("[INFO] start client")
transport.open()

print("[INFO] list the current tables")
print(client.getTableNames())

print("[INFO] create a table, place some data")
client.createTable(table, [ColumnDescriptor(name ='family1:')])
client.mutateRow(table, 'row1',
                 [Mutation(column = 'family1:cq1', value = 'foo'),
                  Mutation(column = 'family1:cq2', value = 'foo')], {})
client.mutateRow(table, 'row2',
                 [Mutation(column = 'family1:cq1', value = 'bar'),
                  Mutation(column = 'family1:cq2', value = 'bar')], {})
client.mutateRow(table, 'row3',
                 [Mutation(column = 'family1:cq1', value = 'foo'),
                  Mutation(column = 'family1:cq2', value = 'foo')], {})
client.mutateRow(table, 'row4',
                 [Mutation(column = 'family1:cq1', value = 'bar'),
                  Mutation(column = 'family1:cq2', value = 'bar')], {})

print("[INFO] scan")
scan_id = client.scannerOpen(table, 'row1', [], {})
for row in client.scannerGetList(scan_id, 25):
  print(row)
client.scannerClose(scan_id)
print("[INFO] get")
for row in client.getRow(table, 'row3', {}):
  print(row)

print("[INFO] clean up")
client.disableTable(table)
client.deleteTable(table)
