#!/usr/bin/env python
"""
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
"""

import sys
import time
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from gen_py.hbase import ttypes
from gen_py.hbase.Hbase import Client, ColumnDescriptor, Mutation


def printVersions(row, versions):
    print("row: " + row.decode() + ", values: ", end="")
    for cell in versions:
        print(cell.value.decode() + "; ", end="")
    print()


def printRow(row_result):
    for res in row_result:
        sorted_cols = {col: cell for col, cell in sorted(res.columns.items())}
        row_str = ""
        for k, v in sorted_cols.items():
            # ignore error to prevent UnicodeDecodeError: 'utf-8' codec can't decode byte 0xfc in
            # position 4: invalid start byte
            row_str += (
                f"{k.decode('utf-8')} => {v.value.decode('utf-8', errors='ignore')}; "
            )
    print(f"row: {res.row.decode('utf-8', errors='ignore')}, cols: {row_str}")


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
    if serverType != ttypes.TThriftServerType.ONE:
        raise RuntimeError(
            f"Mismatch between client and server, server type is {serverType}"
        )

    demo_table = "demo_table".encode()
    disable_table = "disabled_table".encode()

    # Scan all tables, look for the demo table and delete it.
    print("scanning tables...")
    for table in client.getTableNames():
        print(f"  found: {table.decode()}")
        if table == demo_table or table == disable_table:
            if client.isTableEnabled(table):
                print(f"    disabling table: {table.decode()}")
                client.disableTable(table)
            print(f"    deleting table: {table.decode()}")
            client.deleteTable(table)

    # Create the demo table with two column families, entry: and unused:
    columns = []
    col = ColumnDescriptor()
    col.name = "entry:".encode()
    col.maxVersions = 10
    columns.append(col)
    col = ColumnDescriptor()
    col.name = "unused:".encode()
    columns.append(col)

    print(f"creating table: {demo_table.decode()}")
    try:
        client.createTable(demo_table, columns)
        client.createTable(disable_table, columns)
    except ttypes.AlreadyExists as ae:
        print("WARN: " + ae.message)

    cols = client.getColumnDescriptors(demo_table)
    print(f"column families in {demo_table.decode()}: ")
    for col_name in cols.keys():
        col = cols[col_name]
        print(f"  column: {col.name.decode()}, maxVer: {col.maxVersions}")

    if client.isTableEnabled(disable_table):
        print(f"disabling table: {disable_table.decode()}")
        client.disableTable(disable_table)

    dummy_attributes = {}

    # Test UTF-8 handling
    invalid = b"foo-\xfc\xa1\xa1\xa1\xa1"
    valid = b"foo-\xe7\x94\x9f\xe3\x83\x93\xe3\x83\xbc\xe3\x83\xab"

    # non-utf8 is fine for data
    mutations = [Mutation(column="entry:foo".encode(), value=invalid, writeToWAL=False)]
    client.mutateRow(demo_table, "foo".encode(), mutations, dummy_attributes)

    # this row name is valid utf8
    mutations = [Mutation(column="entry:foo".encode(), value=valid, writeToWAL=False)]
    client.mutateRow(demo_table, valid, mutations, dummy_attributes)

    # non-utf8 is now allowed in row names because HBase stores values as binary
    mutations = [Mutation(column="entry:foo".encode(), value=invalid, writeToWAL=False)]
    client.mutateRow(demo_table, invalid, mutations, dummy_attributes)

    # Run a scanner on the rows we just created
    print("Starting scanner...")
    scanner = client.scannerOpen(
        demo_table, "".encode(), ["entry".encode()], dummy_attributes
    )

    while True:
        r = client.scannerGet(scanner)
        if not r:
            break
        printRow(r)
    print("Scanner finished...")

    # Run some operations on a bunch of rows.
    for e in range(100, -1, -1):
        # format row keys as "00000" to "00100"
        row = f"{e:05}".encode()

        mutations = [
            Mutation(
                column="unused:".encode(), value="DELETE_ME".encode(), writeToWAL=False
            )
        ]
        client.mutateRow(demo_table, row, mutations, dummy_attributes)
        printRow(client.getRow(demo_table, row, dummy_attributes))
        client.deleteAllRow(demo_table, row, dummy_attributes)

        # sleep to force later timestamp
        time.sleep(0.05)

        mutations = [
            Mutation(column="entry:num".encode(), value="0".encode(), writeToWAL=False),
            Mutation(
                column="entry:foo".encode(), value="FOO".encode(), writeToWAL=False
            ),
        ]
        client.mutateRow(demo_table, row, mutations, dummy_attributes)
        printRow(client.getRow(demo_table, row, dummy_attributes))

        mutations = [
            Mutation(column="entry:foo".encode(), isDelete=True),
            Mutation(column="entry:num".encode(), value="-1".encode()),
        ]
        client.mutateRow(demo_table, row, mutations, dummy_attributes)
        printRow(client.getRow(demo_table, row, dummy_attributes))

        mutations = [
            Mutation(
                column="entry:num".encode(), value=str(e).encode(), writeToWAL=False
            ),
            Mutation(
                column="entry:sqr".encode(), value=str(e * e).encode(), writeToWAL=False
            ),
        ]
        client.mutateRow(demo_table, row, mutations, dummy_attributes)
        printRow(client.getRow(demo_table, row, dummy_attributes))

        # sleep to force later timestamp
        time.sleep(0.05)

        mutations = [
            Mutation(column="entry:num".encode(), value="-999".encode()),
            Mutation(column="entry:sqr".encode(), isDelete=True),
        ]
        # shouldn't override latest
        client.mutateRowTs(demo_table, row, mutations, 1, dummy_attributes)
        printRow(client.getRow(demo_table, row, dummy_attributes))

        versions = client.getVer(
            demo_table, row, "entry:num".encode(), 10, dummy_attributes
        )
        printVersions(row, versions)
        if len(versions) == 0:
            print("FATAL: wrong # of versions")
            sys.exit(-1)

        r = client.get(demo_table, row, "entry:foo".encode(), dummy_attributes)
        # just to be explicit, we get lists back, if it's empty there was no matching row.
        if len(r) > 0:
            print("FATAL: shouldn't get here")
            sys.exit(-1)
        print()

    # scan all rows/columnNames
    columnNames = []
    for col, desc in client.getColumnDescriptors(demo_table).items():
        print(f"column with name: {desc.name.decode()}")
        print(desc)
        columnNames.append(desc.name[:-1])

    print("\nStarting scanner...")
    scanner = client.scannerOpenWithStop(
        demo_table, "00020".encode(), "00040".encode(), columnNames, dummy_attributes
    )
    while True:
        r = client.scannerGet(scanner)
        if not r:
            break
        printRow(r)
    print("Scanner finished...")
    transport.close()


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(f"usage: {__file__} <host> <port>")
        sys.exit(1)

    host = sys.argv[1]
    port = sys.argv[2]

    is_framed_transport = False
    demo_client(host, port, is_framed_transport)
