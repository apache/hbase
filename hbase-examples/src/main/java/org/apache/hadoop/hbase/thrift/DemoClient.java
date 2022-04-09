/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.thrift;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.sasl.Sasl;
import org.apache.hadoop.hbase.thrift.generated.AlreadyExists;
import org.apache.hadoop.hbase.thrift.generated.ColumnDescriptor;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.Mutation;
import org.apache.hadoop.hbase.thrift.generated.TCell;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClientUtils;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * See the instructions under hbase-examples/README.txt
 */
@InterfaceAudience.Private
public class DemoClient {

  static protected int port;
  static protected String host;

  private static boolean secure = false;
  private static String serverPrincipal = "hbase";

  public static void main(String[] args) throws Exception {
    if (args.length < 2 || args.length > 4 || (args.length > 2 && !isBoolean(args[2]))) {
      System.out.println("Invalid arguments!");
      System.out.println("Usage: DemoClient host port [secure=false [server-principal=hbase] ]");

      System.exit(-1);
    }

    port = Integer.parseInt(args[1]);
    host = args[0];

    if (args.length > 2) {
      secure = Boolean.parseBoolean(args[2]);
    }

    if (args.length == 4) {
      serverPrincipal = args[3];
    }

    final DemoClient client = new DemoClient();
    Subject.doAs(getSubject(),
      new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          client.run();
          return null;
        }
      });
  }

  private static boolean isBoolean(String s){
    return Boolean.TRUE.toString().equalsIgnoreCase(s) ||
            Boolean.FALSE.toString().equalsIgnoreCase(s);
  }

  DemoClient() {
  }

    // Helper to translate strings to UTF8 bytes
    private byte[] bytes(String s) {
        try {
            return s.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        }
    }

  private void run() throws Exception {
    TTransport transport = new TSocket(host, port);
    if (secure) {
      Map<String, String> saslProperties = new HashMap<>();
      saslProperties.put(Sasl.QOP, "auth-conf,auth-int,auth");
      /*
       * The Thrift server the DemoClient is trying to connect to
       * must have a matching principal, and support authentication.
       *
       * The HBase cluster must be secure, allow proxy user.
       */
      transport = new TSaslClientTransport("GSSAPI", null,
              serverPrincipal, // Thrift server user name, should be an authorized proxy user.
              host, // Thrift server domain
              saslProperties, null, transport);
    }

    transport.open();

    TProtocol protocol = new TBinaryProtocol(transport, true, true);
    Hbase.Client client = new Hbase.Client(protocol);

    ByteBuffer demoTable = ByteBuffer.wrap(bytes("demo_table"));
    ByteBuffer disabledTable = ByteBuffer.wrap(bytes("disabled_table"));

    // Scan all tables, look for the demo table and delete it.
    System.out.println("scanning tables...");

    for (ByteBuffer name : client.getTableNames()) {
      System.out.println("  found: " + ClientUtils.utf8(name.array()));

      if (name.equals(demoTable) || name.equals(disabledTable)) {
        if (client.isTableEnabled(name)) {
          System.out.println("    disabling table: " + ClientUtils.utf8(name.array()));
          client.disableTable(name);
        }

        System.out.println("    deleting table: " + ClientUtils.utf8(name.array()));
        client.deleteTable(name);
      }
    }

    // Create the demo table with two column families, entry: and unused:
    ArrayList<ColumnDescriptor> columns = new ArrayList<>(2);
    ColumnDescriptor col;
    col = new ColumnDescriptor();
    col.name = ByteBuffer.wrap(bytes("entry:"));
    col.timeToLive = Integer.MAX_VALUE;
    col.maxVersions = 10;
    columns.add(col);
    col = new ColumnDescriptor();
    col.name = ByteBuffer.wrap(bytes("unused:"));
    col.timeToLive = Integer.MAX_VALUE;
    columns.add(col);

    System.out.println("creating table: " + ClientUtils.utf8(demoTable.array()));

    try {
      client.createTable(demoTable, columns);
      client.createTable(disabledTable, columns);
    } catch (AlreadyExists ae) {
      System.out.println("WARN: " + ae.message);
    }

    System.out.println("column families in " + ClientUtils.utf8(demoTable.array()) + ": ");
    Map<ByteBuffer, ColumnDescriptor> columnMap = client.getColumnDescriptors(demoTable);

    for (ColumnDescriptor col2 : columnMap.values()) {
      System.out.println("  column: " + ClientUtils.utf8(col2.name.array()) + ", maxVer: "
          + col2.maxVersions);
    }

    if (client.isTableEnabled(disabledTable)){
      System.out.println("disabling table: " + ClientUtils.utf8(disabledTable.array()));
      client.disableTable(disabledTable);
    }

    System.out.println("list tables with enabled statuses : ");
    Map<ByteBuffer, Boolean> statusMap = client.getTableNamesWithIsTableEnabled();
    for (Map.Entry<ByteBuffer, Boolean> entry : statusMap.entrySet()) {
      System.out.println(" Table: " + ClientUtils.utf8(entry.getKey().array()) +
                          ", is enabled: " + entry.getValue());
    }

    Map<ByteBuffer, ByteBuffer> dummyAttributes = null;
    boolean writeToWal = false;

    // Test UTF-8 handling
    byte[] invalid = {(byte) 'f', (byte) 'o', (byte) 'o', (byte) '-',
                      (byte) 0xfc, (byte) 0xa1, (byte) 0xa1, (byte) 0xa1, (byte) 0xa1};
    byte[] valid = {(byte) 'f', (byte) 'o', (byte) 'o', (byte) '-',
                    (byte) 0xE7, (byte) 0x94, (byte) 0x9F, (byte) 0xE3, (byte) 0x83,
                    (byte) 0x93, (byte) 0xE3, (byte) 0x83, (byte) 0xBC, (byte) 0xE3,
                    (byte) 0x83, (byte) 0xAB};

    ArrayList<Mutation> mutations;
    // non-utf8 is fine for data
    mutations = new ArrayList<>(1);
    mutations.add(new Mutation(false, ByteBuffer.wrap(bytes("entry:foo")),
            ByteBuffer.wrap(invalid), writeToWal));
    client.mutateRow(demoTable, ByteBuffer.wrap(bytes("foo")),
            mutations, dummyAttributes);

    // this row name is valid utf8
    mutations = new ArrayList<>(1);
    mutations.add(new Mutation(false, ByteBuffer.wrap(bytes("entry:foo")),
            ByteBuffer.wrap(valid), writeToWal));
    client.mutateRow(demoTable, ByteBuffer.wrap(valid), mutations, dummyAttributes);

    // non-utf8 is now allowed in row names because HBase stores values as binary
    mutations = new ArrayList<>(1);
    mutations.add(new Mutation(false, ByteBuffer.wrap(bytes("entry:foo")),
            ByteBuffer.wrap(invalid), writeToWal));
    client.mutateRow(demoTable, ByteBuffer.wrap(invalid), mutations, dummyAttributes);

    // Run a scanner on the rows we just created
    ArrayList<ByteBuffer> columnNames = new ArrayList<>();
    columnNames.add(ByteBuffer.wrap(bytes("entry:")));

    System.out.println("Starting scanner...");
    int scanner = client.scannerOpen(demoTable, ByteBuffer.wrap(bytes("")), columnNames,
            dummyAttributes);

    while (true) {
      List<TRowResult> entry = client.scannerGet(scanner);

      if (entry.isEmpty()) {
        break;
      }

      printRow(entry);
    }

    // Run some operations on a bunch of rows
    for (int i = 100; i >= 0; --i) {
      // format row keys as "00000" to "00100"
      NumberFormat nf = NumberFormat.getInstance();
      nf.setMinimumIntegerDigits(5);
      nf.setGroupingUsed(false);
      byte[] row = bytes(nf.format(i));

      mutations = new ArrayList<>(1);
      mutations.add(new Mutation(false, ByteBuffer.wrap(bytes("unused:")),
              ByteBuffer.wrap(bytes("DELETE_ME")), writeToWal));
      client.mutateRow(demoTable, ByteBuffer.wrap(row), mutations, dummyAttributes);
      printRow(client.getRow(demoTable, ByteBuffer.wrap(row), dummyAttributes));
      client.deleteAllRow(demoTable, ByteBuffer.wrap(row), dummyAttributes);

      // sleep to force later timestamp
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        // no-op
      }

      mutations = new ArrayList<>(2);
      mutations.add(new Mutation(false, ByteBuffer.wrap(bytes("entry:num")),
              ByteBuffer.wrap(bytes("0")), writeToWal));
      mutations.add(new Mutation(false, ByteBuffer.wrap(bytes("entry:foo")),
              ByteBuffer.wrap(bytes("FOO")), writeToWal));
      client.mutateRow(demoTable, ByteBuffer.wrap(row), mutations, dummyAttributes);
      printRow(client.getRow(demoTable, ByteBuffer.wrap(row), dummyAttributes));

      Mutation m;
      mutations = new ArrayList<>(2);
      m = new Mutation();
      m.column = ByteBuffer.wrap(bytes("entry:foo"));
      m.isDelete = true;
      mutations.add(m);
      m = new Mutation();
      m.column = ByteBuffer.wrap(bytes("entry:num"));
      m.value = ByteBuffer.wrap(bytes("-1"));
      mutations.add(m);
      client.mutateRow(demoTable, ByteBuffer.wrap(row), mutations, dummyAttributes);
      printRow(client.getRow(demoTable, ByteBuffer.wrap(row), dummyAttributes));

      mutations = new ArrayList<>();
      mutations.add(new Mutation(false, ByteBuffer.wrap(bytes("entry:num")),
              ByteBuffer.wrap(bytes(Integer.toString(i))), writeToWal));
      mutations.add(new Mutation(false, ByteBuffer.wrap(bytes("entry:sqr")),
              ByteBuffer.wrap(bytes(Integer.toString(i * i))), writeToWal));
      client.mutateRow(demoTable, ByteBuffer.wrap(row), mutations, dummyAttributes);
      printRow(client.getRow(demoTable, ByteBuffer.wrap(row), dummyAttributes));

      // sleep to force later timestamp
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        // no-op
      }

      mutations.clear();
      m = new Mutation();
      m.column = ByteBuffer.wrap(bytes("entry:num"));
      m.value= ByteBuffer.wrap(bytes("-999"));
      mutations.add(m);
      m = new Mutation();
      m.column = ByteBuffer.wrap(bytes("entry:sqr"));
      m.isDelete = true;
      // shouldn't override latest
      client.mutateRowTs(demoTable, ByteBuffer.wrap(row), mutations, 1, dummyAttributes);
      printRow(client.getRow(demoTable, ByteBuffer.wrap(row), dummyAttributes));

      List<TCell> versions = client.getVer(demoTable, ByteBuffer.wrap(row),
              ByteBuffer.wrap(bytes("entry:num")), 10, dummyAttributes);
      printVersions(ByteBuffer.wrap(row), versions);

      if (versions.isEmpty()) {
        System.out.println("FATAL: wrong # of versions");
        System.exit(-1);
      }

      List<TCell> result = client.get(demoTable, ByteBuffer.wrap(row),
              ByteBuffer.wrap(bytes("entry:foo")), dummyAttributes);

      if (!result.isEmpty()) {
        System.out.println("FATAL: shouldn't get here");
        System.exit(-1);
      }

      System.out.println("");
    }

    // scan all rows/columnNames
    columnNames.clear();

    for (ColumnDescriptor col2 : client.getColumnDescriptors(demoTable).values()) {
      System.out.println("column with name: " + new String(col2.name.array()));
      System.out.println(col2.toString());

      columnNames.add(col2.name);
    }

    System.out.println("Starting scanner...");
    scanner = client.scannerOpenWithStop(demoTable, ByteBuffer.wrap(bytes("00020")),
            ByteBuffer.wrap(bytes("00040")), columnNames, dummyAttributes);

    while (true) {
      List<TRowResult> entry = client.scannerGet(scanner);

      if (entry.isEmpty()) {
        System.out.println("Scanner finished");
        break;
      }

      printRow(entry);
    }

    transport.close();
  }

  private void printVersions(ByteBuffer row, List<TCell> versions) {
    StringBuilder rowStr = new StringBuilder();

    for (TCell cell : versions) {
      rowStr.append(ClientUtils.utf8(cell.value.array()));
      rowStr.append("; ");
    }

    System.out.println("row: " + ClientUtils.utf8(row.array()) + ", values: " + rowStr);
  }

  private void printRow(TRowResult rowResult) {
    ClientUtils.printRow(rowResult);
  }

  private void printRow(List<TRowResult> rows) {
    for (TRowResult rowResult : rows) {
      printRow(rowResult);
    }
  }

  static Subject getSubject() throws Exception {
    if (!secure) {
      return new Subject();
    }

    LoginContext context = ClientUtils.getLoginContext();
    context.login();
    return context.getSubject();
  }
}
