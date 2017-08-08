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
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.security.PrivilegedExceptionAction;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.sasl.Sasl;

import org.apache.hadoop.hbase.thrift.generated.AlreadyExists;
import org.apache.hadoop.hbase.thrift.generated.ColumnDescriptor;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.Mutation;
import org.apache.hadoop.hbase.thrift.generated.TCell;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * See the instructions under hbase-examples/README.txt
 */
public class DemoClient {

    static protected int port;
    static protected String host;
    CharsetDecoder decoder = null;

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
      return Boolean.TRUE.toString().equalsIgnoreCase(s) || Boolean.FALSE.toString().equalsIgnoreCase(s);
    }

    DemoClient() {
        decoder = Charset.forName("UTF-8").newDecoder();
    }

    // Helper to translate byte[]'s to UTF8 strings
    private String utf8(byte[] buf) {
        try {
            return decoder.decode(ByteBuffer.wrap(buf)).toString();
        } catch (CharacterCodingException e) {
            return "[INVALID UTF-8]";
        }
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
          Map<String, String> saslProperties = new HashMap<String, String>();
          saslProperties.put(Sasl.QOP, "auth-conf,auth-int,auth");
          /**
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

        byte[] t = bytes("demo_table");

        //
        // Scan all tables, look for the demo table and delete it.
        //
        System.out.println("scanning tables...");
        for (ByteBuffer name : client.getTableNames()) {
            System.out.println("  found: " + utf8(name.array()));
            if (utf8(name.array()).equals(utf8(t))) {
                if (client.isTableEnabled(name)) {
                    System.out.println("    disabling table: " + utf8(name.array()));
                    client.disableTable(name);
                }
                System.out.println("    deleting table: " + utf8(name.array()));
                client.deleteTable(name);
            }
        }

        //
        // Create the demo table with two column families, entry: and unused:
        //
        ArrayList<ColumnDescriptor> columns = new ArrayList<ColumnDescriptor>();
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

        System.out.println("creating table: " + utf8(t));
        try {
            client.createTable(ByteBuffer.wrap(t), columns);
        } catch (AlreadyExists ae) {
            System.out.println("WARN: " + ae.message);
        }

        System.out.println("column families in " + utf8(t) + ": ");
        Map<ByteBuffer, ColumnDescriptor> columnMap = client.getColumnDescriptors(ByteBuffer.wrap(t));
        for (ColumnDescriptor col2 : columnMap.values()) {
            System.out.println("  column: " + utf8(col2.name.array()) + ", maxVer: " + Integer.toString(col2.maxVersions));
        }

        Map<ByteBuffer, ByteBuffer> dummyAttributes = null;
        boolean writeToWal = false;

        //
        // Test UTF-8 handling
        //
        byte[] invalid = {(byte) 'f', (byte) 'o', (byte) 'o', (byte) '-',
            (byte) 0xfc, (byte) 0xa1, (byte) 0xa1, (byte) 0xa1, (byte) 0xa1};
        byte[] valid = {(byte) 'f', (byte) 'o', (byte) 'o', (byte) '-',
            (byte) 0xE7, (byte) 0x94, (byte) 0x9F, (byte) 0xE3, (byte) 0x83,
            (byte) 0x93, (byte) 0xE3, (byte) 0x83, (byte) 0xBC, (byte) 0xE3,
            (byte) 0x83, (byte) 0xAB};

        ArrayList<Mutation> mutations;
        // non-utf8 is fine for data
        mutations = new ArrayList<Mutation>();
        mutations.add(new Mutation(false, ByteBuffer.wrap(bytes("entry:foo")),
            ByteBuffer.wrap(invalid), writeToWal));
        client.mutateRow(ByteBuffer.wrap(t), ByteBuffer.wrap(bytes("foo")),
            mutations, dummyAttributes);


        // this row name is valid utf8
        mutations = new ArrayList<Mutation>();
        mutations.add(new Mutation(false, ByteBuffer.wrap(bytes("entry:foo")), ByteBuffer.wrap(valid), writeToWal));
        client.mutateRow(ByteBuffer.wrap(t), ByteBuffer.wrap(valid), mutations, dummyAttributes);

        // non-utf8 is now allowed in row names because HBase stores values as binary

        mutations = new ArrayList<Mutation>();
        mutations.add(new Mutation(false, ByteBuffer.wrap(bytes("entry:foo")), ByteBuffer.wrap(invalid), writeToWal));
        client.mutateRow(ByteBuffer.wrap(t), ByteBuffer.wrap(invalid), mutations, dummyAttributes);


        // Run a scanner on the rows we just created
        ArrayList<ByteBuffer> columnNames = new ArrayList<ByteBuffer>();
        columnNames.add(ByteBuffer.wrap(bytes("entry:")));

        System.out.println("Starting scanner...");
        int scanner = client.scannerOpen(ByteBuffer.wrap(t), ByteBuffer.wrap(bytes("")), columnNames, dummyAttributes);

        while (true) {
            List<TRowResult> entry = client.scannerGet(scanner);
            if (entry.isEmpty()) {
                break;
            }
            printRow(entry);
        }

        //
        // Run some operations on a bunch of rows
        //
        for (int i = 100; i >= 0; --i) {
            // format row keys as "00000" to "00100"
            NumberFormat nf = NumberFormat.getInstance();
            nf.setMinimumIntegerDigits(5);
            nf.setGroupingUsed(false);
            byte[] row = bytes(nf.format(i));

            mutations = new ArrayList<Mutation>();
            mutations.add(new Mutation(false, ByteBuffer.wrap(bytes("unused:")), ByteBuffer.wrap(bytes("DELETE_ME")), writeToWal));
            client.mutateRow(ByteBuffer.wrap(t), ByteBuffer.wrap(row), mutations, dummyAttributes);
            printRow(client.getRow(ByteBuffer.wrap(t), ByteBuffer.wrap(row), dummyAttributes));
            client.deleteAllRow(ByteBuffer.wrap(t), ByteBuffer.wrap(row), dummyAttributes);

            // sleep to force later timestamp
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                // no-op
            }

            mutations = new ArrayList<Mutation>();
            mutations.add(new Mutation(false, ByteBuffer.wrap(bytes("entry:num")), ByteBuffer.wrap(bytes("0")), writeToWal));
            mutations.add(new Mutation(false, ByteBuffer.wrap(bytes("entry:foo")), ByteBuffer.wrap(bytes("FOO")), writeToWal));
            client.mutateRow(ByteBuffer.wrap(t), ByteBuffer.wrap(row), mutations, dummyAttributes);
            printRow(client.getRow(ByteBuffer.wrap(t), ByteBuffer.wrap(row), dummyAttributes));

            Mutation m;
            mutations = new ArrayList<Mutation>();
            m = new Mutation();
            m.column = ByteBuffer.wrap(bytes("entry:foo"));
            m.isDelete = true;
            mutations.add(m);
            m = new Mutation();
            m.column = ByteBuffer.wrap(bytes("entry:num"));
            m.value = ByteBuffer.wrap(bytes("-1"));
            mutations.add(m);
            client.mutateRow(ByteBuffer.wrap(t), ByteBuffer.wrap(row), mutations, dummyAttributes);
            printRow(client.getRow(ByteBuffer.wrap(t), ByteBuffer.wrap(row), dummyAttributes));

            mutations = new ArrayList<Mutation>();
            mutations.add(new Mutation(false, ByteBuffer.wrap(bytes("entry:num")), ByteBuffer.wrap(bytes(Integer.toString(i))), writeToWal));
            mutations.add(new Mutation(false, ByteBuffer.wrap(bytes("entry:sqr")), ByteBuffer.wrap(bytes(Integer.toString(i * i))), writeToWal));
            client.mutateRow(ByteBuffer.wrap(t), ByteBuffer.wrap(row), mutations, dummyAttributes);
            printRow(client.getRow(ByteBuffer.wrap(t), ByteBuffer.wrap(row), dummyAttributes));

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
            client.mutateRowTs(ByteBuffer.wrap(t), ByteBuffer.wrap(row), mutations, 1, dummyAttributes); // shouldn't override latest
            printRow(client.getRow(ByteBuffer.wrap(t), ByteBuffer.wrap(row), dummyAttributes));

            List<TCell> versions = client.getVer(ByteBuffer.wrap(t), ByteBuffer.wrap(row), ByteBuffer.wrap(bytes("entry:num")), 10, dummyAttributes);
            printVersions(ByteBuffer.wrap(row), versions);
            if (versions.isEmpty()) {
                System.out.println("FATAL: wrong # of versions");
                System.exit(-1);
            }

            List<TCell> result = client.get(ByteBuffer.wrap(t), ByteBuffer.wrap(row), ByteBuffer.wrap(bytes("entry:foo")), dummyAttributes);
            if (!result.isEmpty()) {
                System.out.println("FATAL: shouldn't get here");
                System.exit(-1);
            }

            System.out.println("");
        }

        // scan all rows/columnNames

        columnNames.clear();
        for (ColumnDescriptor col2 : client.getColumnDescriptors(ByteBuffer.wrap(t)).values()) {
            System.out.println("column with name: " + new String(col2.name.array()));
            System.out.println(col2.toString());

            columnNames.add(col2.name);
        }

        System.out.println("Starting scanner...");
        scanner = client.scannerOpenWithStop(ByteBuffer.wrap(t), ByteBuffer.wrap(bytes("00020")), ByteBuffer.wrap(bytes("00040")), columnNames, dummyAttributes);

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
            rowStr.append(utf8(cell.value.array()));
            rowStr.append("; ");
        }
        System.out.println("row: " + utf8(row.array()) + ", values: " + rowStr);
    }

    private void printRow(TRowResult rowResult) {
        // copy values into a TreeMap to get them in sorted order

        TreeMap<String, TCell> sorted = new TreeMap<String, TCell>();
        for (Map.Entry<ByteBuffer, TCell> column : rowResult.columns.entrySet()) {
            sorted.put(utf8(column.getKey().array()), column.getValue());
        }

        StringBuilder rowStr = new StringBuilder();
        for (SortedMap.Entry<String, TCell> entry : sorted.entrySet()) {
            rowStr.append(entry.getKey());
            rowStr.append(" => ");
            rowStr.append(utf8(entry.getValue().value.array()));
            rowStr.append("; ");
        }
        System.out.println("row: " + utf8(rowResult.row.array()) + ", cols: " + rowStr);
    }

    private void printRow(List<TRowResult> rows) {
        for (TRowResult rowResult : rows) {
            printRow(rowResult);
        }
    }

    static Subject getSubject() throws Exception {
      if (!secure) return new Subject();

      /*
       * To authenticate the DemoClient, kinit should be invoked ahead.
       * Here we try to get the Kerberos credential from the ticket cache.
       */
      LoginContext context = new LoginContext("", new Subject(), null,
        new Configuration() {
          @Override
          public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
            Map<String, String> options = new HashMap<String, String>();
            options.put("useKeyTab", "false");
            options.put("storeKey", "false");
            options.put("doNotPrompt", "true");
            options.put("useTicketCache", "true");
            options.put("renewTGT", "true");
            options.put("refreshKrb5Config", "true");
            options.put("isInitiator", "true");
            String ticketCache = System.getenv("KRB5CCNAME");
            if (ticketCache != null) {
              options.put("ticketCache", ticketCache);
            }
            options.put("debug", "true");

            return new AppConfigurationEntry[]{
                new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule",
                    AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                    options)};
          }
        });
      context.login();
      return context.getSubject();
    }
}
