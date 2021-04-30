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

import org.apache.hadoop.hbase.thrift.generated.AlreadyExists;
import org.apache.hadoop.hbase.thrift.generated.ColumnDescriptor;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.TCell;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

/**
 * See the instructions under hbase-examples/README.txt
 */
public class HttpDoAsClient {

  static protected int port;
  static protected String host;
  CharsetDecoder decoder = null;
  private static boolean secure = false;
  static protected String doAsUser = null;
  static protected String principal = null;

  public static void main(String[] args) throws Exception {

    if (args.length < 3 || args.length > 4) {

      System.out.println("Invalid arguments!");
      System.out.println("Usage: HttpDoAsClient host port doAsUserName [security=true]");
      System.exit(-1);
    }

    host = args[0];
    port = Integer.parseInt(args[1]);
    doAsUser = args[2];
    if (args.length > 3) {
      secure = Boolean.parseBoolean(args[3]);
      principal = getSubject().getPrincipals().iterator().next().getName();
    }

    final HttpDoAsClient client = new HttpDoAsClient();
    Subject.doAs(getSubject(),
        new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            client.run();
            return null;
          }
        });
  }

  HttpDoAsClient() {
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

    transport.open();
    String url = "http://" + host + ":" + port;
    THttpClient httpClient = new THttpClient(url);
    httpClient.open();
    TProtocol protocol = new TBinaryProtocol(httpClient);
    Hbase.Client client = new Hbase.Client(protocol);

    byte[] t = bytes("demo_table");

    //
    // Scan all tables, look for the demo table and delete it.
    //
    System.out.println("scanning tables...");
    for (ByteBuffer name : refresh(client, httpClient).getTableNames()) {
      System.out.println("  found: " + utf8(name.array()));
      if (utf8(name.array()).equals(utf8(t))) {
        if (refresh(client, httpClient).isTableEnabled(name)) {
          System.out.println("    disabling table: " + utf8(name.array()));
          refresh(client, httpClient).disableTable(name);
        }
        System.out.println("    deleting table: " + utf8(name.array()));
        refresh(client, httpClient).deleteTable(name);
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

      refresh(client, httpClient).createTable(ByteBuffer.wrap(t), columns);
    } catch (AlreadyExists ae) {
      System.out.println("WARN: " + ae.message);
    }

    System.out.println("column families in " + utf8(t) + ": ");
    Map<ByteBuffer, ColumnDescriptor> columnMap = refresh(client, httpClient)
        .getColumnDescriptors(ByteBuffer.wrap(t));
    for (ColumnDescriptor col2 : columnMap.values()) {
      System.out.println("  column: " + utf8(col2.name.array()) + ", maxVer: " + Integer.toString(col2.maxVersions));
    }

    transport.close();
    httpClient.close();
  }

  private Hbase.Client refresh(Hbase.Client client, THttpClient httpClient) {
    httpClient.setCustomHeader("doAs", doAsUser);
    if(secure) {
      try {
        httpClient.setCustomHeader("Authorization", generateTicket());
      } catch (GSSException e) {
        e.printStackTrace();
      }
    }
    return client;
  }

  private String generateTicket() throws GSSException {
    final GSSManager manager = GSSManager.getInstance();
    // Oid for kerberos principal name
    Oid krb5PrincipalOid = new Oid("1.2.840.113554.1.2.2.1");
    Oid KERB_V5_OID = new Oid("1.2.840.113554.1.2.2");
    final GSSName clientName = manager.createName(principal,
        krb5PrincipalOid);
    final GSSCredential clientCred = manager.createCredential(clientName,
        8 * 3600,
        KERB_V5_OID,
        GSSCredential.INITIATE_ONLY);

    final GSSName serverName = manager.createName(principal, krb5PrincipalOid);

    final GSSContext context = manager.createContext(serverName,
        KERB_V5_OID,
        clientCred,
        GSSContext.DEFAULT_LIFETIME);
    context.requestMutualAuth(true);
    context.requestConf(false);
    context.requestInteg(true);

    final byte[] outToken = context.initSecContext(new byte[0], 0, 0);
    StringBuffer outputBuffer = new StringBuffer();
    outputBuffer.append("Negotiate ");
    outputBuffer.append(Base64.encodeBytes(outToken).replace("\n", ""));
    System.out.print("Ticket is: " + outputBuffer);
    return outputBuffer.toString();
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
