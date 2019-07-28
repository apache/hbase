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

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import org.apache.hadoop.hbase.thrift.generated.AlreadyExists;
import org.apache.hadoop.hbase.thrift.generated.ColumnDescriptor;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.TCell;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClientUtils;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.yetus.audience.InterfaceAudience;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

/**
 * See the instructions under hbase-examples/README.txt
 */
@InterfaceAudience.Private
public class HttpDoAsClient {

  static protected int port;
  static protected String host;
  private static boolean secure = false;
  static protected String doAsUser = null;
  static protected String principal = null;
  static protected String keyTab = null;

  public static void main(String[] args) throws Exception {
    if (args.length < 3 || args.length > 6) {
      System.out.println("Invalid arguments!");
      System.out.println(
          "Usage: HttpDoAsClient host port doAsUserName [security=true] [principal] [keytab]");
      System.exit(-1);
    }

    host = args[0];
    port = Integer.parseInt(args[1]);
    doAsUser = args[2];
    if (args.length > 3) {
      secure = Boolean.parseBoolean(args[3]);
      if (args.length > 4) {
        principal = args[4];
        keyTab = args[5];
        if (!new File(keyTab).exists()) {
          System.err.printf("ERROR: KeyTab File %s not found %n", keyTab);
          System.exit(-1);
        }
      } else {
        principal = getSubject().getPrincipals().iterator().next().getName();
      }
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
      System.out.println("  found: " + ClientUtils.utf8(name.array()));
      if (ClientUtils.utf8(name.array()).equals(ClientUtils.utf8(t))) {
        if (refresh(client, httpClient).isTableEnabled(name)) {
          System.out.println("    disabling table: " + ClientUtils.utf8(name.array()));
          refresh(client, httpClient).disableTable(name);
        }
        System.out.println("    deleting table: " + ClientUtils.utf8(name.array()));
        refresh(client, httpClient).deleteTable(name);
      }
    }

    //
    // Create the demo table with two column families, entry: and unused:
    //
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

    System.out.println("creating table: " + ClientUtils.utf8(t));
    try {

      refresh(client, httpClient).createTable(ByteBuffer.wrap(t), columns);
    } catch (AlreadyExists ae) {
      System.out.println("WARN: " + ae.message);
    }

    System.out.println("column families in " + ClientUtils.utf8(t) + ": ");
    Map<ByteBuffer, ColumnDescriptor> columnMap = refresh(client, httpClient)
        .getColumnDescriptors(ByteBuffer.wrap(t));
    for (ColumnDescriptor col2 : columnMap.values()) {
      System.out.println("  column: " + ClientUtils.utf8(col2.name.array()) + ", maxVer: "
          + col2.maxVersions);
    }

    transport.close();
    httpClient.close();
  }

  private Hbase.Client refresh(Hbase.Client client, THttpClient httpClient) {
    httpClient.setCustomHeader("doAs", doAsUser);
    if (secure) {
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
    outputBuffer.append(Bytes.toString(Base64.getEncoder().encode(outToken)));
    System.out.print("Ticket is: " + outputBuffer);
    return outputBuffer.toString();
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

  static Subject getSubject() throws Exception {
    if (!secure) {
      return new Subject();
    }

    /*
     * To authenticate the DemoClient, kinit should be invoked ahead.
     * Here we try to get the Kerberos credential from the ticket cache.
     */
    LoginContext context;

    if (keyTab != null) {
      // To authenticate the HttpDoAsClient using principal and keyTab
      Set<Principal> principals = new HashSet<>();
      principals.add(new KerberosPrincipal(principal));
      Subject subject =
          new Subject(false, principals, new HashSet<>(), new HashSet<>());

      context = new LoginContext("", subject, null, new KerberosConfiguration(principal, keyTab));
    } else {
      /*
       * To authenticate the HttpDoAsClient, kinit should be invoked ahead. Here we try to
       * get the Kerberos credential from the ticket cache.
       */
      context = new LoginContext("", new Subject(), null, new KerberosConfiguration());
    }
    context.login();
    return context.getSubject();
  }

  private static class KerberosConfiguration extends Configuration {
    private String principal;
    private String keyTab;

    public KerberosConfiguration() {
      // Empty constructor will have no principal or keyTab values
    }

    public KerberosConfiguration(String principal, String keyTab) {
      this.principal = principal;
      this.keyTab = keyTab;
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
      Map<String, String> options = new HashMap<>();
      if (principal != null && keyTab != null) {
        options.put("principal", principal);
        options.put("keyTab", keyTab);
        options.put("useKeyTab", "true");
        options.put("storeKey", "true");
      } else {
        options.put("useKeyTab", "false");
        options.put("storeKey", "false");
      }
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

      return new AppConfigurationEntry[] {
        new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule",
          AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options) };
    }
  }
}
