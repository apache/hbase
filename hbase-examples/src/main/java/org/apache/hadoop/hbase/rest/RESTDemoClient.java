/**
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

package org.apache.hadoop.hbase.rest;

import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.RemoteHTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

@InterfaceAudience.Private
public class RESTDemoClient {

  private static String host = "localhost";
  private static int port = 9090;
  private static boolean secure = false;
  private static org.apache.hadoop.conf.Configuration conf = null;

  public static void main(String[] args) throws Exception {
    System.out.println("REST Demo");
    System.out.println("Usage: RESTDemoClient [host=localhost] [port=9090] [secure=false]");
    System.out.println("This demo assumes you have a table called \"example\""
        + " with a column family called \"family1\"");

    // use passed in arguments instead of defaults
    if (args.length >= 1) {
      host = args[0];
    }
    if (args.length >= 2) {
      port = Integer.parseInt(args[1]);
    }
    conf = HBaseConfiguration.create();
    String principal = conf.get(Constants.REST_KERBEROS_PRINCIPAL);
    if (principal != null) {
      secure = true;
    }
    if (args.length >= 3) {
      secure = Boolean.parseBoolean(args[2]);
    }

    final RESTDemoClient client = new RESTDemoClient();
    Subject.doAs(getSubject(), new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        client.run();
        return null;
      }
    });
  }

  public void run() throws Exception {
    Cluster cluster = new Cluster();
    cluster.add(host, port);
    Client restClient = new Client(cluster, conf.getBoolean(Constants.REST_SSL_ENABLED, false));
    try (RemoteHTable remoteTable = new RemoteHTable(restClient, conf, "example")) {
      // Write data to the table
      String rowKey = "row1";
      Put p = new Put(rowKey.getBytes());
      p.addColumn("family1".getBytes(), "qualifier1".getBytes(), "value1".getBytes());
      remoteTable.put(p);

      // Get the data from the table
      Get g = new Get(rowKey.getBytes());
      Result result = remoteTable.get(g);

      Preconditions.checkArgument(result != null,
        Bytes.toString(remoteTable.getTableName()) + " should have a row with key as " + rowKey);
      System.out.println("row = " + new String(result.getRow()));
      for (Cell cell : result.rawCells()) {
        System.out.print("family = " + Bytes.toString(CellUtil.cloneFamily(cell)) + "\t");
        System.out.print("qualifier = " + Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t");
        System.out.print("value = " + Bytes.toString(CellUtil.cloneValue(cell)) + "\t");
        System.out.println("timestamp = " + Long.toString(cell.getTimestamp()));
      }
    }
  }

  static Subject getSubject() throws Exception {
    if (!secure) {
      return new Subject();
    }

    /*
     * To authenticate the demo client, kinit should be invoked ahead. Here we try to get the
     * Kerberos credential from the ticket cache.
     */
    LoginContext context = new LoginContext("", new Subject(), null, new Configuration() {
      @Override
      public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        Map<String, String> options = new HashMap<>();
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

        return new AppConfigurationEntry[] {
          new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule",
                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options) };
      }
    });
    context.login();
    return context.getSubject();
  }
}
