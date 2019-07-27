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


package org.apache.hadoop.hbase.util;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.thrift.generated.TCell;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Common Utility class for clients
 */
@InterfaceAudience.Private
public final class ClientUtils {

  private ClientUtils() {
    // Empty block
  }

  /**
   * To authenticate the demo client, kinit should be invoked ahead. Here we try to get the
   * Kerberos credential from the ticket cache
   *
   * @return LoginContext Object
   * @throws LoginException Exception thrown if unable to get LoginContext
   */
  public static LoginContext getLoginContext() throws LoginException {

    return new LoginContext(StringUtils.EMPTY, new Subject(), null, new Configuration() {
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

        return new AppConfigurationEntry[]{new AppConfigurationEntry(
            "com.sun.security.auth.module.Krb5LoginModule",
            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options)};
      }
    });

  }

  /**
   * copy values into a TreeMap to get them in sorted order and print it
   *
   * @param rowResult Holds row name and then a map of columns to cells
   */
  public static void printRow(final TRowResult rowResult) {

    TreeMap<String, TCell> sorted = new TreeMap<>();
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

  /**
   * Helper to translate byte[]'s to UTF8 strings
   *
   * @param buf byte array buffer
   * @return UTF8 decoded string value
   */
  public static String utf8(final byte[] buf) {
    try {
      return Bytes.toString(buf);
    } catch (IllegalArgumentException e) {
      return "[INVALID UTF-8]";
    }
  }

}
