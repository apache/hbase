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
package org.apache.hadoop.hbase.mapreduce;

import static org.junit.Assert.*;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SmallTests.class})
public class TestTableInputFormatBase {
  @Test
  public void testTableInputFormatBaseReverseDNSForIPv6()
      throws UnknownHostException {
    String address = "ipv6.google.com";
    String localhost = null;
    InetAddress addr = null;
    TableInputFormat inputFormat = new TableInputFormat();
    try {
      localhost = InetAddress.getByName(address).getCanonicalHostName();
      addr = Inet6Address.getByName(address);
    } catch (UnknownHostException e) {
      // google.com is down, we can probably forgive this test.
      return;
    }
    System.out.println("Should retrun the hostname for this host " +
        localhost + " addr : " + addr);
    String actualHostName = inputFormat.reverseDNS(addr);
    assertEquals("Should retrun the hostname for this host. Expected : " +
        localhost + " Actual : " + actualHostName, localhost, actualHostName);
  }
}
