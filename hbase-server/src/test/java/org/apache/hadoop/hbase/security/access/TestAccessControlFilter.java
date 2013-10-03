/*
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

package org.apache.hadoop.hbase.security.access;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.AccessControlService;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import com.google.protobuf.BlockingRpcChannel;

@Category(LargeTests.class)
public class TestAccessControlFilter {
  @Rule public TestName name = new TestName();
  private static Log LOG = LogFactory.getLog(TestAccessControlFilter.class);
  private static HBaseTestingUtility TEST_UTIL;

  private static User ADMIN;
  private static User READER;
  private static User LIMITED;
  private static User DENIED;

  
  private static TableName TABLE;
  private static byte[] FAMILY = Bytes.toBytes("f1");
  private static byte[] PRIVATE_COL = Bytes.toBytes("private");
  private static byte[] PUBLIC_COL = Bytes.toBytes("public");

  @Before 
  public void setup () {
    TABLE = TableName.valueOf(name.getMethodName());
  }
  
  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    Configuration conf = TEST_UTIL.getConfiguration();
    SecureTestUtil.enableSecurity(conf);
    String baseuser = User.getCurrent().getShortName();
    conf.set("hbase.superuser", conf.get("hbase.superuser", "") +
        String.format(",%s.hfs.0,%s.hfs.1,%s.hfs.2", baseuser, baseuser, baseuser));
    TEST_UTIL.startMiniCluster();
    TEST_UTIL.waitTableEnabled(AccessControlLists.ACL_TABLE_NAME.getName());

    ADMIN = User.createUserForTesting(conf, "admin", new String[]{"supergroup"});
    READER = User.createUserForTesting(conf, "reader", new String[0]);
    LIMITED = User.createUserForTesting(conf, "limited", new String[0]);
    DENIED = User.createUserForTesting(conf, "denied", new String[0]);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testQualifierAccess() throws Exception {
    final HTable table = TEST_UTIL.createTable(TABLE, FAMILY);
    try {
      doQualifierAccess(table);
    } finally {
      table.close();
    }
  }

  private void doQualifierAccess(final HTable table) throws IOException, InterruptedException {
    // set permissions
    ADMIN.runAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        HTable aclmeta = new HTable(TEST_UTIL.getConfiguration(),
            AccessControlLists.ACL_TABLE_NAME);
        byte[] table = Bytes.toBytes(name.getMethodName());
        BlockingRpcChannel service = aclmeta.coprocessorService(table);
        AccessControlService.BlockingInterface protocol =
          AccessControlService.newBlockingStub(service);
        ProtobufUtil.grant(protocol, READER.getShortName(),
          TABLE, null, null, Permission.Action.READ);
        ProtobufUtil.grant(protocol, LIMITED.getShortName(),
          TABLE, FAMILY, PUBLIC_COL, Permission.Action.READ);
        return null;
      }
    });

    // put some test data
    List<Put> puts = new ArrayList<Put>(100);
    for (int i=0; i<100; i++) {
      Put p = new Put(Bytes.toBytes(i));
      p.add(FAMILY, PRIVATE_COL, Bytes.toBytes("secret "+i));
      p.add(FAMILY, PUBLIC_COL, Bytes.toBytes("info "+i));
      puts.add(p);
    }
    table.put(puts);

    // test read
    READER.runAs(new PrivilegedExceptionAction<Object>() {
      public Object run() throws Exception {
        Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
        // force a new RS connection
        conf.set("testkey", UUID.randomUUID().toString());
        HTable t = new HTable(conf, TABLE);
        ResultScanner rs = t.getScanner(new Scan());
        int rowcnt = 0;
        for (Result r : rs) {
          rowcnt++;
          int rownum = Bytes.toInt(r.getRow());
          assertTrue(r.containsColumn(FAMILY, PRIVATE_COL));
          assertEquals("secret "+rownum, Bytes.toString(r.getValue(FAMILY, PRIVATE_COL)));
          assertTrue(r.containsColumn(FAMILY, PUBLIC_COL));
          assertEquals("info "+rownum, Bytes.toString(r.getValue(FAMILY, PUBLIC_COL)));
        }
        assertEquals("Expected 100 rows returned", 100, rowcnt);
        return null;
      }
    });

    // test read with qualifier filter
    LIMITED.runAs(new PrivilegedExceptionAction<Object>() {
      public Object run() throws Exception {
        Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
        // force a new RS connection
        conf.set("testkey", UUID.randomUUID().toString());
        HTable t = new HTable(conf, TABLE);
        ResultScanner rs = t.getScanner(new Scan());
        int rowcnt = 0;
        for (Result r : rs) {
          rowcnt++;
          int rownum = Bytes.toInt(r.getRow());
          assertFalse(r.containsColumn(FAMILY, PRIVATE_COL));
          assertTrue(r.containsColumn(FAMILY, PUBLIC_COL));
          assertEquals("info " + rownum, Bytes.toString(r.getValue(FAMILY, PUBLIC_COL)));
        }
        assertEquals("Expected 100 rows returned", 100, rowcnt);
        return null;
      }
    });

    // test as user with no permission
    DENIED.runAs(new PrivilegedExceptionAction(){
      public Object run() throws Exception {
        try {
          Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
          // force a new RS connection
          conf.set("testkey", UUID.randomUUID().toString());
          HTable t = new HTable(conf, TABLE);
          ResultScanner rs = t.getScanner(new Scan());
          fail("Attempt to open scanner should have been denied");
        } catch (AccessDeniedException ade) {
          // expected
        }
        return null;
      }
    });
  }
}
