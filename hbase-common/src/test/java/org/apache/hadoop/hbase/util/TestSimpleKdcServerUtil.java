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
package org.apache.hadoop.hbase.util;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.kerby.kerberos.kerb.KrbException;
import org.apache.kerby.kerberos.kerb.server.SimpleKdcServer;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, MediumTests.class})
public class TestSimpleKdcServerUtil {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSimpleKdcServerUtil.class);
  private static final HBaseCommonTestingUtility UTIL = new HBaseCommonTestingUtility();

  /**
   * Test we are able to ride over clashing port... BindException.. when starting up a
   * kdc server.
   */
  @Test
  public void testBindException() throws KrbException, IOException {
    SimpleKdcServer kdc = null;
    try {
      File dir = new File(UTIL.getDataTestDir().toString());
      kdc = SimpleKdcServerUtil.
        getRunningSimpleKdcServer(dir, HBaseCommonTestingUtility::randomFreePort, true);
      kdc.createPrincipal("wah");
    } finally {
      kdc.stop();
    }
  }
}
