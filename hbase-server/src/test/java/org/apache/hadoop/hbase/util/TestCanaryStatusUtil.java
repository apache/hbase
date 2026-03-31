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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MiscTests.TAG)
@Tag(SmallTests.TAG)
public class TestCanaryStatusUtil {

  static final ServerName FAKE_HOST = ServerName.valueOf("fakehost", 12345, 1234567890);

  @Test
  public void testServerNameLink() {
    String link = CanaryStatusUtil.serverNameLink(FAKE_HOST);

    assertNotNull(link);
    assertEquals("<a href=\"//fakehost:12346/\">fakehost,12345,1234567890</a>", link);
  }

  @Test
  public void testServerNameLinkNoInfoPort() {
    ServerName serverName = mock(ServerName.class);
    doReturn(-1).when(serverName).getPort();
    doReturn("fakehost,12345,1234567890").when(serverName).getServerName();

    String link = CanaryStatusUtil.serverNameLink(serverName);

    assertNotNull(link);
    assertEquals("fakehost,12345,1234567890", link);
  }
}
