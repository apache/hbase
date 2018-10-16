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
package org.apache.hadoop.hbase.rest.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import javax.xml.bind.UnmarshalException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.rest.Constants;
import org.apache.hadoop.hbase.rest.model.StorageClusterVersionModel;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test class for {@link RemoteAdmin} to verify XML is parsed in a certain manner.
 */
@Category(SmallTests.class)
public class TestXmlParsing {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestXmlParsing.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestXmlParsing.class);

  @Test
  public void testParsingClusterVersion() throws Exception {
    final String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        + "<ClusterVersion Version=\"2.0.0\"/>";
    Client client = mock(Client.class);
    RemoteAdmin admin = new RemoteAdmin(client, HBaseConfiguration.create(), null);
    Response resp = new Response(200, null, Bytes.toBytes(xml));

    when(client.get("/version/cluster", Constants.MIMETYPE_XML)).thenReturn(resp);

    StorageClusterVersionModel cv = admin.getClusterVersion();
    assertEquals("2.0.0", cv.getVersion());
  }

  @Test
  public void testFailOnExternalEntities() throws Exception {
    final String externalEntitiesXml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        + " <!DOCTYPE foo [ <!ENTITY xxe SYSTEM \"/tmp/foo\"> ] >"
        + " <ClusterVersion>&xee;</ClusterVersion>";
    Client client = mock(Client.class);
    RemoteAdmin admin = new RemoteAdmin(client, HBaseConfiguration.create(), null);
    Response resp = new Response(200, null, Bytes.toBytes(externalEntitiesXml));

    when(client.get("/version/cluster", Constants.MIMETYPE_XML)).thenReturn(resp);

    try {
      admin.getClusterVersion();
      fail("Expected getClusterVersion() to throw an exception");
    } catch (IOException e) {
      assertEquals("Cause of exception ought to be a failure to parse the stream due to our " +
          "invalid external entity. Make sure this isn't just a false positive due to " +
          "implementation. see HBASE-19020.", UnmarshalException.class, e.getCause().getClass());
      final String exceptionText = StringUtils.stringifyException(e);
      final String expectedText = "\"xee\"";
      LOG.debug("exception text: '" + exceptionText + "'", e);
      assertTrue("Exception does not contain expected text", exceptionText.contains(expectedText));
    }
  }
}
