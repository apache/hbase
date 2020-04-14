/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.tool;

import java.io.IOException;
import java.io.StringWriter;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.tmpl.tool.CanaryStatusTmpl;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category({ SmallTests.class })
public class TestCanaryStatusServlet {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCanaryStatusServlet.class);

  @Test
  public void testFailures() throws IOException {
    CanaryTool.RegionStdOutSink regionStdOutSink = new CanaryTool.RegionStdOutSink();

    ServerName serverName1 = ServerName.valueOf("staging-st04.server:22600",
      1584180761635L);
    TableName fakeTableName1 = TableName.valueOf("fakeTableName1");
    RegionInfo regionInfo1 = RegionInfoBuilder.newBuilder(fakeTableName1).build();

    ServerName serverName2 = ServerName.valueOf("staging-st05.server:22600",
      1584180761636L);
    TableName fakeTableName2 = TableName.valueOf("fakeTableName2");
    RegionInfo regionInfo2 = RegionInfoBuilder.newBuilder(fakeTableName2).build();

    regionStdOutSink.publishReadFailure(serverName1, regionInfo1, new IOException());
    regionStdOutSink.publishWriteFailure(serverName2, regionInfo2, new IOException());
    CanaryStatusTmpl tmpl = new CanaryStatusTmpl();
    StringWriter renderResultWriter = new StringWriter();
    tmpl.render(renderResultWriter, regionStdOutSink);
    String renderResult = renderResultWriter.toString();
    Assert.assertTrue(renderResult.contains("staging-st04.server,22600"));
    Assert.assertTrue(renderResult.contains("fakeTableName1"));
    Assert.assertTrue(renderResult.contains("staging-st05.server,22600"));
    Assert.assertTrue(renderResult.contains("fakeTableName2"));

  }

  @Test
  public void testReadFailuresOnly() throws IOException {
    CanaryTool.RegionStdOutSink regionStdOutSink = new CanaryTool.RegionStdOutSink();

    ServerName serverName1 = ServerName.valueOf("staging-st04.server:22600",
      1584180761635L);
    TableName fakeTableName1 = TableName.valueOf("fakeTableName1");
    RegionInfo regionInfo1 = RegionInfoBuilder.newBuilder(fakeTableName1).build();

    regionStdOutSink.publishReadFailure(serverName1, regionInfo1, new IOException());
    CanaryStatusTmpl tmpl = new CanaryStatusTmpl();
    StringWriter renderResultWriter = new StringWriter();
    tmpl.render(renderResultWriter, regionStdOutSink);
    String renderResult = renderResultWriter.toString();
    Assert.assertTrue(renderResult.contains("staging-st04.server,22600"));
    Assert.assertTrue(renderResult.contains("fakeTableName1"));
  }

  @Test
  public void testWriteFailuresOnly() throws IOException {
    CanaryTool.RegionStdOutSink regionStdOutSink = new CanaryTool.RegionStdOutSink();

    ServerName serverName2 = ServerName.valueOf("staging-st05.server:22600",
      1584180761636L);
    TableName fakeTableName2 = TableName.valueOf("fakeTableName2");
    RegionInfo regionInfo2 = RegionInfoBuilder.newBuilder(fakeTableName2).build();

    regionStdOutSink.publishReadFailure(serverName2, regionInfo2, new IOException());
    CanaryStatusTmpl tmpl = new CanaryStatusTmpl();
    StringWriter renderResultWriter = new StringWriter();
    tmpl.render(renderResultWriter, regionStdOutSink);
    String renderResult = renderResultWriter.toString();
    Assert.assertTrue(renderResult.contains("staging-st05.server,22600"));
    Assert.assertTrue(renderResult.contains("fakeTableName2"));

  }

  @Test
  public void testNoFailures() throws IOException {
    CanaryTool.RegionStdOutSink regionStdOutSink = new CanaryTool.RegionStdOutSink();
    CanaryStatusTmpl tmpl = new CanaryStatusTmpl();
    StringWriter renderResultWriter = new StringWriter();
    tmpl.render(renderResultWriter, regionStdOutSink);
    String renderResult = renderResultWriter.toString();
    Assert.assertTrue(renderResult.contains("Total Failed Servers: 0"));
    Assert.assertTrue(renderResult.contains("Total Failed Tables: 0"));
  }

}
