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

package org.apache.hadoop.hbase.metrics.file;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hbase.SmallTests;

import org.junit.*;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

/**
 * Test for TimeStampingMetricsContext functionality.
 * (FQN class names are used to suppress javac warnings in imports.)
 */
@Category(SmallTests.class)
@SuppressWarnings("deprecation")
public class TestTimeStampingMetricsContext {

  private static final int updatePeriodSeconds = 2;

  private TimeStampingFileContext mc;

  @Test
  public void testFileUpdate() throws Exception {
    final Date start = new Date();
    final File metricOutFile = FileUtil.createLocalTempFile(
      new File(FileUtils.getTempDirectory(),getClass().getName() + "-out-"), "", true);
    assertTrue(metricOutFile.exists());
    assertEquals(0L, metricOutFile.length());

    mc = new TimeStampingFileContext();
    org.apache.hadoop.metrics.ContextFactory cf
      = org.apache.hadoop.metrics.ContextFactory.getFactory();
    cf.setAttribute("test1.fileName", metricOutFile.getAbsolutePath());
    cf.setAttribute("test1.period", Integer.toString(updatePeriodSeconds));
    mc.init("test1", cf);

    assertEquals("test1", mc.getContextName());

    org.apache.hadoop.metrics.MetricsRecord r = mc.createRecord("testRecord");
    r.setTag("testTag1", "testTagValue1");
    r.setTag("testTag2", "testTagValue2");
    r.setMetric("testMetric1", 1);
    r.setMetric("testMetric2", 33);
    r.update();

    mc.startMonitoring();
    assertTrue(mc.isMonitoring());

    // wait 3/2 of the update period:
    Thread.sleep((1000 * updatePeriodSeconds * 3)/2);

    mc.stopMonitoring();
    assertFalse(mc.isMonitoring());

    mc.close();

    Map<String, Collection<org.apache.hadoop.metrics.spi.OutputRecord>> m = mc.getAllRecords();
    assertEquals(1, m.size());
    Collection<org.apache.hadoop.metrics.spi.OutputRecord> outputRecords = m.get("testRecord");
    assertNotNull(outputRecords);
    assertEquals(1, outputRecords.size());
    org.apache.hadoop.metrics.spi.OutputRecord outputRecord = outputRecords.iterator().next();
    assertNotNull(outputRecord);

    String outStr = FileUtils.readFileToString(metricOutFile);
    assertTrue(outStr.length() > 0);
    int pos = outStr.indexOf(" ");
    String time = outStr.substring(0, pos);

    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    Date date = df.parse(time);
    assertTrue(date.after(start));
    assertTrue(date.before(new Date()));

    String reminder = outStr.substring(pos);
    assertEquals(" test1.testRecord: testTag1=testTagValue1, testTag2=testTagValue2, testMetric1=1,"
      +" testMetric2=33\n", reminder);
  }

}
