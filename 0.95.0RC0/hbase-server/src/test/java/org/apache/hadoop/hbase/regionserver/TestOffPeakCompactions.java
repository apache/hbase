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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Calendar;
import java.util.GregorianCalendar;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.regionserver.compactions.OffPeakCompactions;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestOffPeakCompactions {
  private final static Log LOG = LogFactory.getLog(TestDefaultCompactSelection.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @Test
  public void testOffPeakHours() throws IOException {
    Calendar calendar = new GregorianCalendar();
    int hourOfDay = calendar.get(Calendar.HOUR_OF_DAY);
    LOG.debug("Hour of day = " + hourOfDay);
    int hourPlusOne = ((hourOfDay+1)%24);
    int hourMinusOne = ((hourOfDay-1+24)%24);
    int hourMinusTwo = ((hourOfDay-2+24)%24);

    Configuration conf = TEST_UTIL.getConfiguration();
    OffPeakCompactions opc = new OffPeakCompactions(conf);
    LOG.debug("Testing without off-peak settings...");
    assertFalse(opc.tryStartOffPeakRequest());

    // set peak hour to current time and check compact selection
    conf.setLong("hbase.offpeak.start.hour", hourMinusOne);
    conf.setLong("hbase.offpeak.end.hour", hourPlusOne);
    opc = new OffPeakCompactions(conf);
    LOG.debug("Testing compact selection with off-peak settings (" +
        hourMinusOne + ", " + hourPlusOne + ")");
    assertTrue(opc.tryStartOffPeakRequest());
    opc.endOffPeakRequest();

    // set peak hour outside current selection and check compact selection
    conf.setLong("hbase.offpeak.start.hour", hourMinusTwo);
    conf.setLong("hbase.offpeak.end.hour", hourMinusOne);
    opc = new OffPeakCompactions(conf);
    assertFalse(opc.tryStartOffPeakRequest());
  }
}
