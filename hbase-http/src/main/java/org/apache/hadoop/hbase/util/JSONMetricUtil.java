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
 * */
package org.apache.hadoop.hbase.util;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.RuntimeMXBean;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.openmbean.CompositeData;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public final class JSONMetricUtil {

  private static final Logger LOG = LoggerFactory.getLogger(JSONMetricUtil.class);

  private static MBeanServer mbServer = ManagementFactory.getPlatformMBeanServer();
  // MBeans ObjectName domain names
  public static final String JAVA_LANG_DOMAIN = "java.lang";
  public static final String JAVA_NIO_DOMAIN = "java.nio";
  public static final String SUN_MGMT_DOMAIN = "com.sun.management";
  public static final String HADOOP_DOMAIN = "Hadoop";

  // MBeans ObjectName properties key names
  public static final String TYPE_KEY = "type";
  public static final String NAME_KEY = "name";
  public static final String SERVICE_KEY = "service";
  public static final String SUBSYSTEM_KEY = "sub";

  /**
   * Utility for getting metric values. Collection of static methods intended for easier access to
   * metric values.
   */
  private JSONMetricUtil() {
    // Not to be called
  }

  public static MBeanAttributeInfo[] getMBeanAttributeInfo(ObjectName bean)
      throws IntrospectionException, InstanceNotFoundException, ReflectionException,
      IntrospectionException, javax.management.IntrospectionException {
    MBeanInfo mbinfo = mbServer.getMBeanInfo(bean);
    return mbinfo.getAttributes();
  }

  public static Object getValueFromMBean(ObjectName bean, String attribute) {
    Object value = null;
    try {
      value = mbServer.getAttribute(bean, attribute);
    } catch (Exception e) {
      LOG.error("Unable to get value from MBean= " + bean.toString() + "for attribute=" +
        attribute + " " + e.getMessage());
    }
    return value;
  }

  /**
   * Returns a subset of mbeans defined by qry. Modeled after DumpRegionServerMetrics#dumpMetrics.
   * Example: String qry= "java.lang:type=Memory"
   * @throws MalformedObjectNameException if json have bad format
   * @throws IOException /
   * @return String representation of json array.
   */
  public static String dumpBeanToString(String qry)
      throws MalformedObjectNameException, IOException {
    StringWriter sw = new StringWriter(1024 * 100); // Guess this size
    try (PrintWriter writer = new PrintWriter(sw)) {
      JSONBean dumper = new JSONBean();
      try (JSONBean.Writer jsonBeanWriter = dumper.open(writer)) {
        MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
        jsonBeanWriter.write(mbeanServer, new ObjectName(qry), null, false);
      }
    }
    sw.close();
    return sw.toString();
  }

  /**
   * Method for building map used for constructing ObjectName. Mapping is done with arrays indices
   * @param keys Map keys
   * @param values Map values
   * @return Map or null if arrays are empty * or have different number of elements
   */
  public static Hashtable<String, String> buldKeyValueTable(String[] keys, String[] values) {
    if (keys.length != values.length) {
      LOG.error("keys and values arrays must be same size");
      return null;
    }
    if (keys.length == 0 || values.length == 0) {
      LOG.error("keys and values arrays can not be empty;");
      return null;
    }
    Hashtable<String, String> table = new Hashtable<>();
    for (int i = 0; i < keys.length; i++) {
      table.put(keys[i], values[i]);
    }
    return table;
  }

  public static ObjectName buildObjectName(String pattern) throws MalformedObjectNameException {
    return new ObjectName(pattern);
  }

  public static ObjectName buildObjectName(String domain, Hashtable<String, String> keyValueTable)
      throws MalformedObjectNameException {
    return new ObjectName(domain, keyValueTable);
  }

  public static Set<ObjectName> getRegistredMBeans(ObjectName name, MBeanServer mbs) {
    return mbs.queryNames(name, null);
  }

  public static String getProcessPID() {
    return ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
  }

  public static String getCommmand() throws MalformedObjectNameException, IOException {
    RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
    return runtimeBean.getSystemProperties().get("sun.java.command");
  }

  public static List<GarbageCollectorMXBean> getGcCollectorBeans() {
    List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
    return gcBeans;
  }

  public static long getLastGcDuration(ObjectName gcCollector) {
    long lastGcDuration = 0;
    Object lastGcInfo = getValueFromMBean(gcCollector, "LastGcInfo");
    if (lastGcInfo != null && lastGcInfo instanceof CompositeData) {
      CompositeData cds = (CompositeData) lastGcInfo;
      lastGcDuration = (long) cds.get("duration");
    }
    return lastGcDuration;
  }

  public static List<MemoryPoolMXBean> getMemoryPools() {
    List<MemoryPoolMXBean> mPools = ManagementFactory.getMemoryPoolMXBeans();
    return mPools;
  }

  public static float calcPercentage(long a, long b) {
    if (a == 0 || b == 0) {
      return 0;
    }
    return ((float) a / (float) b) * 100;
  }
}
