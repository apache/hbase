/**
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.regionserver;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;

/**
 * Do a shallow merge of multiple KV configuration pools. This is a very useful
 * utility class to easily add per-object configurations in addition to wider
 * scope settings. This is different from Configuration.addResource()
 * functionality, which performs a deep merge and mutates the common data
 * structure.
 * <p>
 * For clarity: the shallow merge allows the user to mutate either of the
 * configuration objects and have changes reflected everywhere. In contrast to a
 * deep merge, that requires you to explicitly know all applicable copies to
 * propagate changes.
 * <p>
 * This class is package private because we expect significant refactoring here
 * on the HBase side when certain HDFS changes are added & ubiquitous. Will
 * revisit expanding access at that point.
 */
@InterfaceAudience.Private
class CompoundConfiguration extends Configuration {
  /**
   * Default Constructor. Initializes empty configuration
   */
  public CompoundConfiguration() {
  }

  // Devs: these APIs are the same contract as their counterparts in
  // Configuration.java
  private static interface ImmutableConfigMap {
    String get(String key);
    String getRaw(String key);
    Class<?> getClassByName(String name) throws ClassNotFoundException;
    int size();
  }

  protected List<ImmutableConfigMap> configs
    = new ArrayList<ImmutableConfigMap>();

  /****************************************************************************
   * These initial APIs actually required original thought
   ***************************************************************************/

  /**
   * Add Hadoop Configuration object to config list
   * @param conf configuration object
   * @return this, for builder pattern
   */
  public CompoundConfiguration add(final Configuration conf) {
    if (conf instanceof CompoundConfiguration) {
      this.configs.addAll(0, ((CompoundConfiguration) conf).configs);
      return this;
    }
    // put new config at the front of the list (top priority)
    this.configs.add(0, new ImmutableConfigMap() {
      Configuration c = conf;

      @Override
      public String get(String key) {
        return c.get(key);
      }

      @Override
      public String getRaw(String key) {
        return c.getRaw(key);
      }

      @Override
      public Class<?> getClassByName(String name)
          throws ClassNotFoundException {
        return c.getClassByName(name);
      }

      @Override
      public int size() {
        return c.size();
      }

      @Override
      public String toString() {
        return c.toString();
      }
    });
    return this;
  }

  /**
   * Add ImmutableBytesWritable map to config list. This map is generally
   * created by HTableDescriptor or HColumnDescriptor, but can be abstractly
   * used.
   *
   * @param map
   *          ImmutableBytesWritable map
   * @return this, for builder pattern
   */
  public CompoundConfiguration add(
      final Map<ImmutableBytesWritable, ImmutableBytesWritable> map) {
    // put new map at the front of the list (top priority)
    this.configs.add(0, new ImmutableConfigMap() {
      Map<ImmutableBytesWritable, ImmutableBytesWritable> m = map;

      @Override
      public String get(String key) {
        ImmutableBytesWritable ibw = new ImmutableBytesWritable(Bytes
            .toBytes(key));
        if (!m.containsKey(ibw))
          return null;
        ImmutableBytesWritable value = m.get(ibw);
        if (value == null || value.get() == null)
          return null;
        return Bytes.toString(value.get());
      }

      @Override
      public String getRaw(String key) {
        return get(key);
      }

      @Override
      public Class<?> getClassByName(String name)
      throws ClassNotFoundException {
        return null;
      }

      @Override
      public int size() {
        // TODO Auto-generated method stub
        return m.size();
      }

      @Override
      public String toString() {
        return m.toString();
      }
    });
    return this;
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("CompoundConfiguration: " + this.configs.size() + " configs");
    for (ImmutableConfigMap m : this.configs) {
      sb.append(this.configs);
    }
    return sb.toString();
  }

  @Override
  public String get(String key) {
    for (ImmutableConfigMap m : this.configs) {
      String value = m.get(key);
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  @Override
  public String getRaw(String key) {
    for (ImmutableConfigMap m : this.configs) {
      String value = m.getRaw(key);
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  @Override
  public Class<?> getClassByName(String name) throws ClassNotFoundException {
    for (ImmutableConfigMap m : this.configs) {
      try {
        Class<?> value = m.getClassByName(name);
        if (value != null) {
          return value;
        }
      } catch (ClassNotFoundException e) {
        // don't propagate an exception until all configs fail
        continue;
      }
    }
    throw new ClassNotFoundException();
  }

  @Override
  public int size() {
    int ret = 0;
    for (ImmutableConfigMap m : this.configs) {
      ret += m.size();
    }
    return ret;
  }

  /***************************************************************************
   * You should just ignore everything below this line unless there's a bug in
   * Configuration.java...
   *
   * Below get APIs are directly copied from Configuration.java Oh, how I wish
   * this wasn't so! A tragically-sad example of why you use interfaces instead
   * of inheritance.
   *
   * Why the duplication? We basically need to override Configuration.getProps
   * or we'd need protected access to Configuration.properties so we can modify
   * that pointer. There are a bunch of functions in the base Configuration that
   * call getProps() and we need to use our derived version instead of the base
   * version. We need to make a generic implementation that works across all
   * HDFS versions. We should modify Configuration.properties in HDFS 1.0 to be
   * protected, but we still need to have this code until that patch makes it to
   * all the HDFS versions we support.
   ***************************************************************************/

  @Override
  public String get(String name, String defaultValue) {
    String ret = get(name);
    return ret == null ? defaultValue : ret;
  }

  @Override
  public int getInt(String name, int defaultValue) {
    String valueString = get(name);
    if (valueString == null)
      return defaultValue;
    try {
      String hexString = getHexDigits(valueString);
      if (hexString != null) {
        return Integer.parseInt(hexString, 16);
      }
      return Integer.parseInt(valueString);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  @Override
  public long getLong(String name, long defaultValue) {
    String valueString = get(name);
    if (valueString == null)
      return defaultValue;
    try {
      String hexString = getHexDigits(valueString);
      if (hexString != null) {
        return Long.parseLong(hexString, 16);
      }
      return Long.parseLong(valueString);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  protected String getHexDigits(String value) {
    boolean negative = false;
    String str = value;
    String hexString = null;
    if (value.startsWith("-")) {
      negative = true;
      str = value.substring(1);
    }
    if (str.startsWith("0x") || str.startsWith("0X")) {
      hexString = str.substring(2);
      if (negative) {
        hexString = "-" + hexString;
      }
      return hexString;
    }
    return null;
  }

  @Override
  public float getFloat(String name, float defaultValue) {
    String valueString = get(name);
    if (valueString == null)
      return defaultValue;
    try {
      return Float.parseFloat(valueString);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  @Override
  public boolean getBoolean(String name, boolean defaultValue) {
    String valueString = get(name);
    if ("true".equals(valueString))
      return true;
    else if ("false".equals(valueString))
      return false;
    else return defaultValue;
  }

  @Override
  public IntegerRanges getRange(String name, String defaultValue) {
    return new IntegerRanges(get(name, defaultValue));
  }

  @Override
  public Collection<String> getStringCollection(String name) {
    String valueString = get(name);
    return StringUtils.getStringCollection(valueString);
  }

  @Override
  public String[] getStrings(String name) {
    String valueString = get(name);
    return StringUtils.getStrings(valueString);
  }

  @Override
  public String[] getStrings(String name, String... defaultValue) {
    String valueString = get(name);
    if (valueString == null) {
      return defaultValue;
    } else {
      return StringUtils.getStrings(valueString);
    }
  }

  @Override
  public Class<?>[] getClasses(String name, Class<?>... defaultValue) {
    String[] classnames = getStrings(name);
    if (classnames == null)
      return defaultValue;
    try {
      Class<?>[] classes = new Class<?>[classnames.length];
      for (int i = 0; i < classnames.length; i++) {
        classes[i] = getClassByName(classnames[i]);
      }
      return classes;
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Class<?> getClass(String name, Class<?> defaultValue) {
    String valueString = get(name);
    if (valueString == null)
      return defaultValue;
    try {
      return getClassByName(valueString);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public <U> Class<? extends U> getClass(String name,
      Class<? extends U> defaultValue, Class<U> xface) {
    try {
      Class<?> theClass = getClass(name, defaultValue);
      if (theClass != null && !xface.isAssignableFrom(theClass))
        throw new RuntimeException(theClass + " not " + xface.getName());
      else if (theClass != null)
        return theClass.asSubclass(xface);
      else
        return null;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /*******************************************************************
   * This class is immutable. Quickly abort any attempts to alter it *
   *******************************************************************/

  @Override
  public void clear() {
    throw new UnsupportedOperationException("Immutable Configuration");
  }

  @Override
  public Iterator<Map.Entry<String, String>> iterator() {
    throw new UnsupportedOperationException("Immutable Configuration");
  }

  @Override
  public void set(String name, String value) {
    throw new UnsupportedOperationException("Immutable Configuration");
  }
  @Override
  public void setIfUnset(String name, String value) {
    throw new UnsupportedOperationException("Immutable Configuration");
  }
  @Override
  public void setInt(String name, int value) {
    throw new UnsupportedOperationException("Immutable Configuration");
  }
  @Override
  public void setLong(String name, long value) {
    throw new UnsupportedOperationException("Immutable Configuration");
  }
  @Override
  public void setFloat(String name, float value) {
    throw new UnsupportedOperationException("Immutable Configuration");
  }
  @Override
  public void setBoolean(String name, boolean value) {
    throw new UnsupportedOperationException("Immutable Configuration");
  }
  @Override
  public void setBooleanIfUnset(String name, boolean value) {
    throw new UnsupportedOperationException("Immutable Configuration");
  }
  @Override
  public void setStrings(String name, String... values) {
    throw new UnsupportedOperationException("Immutable Configuration");
  }
  @Override
  public void setClass(String name, Class<?> theClass, Class<?> xface) {
    throw new UnsupportedOperationException("Immutable Configuration");
  }
  @Override
  public void setClassLoader(ClassLoader classLoader) {
    throw new UnsupportedOperationException("Immutable Configuration");
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    throw new UnsupportedOperationException("Immutable Configuration");
  }

  @Override
  public void write(DataOutput out) throws IOException {
    throw new UnsupportedOperationException("Immutable Configuration");
  }

  @Override
  public void writeXml(OutputStream out) throws IOException {
    throw new UnsupportedOperationException("Immutable Configuration");
  }
};
