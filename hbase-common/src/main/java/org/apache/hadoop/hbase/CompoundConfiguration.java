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
package org.apache.hadoop.hbase;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.iterators.UnmodifiableIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Do a shallow merge of multiple KV configuration pools. This is a very useful
 * utility class to easily add per-object configurations in addition to wider
 * scope settings. This is different from Configuration.addResource()
 * functionality, which performs a deep merge and mutates the common data
 * structure.
 * <p>
 * The iterator on CompoundConfiguration is unmodifiable. Obtaining iterator is an expensive
 * operation.
 * <p>
 * For clarity: the shallow merge allows the user to mutate either of the
 * configuration objects and have changes reflected everywhere. In contrast to a
 * deep merge, that requires you to explicitly know all applicable copies to
 * propagate changes.
 * 
 * WARNING: The values set in the CompoundConfiguration are do not handle Property variable
 * substitution.  However, if they are set in the underlying configuration substitutions are
 * done. 
 */
@InterfaceAudience.Private
public class CompoundConfiguration extends Configuration {

  private Configuration mutableConf = null;

  /**
   * Default Constructor. Initializes empty configuration
   */
  public CompoundConfiguration() {
  }

  // Devs: these APIs are the same contract as their counterparts in
  // Configuration.java
  private interface ImmutableConfigMap extends Iterable<Map.Entry<String,String>> {
    String get(String key);
    String getRaw(String key);
    Class<?> getClassByName(String name) throws ClassNotFoundException;
    int size();
  }

  private final List<ImmutableConfigMap> configs
    = new ArrayList<ImmutableConfigMap>();

  static class ImmutableConfWrapper implements  ImmutableConfigMap {
   private final Configuration c;
    
    ImmutableConfWrapper(Configuration conf) {
      c = conf;
    }

    @Override
    public Iterator<Map.Entry<String,String>> iterator() {
      return c.iterator();
    }
    
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
  }

  /**
   * If set has been called, it will create a mutableConf.  This converts the mutableConf to an
   * immutable one and resets it to allow a new mutable conf.  This is used when a new map or
   * conf is added to the compound configuration to preserve proper override semantics.
   */
  void freezeMutableConf() {
    if (mutableConf == null) {
      // do nothing if there is no current mutableConf
      return;
    }

    this.configs.add(0, new ImmutableConfWrapper(mutableConf));
    mutableConf = null;
  }

  /**
   * Add Hadoop Configuration object to config list.
   * The added configuration overrides the previous ones if there are name collisions.
   * @param conf configuration object
   * @return this, for builder pattern
   */
  public CompoundConfiguration add(final Configuration conf) {
    freezeMutableConf();

    if (conf instanceof CompoundConfiguration) {
      this.configs.addAll(0, ((CompoundConfiguration) conf).configs);
      return this;
    }
    // put new config at the front of the list (top priority)
    this.configs.add(0, new ImmutableConfWrapper(conf));
    return this;
  }

  /**
   * Add Bytes map to config list. This map is generally
   * created by HTableDescriptor or HColumnDescriptor, but can be abstractly
   * used. The added configuration overrides the previous ones if there are
   * name collisions.
   *
   * @param map
   *          Bytes map
   * @return this, for builder pattern
   */
  public CompoundConfiguration addBytesMap(
      final Map<Bytes, Bytes> map) {
    freezeMutableConf();

    // put new map at the front of the list (top priority)
    this.configs.add(0, new ImmutableConfigMap() {
      private final Map<Bytes, Bytes> m = map;

      @Override
      public Iterator<Map.Entry<String,String>> iterator() {
        Map<String, String> ret = new HashMap<String, String>();
        for (Map.Entry<Bytes, Bytes> entry : map.entrySet()) {
          String key = Bytes.toString(entry.getKey().get());
          String val = entry.getValue() == null ? null : Bytes.toString(entry.getValue().get());
          ret.put(key, val);
        }
        return ret.entrySet().iterator();
      }
      
      @Override
      public String get(String key) {
        Bytes ibw = new Bytes(Bytes
            .toBytes(key));
        if (!m.containsKey(ibw))
          return null;
        Bytes value = m.get(ibw);
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
        return m.size();
      }

      @Override
      public String toString() {
        return m.toString();
      }
    });
    return this;
  }

  /**
   * Add String map to config list. This map is generally created by HTableDescriptor
   * or HColumnDescriptor, but can be abstractly used. The added configuration
   * overrides the previous ones if there are name collisions.
   *
   * @return this, for builder pattern
   */
  public CompoundConfiguration addStringMap(final Map<String, String> map) {
    freezeMutableConf();

    // put new map at the front of the list (top priority)
    this.configs.add(0, new ImmutableConfigMap() {
      private final Map<String, String> m = map;

      @Override
      public Iterator<Map.Entry<String,String>> iterator() {
        return map.entrySet().iterator();
      }

      @Override
      public String get(String key) {
        return m.get(key);
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
    if (mutableConf != null) {
      String value = mutableConf.get(key);
      if (value != null) {
        return value;
      }
    }

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
    if (mutableConf != null) {
      String value = mutableConf.getRaw(key);
      if (value != null) {
        return value;
      }
    }

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
    if (mutableConf != null) {
      Class<?> value = mutableConf.getClassByName(name);
      if (value != null) {
        return value;
      }
    }

    for (ImmutableConfigMap m : this.configs) {
      Class<?> value = m.getClassByName(name);
      if (value != null) {
        return value;
      }
    }
    throw new ClassNotFoundException();
  }

  // TODO: This method overestimates the number of configuration settings -- if a value is masked
  // by an overriding config or map, it will be counted multiple times. 
  @Override
  public int size() {
    int ret = 0;

    if (mutableConf != null) {
      ret += mutableConf.size();
    }

    for (ImmutableConfigMap m : this.configs) {
      ret += m.size();
    }
    return ret;
  }

  /**
   * Get the value of the <code>name</code>. If the key is deprecated,
   * it returns the value of the first key which replaces the deprecated key
   * and is not null.
   * If no such property exists,
   * then <code>defaultValue</code> is returned.

   * The CompooundConfiguration does not do property substitution.  To do so we need
   * Configuration.getProps to be protected or package visible.  Though in hadoop2 it is
   * protected, in hadoop1 the method is private and not accessible.
   * 
   * All of the get* methods call this overridden get method.
   * 
   * @param name property name.
   * @param defaultValue default value.
   * @return property value, or <code>defaultValue</code> if the property 
   *         doesn't exist.                    
   **/
  @Override
  public String get(String name, String defaultValue) {
    String ret = get(name);
    return ret == null ? defaultValue : ret;
  }

  @Override
  public Iterator<Map.Entry<String, String>> iterator() {
    Map<String, String> ret = new HashMap<String, String>();

    // add in reverse order so that oldest get overridden.
    if (!configs.isEmpty()) {
      for (int i = configs.size() - 1; i >= 0; i--) {
        ImmutableConfigMap map = configs.get(i);
        Iterator<Map.Entry<String, String>> iter = map.iterator();
        while (iter.hasNext()) {
          Map.Entry<String, String> entry = iter.next();
          ret.put(entry.getKey(), entry.getValue());
        }
      }
    }

    // add mutations to this CompoundConfiguration last.
    if (mutableConf != null) {
      Iterator<Map.Entry<String, String>> miter = mutableConf.iterator();
      while (miter.hasNext()) {
        Map.Entry<String, String> entry = miter.next();
        ret.put(entry.getKey(), entry.getValue());
      }
    }

    return UnmodifiableIterator.decorate(ret.entrySet().iterator());
  }

  @Override
  public void set(String name, String value) {
    if (mutableConf == null) {
      // not thread safe
      mutableConf = new Configuration(false); // an empty configuration
    }
    mutableConf.set(name,  value);
  }

  /***********************************************************************************************
   * These methods are unsupported, and no code using CompoundConfiguration depend upon them.
   * Quickly abort upon any attempts to use them. 
   **********************************************************************************************/

  @Override
  public void clear() {
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
