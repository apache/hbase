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
package org.apache.hadoop.hbase.coprocessor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.yetus.audience.InterfaceAudience;


/**
 * Wraps a Configuration to make it read-only.
 */
@InterfaceAudience.Private
public class ReadOnlyConfiguration extends Configuration {
  private final Configuration conf;

  public ReadOnlyConfiguration(final Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void setDeprecatedProperties() {
    throw new UnsupportedOperationException("Read-only Configuration");
  }

  @Override
  public void addResource(String name) {
    throw new UnsupportedOperationException("Read-only Configuration");
  }

  @Override
  public void addResource(URL url) {
    throw new UnsupportedOperationException("Read-only Configuration");
  }

  @Override
  public void addResource(Path file) {
    throw new UnsupportedOperationException("Read-only Configuration");
  }

  @Override
  public void addResource(InputStream in) {
    throw new UnsupportedOperationException("Read-only Configuration");
  }

  @Override
  public void addResource(InputStream in, String name) {
    throw new UnsupportedOperationException("Read-only Configuration");
  }

  @Override
  public void addResource(Configuration conf) {
    throw new UnsupportedOperationException("Read-only Configuration");
  }

  @Override
  public synchronized void reloadConfiguration() {
    // This is a write operation. We need to allow it though because if any Configuration in
    // current JVM context calls addDefaultResource, this forces a reload of all Configurations
    // (all Configurations are 'registered' by the default constructor. Rather than turn
    // somersaults, let this 'write' operation through.
    this.conf.reloadConfiguration();
  }

  @Override
  public String get(String name) {
    return conf.get(name);
  }

  // Do not add @Override because it is not in Hadoop 2.6.5
  public void setAllowNullValueProperties(boolean val) {
    throw new UnsupportedOperationException("Read-only Configuration");
  }

  @Override
  public String getTrimmed(String name) {
    return conf.getTrimmed(name);
  }

  @Override
  public String getTrimmed(String name, String defaultValue) {
    return conf.getTrimmed(name, defaultValue);
  }

  @Override
  public String getRaw(String name) {
    return conf.getRaw(name);
  }

  @Override
  public void set(String name, String value) {
    throw new UnsupportedOperationException("Read-only Configuration");
  }

  @Override
  public void set(String name, String value, String source) {
    throw new UnsupportedOperationException("Read-only Configuration");
  }

  @Override
  public synchronized void unset(String name) {
    throw new UnsupportedOperationException("Read-only Configuration");
  }

  @Override
  public synchronized void setIfUnset(String name, String value) {
    throw new UnsupportedOperationException("Read-only Configuration");
  }

  @Override
  public String get(String name, String defaultValue) {
    return conf.get(name, defaultValue);
  }

  @Override
  public int getInt(String name, int defaultValue) {
    return conf.getInt(name, defaultValue);
  }

  @Override
  public int[] getInts(String name) {
    return conf.getInts(name);
  }

  @Override
  public void setInt(String name, int value) {
    throw new UnsupportedOperationException("Read-only Configuration");
  }

  @Override
  public long getLong(String name, long defaultValue) {
    return conf.getLong(name, defaultValue);
  }

  @Override
  public long getLongBytes(String name, long defaultValue) {
    return conf.getLongBytes(name, defaultValue);
  }

  @Override
  public void setLong(String name, long value) {
    throw new UnsupportedOperationException("Read-only Configuration");
  }

  @Override
  public float getFloat(String name, float defaultValue) {
    return conf.getFloat(name, defaultValue);
  }

  @Override
  public void setFloat(String name, float value) {
    throw new UnsupportedOperationException("Read-only Configuration");
  }

  @Override
  public double getDouble(String name, double defaultValue) {
    return conf.getDouble(name, defaultValue);
  }

  @Override
  public void setDouble(String name, double value) {
    throw new UnsupportedOperationException("Read-only Configuration");
  }

  @Override
  public boolean getBoolean(String name, boolean defaultValue) {
    return conf.getBoolean(name, defaultValue);
  }

  @Override
  public void setBoolean(String name, boolean value) {
    throw new UnsupportedOperationException("Read-only Configuration");
  }

  @Override
  public void setBooleanIfUnset(String name, boolean value) {
    throw new UnsupportedOperationException("Read-only Configuration");
  }

  @Override
  public <T extends Enum<T>> void setEnum(String name, T value) {
    throw new UnsupportedOperationException("Read-only Configuration");
  }

  @Override
  public <T extends Enum<T>> T getEnum(String name, T defaultValue) {
    return conf.getEnum(name, defaultValue);
  }

  @Override
  public void setTimeDuration(String name, long value, TimeUnit unit) {
    throw new UnsupportedOperationException("Read-only Configuration");
  }

  @Override
  public long getTimeDuration(String name, long defaultValue, TimeUnit unit) {
    return conf.getTimeDuration(name, defaultValue, unit);
  }

  @Override
  public Pattern getPattern(String name, Pattern defaultValue) {
    return conf.getPattern(name, defaultValue);
  }

  @Override
  public void setPattern(String name, Pattern pattern) {
    throw new UnsupportedOperationException("Read-only Configuration");
  }

  @Override
  public synchronized String[] getPropertySources(String name) {
    return conf.getPropertySources(name);
  }

  @Override
  public Configuration.IntegerRanges getRange(String name, String defaultValue) {
    return conf.getRange(name, defaultValue);
  }

  @Override
  public Collection<String> getStringCollection(String name) {
    return conf.getStringCollection(name);
  }

  @Override
  public String[] getStrings(String name) {
    return conf.getStrings(name);
  }

  @Override
  public String[] getStrings(String name, String... defaultValue) {
    return conf.getStrings(name, defaultValue);
  }

  @Override
  public Collection<String> getTrimmedStringCollection(String name) {
    return conf.getTrimmedStringCollection(name);
  }

  @Override
  public String[] getTrimmedStrings(String name) {
    return conf.getTrimmedStrings(name);
  }

  @Override
  public String[] getTrimmedStrings(String name, String... defaultValue) {
    return conf.getTrimmedStrings(name, defaultValue);
  }

  @Override
  public void setStrings(String name, String... values) {
    throw new UnsupportedOperationException("Read-only Configuration");
  }

  @Override
  public char[] getPassword(String name) throws IOException {
    return conf.getPassword(name);
  }

  @Override
  public InetSocketAddress getSocketAddr(String hostProperty, String addressProperty,
      String defaultAddressValue, int defaultPort) {
    return conf.getSocketAddr(hostProperty, addressProperty, defaultAddressValue, defaultPort);
  }

  @Override
  public InetSocketAddress getSocketAddr(String name, String defaultAddress, int defaultPort) {
    return conf.getSocketAddr(name, defaultAddress, defaultPort);
  }

  @Override
  public void setSocketAddr(String name, InetSocketAddress addr) {
    throw new UnsupportedOperationException("Read-only Configuration");
  }

  @Override
  public InetSocketAddress updateConnectAddr(String hostProperty, String addressProperty,
      String defaultAddressValue, InetSocketAddress addr) {
    throw new UnsupportedOperationException("Read-only Configuration");
  }

  @Override
  public InetSocketAddress updateConnectAddr(String name, InetSocketAddress addr) {
    throw new UnsupportedOperationException("Read-only Configuration");
  }

  @Override
  public Class<?> getClassByName(String name) throws ClassNotFoundException {
    return conf.getClassByName(name);
  }

  @Override
  public Class<?> getClassByNameOrNull(String name) {
    return conf.getClassByNameOrNull(name);
  }

  @Override
  public Class<?>[] getClasses(String name, Class<?>... defaultValue) {
    return conf.getClasses(name, defaultValue);
  }

  @Override
  public Class<?> getClass(String name, Class<?> defaultValue) {
    return conf.getClass(name, defaultValue);
  }

  @Override
  public <U> Class<? extends U> getClass(String name, Class<? extends U> defaultValue,
      Class<U> xface) {
    return conf.getClass(name, defaultValue, xface);
  }

  @Override
  public <U> List<U> getInstances(String name, Class<U> xface) {
    return conf.getInstances(name, xface);
  }

  @Override
  public void setClass(String name, Class<?> theClass, Class<?> xface) {
    throw new UnsupportedOperationException("Read-only Configuration");
  }

  @Override
  public Path getLocalPath(String dirsProp, String path) throws IOException {
    return conf.getLocalPath(dirsProp, path);
  }

  @Override
  public File getFile(String dirsProp, String path) throws IOException {
    return conf.getFile(dirsProp, path);
  }

  @Override
  public URL getResource(String name) {
    return conf.getResource(name);
  }

  @Override
  public InputStream getConfResourceAsInputStream(String name) {
    return conf.getConfResourceAsInputStream(name);
  }

  @Override
  public Reader getConfResourceAsReader(String name) {
    return conf.getConfResourceAsReader(name);
  }

  @Override
  public Set<String> getFinalParameters() {
    return conf.getFinalParameters();
  }

  @Override
  public int size() {
    return conf.size();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException("Read-only Configuration");
  }

  @Override
  public Iterator<Map.Entry<String, String>> iterator() {
    return conf.iterator();
  }

  @Override
  public void writeXml(OutputStream out) throws IOException {
    conf.writeXml(out);
  }

  @Override
  public void writeXml(Writer out) throws IOException {
    conf.writeXml(out);
  }

  @Override
  public ClassLoader getClassLoader() {
    return conf.getClassLoader();
  }

  @Override
  public void setClassLoader(ClassLoader classLoader) {
    throw new UnsupportedOperationException("Read-only Configuration");
  }

  @Override
  public String toString() {
    return conf.toString();
  }

  @Override
  public synchronized void setQuietMode(boolean quietmode) {
    throw new UnsupportedOperationException("Read-only Configuration");
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    throw new UnsupportedOperationException("Read-only Configuration");
  }

  @Override
  public void write(DataOutput out) throws IOException {
    conf.write(out);
  }

  @Override
  public Map<String, String> getValByRegex(String regex) {
    return conf.getValByRegex(regex);
  }
}
