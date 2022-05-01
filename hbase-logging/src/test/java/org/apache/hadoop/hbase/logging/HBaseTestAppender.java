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
package org.apache.hadoop.hbase.logging;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractOutputStreamAppender;
import org.apache.logging.log4j.core.appender.ManagerFactory;
import org.apache.logging.log4j.core.appender.OutputStreamManager;
import org.apache.logging.log4j.core.appender.rolling.FileSize;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;
import org.apache.logging.log4j.core.config.plugins.validation.constraints.Required;

/**
 * Log4j2 appender to be used when running UTs.
 * <p/>
 * The main point here is to limit the total output size to prevent eating all the space of our ci
 * system when something wrong in our code.
 * <p/>
 * See HBASE-26947 for more details.
 */
@Plugin(name = HBaseTestAppender.PLUGIN_NAME, category = Core.CATEGORY_NAME,
    elementType = Appender.ELEMENT_TYPE, printObject = true)
public final class HBaseTestAppender extends AbstractOutputStreamAppender<OutputStreamManager> {

  public static final String PLUGIN_NAME = "HBaseTest";
  private static final HBaseTestManagerFactory FACTORY = new HBaseTestManagerFactory();

  public static class Builder<B extends Builder<B>> extends AbstractOutputStreamAppender.Builder<B>
    implements org.apache.logging.log4j.core.util.Builder<HBaseTestAppender> {

    @PluginBuilderAttribute
    @Required
    private Target target;

    @PluginBuilderAttribute
    @Required
    private String maxSize;

    public B setTarget(Target target) {
      this.target = target;
      return asBuilder();
    }

    public B setMaxSize(String maxSize) {
      this.maxSize = maxSize;
      return asBuilder();
    }

    @Override
    public HBaseTestAppender build() {
      long size = FileSize.parse(maxSize, -1);
      if (size <= 0) {
        LOGGER.error("Invalid maxSize {}", size);
      }
      Layout<? extends Serializable> layout = getOrCreateLayout(StandardCharsets.UTF_8);
      OutputStreamManager manager =
        OutputStreamManager.getManager(target.name(), FACTORY, new FactoryData(target, layout));
      return new HBaseTestAppender(getName(), layout, getFilter(), isIgnoreExceptions(),
        isImmediateFlush(), getPropertyArray(), manager, size);
    }
  }

  /**
   * Data to pass to factory method.Unable to instantiate
   */
  private static class FactoryData {
    private final Target target;
    private final Layout<? extends Serializable> layout;

    public FactoryData(Target target, Layout<? extends Serializable> layout) {
      this.target = target;
      this.layout = layout;
    }
  }

  /**
   * Factory to create the Appender.
   */
  private static class HBaseTestManagerFactory
    implements ManagerFactory<HBaseTestOutputStreamManager, FactoryData> {

    /**
     * Create an OutputStreamManager.
     * @param name The name of the entity to manage.
     * @param data The data required to create the entity.
     * @return The OutputStreamManager
     */
    @Override
    public HBaseTestOutputStreamManager createManager(final String name, final FactoryData data) {
      return new HBaseTestOutputStreamManager(data.target, data.layout);
    }
  }

  @PluginBuilderFactory
  public static <B extends Builder<B>> B newBuilder() {
    return new Builder<B>().asBuilder();
  }

  private final long maxSize;

  private final AtomicLong size = new AtomicLong(0);

  private final AtomicBoolean stop = new AtomicBoolean(false);

  private HBaseTestAppender(String name, Layout<? extends Serializable> layout, Filter filter,
    boolean ignoreExceptions, boolean immediateFlush, Property[] properties,
    OutputStreamManager manager, long maxSize) {
    super(name, layout, filter, ignoreExceptions, immediateFlush, properties, manager);
    this.maxSize = maxSize;
  }

  @Override
  public void append(LogEvent event) {
    if (stop.get()) {
      return;
    }
    // for accounting, here we always convert the event to byte array first
    // this will effect performance a bit but it is OK since this is for UT only
    byte[] bytes = getLayout().toByteArray(event);
    if (bytes == null || bytes.length == 0) {
      return;
    }
    long sizeAfterAppend = size.addAndGet(bytes.length);
    if (sizeAfterAppend >= maxSize) {
      // stop logging if the log size exceeded the limit
      if (stop.compareAndSet(false, true)) {
        LOGGER.error("Log size exceeded the limit {}, will stop logging to prevent eating"
          + " too much disk space", maxSize);
      }
      return;
    }
    super.append(event);
  }
}
