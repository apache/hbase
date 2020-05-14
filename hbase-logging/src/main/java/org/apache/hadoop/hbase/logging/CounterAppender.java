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
package org.apache.hadoop.hbase.logging;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * A log appender for collecting the log event count.
 */
@org.apache.logging.log4j.core.config.plugins.Plugin(name = CounterAppender.PLUGIN_NAME,
  category = org.apache.logging.log4j.core.Core.CATEGORY_NAME,
  elementType = org.apache.logging.log4j.core.Appender.ELEMENT_TYPE, printObject = true)
@InterfaceAudience.Private
public class CounterAppender extends org.apache.logging.log4j.core.appender.AbstractAppender {

  public static final String PLUGIN_NAME = "Counter";

  @org.apache.logging.log4j.core.config.plugins.PluginFactory
  public static CounterAppender createAppender(
    @org.apache.logging.log4j.core.config.plugins.PluginAttribute(value = "name") String name) {
    return new CounterAppender(name);
  }

  private CounterAppender(String name) {
    super(name, null, null, true, org.apache.logging.log4j.core.config.Property.EMPTY_ARRAY);
  }

  @Override
  public void append(org.apache.logging.log4j.core.LogEvent event) {
    org.apache.logging.log4j.Level level = event.getLevel();
    if (level == org.apache.logging.log4j.Level.INFO) {
      EventCounter.info();
    } else if (level == org.apache.logging.log4j.Level.WARN) {
      EventCounter.warn();
    } else if (level == org.apache.logging.log4j.Level.ERROR) {
      EventCounter.error();
    } else if (level == org.apache.logging.log4j.Level.FATAL) {
      EventCounter.fatal();
    }
  }
}
