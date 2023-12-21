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

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The actual class for operating on log4j2.
 * <p/>
 * This class will depend on log4j directly, so callers should not use this class directly to avoid
 * introducing log4j2 dependencies to downstream users. Please call the methods in
 * {@link Log4jUtils}, as they will call the methods here through reflection.
 */
@InterfaceAudience.Private
final class InternalLog4jUtils {

  private InternalLog4jUtils() {
  }

  private static org.apache.logging.log4j.Level getLevel(String levelName)
    throws IllegalArgumentException {
    org.apache.logging.log4j.Level level =
      org.apache.logging.log4j.Level.toLevel(levelName.toUpperCase());
    if (!level.toString().equalsIgnoreCase(levelName)) {
      throw new IllegalArgumentException("Unsupported log level " + levelName);
    }
    return level;
  }

  static void setAllLevels(String loggerName, String levelName) {
    org.apache.logging.log4j.core.config.Configurator.setAllLevels(loggerName, getLevel(levelName));
  }

  static void setLogLevel(String loggerName, String levelName) {
    org.apache.logging.log4j.core.config.Configurator.setLevel(loggerName, getLevel(levelName));
  }

  static void setRootLevel(String levelName) {
    String loggerName = org.apache.logging.log4j.LogManager.getRootLogger().getName();
    setLogLevel(loggerName, levelName);
  }

  static String getEffectiveLevel(String loggerName) {
    org.apache.logging.log4j.Logger logger =
      org.apache.logging.log4j.LogManager.getLogger(loggerName);
    return logger.getLevel().name();
  }

  static Set<File> getActiveLogFiles() throws IOException {
    Set<File> ret = new HashSet<>();
    org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getRootLogger();
    if (!(logger instanceof org.apache.logging.log4j.core.Logger)) {
      return ret;
    }
    org.apache.logging.log4j.core.Logger coreLogger = (org.apache.logging.log4j.core.Logger) logger;
    for (org.apache.logging.log4j.core.Appender appender : coreLogger.getAppenders().values()) {
      if (appender instanceof org.apache.logging.log4j.core.appender.FileAppender) {
        String fileName =
          ((org.apache.logging.log4j.core.appender.FileAppender) appender).getFileName();
        ret.add(new File(fileName));
      } else if (appender instanceof org.apache.logging.log4j.core.appender.AbstractFileAppender) {
        String fileName =
          ((org.apache.logging.log4j.core.appender.AbstractFileAppender<?>) appender).getFileName();
        ret.add(new File(fileName));
      } else if (appender instanceof org.apache.logging.log4j.core.appender.RollingFileAppender) {
        String fileName =
          ((org.apache.logging.log4j.core.appender.RollingFileAppender) appender).getFileName();
        ret.add(new File(fileName));
      } else
        if (appender instanceof org.apache.logging.log4j.core.appender.RandomAccessFileAppender) {
          String fileName =
            ((org.apache.logging.log4j.core.appender.RandomAccessFileAppender) appender)
              .getFileName();
          ret.add(new File(fileName));
        } else
          if (appender instanceof org.apache.logging.log4j.core.appender.MemoryMappedFileAppender) {
            String fileName =
              ((org.apache.logging.log4j.core.appender.MemoryMappedFileAppender) appender)
                .getFileName();
            ret.add(new File(fileName));
          }
    }
    return ret;
  }
}
