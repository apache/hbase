package org.apache.hadoop.hbase.coprocessor.example.row.stats.utils;

import org.apache.hadoop.conf.Configuration;

@InterfaceAudience.Private
public class ConfigurationUtil {

  private static final String ROW_STATISTICS_PREFIX = "hubspot.row.statistics.";

  public static int getInt(Configuration conf, String name, int defaultValue) {
    return conf.getInt(ROW_STATISTICS_PREFIX + name, defaultValue);
  }

  public static long getLong(Configuration conf, String name, long defaultValue) {
    return conf.getLong(ROW_STATISTICS_PREFIX + name, defaultValue);
  }

  private static void setInt(Configuration conf, String name, int defaultValue) {
    conf.setInt(name, conf.getInt(ROW_STATISTICS_PREFIX + name, defaultValue));
  }

  private static void setLong(Configuration conf, String name, long defaultValue) {
    conf.setLong(name, conf.getLong(ROW_STATISTICS_PREFIX + name, defaultValue));
  }
}
