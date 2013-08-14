package org.apache.hadoop.hbase.trace;

import org.apache.hadoop.conf.Configuration;
import org.cloudera.htrace.HTraceConfiguration;

public class HBaseHTraceConfiguration extends HTraceConfiguration {

  public static final String KEY_PREFIX = "hbase.";
  private Configuration conf;

  public HBaseHTraceConfiguration(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public String get(String key) {
    return conf.get(KEY_PREFIX +key);
  }

  @Override
  public String get(String key, String defaultValue) {
    return conf.get(KEY_PREFIX + key,defaultValue);

  }

  @Override
  public boolean getBoolean(String key, boolean defaultValue) {
    return conf.getBoolean(KEY_PREFIX + key, defaultValue);
  }
}
