package org.apache.hadoop.hbase.util;

import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.IPv4AddressTruncationMapping;

public class Rack {
  static final Log LOG = LogFactory.getLog(Rack.class);
  private DNSToSwitchMapping switchMapping;
  public Rack(Configuration conf) {
    Class<DNSToSwitchMapping> clz = (Class<DNSToSwitchMapping>)
        conf.getClass("hbase.util.ip.to.rack.determiner",
        IPv4AddressTruncationMapping.class);
    try {
      switchMapping = clz.newInstance();
    } catch (InstantiationException e) {
      LOG.warn("using IPv4AddressTruncationMapping, failed to instantiate " +
          clz.getName(), e);
    } catch (IllegalAccessException e) {
      LOG.warn("using IPv4AddressTruncationMapping, failed to instantiate " +
          clz.getName(), e);
    }
    if (switchMapping == null) {
      switchMapping = new IPv4AddressTruncationMapping();
    }
  }

  public String getRack(InetSocketAddress addr) {
    String rack = switchMapping.resolve(Arrays.asList(
        new String[]{addr.getAddress().getHostAddress()})).get(0);
    if (rack != null && rack.length() > 0) {
      return rack;
    }
    return "unknown";
  }
}
