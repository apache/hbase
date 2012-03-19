package org.apache.hadoop.hbase.util;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.IPv4AddressTruncationMapping;

public class RackManager {
  static final Log LOG = LogFactory.getLog(RackManager.class);
  private DNSToSwitchMapping switchMapping;

  public RackManager(Configuration conf) {
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

  /**
   * Get the name of the rack containing a server, according to the DNS to
   * switch mapping.
   * @param info the server for which to get the rack name
   * @return the rack name of the server
   */
  public String getRack(HServerInfo info) {
    List<String> racks = switchMapping.resolve(Arrays.asList(
        new String[]{info.getServerAddress().getInetSocketAddress()
            .getAddress().getHostAddress()}));
    if (racks != null && racks.size() > 0) {
      return racks.get(0);
    }

    return HConstants.UNKOWN_RACK;
  }
}
