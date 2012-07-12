package org.apache.hadoop.hbase.thrift;

import javax.management.ObjectName;

import org.apache.hadoop.hbase.metrics.MetricsMBeanBase;
import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.hadoop.metrics.util.MetricsRegistry;

public class ThriftMBean extends MetricsMBeanBase{
  
  private final ObjectName mbeanName;

  public ThriftMBean(MetricsRegistry registry, String rsName) {
    super(registry, rsName);

    mbeanName = MBeanUtil.registerMBean(rsName,
        rsName, this);
  }
  
  public void shutdown() {
    if (mbeanName != null){
      MBeanUtil.unregisterMBean(mbeanName);
    }
  }
  
}
