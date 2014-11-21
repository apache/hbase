package org.apache.hadoop.hbase.consensus.fsm;

import org.apache.hadoop.hbase.metrics.MetricsBase;
import org.weakref.jmx.MBeanExporter;

import java.util.Collections;

public class FSMMetrics extends MetricsBase {
  /** Type string used when exporting an MBean for these metrics */
  public static final String TYPE = "FSMMetrics";
  /** Domain string used when exporting an MBean for these metrics */
  public static final String DOMAIN = "org.apache.hadoop.hbase.consensus.fsm";

  String name;
  String procId;

  public FSMMetrics(final String name, final String procId,
    final MBeanExporter exporter) {
      super(DOMAIN, TYPE, name, procId, Collections.<String, String>emptyMap(),
        exporter);
      this.name = name;
      this.procId = procId;
  }
}
