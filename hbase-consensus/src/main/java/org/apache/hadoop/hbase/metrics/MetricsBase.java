package org.apache.hadoop.hbase.metrics;

import com.google.common.base.Joiner;
import org.weakref.jmx.JmxException;
import org.weakref.jmx.MBeanExporter;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Collections;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This class provides a skeleton implementation of class which can be
 * exported as a {@link javax.management.DynamicMBean}, using the
 * {@link org.weakref.jmx} package.
 */
@ThreadSafe
public abstract class MetricsBase {
  private final String mbeanName;
  private MBeanExporter exporter;

  /**
   * Construct an object which can be exported as MBean with the given name and
   * exporter. The given name will be used to construct an
   * {@link javax.management.ObjectName}, which has specific requirements.
   * The caller is responsible to pass a valid name.
   * @param mbeanName name to be used to export the MBean
   * @param exporter exporter to be used to export this object
   */
  public MetricsBase(final String mbeanName, final MBeanExporter exporter) {
    this.mbeanName = checkNotNull(mbeanName, "Name can not be null");
    this.exporter = exporter;
  }


  /**
   * Construct an object which will exported as MBean using a
   * {@link javax.management.ObjectName} that follows the following pattern:
   * <domain>:type=<type>,name=<name>,proc=<prodId>,<extendedAttributes> where
   * the extended attributes are a map of key value strings.
   *
   * @param domain the domain this MBean should belong to
   * @param type the type of the MBean
   * @param name the name of the MBean
   * @param procId a identifier making this MBean unique, for example the PID of
   *               the running JVM
   * @param extendedAttributes a key value map of strings containing additional
   *                           attributes to be added
   * @param exporter the exporter to be used to export this MBean
   */
  public MetricsBase(final String domain, final String type, final String name,
          final String procId, final Map<String, String> extendedAttributes,
          final MBeanExporter exporter) {
    this(getMBeanName(domain, type, name, procId, extendedAttributes),
            exporter);
  }

  /**
   * Get the {@link javax.management.ObjectName} as a string of the MBean
   * backed by this object.
   */
  public String getMBeanName() {
    return mbeanName;
  }

  /**
   * Get the {@link MBeanExporter} used to export this object.
   */
  public synchronized MBeanExporter getMBeanExporter() {
    return exporter;
  }

  /**
   * Set the given {@link MBeanExporter} as the exporter to be used to
   * export/un-export this object.
   * @param exporter exporter to be used to export this object
   */
  public synchronized void setMBeanExporter(final MBeanExporter exporter) {
    this.exporter = exporter;
  }

  /**
   * Check if this object is exported as MBean by the set {@link MBeanExporter}.
   * @return true if this object is exported as MBean
   */
  public boolean isExported() {
    MBeanExporter exporter = getMBeanExporter();
    Map<String, Object> exportedObjects = Collections.emptyMap();
    if (exporter != null) {
      exportedObjects = exporter.getExportedObjects();
    }
    return exportedObjects.containsKey(mbeanName);
  }

  /**
   * Export this object as MBean.
   * @throws JmxException if the object could not be exported
   */
  public void export() throws JmxException {
    MBeanExporter exporter = getMBeanExporter();
    if (exporter != null) {
      exporter.export(mbeanName, this);
    }
  }

  /**
   * Convenience method which will set the given {@link MBeanExporter} and
   * export this object as MBean.
   * @param exporter MBeanExporter to use when exporting the object
   * @throws JmxException if the object could not be exported
   */
  public synchronized void export(final MBeanExporter exporter)
          throws JmxException {
    setMBeanExporter(checkNotNull(exporter, "MBeanExporter can not be null"));
    export();
  }

  /**
   * Un-export the MBean backed by this object.
   * @throws JmxException if the MBean could not be un-exported
   */
  public void unexport() throws JmxException {
    MBeanExporter exporter = getMBeanExporter();
    if (exporter != null) {
      exporter.unexport(mbeanName);
    }
  }

  @Override
  public String toString() {
    return mbeanName;
  }

  /**
   * Get an MBean name that follows the following pattern:
   * <domain>:type=<type>,name=<name>,proc=<prodId>,<extendedAttributes> where
   * the extended attributes are a map of key value strings.

   * @param domain the domain this MBean should belong to
   * @param type the type of the MBean
   * @param name the name of the MBean
   * @param procId a identifier making this MBean unique, such as the PID of
   *               the running JVM
   * @param extendedAttributes a key value map of strings containing additional
   *                           attributes to be added
   * @return the MBean name as string
   */
  public static String getMBeanName(final String domain, final String type,
          final String name, final String procId,
          final Map<String, String> extendedAttributes) {
    if (!extendedAttributes.isEmpty()) {
      return String.format("%s:type=%s,name=%s,proc=%s,%s", domain, type, name,
              procId, Joiner.on(",").withKeyValueSeparator("=").join(
                      extendedAttributes));
    }
    return String.format("%s:type=%s,name=%s,proc=%s", domain, type, name,
            procId);
  }
}
