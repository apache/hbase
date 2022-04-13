package org.apache.hadoop.hbase.util.customthreadattribute;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * Defines the type of the custom attribute and a property prefix for it
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public enum AttributeType {
  // These string literals will be moved to HConstants
  REQUEST_ID_CONTEXT(HConstants.CUSTOM_THREAD_ATTRIBUTE_REQUEST_ID_CONTEXT_PREFIX);

  private String propertyPrefix;

  private AttributeType(String propertyPrefix) {
    this.propertyPrefix = propertyPrefix;
  }

  @Override public String toString() {
    return this.propertyPrefix;
  }
}
