package org.apache.hadoop.hbase.util.customthreadattribute;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import java.util.List;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface AttributeTypeHandler {

  /**
   * Get all the attributes that are enabled from the current thread's context
   * @return List of {@link CustomThreadAttribute}
   */
  public List<CustomThreadAttribute> getAllAttributes();

  /**
   * Sets the attributes into current thread's context
   */
  public void setAttribute(String key, Object value);

  /**
   * Clears the attributes from the current thread's context
   */
  public void clearAttribute(String key);

  /**
   * Get an attribute from the current thread's context
   * @return {@link CustomThreadAttribute}
   */
  public CustomThreadAttribute getAttribute(String key);
}
