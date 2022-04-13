package org.apache.hadoop.hbase.util.customthreadattribute;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * This class defines a custom thread attribute
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CustomThreadAttribute {
  private String key;
  private Object value;
  private AttributeType type;

  /**
   * Constructor for initializing a custom thread attribute
   * @param key An attribute's Key
   * @param value An attribute's Value
   * @param type An attribute's Type
   */
  public CustomThreadAttribute(String key, Object value, AttributeType type) {
    this.key = key;
    this.value = value;
    this.type = type;
  }

  /**
   * @return Attribute's Key
   */
  public String getKey() {
    return key;
  }

  /**
   * @return Attribute's Value
   */
  public Object getValue() {
    return value;
  }

  /**
   * @return Attribute's Type
   */
  public AttributeType getType() {
    return type;
  }

  /**
   * set Attribute's Type
   */
  public void setType(AttributeType type) {
    this.type = type;
  }
}
