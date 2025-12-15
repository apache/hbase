package org.apache.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hbase.thirdparty.com.google.common.base.Strings;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Singleton class for managing the META_TABLE_NAME instance.
 * This allows the meta table name to be overridden for testing using reflection.
 */
@InterfaceAudience.Public
public class MetaTableName {
  private static final Logger LOG = LoggerFactory.getLogger(MetaTableName.class);
  
  /**
   * The singleton instance of the meta table name.
   * This field can be overridden for testing using reflection.
   */
  private static volatile TableName instance;

  private MetaTableName() {
    // Private constructor to prevent instantiation
  }

  /**
   * Get the singleton instance of the meta table name.
   * Initializes lazily using the default configuration if not already set.
   * 
   * @return The meta table name instance
   */
  public static TableName getInstance() {
    if (instance == null) {
      synchronized (MetaTableName.class) {
        if (instance == null) {
          instance = initializeHbaseMetaTableName(HBaseConfiguration.create());
          LOG.info("Meta table name initialized: {}", instance);
        }
      }
    }
    return instance;
  }

  /**
   * Initialize the meta table name from the given configuration.
   * 
   * @param conf The configuration to use
   * @return The initialized meta table name
   */
  private static TableName initializeHbaseMetaTableName(Configuration conf) {
    String suffix_val = conf.get(HConstants.HBASE_META_TABLE_SUFFIX,
      HConstants.HBASE_META_TABLE_SUFFIX_DEFAULT_VALUE);
    LOG.info("Meta table suffix value: {}", suffix_val);
    if (Strings.isNullOrEmpty(suffix_val)) {
      return TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "meta");
    } else {
      return TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "meta_" + suffix_val);
    }
  }

  /**
   * Get the instance field for reflection-based testing.
   * This method is package-private to allow test classes to access the field.
   * 
   * @return The Field object for the instance field
   */
  static java.lang.reflect.Field getInstanceField() throws NoSuchFieldException {
    java.lang.reflect.Field field = MetaTableName.class.getDeclaredField("instance");
    field.setAccessible(true);
    return field;
  }
}

