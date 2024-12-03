package org.apache.hadoop.hbase.backup.replication;

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class BackupConfigurationException extends Exception {
  public BackupConfigurationException(String message) {
    super(message);
  }

  public BackupConfigurationException(String message, Throwable cause) {
    super(message, cause);
  }
}
