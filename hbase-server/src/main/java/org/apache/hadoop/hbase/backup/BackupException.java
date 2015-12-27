/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.backup;

import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * Backup exception
 */
@SuppressWarnings("serial")
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BackupException extends HBaseIOException {
  private BackupContext description;

  /**
   * Some exception happened for a backup and don't even know the backup that it was about
   * @param msg Full description of the failure
   */
  public BackupException(String msg) {
    super(msg);
  }

  /**
   * Some exception happened for a backup with a cause
   * @param cause the cause
   */
  public BackupException(Throwable cause) {
    super(cause);
  }

  /**
   * Exception for the given backup that has no previous root cause
   * @param msg reason why the backup failed
   * @param desc description of the backup that is being failed
   */
  public BackupException(String msg, BackupContext desc) {
    super(msg);
    this.description = desc;
  }

  /**
   * Exception for the given backup due to another exception
   * @param msg reason why the backup failed
   * @param cause root cause of the failure
   * @param desc description of the backup that is being failed
   */
  public BackupException(String msg, Throwable cause, BackupContext desc) {
    super(msg, cause);
    this.description = desc;
  }

  /**
   * Exception when the description of the backup cannot be determined, due to some other root
   * cause
   * @param message description of what caused the failure
   * @param e root cause
   */
  public BackupException(String message, Exception e) {
    super(message, e);
  }

  public BackupContext getBackupContext() {
    return this.description;
  }

}
