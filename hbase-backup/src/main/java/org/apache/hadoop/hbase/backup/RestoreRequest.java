/*
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

import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * POJO class for restore request
 */
@InterfaceAudience.Private
public final class RestoreRequest {
  public static class Builder {
    RestoreRequest request;

    public Builder() {
      request = new RestoreRequest();
    }

    public Builder withBackupRootDir(String backupRootDir) {
      request.setBackupRootDir(backupRootDir);
      return this;
    }

    public Builder withRestoreRootDir(String restoreRootDir) {
      request.setRestoreRootDir(restoreRootDir);
      return this;
    }

    public Builder withBackupId(String backupId) {
      request.setBackupId(backupId);
      return this;
    }

    public Builder withCheck(boolean check) {
      request.setCheck(check);
      return this;
    }

    public Builder withFromTables(TableName[] fromTables) {
      request.setFromTables(fromTables);
      return this;
    }

    public Builder withToTables(TableName[] toTables) {
      request.setToTables(toTables);
      return this;
    }

    public Builder withOvewrite(boolean overwrite) {
      request.setOverwrite(overwrite);
      return this;
    }

    public Builder withKeepOriginalSplits(boolean keepOriginalSplits) {
      request.setKeepOriginalSplits(keepOriginalSplits);
      return this;
    }

    public RestoreRequest build() {
      return request;
    }
  }

  private String backupRootDir;
  private String restoreRootDir;
  private String backupId;
  private boolean check = false;
  private TableName[] fromTables;
  private TableName[] toTables;
  private boolean overwrite = false;

  private boolean keepOriginalSplits = false;

  private RestoreRequest() {
  }

  public String getBackupRootDir() {
    return backupRootDir;
  }

  private RestoreRequest setBackupRootDir(String backupRootDir) {
    this.backupRootDir = backupRootDir;
    return this;
  }

  public String getRestoreRootDir() {
    return restoreRootDir;
  }

  private RestoreRequest setRestoreRootDir(String restoreRootDir) {
    this.restoreRootDir = restoreRootDir;
    return this;
  }

  public String getBackupId() {
    return backupId;
  }

  private RestoreRequest setBackupId(String backupId) {
    this.backupId = backupId;
    return this;
  }

  public boolean isCheck() {
    return check;
  }

  private RestoreRequest setCheck(boolean check) {
    this.check = check;
    return this;
  }

  public TableName[] getFromTables() {
    return fromTables;
  }

  private RestoreRequest setFromTables(TableName[] fromTables) {
    this.fromTables = fromTables;
    return this;
  }

  public TableName[] getToTables() {
    return toTables;
  }

  private RestoreRequest setToTables(TableName[] toTables) {
    this.toTables = toTables;
    return this;
  }

  public boolean isOverwrite() {
    return overwrite;
  }

  private RestoreRequest setOverwrite(boolean overwrite) {
    this.overwrite = overwrite;
    return this;
  }

  public boolean isKeepOriginalSplits() {
    return keepOriginalSplits;
  }

  private RestoreRequest setKeepOriginalSplits(boolean keepOriginalSplits) {
    this.keepOriginalSplits = keepOriginalSplits;
    return this;
  }
}
