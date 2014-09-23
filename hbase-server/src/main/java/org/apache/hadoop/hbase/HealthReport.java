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
package org.apache.hadoop.hbase;

import org.apache.hadoop.hbase.HealthChecker.HealthCheckerExitStatus;

/**
 * The Class HealthReport containing information about health of the node.
 */
class HealthReport {

  private HealthCheckerExitStatus status;
  private String healthReport;

  HealthReport(HealthCheckerExitStatus status, String healthReport) {
    super();
    this.status = status;
    this.healthReport = healthReport;
  }

  /**
   * Gets the status of the region server.
   *
   * @return HealthCheckerExitStatus
   */
  HealthCheckerExitStatus getStatus() {
    return status;
  }

  @Override
  public String toString() {
    return this.status + " " + this.healthReport;
  }

  /**
   * Gets the health report of the region server.
   *
   * @return String
   */
  String getHealthReport() {
    return healthReport;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((healthReport == null) ? 0 : healthReport.hashCode());
    result = prime * result + ((status == null) ? 0 : status.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof HealthReport)) {
      return false;
    }
    HealthReport other = (HealthReport) obj;
    if (healthReport == null) {
      if (other.healthReport != null) {
        return false;
      }
    } else if (!healthReport.equals(other.healthReport)) {
      return false;
    }
    if (status != other.status) {
      return false;
    }
    return true;
  }
}
