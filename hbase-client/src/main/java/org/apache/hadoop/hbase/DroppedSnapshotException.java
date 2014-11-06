/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.io.IOException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;


/**
 * Thrown during flush if the possibility snapshot content was not properly
 * persisted into store files.  Response should include replay of hlog content.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class DroppedSnapshotException extends IOException {

  private static final long serialVersionUID = -5463156580831677374L;

  /**
   * @param msg
   */
  public DroppedSnapshotException(String msg) {
    super(msg);
  }

  /**
   * default constructor
   */
  public DroppedSnapshotException() {
    super();
  }
}
