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
package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Exception thrown by HTable methods when an attempt to do something (like
 * commit changes) fails after a bunch of retries.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class RetriesExhaustedException extends IOException {
  private static final long serialVersionUID = 1876775844L;

  public RetriesExhaustedException(final String msg) {
    super(msg);
  }

  public RetriesExhaustedException(final String msg, final IOException e) {
    super(msg, e);
  }

  /**
   * Datastructure that allows adding more info around Throwable incident.
   */
  public static class ThrowableWithExtraContext {
    private final Throwable t;
    private final long when;
    private final String extras;

    public ThrowableWithExtraContext(final Throwable t, final long when,
        final String extras) {
      this.t = t;
      this.when = when;
      this.extras = extras;
    }
 
    @Override
    public String toString() {
      return new Date(this.when).toString() + ", " + extras + ", " + t.toString();
    }
  }

  /**
   * Create a new RetriesExhaustedException from the list of prior failures.
   * @param callableVitals Details from the Callable we were using
   * when we got this exception.
   * @param numTries The number of tries we made
   * @param exceptions List of exceptions that failed before giving up
   */
  public RetriesExhaustedException(final String callableVitals, int numTries,
      List<Throwable> exceptions) {
    super(getMessage(callableVitals, numTries, exceptions));
  }

  /**
   * Create a new RetriesExhaustedException from the list of prior failures.
   * @param numTries
   * @param exceptions List of exceptions that failed before giving up
   */
  public RetriesExhaustedException(final int numTries,
                                   final List<ThrowableWithExtraContext> exceptions) {
    super(getMessage(numTries, exceptions),
        (exceptions != null && !exceptions.isEmpty() ?
            exceptions.get(exceptions.size() - 1).t : null));
  }

  private static String getMessage(String callableVitals, int numTries,
      List<Throwable> exceptions) {
    StringBuilder buffer = new StringBuilder("Failed contacting ");
    buffer.append(callableVitals);
    buffer.append(" after ");
    buffer.append(numTries + 1);
    buffer.append(" attempts.\nExceptions:\n");
    for (Throwable t : exceptions) {
      buffer.append(t.toString());
      buffer.append("\n");
    }
    return buffer.toString();
  }

  private static String getMessage(final int numTries,
      final List<ThrowableWithExtraContext> exceptions) {
    StringBuilder buffer = new StringBuilder("Failed after attempts=");
    buffer.append(numTries + 1);
    buffer.append(", exceptions:\n");
    for (ThrowableWithExtraContext t : exceptions) {
      buffer.append(t.toString());
      buffer.append("\n");
    }
    return buffer.toString();
  }
}
