/**
 *
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
package org.apache.hadoop.hbase.filter;

import com.google.protobuf.InvalidProtocolBufferException;

import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos;
import org.apache.hadoop.hbase.util.Bytes;

import org.jcodings.Encoding;
import org.jcodings.EncodingDB;
import org.jcodings.specific.UTF8Encoding;
import org.joni.Matcher;
import org.joni.Option;
import org.joni.Regex;
import org.joni.Syntax;

/**
 * This comparator is for use with {@link CompareFilter} implementations, such
 * as {@link RowFilter}, {@link QualifierFilter}, and {@link ValueFilter}, for
 * filtering based on the value of a given column. Use it to test if a given
 * regular expression matches a cell value in the column.
 * <p>
 * Only EQUAL or NOT_EQUAL comparisons are valid with this comparator.
 * <p>
 * For example:
 * <p>
 * <pre>
 * ValueFilter vf = new ValueFilter(CompareOp.EQUAL,
 *     new RegexStringComparator(
 *       // v4 IP address
 *       "(((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3,3}" +
 *         "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))(\\/[0-9]+)?" +
 *         "|" +
 *       // v6 IP address
 *       "((([\\dA-Fa-f]{1,4}:){7}[\\dA-Fa-f]{1,4})(:([\\d]{1,3}.)" +
 *         "{3}[\\d]{1,3})?)(\\/[0-9]+)?"));
 * </pre>
 * <p>
 * Supports {@link java.util.regex.Pattern} flags as well:
 * <p>
 * <pre>
 * ValueFilter vf = new ValueFilter(CompareOp.EQUAL,
 *     new RegexStringComparator("regex", Pattern.CASE_INSENSITIVE | Pattern.DOTALL));
 * </pre>
 * @see java.util.regex.Pattern
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class RegexStringComparator extends ByteArrayComparable {

  private static final Log LOG = LogFactory.getLog(RegexStringComparator.class);

  private Engine engine;

  /** Engine implementation type (default=JAVA) */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public enum EngineType {
    JAVA,
    JONI
  }

  /**
   * Constructor
   * Adds Pattern.DOTALL to the underlying Pattern
   * @param expr a valid regular expression
   */
  public RegexStringComparator(String expr) {
    this(expr, Pattern.DOTALL);
  }

  /**
   * Constructor
   * Adds Pattern.DOTALL to the underlying Pattern
   * @param expr a valid regular expression
   * @param engine engine implementation type
   */
  public RegexStringComparator(String expr, EngineType engine) {
    this(expr, Pattern.DOTALL, engine);
  }

  /**
   * Constructor
   * @param expr a valid regular expression
   * @param flags java.util.regex.Pattern flags
   */
  public RegexStringComparator(String expr, int flags) {
    this(expr, flags, EngineType.JAVA);
  }

  /**
   * Constructor
   * @param expr a valid regular expression
   * @param flags java.util.regex.Pattern flags
   * @param engine engine implementation type
   */
  public RegexStringComparator(String expr, int flags, EngineType engine) {
    super(Bytes.toBytes(expr));
    switch (engine) {
      case JAVA:
        this.engine = new JavaRegexEngine(expr, flags);
        break;
      case JONI:
        this.engine = new JoniRegexEngine(expr, flags);
        break;
    }
  }

  /**
   * Specifies the {@link Charset} to use to convert the row key to a String.
   * <p>
   * The row key needs to be converted to a String in order to be matched
   * against the regular expression.  This method controls which charset is
   * used to do this conversion.
   * <p>
   * If the row key is made of arbitrary bytes, the charset {@code ISO-8859-1}
   * is recommended.
   * @param charset The charset to use.
   */
  public void setCharset(final Charset charset) {
    engine.setCharset(charset.name());
  }

  @Override
  public int compareTo(byte[] value, int offset, int length) {
    return engine.compareTo(value, offset, length);
  }

  /**
   * @return The comparator serialized using pb
   */
  public byte [] toByteArray() {
    return engine.toByteArray();
  }

  /**
   * @param pbBytes A pb serialized {@link RegexStringComparator} instance
   * @return An instance of {@link RegexStringComparator} made from <code>bytes</code>
   * @throws DeserializationException
   * @see #toByteArray
   */
  public static RegexStringComparator parseFrom(final byte [] pbBytes)
  throws DeserializationException {
    ComparatorProtos.RegexStringComparator proto;
    try {
      proto = ComparatorProtos.RegexStringComparator.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    RegexStringComparator comparator;
    if (proto.hasEngine()) {
      EngineType engine = EngineType.valueOf(proto.getEngine());
      comparator = new RegexStringComparator(proto.getPattern(), proto.getPatternFlags(),
        engine);      
    } else {
      comparator = new RegexStringComparator(proto.getPattern(), proto.getPatternFlags());
    }
    String charset = proto.getCharset();
    if (charset.length() > 0) {
      try {
        comparator.getEngine().setCharset(charset);
      } catch (IllegalCharsetNameException e) {
        LOG.error("invalid charset", e);
      }
    }
    return comparator;
  }

  /**
   * @param other
   * @return true if and only if the fields of the comparator that are serialized
   * are equal to the corresponding fields in other.  Used for testing.
   */
  boolean areSerializedFieldsEqual(ByteArrayComparable other) {
    if (other == this) return true;
    if (!(other instanceof RegexStringComparator)) return false;
    RegexStringComparator comparator = (RegexStringComparator)other;
    return super.areSerializedFieldsEqual(comparator)
      && engine.getClass().isInstance(comparator.getEngine())
      && engine.getPattern().equals(comparator.getEngine().getPattern())
      && engine.getFlags() == comparator.getEngine().getFlags()
      && engine.getCharset().equals(comparator.getEngine().getCharset());
  }

  Engine getEngine() {
    return engine;
  }

  /**
   * This is an internal interface for abstracting access to different regular
   * expression matching engines. 
   */
  static interface Engine {
    /**
     * Returns the string representation of the configured regular expression
     * for matching
     */
    String getPattern();
    
    /**
     * Returns the set of configured match flags, a bit mask that may include
     * {@link Pattern} flags
     */
    int getFlags();

    /**
     * Returns the name of the configured charset
     */
    String getCharset();

    /**
     * Set the charset used when matching
     * @param charset the name of the desired charset for matching
     */
    void setCharset(final String charset);

    /**
     * Return the serialized form of the configured matcher
     */
    byte [] toByteArray();

    /**
     * Match the given input against the configured pattern
     * @param value the data to be matched
     * @param offset offset of the data to be matched
     * @param length length of the data to be matched
     * @return 0 if a match was made, 1 otherwise
     */
    int compareTo(byte[] value, int offset, int length);
  }

  /**
   * Implementation of the Engine interface using Java's Pattern.
   * <p>
   * This is the default engine.
   */
  static class JavaRegexEngine implements Engine {
    private Charset charset = Charset.forName("UTF-8");
    private Pattern pattern;

    public JavaRegexEngine(String regex, int flags) {
      this.pattern = Pattern.compile(regex, flags);
    }

    @Override
    public String getPattern() {
      return pattern.toString();
    }

    @Override
    public int getFlags() {
      return pattern.flags();
    }

    @Override
    public String getCharset() {
      return charset.name();
    }

    @Override
    public void setCharset(String charset) {
      this.charset = Charset.forName(charset);
    }

    @Override
    public int compareTo(byte[] value, int offset, int length) {
      // Use find() for subsequence match instead of matches() (full sequence
      // match) to adhere to the principle of least surprise.
      String tmp;
      if (length < value.length / 2) {
        // See HBASE-9428. Make a copy of the relevant part of the byte[],
        // or the JDK will copy the entire byte[] during String decode
        tmp = new String(Arrays.copyOfRange(value, offset, offset + length), charset);
      } else {
        tmp = new String(value, offset, length, charset);
      }
      return pattern.matcher(tmp).find() ? 0 : 1;
    }

    @Override
    public byte[] toByteArray() {
      ComparatorProtos.RegexStringComparator.Builder builder =
          ComparatorProtos.RegexStringComparator.newBuilder();
      builder.setPattern(pattern.pattern());
      builder.setPatternFlags(pattern.flags());
      builder.setCharset(charset.name());
      builder.setEngine(EngineType.JAVA.name());
      return builder.build().toByteArray();
    }
  }

  /**
   * Implementation of the Engine interface using Jruby's joni regex engine.
   * <p>
   * This engine operates on byte arrays directly so is expected to be more GC
   * friendly, and reportedly is twice as fast as Java's Pattern engine.
   * <p>
   * NOTE: Only the {@link Pattern} flags CASE_INSENSITIVE, DOTALL, and
   * MULTILINE are supported.
   */
  static class JoniRegexEngine implements Engine {
    private Encoding encoding = UTF8Encoding.INSTANCE;
    private String regex;
    private Regex pattern;

    public JoniRegexEngine(String regex, int flags) {
      this.regex = regex;
      byte[] b = Bytes.toBytes(regex);
      this.pattern = new Regex(b, 0, b.length, patternToJoniFlags(flags), encoding, Syntax.Java);
    }

    @Override
    public String getPattern() {
      return regex;
    }

    @Override
    public int getFlags() {
      return pattern.getOptions();
    }

    @Override
    public String getCharset() {
      return encoding.getCharsetName();
    }

    @Override
    public void setCharset(String name) {
      setEncoding(name);
    }

    @Override
    public int compareTo(byte[] value, int offset, int length) {
      // Use subsequence match instead of full sequence match to adhere to the
      // principle of least surprise.
      Matcher m = pattern.matcher(value);
      return m.search(offset, length, pattern.getOptions()) < 0 ? 1 : 0;
    }

    @Override
    public byte[] toByteArray() {
      ComparatorProtos.RegexStringComparator.Builder builder =
          ComparatorProtos.RegexStringComparator.newBuilder();
        builder.setPattern(regex);
        builder.setPatternFlags(joniToPatternFlags(pattern.getOptions()));
        builder.setCharset(encoding.getCharsetName());
        builder.setEngine(EngineType.JONI.name());
        return builder.build().toByteArray();
    }

    private int patternToJoniFlags(int flags) {
      int newFlags = 0;
      if ((flags & Pattern.CASE_INSENSITIVE) != 0) {
        newFlags |= Option.IGNORECASE;
      }
      if ((flags & Pattern.DOTALL) != 0) {
        // This does NOT mean Pattern.MULTILINE
        newFlags |= Option.MULTILINE;
      }
      if ((flags & Pattern.MULTILINE) != 0) {
        // This is what Java 8's Nashorn engine does when using joni and
        // translating Pattern's MULTILINE flag
        newFlags &= ~Option.SINGLELINE;
        newFlags |= Option.NEGATE_SINGLELINE;
      }
      return newFlags;
    }

    private int joniToPatternFlags(int flags) {
      int newFlags = 0;
      if ((flags & Option.IGNORECASE) != 0) {
        newFlags |= Pattern.CASE_INSENSITIVE;
      }
      // This does NOT mean Pattern.MULTILINE, this is equivalent to Pattern.DOTALL
      if ((flags & Option.MULTILINE) != 0) {
        newFlags |= Pattern.DOTALL;
      }
      // This means Pattern.MULTILINE. Nice
      if ((flags & Option.NEGATE_SINGLELINE) != 0) {
        newFlags |= Pattern.MULTILINE;
      }
      return newFlags;
    }

    private void setEncoding(String name) {
      EncodingDB.Entry e = EncodingDB.getEncodings().get(Bytes.toBytes(name));
      if (e != null) {
        encoding = e.getEncoding();
      } else {
        throw new IllegalCharsetNameException(name);
      }    
    }
  }
}
