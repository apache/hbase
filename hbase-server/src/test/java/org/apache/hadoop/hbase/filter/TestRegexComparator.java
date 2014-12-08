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

import static org.junit.Assert.*;

import java.util.regex.Pattern;

import org.apache.hadoop.hbase.filter.RegexStringComparator.EngineType;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestRegexComparator {

  @Test
  public void testSerialization() throws Exception {
    // Default engine is the Java engine
    RegexStringComparator a = new RegexStringComparator("a|b");
    RegexStringComparator b = RegexStringComparator.parseFrom(a.toByteArray());
    assertTrue(a.areSerializedFieldsEqual(b));
    assertTrue(b.getEngine() instanceof RegexStringComparator.JavaRegexEngine);

    // joni engine
    a = new RegexStringComparator("a|b", EngineType.JONI);
    b = RegexStringComparator.parseFrom(a.toByteArray());
    assertTrue(a.areSerializedFieldsEqual(b));
    assertTrue(b.getEngine() instanceof RegexStringComparator.JoniRegexEngine);
  }

  @Test
  public void testJavaEngine() throws Exception {
    for (TestCase t: TEST_CASES) {
      boolean result = new RegexStringComparator(t.regex, t.flags, EngineType.JAVA)
        .compareTo(Bytes.toBytes(t.haystack)) == 0;
      assertEquals("Regex '" + t.regex + "' failed test '" + t.haystack + "'", result,
        t.expected);
    }
  }

  @Test
  public void testJoniEngine() throws Exception {
    for (TestCase t: TEST_CASES) {
      boolean result = new RegexStringComparator(t.regex, t.flags, EngineType.JONI)
        .compareTo(Bytes.toBytes(t.haystack)) == 0;
      assertEquals("Regex '" + t.regex + "' failed test '" + t.haystack + "'", result,
        t.expected);
    }
  }

  private static class TestCase {
    String regex;
    String haystack;
    int flags;
    boolean expected;

    public TestCase(String regex, String haystack, boolean expected) {
      this(regex, Pattern.DOTALL, haystack, expected);
    }

    public TestCase(String regex, int flags, String haystack, boolean expected) {
      this.regex = regex;
      this.flags = flags;
      this.haystack = haystack;
      this.expected = expected;
    }
  }

  // These are a subset of the regex tests from OpenJDK 7
  private static TestCase TEST_CASES[] = {
    new TestCase("a|b", "a", true),
    new TestCase("a|b", "b", true),
    new TestCase("a|b", Pattern.CASE_INSENSITIVE, "A", true),
    new TestCase("a|b", Pattern.CASE_INSENSITIVE, "B", true),
    new TestCase("a|b", "z", false),
    new TestCase("a|b|cd", "cd", true),
    new TestCase("z(a|ac)b", "zacb", true),
    new TestCase("[abc]+", "ababab", true),
    new TestCase("[abc]+", "defg", false),
    new TestCase("[abc]+[def]+[ghi]+", "zzzaaddggzzz", true),
    new TestCase("[a-\\u4444]+", "za-9z", true),
    new TestCase("[^abc]+", "ababab", false),
    new TestCase("[^abc]+", "aaabbbcccdefg", true),
    new TestCase("[abc^b]", "b", true),
    new TestCase("[abc[def]]", "b", true),
    new TestCase("[abc[def]]", "e", true),
    new TestCase("[a-c[d-f[g-i]]]", "h", true),
    new TestCase("[a-c[d-f[g-i]]m]", "m", true),
    new TestCase("[a-c&&[d-f]]", "a", false),
    new TestCase("[a-c&&[d-f]]", "z", false),
    new TestCase("[a-m&&m-z&&a-c]", "m", false),
    new TestCase("[a-m&&m-z&&a-z]", "m", true),
    new TestCase("[[a-m]&&[^a-c]]", "a", false),
    new TestCase("[[a-m]&&[^a-c]]", "d", true),
    new TestCase("[[a-c][d-f]&&abc[def]]", "e", true),
    new TestCase("[[a-c]&&[b-d]&&[c-e]]", "c", true),
    new TestCase("[[a-c]&&[b-d][c-e]&&[u-z]]", "c", false),
    new TestCase("[[a]&&[b][c][a]&&[^d]]", "a", true),
    new TestCase("[[a]&&[b][c][a]&&[^d]]", "d", false),
    new TestCase("[[[a-d]&&[c-f]]&&[c]&&c&&[cde]]", "c", true),
    new TestCase("[x[[wz]abc&&bcd[z]]&&[u-z]]", "z", true),
    new TestCase("a.c.+", "a#c%&", true),
    new TestCase("ab.", "ab\n", true),
    new TestCase("(?s)ab.", "ab\n", true),
    new TestCase("ab\\wc", "abcc", true),
    new TestCase("\\W\\w\\W", "#r#", true),
    new TestCase("\\W\\w\\W", "rrrr#ggg", false),
    new TestCase("abc[\\sdef]*", "abc  def", true),
    new TestCase("abc[\\sy-z]*", "abc y z", true),
    new TestCase("abc[a-d\\sm-p]*", "abcaa mn  p", true),
    new TestCase("\\s\\s\\s", "blah  err", false),
    new TestCase("\\S\\S\\s", "blah  err", true),
    new TestCase("ab\\dc", "ab9c", true),
    new TestCase("\\d\\d\\d", "blah45", false),
    new TestCase("^abc", "abcdef", true),
    new TestCase("^abc", "bcdabc", false),
    new TestCase("^(a)?a", "a", true),
    new TestCase("^(aa(bb)?)+$", "aabbaa", true),
    new TestCase("((a|b)?b)+", "b", true),
    new TestCase("^(a(b)?)+$", "aba", true),
    new TestCase("^(a(b(c)?)?)?abc", "abc", true),
    new TestCase("^(a(b(c))).*", "abc", true),
    new TestCase("a?b", "aaaab", true),
    new TestCase("a?b", "aaacc", false),
    new TestCase("a??b", "aaaab", true),
    new TestCase("a??b", "aaacc", false),
    new TestCase("a?+b", "aaaab", true),
    new TestCase("a?+b", "aaacc", false),
    new TestCase("a+b", "aaaab", true),
    new TestCase("a+b", "aaacc", false),
    new TestCase("a+?b", "aaaab", true),
    new TestCase("a+?b", "aaacc", false),
    new TestCase("a++b", "aaaab", true),
    new TestCase("a++b", "aaacc", false),
    new TestCase("a{2,3}", "a", false),
    new TestCase("a{2,3}", "aa", true),
    new TestCase("a{2,3}", "aaa", true),
    new TestCase("a{3,}", "zzzaaaazzz", true),
    new TestCase("a{3,}", "zzzaazzz", false),
    new TestCase("abc(?=d)", "zzzabcd", true),
    new TestCase("abc(?=d)", "zzzabced", false),
    new TestCase("abc(?!d)", "zzabcd", false),
    new TestCase("abc(?!d)", "zzabced", true),
    new TestCase("\\w(?<=a)", "###abc###", true),
    new TestCase("\\w(?<=a)", "###ert###", false),
    new TestCase("(?<!a)c", "bc", true),
    new TestCase("(?<!a)c", "ac", false),
    new TestCase("(a+b)+", "ababab", true),
    new TestCase("(a+b)+", "accccd", false),
    new TestCase("(ab)+", "ababab", true),
    new TestCase("(ab)+", "accccd", false),
    new TestCase("(ab)(cd*)", "zzzabczzz", true),
    new TestCase("abc(d)*abc", "abcdddddabc", true),
    new TestCase("a*b", "aaaab", true),
    new TestCase("a*b", "b", true),
    new TestCase("a*b", "aaaac", false),
    new TestCase(".*?b", "aaaab", true),
    new TestCase("a*+b", "aaaab", true),
    new TestCase("a*+b", "b", true),
    new TestCase("a*+b", "aaaac", false),
    new TestCase("(?i)foobar", "fOobAr", true),
    new TestCase("f(?i)oobar", "fOobAr", true),
    new TestCase("f(?i)oobar", "FOobAr", false),
    new TestCase("foo(?i)bar", "fOobAr", false),
    new TestCase("(?i)foo[bar]+", "foObAr", true),
    new TestCase("(?i)foo[a-r]+", "foObAr", true),
    new TestCase("abc(?x)blah", "abcblah", true),
    new TestCase("abc(?x)  blah", "abcblah", true),
    new TestCase("abc(?x)  blah  blech", "abcblahblech", true),
    new TestCase("[\\n-#]", "!", true),
    new TestCase("[\\n-#]", "-", false),
    new TestCase("[\\043]+", "blahblah#blech", true),
    new TestCase("[\\042-\\044]+", "blahblah#blech", true),
    new TestCase("[\\u1234-\\u1236]", "blahblah\u1235blech", true),
    new TestCase("[^\043]*", "blahblah#blech", true),
    new TestCase("(|f)?+", "foo", true),
  };
}
