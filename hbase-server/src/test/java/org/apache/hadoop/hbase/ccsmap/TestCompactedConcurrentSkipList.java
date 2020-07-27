/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hbase.ccsmap;

import java.security.InvalidParameterException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.ccsmap.TestCompactedConcurrentSkipList.BehaviorCollections.RandomBehavior;
import org.apache.hadoop.hbase.testclassification.SmallTests;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestCompactedConcurrentSkipList {
  static int BIG_KV_THRESHOLD = 1024;
  static int DATA_PAGE_SIZE = 16 * 1024;
  static int DEFAULT_MIN_GROUP = 5;
  static int DEFAULT_MAX_GROUP = 10;

  static class TypeHelper implements CompactedTypeHelper<Integer, String> {

    @Override
    public int getCompactedSize(Integer key, String value) {
      return 4 + value.length();
    }

    private void copyIntToArray(int val, byte[] data, int offset) {
      for (int pos = offset; pos < offset + 4; ++pos) {
        data[pos] = (byte) (val & BYTE);
        val = val >> 8;
      }
    }

    static int BYTE = 0xFF;

    private int getIntFromArray(byte[] data, int offset) {
      int ret = 0;
      for (int pos = offset + 3; pos >= offset; pos--) {
        int b = data[pos];
        ret = (ret << 8) | (b & BYTE);
      }
      return ret;
    }

    @Override
    public void compact(Integer key, String value, byte[] data, int offset,
      int len) {
      Assert.assertEquals(4 + value.length(), len);
      copyIntToArray(key, data, offset);
      byte[] src = value.getBytes();
      Assert.assertEquals(value.length(), src.length);
    }

    @Override
    public KVPair<Integer, String> decomposite(byte[] data, int offset, int len) {
      Assert.assertTrue(len > 4);
      int intkey = getIntFromArray(data, offset);
      String value = new String(data, offset + 4, len - 4);
      return new KVPair<>(intkey, value);
    }

    private int compare(int key1, int key2) {
      return Integer.compare(key1, key2);
    }

    @Override
    public int compare(byte[] data1, int offset1, int len1, byte[] data2,
      int offset2, int len2) {
      Assert.assertTrue(len1 > 4);
      Assert.assertTrue(len2 > 4);
      int key1 = getIntFromArray(data1, offset1);
      int key2 = getIntFromArray(data2, offset2);
      return compare(key1, key2);
    }

    @Override
    public int compare(Integer key, byte[] data, int offset, int len) {
      Assert.assertTrue(len > 4);
      return compare(key.intValue(), getIntFromArray(data, offset));
    }

    @Override
    public int compare(Integer key1, Integer key2) {
      return compare(key1.intValue(), key2.intValue());
    }

  }

  interface KVGenerator {
    KVPair<Integer, String>[] getNextGroup(Random random);

    KVPair<Integer, String> getNextKV(Random random);

    void validate(int key, String value);
  }

  static class KVGeneratorFactory {
    static abstract class KVGeneratorBase implements KVGenerator {
      private boolean single;
      private boolean group;

      public KVGeneratorBase(boolean supportSingle, boolean supportGroup) {
        this.single = supportSingle;
        this.group = supportGroup;
      }

      @Override
      public KVPair<Integer, String>[] getNextGroup(Random random) {
        throw new UnsupportedOperationException();
      }

      @Override
      public KVPair<Integer, String> getNextKV(Random random) {
        throw new UnsupportedOperationException();
      }
    }

    static private int getHashWithKeyRange(int keyRange, String value) {
      return (int) ((long) value.hashCode() % keyRange);
    }

    static private int getGroupSize(Random random, int minGroup, int maxGroup) {
      if (minGroup == maxGroup) {
        return minGroup;
      }
      int s = random.nextInt(maxGroup - minGroup);
      return minGroup + s;
    }

    static private char[] ALPHABELT = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h',
      'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
      'w', 'x', 'y', 'z', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0' };

    static private String getValue(Random random, final double bigValueRate) {
      int sz = (bigValueRate >= 0.0d && random.nextDouble() < bigValueRate) ? random
        .nextInt(1024) + 1024 : random.nextInt(8) + 8;
      StringBuilder sb = new StringBuilder(sz);
      for (int i = 0; i < sz; ++i) {
        sb.append(ALPHABELT[random.nextInt(ALPHABELT.length)]);
      }
      return sb.toString();
    }

    static public KVGenerator arbitaryKVGenerator(final int keyRange) {
      return arbitaryKVGenerator(keyRange, -1.0f, DEFAULT_MIN_GROUP,
        DEFAULT_MAX_GROUP);
    }

    static public KVGenerator arbitaryKVGenerator(final int keyRange,
      final double bigValueRate, final int minGroup, final int maxGroup) {
      if (keyRange <= 0) {
        throw new InvalidParameterException();
      }
      return new KVGeneratorBase(true, true) {
        @Override
        public void validate(int key, String value) {
          Assert.assertEquals(key, getHashWithKeyRange(keyRange, value));
        }

        @Override
        public KVPair<Integer, String>[] getNextGroup(Random random) {
          int sz = getGroupSize(random, minGroup, maxGroup);
          KVPair<Integer, String>[] ret = new KVPair[sz];
          for (int i = 0; i < sz; ++i) {
            ret[i] = getNextKV(random);
          }
          return ret;
        }

        @Override
        public KVPair<Integer, String> getNextKV(Random random) {
          String value = getValue(random, bigValueRate);
          return new KVPair<>(getHashWithKeyRange(keyRange, value), value);
        }
      };
    }
  }

  class Operators {
    final List<Runnable> functions = new ArrayList<>();
    final List<String> threadNames = new ArrayList<>();
    final List<Thread> threads = new ArrayList<>();
    Throwable[] exceptions;
    String operatorsName = "Operator";
    final AtomicInteger runningThreads = new AtomicInteger(0);

    public void addFunctions(Runnable f) {
      int idx = functions.size();
      String defaultname = operatorsName + "-Thread-" + idx;
      addFunctions(f, defaultname);
    }

    public void addFunctions(Runnable f, String threadName) {
      functions.add(f);
      threadNames.add(threadName);
    }

    public void startAll() {
      exceptions = new Throwable[functions.size()];

      for (int i = 0; i < functions.size(); ++i) {
        final int ii = i;
        synchronized (runningThreads) {
          runningThreads.incrementAndGet();
        }
        Thread t = new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              functions.get(ii).run();
            } catch (Throwable e) {
              exceptions[ii] = e;
            } finally {
              synchronized (runningThreads) {
                runningThreads.decrementAndGet();
                runningThreads.notify();
              }
            }
          }
        }, threadNames.get(i));
        threads.add(t);
      }
      for (Thread t : threads) {
        t.start();
      }
    }

    public void waitForAll() throws Throwable {
      synchronized (runningThreads) {
        while (runningThreads.get() > 0) {
          try {
            runningThreads.wait();
          } catch (InterruptedException e) {
            break;
          }
        }
      }
      for (Thread t : threads) {
        try {
          t.join();
        } catch (InterruptedException e) {
          // ignored
        }
      }
      Throwable first = null;
      for (int i = 0; i < exceptions.length; ++i) {
        if (exceptions[i] != null) {
          if (first != null) {
            first = exceptions[i];
          }
          exceptions[i].printStackTrace();
        }
      }
      if (first != null) {
        throw first;
      }
    }
  }

  private CompactedConcurrentSkipListMap<Integer, String> createMapForTest(
    boolean largeMap) {
    if (!largeMap) {
      return new CompactedConcurrentSkipListMap<>(
        new TypeHelper(), PageSetting.create()
        .setDataPageSize(DATA_PAGE_SIZE).setHeapKVThreshold(BIG_KV_THRESHOLD));
    } else {
      PageSetting ps = PageSetting.create();
      ps.setDataPageSize(256 * 1024).setHeapKVThreshold(4 * 1024);
      return new CompactedConcurrentSkipListMap<>(new TypeHelper(), ps);
    }
  }

  private ConcurrentSkipListMap<Integer, String> createMapForStand() {
    return new ConcurrentSkipListMap<>();
  }

  @Test
  public void testPutAndGet() {
    CompactedConcurrentSkipListMap<Integer, String> m = createMapForTest(false);
    m.put(100, "thanks");
    m.put(2, "hello");
    m.put(9, "year");
    Assert.assertNull(m.get(-50));
    Assert.assertNull(m.get(99));
    Assert.assertEquals(m.get(2), "hello");
    Assert.assertEquals(m.get(9), "year");
    Assert.assertEquals(m.get(100), "thanks");
  }

  @Test
  public void randomWriteTest() {
    CompactedConcurrentSkipListMap<Integer, String> m = createMapForTest(false);

    // random put with overlap
    final int testTimes = 128 * 1024;
    final int keyRange = 32768;
    Random random = new Random();
    KVGenerator kvg = KVGeneratorFactory.arbitaryKVGenerator(keyRange);
    long start = System.currentTimeMillis();
    for (int i = 0; i < testTimes; ++i) {
      KVPair<Integer, String> kv = kvg.getNextKV(random);
      m.put(kv.key(), kv.value());
    }

    // iterator map
    int prev = Integer.MIN_VALUE;
    int elementCount = 0;
    for (Entry<Integer, String> e : m.entrySet()) {
      kvg.validate(e.getKey(), e.getValue());
      Assert.assertTrue(e.getKey() > prev);
      prev = e.getKey();
      elementCount++;
    }
    System.out.println("There are " + elementCount + " in map");
    System.out.println("It takes " + (System.currentTimeMillis() - start)
      + " ms do insertion " + testTimes + " times");
    System.out.println("MemoryUsage:");
    System.out.println(m.getMemoryUsage().toString());
  }

  @Test
  public void testPutAndGetParallelly() throws Throwable {
    final int THREADS = 10;
    final int GROUPSIZE = 5;
    final int COUNT = 10000;

    final CompactedConcurrentSkipListMap<Integer, String> m = createMapForTest(false);
    Operators ops = new Operators();
    for (int i = 0; i < THREADS; ++i) {
      ops.addFunctions(new Runnable() {
        @Override
        public void run() {
          KVGenerator kvg = KVGeneratorFactory.arbitaryKVGenerator(32767, 0.0f,
            1, GROUPSIZE);
          for (int c = 0; c < COUNT; ++c) {
            KVPair<Integer, String>[] group = kvg.getNextGroup(new Random());
            for (KVPair<Integer, String> kv : group) {
              m.put(kv.key(), kv.value());
            }
            for (KVPair<Integer, String> kv : group) {
              String value = m.get(kv.key());
              kvg.validate(kv.key(), value);
            }
          }
        }
      });
    }

    ops.startAll();
    ops.waitForAll();

    System.out.println("There are " + m.size() + " element in map");
  }

  static void putSimpleTestDataIntoMap(final Map<Integer, String> map) {
    map.put(1, "hello");
    map.put(6, "last");
    map.put(5, "quick");
    map.put(9, "tt");
    map.put(-30, "hbase");
  }

  enum InputType {
    MAIN_MAP, SUB_MAP, ITERATOR, ENTRYVIEW, KEYVIEW, VALUEVIEW,
  }

  static class MapContext {
    final int keyRange;

    int subRangeStart;
    int subRangeEnd;
    boolean descending;
    final int multiThread;

    final ConcurrentNavigableMap<Integer, String> mainMap1;
    final ConcurrentNavigableMap<Integer, String> mainMap2;
    ConcurrentNavigableMap<Integer, String> subMap1;
    ConcurrentNavigableMap<Integer, String> subMap2;
    NavigableSet<Integer> keyView1;
    NavigableSet<Integer> keyView2;
    Collection<String> valueView1;
    Collection<String> valueView2;
    Set<Entry<Integer, String>> entryView1;
    Set<Entry<Integer, String>> entryView2;

    InputType type;

    final int label; // different thread has different label
    int viewOps; // how may behaviors has been done in this view

    public MapContext(ConcurrentNavigableMap<Integer, String> map1,
      ConcurrentNavigableMap<Integer, String> map2, int range, int label,
      int multiThread) {
      this.keyRange = range;
      this.mainMap1 = map1;
      this.mainMap2 = map2;
      this.label = label;
      this.multiThread = multiThread;
      reset();
    }

    public MapContext(ConcurrentNavigableMap<Integer, String> map1,
      ConcurrentNavigableMap<Integer, String> map2, int range) {
      this(map1, map2, range, -1, 0);
    }

    public void reset() {
      subRangeStart = Integer.MIN_VALUE;
      subRangeEnd = Integer.MIN_VALUE;
      descending = false;
      subMap1 = null;
      subMap2 = null;
      keyView1 = null;
      keyView2 = null;
      valueView1 = null;
      valueView2 = null;
      entryView1 = null;
      entryView2 = null;
      type = InputType.MAIN_MAP;
      viewOps = 0;
    }

    public void reverse() {
      this.descending = !descending;
    }
  }

  @Test
  public void testEqual() throws Throwable {
    final CompactedConcurrentSkipListMap<Integer, String> mForTest = createMapForTest(false);
    final ConcurrentSkipListMap<Integer, String> mForStand = createMapForStand();
    putSimpleTestDataIntoMap(mForTest);
    putSimpleTestDataIntoMap(mForStand);
    Assert.assertEquals(mForTest, mForStand);
    Assert.assertEquals(mForStand, mForTest);
  }

  static interface Behavior {
    InputType doBehavior(MapContext mc);

    String getName();
  }

  static class BehaviorCollections {
    static ThreadLocal<Random> random = new ThreadLocal<Random>();
    static KVGenerator kvg;

    static Random getRandom() {
      Random ret = random.get();
      if (ret == null) {
        ret = new Random();
        random.set(ret);
      }
      return ret;
    }

    public static abstract class AbstractBehavior implements Behavior {
      String name;
      int weight;
      InputType type;
      boolean supportMultiThread;

      public AbstractBehavior(String name, int w, InputType type,
        boolean multiThread) {
        this.name = name;
        this.weight = w;
        this.type = type;
        this.supportMultiThread = multiThread;
      }

      public String getName() {
        return this.name + "(multithread=" + this.supportMultiThread + ",type="
          + this.type + ")";
      }

      public int getWeight() {
        return this.weight;
      }

      public boolean supportMultiThread() {
        return this.supportMultiThread;
      }

      public InputType getType() {
        return this.type;
      }
    }

    static void initRandom(int range) {
      kvg = KVGeneratorFactory.arbitaryKVGenerator(range);
    }

    static int getRandomKey(MapContext mc) {
      Random r = getRandom();
      if (mc.multiThread <= 1) {
        // single thread
        if (mc.subRangeStart == Integer.MIN_VALUE
          || mc.subRangeEnd == Integer.MIN_VALUE) {
          // get random key in whole range
          return r.nextInt(mc.keyRange);
        } else {
          // get random key in sub range
          int s = Math.max(Math.abs(mc.subRangeStart), Math.abs(mc.subRangeEnd));
          s = s == 0 ? 1 : s;
          int key = r.nextInt(s);
          return r.nextBoolean() ? key : -key;
        }
      } else {
        // get random key in whole range
        int threads = mc.multiThread;
        if (mc.subRangeStart == Integer.MIN_VALUE || mc.subRangeEnd == Integer.MIN_VALUE) {
          int base = mc.keyRange / threads;
          base = base <= 0 ? 1 : base;
          int k = r.nextInt(base);
          int ret =  k * threads + mc.label;
          return ret;
        } else {
          // get random key in sub range
          int s = Math.max(Math.abs(mc.subRangeStart), Math.abs(mc.subRangeEnd));
          int base = s / threads;
          base = base == 0 ? 1 : 0;
          int ss = r.nextInt(base);
          int key = ss * base + mc.label;
          int ret =  r.nextBoolean() ? key : -key;
          return ret;
        }
      }
    }

    static KVPair<Integer, String> getRandomKV(MapContext mc) {
      if (mc.multiThread <= 1) {
        if (mc.subRangeStart == Integer.MIN_VALUE
          || mc.subRangeEnd == Integer.MIN_VALUE) {
          // get kv in whole range
          return kvg.getNextKV(getRandom());
        } else {
          // get kv in sub range
          KVPair<Integer, String> kv = kvg.getNextKV(getRandom());
          int x = kv.value().hashCode();
          int s = Math.max(Math.abs(mc.subRangeStart), Math.abs(mc.subRangeEnd));
          kv.setKey(x % s);;
          return kv;
        }
      } else {
        if (mc.subRangeStart == Integer.MIN_VALUE
          || mc.subRangeEnd == Integer.MIN_VALUE) {
          // get kv in whole range
          KVPair<Integer, String> kv = kvg.getNextKV(getRandom());
          kv.setKey(getRandomKey(mc));
          return kv;
        } else {
          // get kv in sub range
          KVPair<Integer, String> kv = kvg.getNextKV(getRandom());
          kv.setKey(getRandomKey(mc));
          return kv;
        }
      }
    }

    static class MapRange {
      int start; // always smaller than end
      int end; // always bigger than start
      boolean startInclusive;
      boolean endInclusive;
    }

    private static int randomEdge(int start, int end, int keyRange) {
      if (getRandom().nextInt(5) < 1) { // have 1/10 chance to get a edge
        // outside keyrange
        int ret = keyRange + getRandom().nextInt(keyRange);
        return getRandom().nextBoolean() ? ret : -ret;
      }
      if (start == Integer.MIN_VALUE) {
        start = -keyRange;
      }
      if (end == Integer.MIN_VALUE) {
        end = keyRange;
      }
      if (end - start < 0) {
        int tmp = start;
        start = end;
        end = tmp;
      }
      return getRandom().nextInt(end - start) + start;
    }

    static MapRange getRandomMapRange(MapContext mc) {
      MapRange mr = new MapRange();
      int start = randomEdge(mc.subRangeStart, mc.subRangeEnd, mc.keyRange);
      int end = randomEdge(mc.subRangeStart, mc.subRangeEnd, mc.keyRange);
      if (start > end) {
        mr.start = end;
        mr.end = start;
      } else {
        mr.start = start;
        mr.end = start;
      }
      mr.startInclusive = getRandom().nextBoolean();
      mr.endInclusive = getRandom().nextBoolean();
      return mr;
    }

    public static class Put extends AbstractBehavior {
      public Put() {
        super("put", 1000, InputType.MAIN_MAP, true);
      }

      @Override
      public InputType doBehavior(MapContext mc) {
        KVPair<Integer, String> kv = getRandomKV(mc);
        String ret1 = mc.mainMap1.put(kv.key(), kv.value());
        String ret2 = mc.mainMap2.put(kv.key(), kv.value());
        Assert.assertEquals(ret1, ret2);
        return InputType.MAIN_MAP;
      }
    }

    public static class SubMapPut extends AbstractBehavior {
      public SubMapPut() {
        super("subMapPut", 1000, InputType.SUB_MAP, true);
      }

      @Override
      public InputType doBehavior(MapContext mc) {
        Throwable e1 = null;
        Throwable e2 = null;
        String ret1 = null;
        String ret2 = null;
        KVPair<Integer, String> kv = getRandomKV(mc);
        try {
          ret1 = mc.mainMap1.put(kv.key(), kv.value());
        } catch (Throwable e) {
          e1 = e;
        }
        try {
          ret2 = mc.mainMap2.put(kv.key(), kv.value());
        } catch (Throwable e) {
          e2 = e;
        }
        Assert.assertEquals(ret1, ret2);
        if (e2 != null || e1 != null) {
          System.out.println(e1);
          System.out.println(e2);
          Assert.assertEquals(e1.getClass(), e2.getClass());
        }
        return InputType.SUB_MAP;
      }
    }

    public static class PutIfAbsent extends AbstractBehavior {
      public PutIfAbsent() {
        super("putIfAbsent", 1000, InputType.MAIN_MAP, true);
      }

      @Override
      public InputType doBehavior(MapContext mc) {
        KVPair<Integer, String> kv = getRandomKV(mc);
        String ret1 = mc.mainMap1.putIfAbsent(kv.key(), kv.value());
        String ret2 = mc.mainMap2.putIfAbsent(kv.key(), kv.value());
        Assert.assertEquals(ret1, ret2);
        return InputType.MAIN_MAP;
      }
    }

    public static class SubMapPutIfAbsent extends AbstractBehavior {
      public SubMapPutIfAbsent() {
        super("subMapPutIfAbsent", 1000, InputType.SUB_MAP, true);
      }

      @Override
      public InputType doBehavior(MapContext mc) {
        KVPair<Integer, String> kv = getRandomKV(mc);
        Throwable e1 = null;
        Throwable e2 = null;
        String ret1 = null;
        String ret2 = null;
        try {
          ret1 = mc.mainMap1.putIfAbsent(kv.key(), kv.value());
        } catch (Throwable e) {
          e1 = e;
        }
        try {
          ret2 = mc.mainMap2.putIfAbsent(kv.key(), kv.value());
        } catch (Throwable e) {
          e2 = e;
        }
        Assert.assertEquals(ret1, ret2);
        if (e2 != null || e1 != null) {
          System.out.println(e1);
          System.out.println(e2);
          Assert.assertEquals(e1.getClass(), e2.getClass());
        }
        return InputType.SUB_MAP;
      }
    }

    public static class PutAll extends AbstractBehavior {
      public PutAll() {
        super("putAll", 200, InputType.MAIN_MAP, true);
      }

      @Override
      public InputType doBehavior(MapContext mc) {
        Map<Integer, String> smallMap = new TreeMap<Integer, String>();
        for (int i = 0; i < 5; ++i) {
          KVPair<Integer, String> kv = getRandomKV(mc);
          smallMap.put(kv.key(), kv.value());
        }
        mc.mainMap1.putAll(smallMap);
        mc.mainMap2.putAll(smallMap);
        return InputType.MAIN_MAP;
      }
    }

    public static class Remove extends AbstractBehavior {
      public Remove() {
        super("removeByKV", 300, InputType.MAIN_MAP, true);
      }

      @Override
      public InputType doBehavior(MapContext mc) {
        KVPair<Integer, String> kv = getRandomKV(mc);
        int key = getRandomKey(mc);
        String value = null;
        if (getRandom().nextBoolean()) {
          String v1 = mc.mainMap1.get(key);
          String v2 = mc.mainMap2.get(key);
          Assert.assertEquals(v1, v2);
          value = v1;
        }

        if (value == null) {
          String ret1 = mc.mainMap1.remove(key);
          String ret2 = mc.mainMap2.remove(key);
          Assert.assertEquals(ret1, ret2);
        } else {
          boolean succ = getRandom().nextBoolean();
          if (!succ) {
            value = kv.value();
          }
          boolean ret1 = mc.mainMap1.remove(key, value);
          boolean ret2 = mc.mainMap2.remove(key, value);
          Assert.assertEquals(ret1, ret2);
        }
        return InputType.MAIN_MAP;
      }
    }

    public static class SubMapRemove extends AbstractBehavior {
      public SubMapRemove() {
        super("subMapRemoveByKV", 300, InputType.SUB_MAP, true);
      }

      @Override
      public InputType doBehavior(MapContext mc) {
        KVPair<Integer, String> kv = getRandomKV(mc);
        int key = getRandomKey(mc);
        String value = null;
        if (getRandom().nextBoolean()) {
          String v1 = mc.mainMap1.get(key);
          String v2 = mc.mainMap2.get(key);
          Assert.assertEquals(v1, v2);
          value = v1;
        }

        if (value == null) {
          String ret1 = mc.subMap1.remove(key);
          String ret2 = mc.subMap2.remove(key);
          Assert.assertEquals(ret1, ret2);
        } else {
          boolean succ = getRandom().nextBoolean();
          if (!succ) {
            value = kv.value();
          }
          boolean ret1 = mc.subMap1.remove(key, value);
          boolean ret2 = mc.subMap2.remove(key, value);
          Assert.assertEquals(ret1, ret2);
        }
        return InputType.SUB_MAP;
      }
    }

    public static class Replace extends AbstractBehavior {
      public Replace() {
        super("replace", 300, InputType.MAIN_MAP, true);
      }

      @Override
      public InputType doBehavior(MapContext mc) {
        KVPair<Integer, String> kv = getRandomKV(mc);
        String oldValue = null;
        if (getRandom().nextBoolean()) {
          oldValue = mc.mainMap1.get(kv.key());
          if (getRandom().nextBoolean()) {
            oldValue = oldValue + "a";
          }
        }

        if (oldValue != null) {
          boolean ret1 = mc.mainMap1.replace(kv.key(), oldValue, kv.value());
          boolean ret2 = mc.mainMap2.replace(kv.key(), oldValue, kv.value());
          Assert.assertEquals(ret1, ret2);
        } else {
          String ret1 = mc.mainMap1.replace(kv.key(), kv.value());
          String ret2 = mc.mainMap2.replace(kv.key(), kv.value());
          Assert.assertEquals(ret1, ret2);
        }
        return InputType.MAIN_MAP;
      }
    }

    public static class SubMapReplace extends AbstractBehavior {
      public SubMapReplace() {
        super("subMapReplace", 300, InputType.SUB_MAP, true);
      }

      @Override
      public InputType doBehavior(MapContext mc) {
        KVPair<Integer, String> kv = getRandomKV(mc);
        String oldValue = null;
        if (getRandom().nextBoolean()) {
          oldValue = mc.mainMap1.get(kv.key());
          if (getRandom().nextBoolean()) {
            oldValue = oldValue + "a";
          }
        }

        Throwable e1 = null;
        Throwable e2 = null;
        if (oldValue != null) {
          boolean ret1 = false;
          boolean ret2 = false;
          try {
            ret1 = mc.subMap1.replace(kv.key(), oldValue, kv.value());
          } catch (Throwable e) {
            e1 = e;
          }
          try {
            ret2 = mc.subMap2.replace(kv.key(), oldValue, kv.value());
          } catch (Throwable e) {
            e2 = e;
          }
          Assert.assertEquals(ret1, ret2);
          if (e1 != null || e2 != null) {
            Assert.assertEquals(e1.getClass(), e2.getClass());
          }
        } else {
          String ret1 = null;
          String ret2 = null;
          try {
            ret1 = mc.subMap1.replace(kv.key(), kv.value());
          } catch (Throwable e) {
            e1 = e;
          }
          try {
            ret2 = mc.subMap2.replace(kv.key(), kv.value());
          } catch (Throwable e) {
            e2 = e;
          }
          Assert.assertEquals(ret1, ret2);
          if (e1 != null || e2 != null) {
            Assert.assertEquals(e1.getClass(), e2.getClass());
          }
        }
        return InputType.SUB_MAP;
      }
    }

    public static class Get extends AbstractBehavior {
      public Get() {
        super("get", 1000, InputType.MAIN_MAP, true);
      }

      @Override
      public InputType doBehavior(MapContext mc) {
        int key = getRandomKey(mc);
        String ret1 = mc.mainMap1.get(key);
        String ret2 = mc.mainMap2.get(key);
        Assert.assertEquals(ret1, ret2);
        return InputType.MAIN_MAP;
      }
    }

    public static class SubMapGet extends AbstractBehavior {
      public SubMapGet() {
        super("subMapGet", 1000, InputType.SUB_MAP, true);
      }

      @Override
      public InputType doBehavior(MapContext mc) {
        int key = getRandomKey(mc);
        String ret1 = mc.subMap1.get(key);
        String ret2 = mc.subMap2.get(key);
        Assert.assertEquals(ret1, ret2);
        return InputType.SUB_MAP;
      }
    }

    public static class Near extends AbstractBehavior {
      public Near() {
        super("near", 1000, InputType.MAIN_MAP, false);
      }

      @Override
      public InputType doBehavior(MapContext mc) {
        int key = getRandomKey(mc);
        int select = getRandom().nextInt(4);
        if (getRandom().nextBoolean()) {
          if (select == 0) {
            Assert.assertEquals(mc.mainMap1.ceilingKey(key),
              mc.mainMap2.ceilingKey(key));
          } else if (select == 1) {
            Assert.assertEquals(mc.mainMap1.floorKey(key),
              mc.mainMap2.floorKey(key));
          } else if (select == 2) {
            Assert.assertEquals(mc.mainMap1.higherKey(key),
              mc.mainMap2.higherKey(key));
          } else if (select == 3) {
            Assert.assertEquals(mc.mainMap1.higherKey(key),
              mc.mainMap2.higherKey(key));
          }
        } else {
          if (select == 0) {
            Assert.assertEquals(mc.mainMap1.ceilingEntry(key),
              mc.mainMap2.ceilingEntry(key));
          } else if (select == 1) {
            Assert.assertEquals(mc.mainMap1.floorEntry(key),
              mc.mainMap2.floorEntry(key));
          } else if (select == 2) {
            Assert.assertEquals(mc.mainMap1.higherEntry(key),
              mc.mainMap2.higherEntry(key));
          } else if (select == 3) {
            Assert.assertEquals(mc.mainMap1.lowerEntry(key),
              mc.mainMap2.lowerEntry(key));
          }
        }
        return InputType.MAIN_MAP;
      }
    }

    public static class SubMapNear extends AbstractBehavior {
      public SubMapNear() {
        super("subMapNear", 1000, InputType.SUB_MAP, false);
      }

      @Override
      public InputType doBehavior(MapContext mc) {
        int key = getRandomKey(mc);
        int select = getRandom().nextInt(4);
        if (getRandom().nextBoolean()) {
          if (select == 0) {
            Integer ret1 = mc.subMap1.ceilingKey(key);
            Integer ret2 = mc.subMap2.ceilingKey(key);
            Assert.assertEquals(ret1, ret2);
          } else if (select == 1) {
            Integer ret1 = mc.subMap1.floorKey(key);
            Integer ret2 = mc.subMap1.floorKey(key);
            Assert.assertEquals(ret1, ret2);
          } else if (select == 2) {
            Integer ret1 = mc.subMap1.higherKey(key);
            Integer ret2 = mc.subMap2.higherKey(key);
            Assert.assertEquals(ret1, ret2);
          } else if (select == 3) {
            Integer ret1 = mc.subMap1.lowerKey(key);
            Integer ret2 = mc.subMap2.lowerKey(key);
            Assert.assertEquals(ret1, ret2);
          }
        } else {

          if (select == 0) {
            Entry<Integer, String> ret1 = mc.subMap1.ceilingEntry(key);
            Entry<Integer, String> ret2 = mc.subMap2.ceilingEntry(key);
            Assert.assertEquals(ret1, ret2);
          } else if (select == 1) {
            Entry<Integer, String> ret1 = mc.subMap1.floorEntry(key);
            Entry<Integer, String> ret2 = mc.subMap2.floorEntry(key);
            Assert.assertEquals(ret1, ret2);
          } else if (select == 2) {
            Entry<Integer, String> ret1 = mc.subMap1.higherEntry(key);
            Entry<Integer, String> ret2 = mc.subMap2.higherEntry(key);
            Assert.assertEquals(ret1, ret2);
          } else if (select == 3) {
            Entry<Integer, String> ret1 = mc.subMap1.lowerEntry(key);
            Entry<Integer, String> ret2 = mc.subMap2.lowerEntry(key);
            Assert.assertEquals(ret1, ret2);
          }
        }

        return InputType.SUB_MAP;
      }
    }

    public static class ContainsKey extends AbstractBehavior {
      public ContainsKey() {
        super("containsKey", 1000, InputType.MAIN_MAP, true);
      }

      @Override
      public InputType doBehavior(MapContext mc) {
        int key = getRandomKey(mc);
        boolean ret1 = mc.mainMap1.containsKey(key);
        boolean ret2 = mc.mainMap2.containsKey(key);
        Assert.assertEquals(ret1, ret2);
        return InputType.MAIN_MAP;
      }
    }

    public static class SubMapContainsKey extends AbstractBehavior {
      public SubMapContainsKey() {
        super("subMapContainsKey", 1000, InputType.SUB_MAP, true);
      }

      @Override
      public InputType doBehavior(MapContext mc) {
        int key = getRandomKey(mc);
        boolean ret1 = mc.subMap1.containsKey(key);
        boolean ret2 = mc.subMap2.containsKey(key);
        Assert.assertEquals(ret1, ret2);
        return InputType.SUB_MAP;
      }
    }

    public static Entry<Integer, String> newEntry(int key, String value) {
      return new AbstractMap.SimpleEntry<Integer, String>(key, value);
    }

    public static class EntrySetContains extends AbstractBehavior {
      public EntrySetContains() {
        super("entrySetContains", 500, InputType.ENTRYVIEW, true);
      }

      @Override
      public InputType doBehavior(MapContext mc) {
        KVPair<Integer, String> kv = getRandomKV(mc);
        String value = mc.mainMap1.get(kv.key());
        if (value != null) {
          if (getRandom().nextBoolean()) {
            value = value + 'x';
          }
        } else {
          value = kv.value();
        }
        Entry<Integer, String> e = newEntry(kv.key(), value);
        boolean ret1 = mc.entryView1.contains(e);
        boolean ret2 = mc.entryView2.contains(e);
        Assert.assertEquals(ret1, ret2);
        return InputType.ENTRYVIEW;
      }
    }

    public static class KeySetContains extends AbstractBehavior {
      public KeySetContains() {
        super("keySetContains", 1000, InputType.KEYVIEW, true);
      }

      @Override
      public InputType doBehavior(MapContext mc) {
        int key = getRandomKey(mc);
        boolean ret1 = mc.keyView1.contains(key);
        boolean ret2 = mc.keyView2.contains(key);
        Assert.assertEquals(ret1, ret2);
        return InputType.KEYVIEW;
      }
    }

    public static class EntrySetRemove extends AbstractBehavior {
      public EntrySetRemove() {
        super("entrySetRemove", 100, InputType.ENTRYVIEW, true);
      }

      @Override
      public InputType doBehavior(MapContext mc) {
        KVPair<Integer, String> kv = getRandomKV(mc);
        String value = mc.mainMap2.get(kv.key());
        if (value != null) {
          if (getRandom().nextBoolean()) {
            value = value + 'x';
          } else {
            value = kv.value();
          }
        }
        Entry<Integer, String> entry = newEntry(kv.key(), value);
        boolean ret1 = false;
        boolean ret2 = false;
        Throwable e1 = null;
        Throwable e2 = null;
        try {
          ret1 = mc.entryView1.remove(entry);
        } catch (Throwable e) {
          e1 = e;
        }
        try {
          ret2 = mc.entryView2.remove(entry);
        } catch (Throwable e) {
          e2 = e;
        }
        Assert.assertEquals(ret1, ret2);
        if (e1 != null || e2 != null) {
          Assert.assertEquals(e1.getClass(), e2.getClass());
        }
        return InputType.ENTRYVIEW;
      }
    }

    public static class KeySetRemove extends AbstractBehavior {
      public KeySetRemove() {
        super("keySetRemove", 1000, InputType.KEYVIEW, true);
      }

      @Override
      public InputType doBehavior(MapContext mc) {
        int key = getRandomKey(mc);
        boolean ret1 = false;
        boolean ret2 = false;
        Throwable e1 = null;
        Throwable e2 = null;
        try {
          ret1 = mc.keyView1.remove(key);
        } catch (Throwable e) {
          e1 = e;
        }
        try {
          ret2 = mc.keyView2.remove(key);
        } catch (Throwable e) {
          e2 = e;
        }
        Assert.assertEquals(ret1, ret2);
        if (e1 != null || e2 != null) {
          Assert.assertEquals(e1.getClass(), e2.getClass());
        }
        return InputType.KEYVIEW;
      }
    }

    public static class EntryIterator extends AbstractBehavior {
      public EntryIterator() {
        super("entrySetIterator", 1000, InputType.ENTRYVIEW, false);
      }

      @Override
      public InputType doBehavior(MapContext mc) {
        Iterator<Entry<Integer, String>> iter1 = mc.entryView1.iterator();
        Iterator<Entry<Integer, String>> iter2 = mc.entryView2.iterator();
        int step = getRandom().nextInt(30); // max up to 30 step
        while (iter2.hasNext()) {
          Assert.assertTrue(iter1.hasNext());
          Entry<Integer, String> ret1 = iter1.next();
          Entry<Integer, String> ret2 = iter2.next();
          Assert.assertEquals(ret1, ret2);
          if (getRandom().nextInt(100) < 1) { // 1/100 chance to remove this
            // value
            iter1.remove();
            iter2.remove();
          }
          if (--step <= 0) {
            break;
          }
        }
        return InputType.ENTRYVIEW;
      }
    }

    public static class SubMap extends AbstractBehavior {
      public SubMap() {
        super("subMap", 400, InputType.MAIN_MAP, true);
      }

      @Override
      public InputType doBehavior(MapContext mc) {
        MapRange mr = getRandomMapRange(mc);
        int select = getRandom().nextInt(3);
        if (select == 0) { // submap
          mc.subMap1 = mc.mainMap1.subMap(mr.start, mr.startInclusive, mr.end,
            mr.endInclusive);
          mc.subMap2 = mc.mainMap2.subMap(mr.start, mr.startInclusive, mr.end,
            mr.endInclusive);
        } else if (select == 1) { // headmap
          mc.subMap1 = mc.mainMap1.headMap(mr.end, mr.endInclusive);
          mc.subMap2 = mc.mainMap2.headMap(mr.end, mr.endInclusive);
        } else { // tailmap
          mc.subMap1 = mc.mainMap1.tailMap(mr.start, mr.startInclusive);
          mc.subMap2 = mc.mainMap2.tailMap(mr.start, mr.startInclusive);
        }
        mc.type = InputType.SUB_MAP;
        return InputType.SUB_MAP;
      }
    }

    public static class KeySetIterator extends AbstractBehavior {
      public KeySetIterator() {
        super("keySetIterator", 1000, InputType.KEYVIEW, false);
      }

      @Override
      public InputType doBehavior(MapContext mc) {
        Iterator<Integer> iter1;
        Iterator<Integer> iter2;
        if (getRandom().nextBoolean()) {
          iter1 = mc.keyView1.descendingIterator();
          iter2 = mc.keyView2.descendingIterator();
        } else {
          iter1 = mc.keyView1.iterator();
          iter2 = mc.keyView2.iterator();
        }
        int step = getRandom().nextInt(30); // max up to 30 step
        while (iter2.hasNext()) {
          Assert.assertTrue(iter2.hasNext());
          iter1.next();
          iter2.next();
          if (getRandom().nextInt(100) < 1) { // 1/100 chance to remove this key
            iter1.remove();
            iter2.remove();
          }
          if (--step <= 0) {
            break;
          }
        }
        return InputType.KEYVIEW;
      }
    }

    public static class EntrySetSize extends AbstractBehavior {
      public EntrySetSize() {
        super("entrySetSize", 1, InputType.ENTRYVIEW, false);
      }

      @Override
      public InputType doBehavior(MapContext mc) {
        int ret1 = mc.entryView1.size();
        int ret2 = mc.entryView2.size();
        Assert.assertEquals(ret1, ret2);
        return InputType.ENTRYVIEW;
      }
    }

    public static class KeySetSize extends AbstractBehavior {
      public KeySetSize() {
        super("keySetSize", 1, InputType.KEYVIEW, false);
      }

      @Override
      public InputType doBehavior(MapContext mc) {
        int ret1 = mc.keyView1.size();
        int ret2 = mc.keyView2.size();
        Assert.assertEquals(ret1, ret2);
        return InputType.KEYVIEW;
      }
    }

    public static class FirstOrLast extends AbstractBehavior {
      public FirstOrLast() {
        super("firstOrLast", 1000, InputType.MAIN_MAP, false);
      }

      @Override
      public InputType doBehavior(MapContext mc) {
        if (getRandom().nextBoolean()) {
          Integer ret1;
          Integer ret2;
          if (getRandom().nextBoolean()) {
            ret1 = mc.mainMap1.firstKey();
            ret2 = mc.mainMap2.firstKey();
          } else {
            ret1 = mc.mainMap1.lastKey();
            ret2 = mc.mainMap2.lastKey();
          }
          Assert.assertEquals(ret1, ret2);
        } else {
          Entry<Integer, String> ret1;
          Entry<Integer, String> ret2;
          if (getRandom().nextBoolean()) {
            ret1 = mc.mainMap1.firstEntry();
            ret2 = mc.mainMap2.firstEntry();
          } else {
            ret1 = mc.mainMap1.lastEntry();
            ret2 = mc.mainMap2.lastEntry();
          }
          Assert.assertEquals(ret1, ret2);
        }
        return InputType.MAIN_MAP;
      }
    }

    public static class SubMapFirstOrLast extends AbstractBehavior {
      public SubMapFirstOrLast() {
        super("subMapFirstOrLast", 1000, InputType.SUB_MAP, false);
      }

      @Override
      public InputType doBehavior(MapContext mc) {
        Assert.assertEquals(mc.subMap1.isEmpty(), mc.subMap2.isEmpty());
        Object ret1 = null;
        Object ret2 = null;
        Throwable e1 = null;
        Throwable e2 = null;
        if (getRandom().nextBoolean()) {
          boolean first = getRandom().nextBoolean();
          try {
            ret1 = first ? mc.subMap1.firstKey() : mc.subMap1.lastKey();
          } catch (Throwable e) {
            e1 = e;
          }
          try {
            ret2 = first ? mc.subMap2.firstKey() : mc.subMap2.lastKey();
          } catch (Throwable e) {
            e2 = e;
          }
        } else {
          boolean first = getRandom().nextBoolean();
          try {
            ret1 = first ? mc.subMap1.firstEntry() : mc.subMap1.lastEntry();
          } catch (Throwable e) {
            e1 = e;
          }
          try {
            ret2 = first ? mc.subMap2.firstEntry() : mc.subMap2.lastEntry();
          } catch (Throwable e) {
            e2 = e;
          }
        }
        Assert.assertEquals(ret1, ret2);
        if (e1 != null || e2 != null) {
          if (e2 == null || e1 == null) {
            System.out.println(e1);
            System.out.println(e2);
          }
          Assert.assertEquals(e1.getClass(), e2.getClass());
        }
        return InputType.SUB_MAP;
      }
    }

    public static class Poll extends AbstractBehavior {
      public Poll() {
        super("poll", 250, InputType.MAIN_MAP, false);
      }

      @Override
      public InputType doBehavior(MapContext mc) {
        Assert.assertEquals(mc.mainMap1.isEmpty(), mc.mainMap2.isEmpty());
        if (mc.mainMap1.isEmpty()) {
          return InputType.MAIN_MAP;
        }
        Entry<Integer, String> ret1 = null;
        Entry<Integer, String> ret2 = null;
        if (getRandom().nextBoolean()) {
          ret1 = mc.mainMap1.pollFirstEntry();
          ret2 = mc.mainMap2.pollFirstEntry();
        } else {
          ret1 = mc.mainMap1.pollLastEntry();
          ret2 = mc.mainMap2.pollLastEntry();
        }
        Assert.assertEquals(ret1, ret2);
        return InputType.MAIN_MAP;
      }
    }

    public static class SubMapPoll extends AbstractBehavior {
      public SubMapPoll() {
        super("subMapPoll", 250, InputType.SUB_MAP, false);
      }

      @Override
      public InputType doBehavior(MapContext mc) {
        Assert.assertEquals(mc.subMap1.isEmpty(), mc.subMap2.isEmpty());
        Entry<Integer, String> ret1 = null;
        Entry<Integer, String> ret2 = null;
        Throwable e1 = null;
        Throwable e2 = null;
        boolean first = getRandom().nextBoolean();
        try {
          ret1 = first ? mc.subMap1.pollFirstEntry() : mc.subMap1
            .pollLastEntry();
        } catch (Throwable e) {
          e1 = e;
        }
        try {
          ret2 = first ? mc.subMap2.pollFirstEntry() : mc.subMap2
            .pollLastEntry();
        } catch (Throwable e) {
          e2 = e;
        }
        Assert.assertEquals(ret1, ret2);
        if (e1 != null || e2 != null) {
          Assert.assertEquals(e1.getClass(), e2.getClass());
          ;
        }
        return InputType.SUB_MAP;
      }
    }

    public static class Size extends AbstractBehavior {
      public Size() {
        super("size", 1, InputType.MAIN_MAP, false);
      }

      @Override
      public InputType doBehavior(MapContext mc) {
        int ret1 = mc.mainMap1.size();
        int ret2 = mc.mainMap2.size();
        Assert.assertEquals(ret1, ret2);
        return InputType.MAIN_MAP;
      }
    }

    public static class SubMapSize extends AbstractBehavior {
      public SubMapSize() {
        super("subMapSize", 1, InputType.SUB_MAP, false);
      }

      @Override
      public InputType doBehavior(MapContext mc) {
        Assert.assertEquals(mc.subMap1.size(), mc.subMap2.size());
        return InputType.SUB_MAP;
      }
    }

    public static class EntrySet extends AbstractBehavior {
      public EntrySet() {
        super("entrySet", 200, InputType.MAIN_MAP, true);
      }

      @Override
      public InputType doBehavior(MapContext mc) {
        mc.entryView1 = mc.mainMap1.entrySet();
        mc.entryView2 = mc.mainMap2.entrySet();
        mc.viewOps = 0;
        return (mc.type = InputType.ENTRYVIEW);
      }
    }

    public static class SubMapEntrySet extends AbstractBehavior {
      public SubMapEntrySet() {
        super("SubMapEntrySet", 200, InputType.SUB_MAP, true);
      }

      @Override
      public InputType doBehavior(MapContext mc) {
        mc.entryView1 = mc.subMap1.entrySet();
        mc.entryView2 = mc.subMap2.entrySet();
        return (mc.type = InputType.ENTRYVIEW);
      }
    }

    public static class KeySet extends AbstractBehavior {
      public KeySet() {
        super("keySet", 200, InputType.MAIN_MAP, true);
      }

      @Override
      public InputType doBehavior(MapContext mc) {
        int select = getRandom().nextInt(3);
        if (select == 0) {
          mc.keyView1 = mc.mainMap1.keySet();
          mc.keyView2 = mc.mainMap2.keySet();
        } else if (select == 1) {
          mc.keyView1 = mc.mainMap1.navigableKeySet();
          mc.keyView2 = mc.mainMap2.navigableKeySet();
        } else if (select == 2) {
          mc.keyView1 = mc.mainMap1.descendingKeySet();
          mc.keyView2 = mc.mainMap2.descendingKeySet();
          mc.reverse();
        }

        return (mc.type = InputType.KEYVIEW);
      }
    }

    public static class SubMapKeySet extends AbstractBehavior {
      public SubMapKeySet() {
        super("subMapKeySet", 200, InputType.SUB_MAP, true);
      }

      @Override
      public InputType doBehavior(MapContext mc) {
        int select = getRandom().nextInt(3);
        if (select == 0) {
          mc.keyView1 = mc.subMap1.keySet();
          mc.keyView2 = mc.subMap2.keySet();
        } else if (select == 1) {
          mc.keyView1 = mc.subMap1.navigableKeySet();
          mc.keyView2 = mc.subMap2.navigableKeySet();
        } else if (select == 2) {
          mc.keyView1 = mc.subMap1.descendingKeySet();
          mc.keyView2 = mc.subMap2.descendingKeySet();
          mc.reverse();
        }
        return (mc.type = InputType.KEYVIEW);
      }
    }

    public static class KeySetNeer extends AbstractBehavior {
      public KeySetNeer() {
        super("keySetNeer", 1000, InputType.KEYVIEW, false);
      }

      @Override
      public InputType doBehavior(MapContext mc) {
        int select = getRandom().nextInt(4);
        int key = getRandomKey(mc);
        if (select == 0) {
          Assert.assertEquals(mc.keyView1.ceiling(key),
            mc.keyView2.ceiling(key));
        } else if (select == 1) {
          Assert.assertEquals(mc.keyView1.lower(key), mc.keyView1.lower(key));
        } else if (select == 2) {
          Assert.assertEquals(mc.keyView1.higher(key), mc.keyView2.higher(key));
        } else {
          Assert.assertEquals(mc.keyView1.floor(key), mc.keyView2.floor(key));
        }

        return (mc.type = InputType.KEYVIEW);
      }
    }

    public static class KeySetFirstOrLast extends AbstractBehavior {
      public KeySetFirstOrLast() {
        super("keySetFirstOrLast", 1000, InputType.KEYVIEW, false);
      }

      @Override
      public InputType doBehavior(MapContext mc) {
        Integer ret1 = null;
        Integer ret2 = null;
        Throwable e1 = null;
        Throwable e2 = null;
        boolean first = getRandom().nextBoolean();

        try {
          ret1 = first ? mc.keyView1.first() : mc.keyView1.last();
        } catch (Throwable e) {
          e1 = e;
        }
        try {
          ret2 = first ? mc.keyView2.first() : mc.keyView2.last();
        } catch (Throwable e) {
          e2 = e;
        }
        Assert.assertEquals(ret1, ret2);
        if (e1 != null || e2 != null) {
          Assert.assertEquals(e1.getClass(), e2.getClass());
        }
        return InputType.KEYVIEW;
      }
    }

    public static class KeySetPoll extends AbstractBehavior {
      public KeySetPoll() {
        super("keySetPool", 1000, InputType.KEYVIEW, false);
      }

      @Override
      public InputType doBehavior(MapContext mc) {
        Integer ret1;
        Integer ret2;
        if (getRandom().nextBoolean()) {
          ret1 = mc.keyView1.pollFirst();
          ret2 = mc.keyView2.pollFirst();
        } else {
          ret1 = mc.keyView1.pollLast();
          ret2 = mc.keyView2.pollLast();
        }
        Assert.assertEquals(ret1, ret2);
        return InputType.KEYVIEW;
      }
    }

    public static class RandomBehavior {
      NavigableMap<InputType, List<AbstractBehavior>> behaviors = new TreeMap<>();
      NavigableMap<InputType, NavigableMap<Integer, AbstractBehavior>> weightedBehaviors = new TreeMap<>();
      NavigableMap<InputType, Integer> totalWeight = new TreeMap<>();

      public RandomBehavior(boolean multiThread) {

        // get behaviors from Behavior collections
        try {
          Class<?>[] classes = BehaviorCollections.class.getClasses();
          System.out.println("Get " + classes.length
            + " classes from BehaviorCollections");
          for (Class<?> cls : classes) {
            if (AbstractBehavior.class.isAssignableFrom(cls) && !cls
              .equals(AbstractBehavior.class)) {
              AbstractBehavior newBehavior;

              newBehavior = (AbstractBehavior) cls.newInstance();
              if (multiThread && !newBehavior.supportMultiThread()) {
                continue;
              }

              List<AbstractBehavior> list = behaviors.get(newBehavior.getType());
              if (list == null) {
                list = new ArrayList<>();
                behaviors.put(newBehavior.getType(), list);
              }
              list.add(newBehavior);

              System.out.println("Created new behavior " + newBehavior.getName());
            }
          }
        } catch (InstantiationException | IllegalAccessException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }

        // create random map
        for (Entry<InputType, List<AbstractBehavior>> entry : behaviors
          .entrySet()) {
          InputType type = entry.getKey();
          List<AbstractBehavior> bhs = entry.getValue();
          NavigableMap<Integer, AbstractBehavior> wm = new TreeMap<>();
          int totalw = 0;
          for (AbstractBehavior bh : bhs) {
            totalw += bh.getWeight();
            wm.put(totalw, bh);
            System.out.println("Put " + bh.getName() + " into at " + type
              + " of weight " + totalw);
          }
          totalWeight.put(type, totalw);
          weightedBehaviors.put(type, wm);
        }
      }

      public Behavior nextBehavior(InputType inputType) {
        int totalw = totalWeight.get(inputType);
        int l = getRandom().nextInt(totalw);
        Entry<Integer, AbstractBehavior> entry = weightedBehaviors.get(
          inputType).higherEntry(l);
        return entry.getValue();
      }
    }

    static public void initData(MapContext mc, int count) {
      for (int i = 0; i < count; ++i) {
        KVPair<Integer, String> kv = getRandomKV(mc);
        mc.mainMap1.put(kv.key(), kv.value());
        mc.mainMap2.put(kv.key(), kv.value());
      }
    }
  } // end of behavior collctions

  static class Performace {
    Map<String, Integer> behaviorToOps = new ConcurrentHashMap<>();
    Map<String, Long> behaviorToLatency = new ConcurrentHashMap<>();

    void addOps(String behavior, long latencyInNs) {
      Integer o = behaviorToOps.get(behavior);
      o = o == null ? 1 : o + 1;
      behaviorToOps.put(behavior, o);

      Long l = behaviorToLatency.get(behavior);
      l = l == null ? latencyInNs : l + latencyInNs;
      behaviorToLatency.put(behavior, l);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      for (Entry<String, Integer> e : behaviorToOps.entrySet()) {
        int ops = e.getValue();
        long lat = behaviorToLatency.get(e.getKey()) / ops / 1000;
        sb.append("Ops:").append(ops).append('\t').append("AvgLatency:")
          .append(lat).append('\t').append("B:").append(e.getKey())
          .append('\n');
      }
      return sb.toString();
    }
  }

  @Test
  public void testRandomBehavior() {
    final int RANGE = 32768;
    final int INIT_DATA = 50000;
    final int COUNT = 10000000;
    MapContext mc = new MapContext(createMapForTest(true),
      createMapForStand(), RANGE);
    BehaviorCollections.initRandom(RANGE);
    BehaviorCollections.initData(mc, INIT_DATA);
    System.out.println("There are " + mc.mainMap2.size()
      + " element in this map");
    Performace performance = new Performace();
    BehaviorCollections.RandomBehavior rb = new BehaviorCollections.RandomBehavior(false);
    for (int i = 0; i < COUNT; ++i) {
      Behavior b = rb.nextBehavior(mc.type);
      if (mc.type != InputType.MAIN_MAP) {
        mc.viewOps += 1;
      }
      long start = System.nanoTime();
      b.doBehavior(mc);
      long lat = System.nanoTime() - start;
      performance.addOps(b.getName(), lat);
      if (mc.viewOps > 20) { // up to 20 view operations
        mc.reset();
      }
    }
    System.out.println("There are " + mc.mainMap2.size()
      + " element in this map");
    System.out.println("Performace:");
    System.out.print(performance.toString());
  }

  @Test
  public void testRandomBehaviorParallelly() throws InterruptedException {
    final int RANGE = 32768;
    final int INIT_DATA = 50000;
    final int THREAD = Runtime.getRuntime().availableProcessors();
    final int COUNT = 1000000; // all threads will run COUNT times
    final RandomBehavior rb = new RandomBehavior(true);
    CompactedConcurrentSkipListMap<Integer, String> mapForTest = createMapForTest(true);
    ConcurrentSkipListMap<Integer, String> mapForStand = createMapForStand();
    final MapContext[] mc = new MapContext[THREAD];
    for (int i = 0; i < THREAD; ++i) {
      mc[i] = new MapContext(mapForTest, mapForStand, RANGE, i, THREAD);
    }

    BehaviorCollections.initRandom(RANGE);
    BehaviorCollections.initData(
      new MapContext(mapForTest, mapForStand, RANGE), INIT_DATA);
    System.out.println("There are " + mc[0].mainMap2.size()
      + " element in this map");
    final Performace performance = new Performace();
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < THREAD; ++i) {
      final MapContext context = mc[i];
      Thread t = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            for (int c = 0; c < COUNT; ++c) {
              Behavior b = rb.nextBehavior(context.type);
              if (context.type != InputType.MAIN_MAP) {
                context.viewOps += 1;
              }
              long start = System.nanoTime();
              b.doBehavior(context);
              long lat = System.nanoTime() - start;
              performance.addOps(b.getName(), lat);
              if (context.viewOps > 20) { // up to 20 view operations
                context.reset();
              }
            }
          } catch (Throwable e) {
            e.printStackTrace();
            Assert.assertFalse(e.getMessage(), true);
          }

        }
      });
      threads.add(t);
    }

    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    System.out.println("There are " + mc[0].mainMap2.size()
      + " element in this map");
    System.out.println("Performace:");
    System.out.print(performance.toString());
  }

  @Test
  public void testSingleKVLargerThanPage() {
    int old = BIG_KV_THRESHOLD;
    BIG_KV_THRESHOLD = DATA_PAGE_SIZE * 2;
    try {
      CompactedConcurrentSkipListMap<Integer, String> m = createMapForTest(false);
      int sz = DATA_PAGE_SIZE + 10;
      StringBuilder sb = new StringBuilder(sz);
      for (int i = 0; i < sz; ++i) {
        sb.append('a');
      }
      m.put(10, sb.toString());
    } finally {
      BIG_KV_THRESHOLD = old;
    }
  }

  @Test
  public void testSingleKVLargerThanDataLengthLimit() {
    int len = 0x00FFFFFF + 1;
    StringBuilder sb = new StringBuilder(len+1);
    for (int i = 0; i < len; ++i) {
      sb.append('a');
    }
    CompactedConcurrentSkipListMap<Integer, String> m = createMapForTest(false);
    m.put(10, sb.toString());
    System.out.println(m.getMemoryUsage().toString());
  }

  @Test
  public void testPageSize() {
    CompactedConcurrentSkipListMap<Integer, String> m = createMapForTest(false);
    m.put(1, "a");
    MemoryUsage usage = m.getMemoryUsage();
    Assert.assertEquals(usage.dataSpace, DATA_PAGE_SIZE);
  }

  @Test
  public void testEmptyMapMemoryUsed() {
    CompactedConcurrentSkipListMap<Integer, String> m = createMapForTest(true);
    MemoryUsage usage = m.getMemoryUsage();
    System.out.println(usage.toString());
    Assert.assertEquals(usage.dataSpace, 0);
    Assert.assertEquals(usage.heapKVCapacity, 0);
  }

}
