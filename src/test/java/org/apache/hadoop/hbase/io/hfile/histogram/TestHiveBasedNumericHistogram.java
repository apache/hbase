package org.apache.hadoop.hbase.io.hfile.histogram;

import static org.junit.Assert.*;

import java.util.Random;

import org.apache.hadoop.hbase.io.hfile.histogram.HiveBasedNumericHistogram.Coord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestHiveBasedNumericHistogram {
  private HiveBasedNumericHistogram hist;
  double min = 0;
  double max = 10000;
  int count = 1000;
  int buckets = 100;
  Random r;
  @Before
  public void setUp() throws Exception {
    setup(min, max, buckets, count);
  }

  @After
  public void tearDown() throws Exception {

  }

  private void setup(double min, double max, int buckets, int count) {
    hist = new HiveBasedNumericHistogram(min, max);
    r = new Random();
    // Inserting elements into the histogram.
    hist.allocate(buckets);
    for (int i = 0; i<count; i++) {
      hist.add(r.nextDouble() * max);
    }
    assertTrue(hist.getNumBins() == 100);
  }

  @Test
  public void testInsertionsAndSum() {
    double sum = 0;
    for (int i = 0; i< (hist.getUsedBins() - 1); i++) {
      Coord bin1 = hist.getBin(i);
      Coord bin2 = hist.getBin(i+1);
      if (i == 0) sum += bin1.y;
      sum += bin2.y;
      assertTrue(bin1.x <= bin2.x);
    }
    assertTrue(sum == count);
  }

  @Test
  public void testUniform() {
    HiveBasedNumericHistogram uniformhist = this.hist.uniform(buckets);
    for (int i = 0; i < (uniformhist.getUsedBins() - 1); i++) {
      Coord bin1 = uniformhist.getBin(i);
      Coord bin2 = uniformhist.getBin(i+1);
      assertTrue(bin1.x <= bin2.x);
    }
  }

  @Test
  /**
   * Testing the following case:
   *
   */
  public void testSpecificTest1() {
    this.min = 0;
    this.max = 10;
    this.count = 10;
    this.buckets = 10;
    hist = new HiveBasedNumericHistogram(0, 10);
    hist.allocate(10);
    for (int i=0; i<10; i++) {
      hist.add(0);
    }
    assertTrue(hist.getUsedBins() == 1);
    assertTrue(hist.getBin(0).x == 0);
    assertTrue(hist.getBin(0).y == 10);
  }
}
