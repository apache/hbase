package org.apache.hadoop.hbase.benchmarks;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * General purpose utility class that tracks the results of a benchmark 
 * experiment. Abstracts the X-axis and Y-axis of the table, and the 
 * result object.
 * 
 * @param <DimensionX> X axis of the result
 * @param <DimensionY> Y axis of the result
 * @param <Result> Result object
 */
public class BenchmarkResults<DimensionX, DimensionY, Result> {
  String xFormat;
  String resultFormat;
  DimensionX[] xValues;
  DimensionY[] yValues;
  List<String> headerEntries;
  private Map<DimensionX, Map<DimensionY, Result>> results = 
    new HashMap<DimensionX, Map<DimensionY, Result>>();

  public BenchmarkResults(DimensionX[] xValues, DimensionY[] yValues, 
      String xFormat, String resultFormat, List<String> headerEntries) {
    this.xValues = xValues;
    this.yValues = yValues;
    this.xFormat = xFormat;
    this.resultFormat = resultFormat;
    this.headerEntries = headerEntries;
  }

  /**
   * Add a single result entry
   * @param xValue
   * @param yValue
   * @param result
   */
  public void addResult(DimensionX xValue, DimensionY yValue, Result result) {
    Map<DimensionY, Result> yValue_to_result_map = results.get(xValue);
    if (yValue_to_result_map == null) {
      yValue_to_result_map = new HashMap<DimensionY, Result>();
    }
    yValue_to_result_map.put(yValue, result);
    results.put(xValue, yValue_to_result_map);
  }
  
  /**
   * Pretty print the results to STDOUT
   */
  public void prettyPrint() {
    // print the headers
    for (String s : headerEntries) {
      System.out.printf("%s\t", s);
    }
    System.out.println("");
    // print all the values in the results
    for (DimensionX x : xValues) {
      System.out.printf(xFormat, x);
      Map<DimensionY, Result> yValue_to_result_map = results.get(x);
      for (DimensionY y : yValues) {
        System.out.printf("\t" + resultFormat, yValue_to_result_map.get(y));
      }
      // row done
      System.out.println("");
    }
    // done with the results
    System.out.println("\n");
  }
}
