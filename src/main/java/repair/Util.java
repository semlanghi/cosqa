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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package repair;

import electricgrid.EGC;
import gps.GPS;
import linearroad.SpeedEvent;
import org.apache.commons.math3.stat.descriptive.rank.Median;
import reviews.Review;
import stocks.Stock;

import java.io.IOException;
import java.util.List;

import static repair.Type.*;

/** This class offers functions of getting and putting values from iotdb interface. */
public class Util {
  private Util() {
    throw new IllegalStateException("Utility class");
  }

  /**
   * Get value from specific column from Row, and cast to double. Make sure never get null from Row.
   *
   * @param row data row
   * @param index the column index
   * @return value of specific column from Row
   * @throws NoNumberException when getting a no number datatype
   */
  public static double getValueAsDouble(GPS row, int index) throws IOException, NoNumberException {
    if (index==0)
      return row.getX();
    else return row.getY();
  }

  public static double getValueAsDouble(SpeedEvent row) throws IOException, NoNumberException {
      return row.getSpeed();
  }

  public static double getValueAsDouble(Stock row) throws IOException, NoNumberException {
    return row.getValue();
  }

  public static double getValueAsDouble(Review row) throws IOException, NoNumberException {
    return row.value();
  }

  public static double getValueAsDouble(EGC row, int index) throws IOException, NoNumberException {
    if (index==0)
      return row.getConsA();
    else return row.getConsB();
  }

  /**
   * Get value from 0th column from Row, and cast to double. Make sure never get null from Row.
   *
   * @param row data row
   * @return value from 0th column from Row
   * @throws NoNumberException when getting a no number datatype
   */
  public static <V> double getValueAsDouble(V row, int... opt) throws IOException, NoNumberException {
    double ans = 0;

    if (row instanceof GPS && opt.length == 1) {
      ans = getValueAsDouble((GPS) row, opt[0]);
    } else if (row instanceof SpeedEvent) {
      ans = getValueAsDouble((SpeedEvent) row);
    } else if (row instanceof Stock) {
      ans = getValueAsDouble((Stock) row);
    } else if (row instanceof Review) {
      ans = getValueAsDouble((Review) row);
    } else if (row instanceof EGC && opt.length == 1) {
      ans = getValueAsDouble((EGC) row, opt[0]);
    } else {
      throw new IOException("Fail to get data type in row " + row);
    }


    return ans;
  }


  /**
   * Get value from specific column from Row, and cast to double. Make sure never get null from Row.
   *
   * @param row data row
   * @return value of specific column from Row
   * @throws NoNumberException when getting a no number datatype
   */
  public static long getTs(GPS row){
    return row.getTs();
  }

  public static long getTs(SpeedEvent row) {
    return row.getTimestamp();
  }

  public static long getTs(Stock row) {
    return row.getTs();
  }

  public static long getTs(Review row){
    return row.timestamp();
  }

  public static long getTs(EGC row){
    return row.getTs();
  }

  /**
   * Get value from 0th column from Row, and cast to double. Make sure never get null from Row.
   *
   * @param row data row
   * @return value from 0th column from Row
   * @throws NoNumberException when getting a no number datatype
   */
  public static <V> long getTs(V row) {
    long ans = -1;

    if (row instanceof GPS) {
      ans = getTs((GPS) row);
    } else if (row instanceof SpeedEvent) {
      ans = getTs((SpeedEvent) row);
    } else if (row instanceof Stock) {
      ans = getTs((Stock) row);
    } else if (row instanceof Review) {
      ans = getTs((Review) row);
    } else if (row instanceof EGC) {
      ans = getTs((EGC) row);
    } else {
      throw new RuntimeException("Fail to get data type in row " + row);
    }


    return ans;
  }

  /**
   * cast {@code ArrayList<Double>} to {@code double[]}.
   *
   * @param list ArrayList to cast
   * @return cast result
   */
  public static double[] toDoubleArray(List<Double> list) {
    return list.stream().mapToDouble(Double::valueOf).toArray();
  }

  /**
   * cast {@code ArrayList<Long>} to {@code long[]}.
   *
   * @param list ArrayList to cast
   * @return cast result
   */
  public static long[] toLongArray(List<Long> list) {
    return list.stream().mapToLong(Long::valueOf).toArray();
  }

  /**
   * calculate median absolute deviation of input series. 1.4826 is multiplied in order to achieve
   * asymptotic normality. Note: 1.4826 = 1/qnorm(3/4)
   *
   * @param value input series
   * @return median absolute deviation MAD
   */
  public static double mad(double[] value) {
    Median median = new Median();
    double mid = median.evaluate(value);
    double[] d = new double[value.length];
    for (int i = 0; i < value.length; i++) {
      d[i] = Math.abs(value[i] - mid);
    }
    return 1.4826 * median.evaluate(d);
  }

  /**
   * calculate 1-order difference of input series.
   *
   * @param origin original series
   * @return 1-order difference
   */
  public static double[] variation(double[] origin) {
    int n = origin.length;
    double[] variance = new double[n - 1];
    for (int i = 0; i < n - 1; i++) {
      variance[i] = origin[i + 1] - origin[i];
    }
    return variance;
  }

  /**
   * calculate 1-order difference of input series.
   *
   * @param origin original series
   * @return 1-order difference
   */
  public static double[] variation(long[] origin) {
    int n = origin.length;
    double[] variance = new double[n - 1];
    for (int i = 0; i < n - 1; i++) {
      variance[i] = (origin[i + 1] - origin[i]);
    }
    return variance;
  }

  /**
   * calculate 1-order difference of input series.
   *
   * @param origin original series
   * @return 1-order difference
   */
  public static int[] variation(int[] origin) {
    int n = origin.length;
    int[] variance = new int[n - 1];
    for (int i = 0; i < n - 1; i++) {
      variance[i] = origin[i + 1] - origin[i];
    }
    return variance;
  }

  /**
   * calculate speed (1-order derivative with backward difference).
   *
   * @param origin value series
   * @param time timestamp series
   * @return speed series
   */
  public static double[] speed(double[] origin, double[] time) {
    int n = origin.length;
    double[] speed = new double[n - 1];
    for (int i = 0; i < n - 1; i++) {
      speed[i] = (origin[i + 1] - origin[i]) / (time[i + 1] - time[i]);
    }
    return speed;
  }

  /**
   * calculate speed (1-order derivative with backward difference).
   *
   * @param origin value series
   * @param time timestamp series
   * @return speed series
   */
  public static double[] speed(double[] origin, long[] time) {
    int n = origin.length;
    double[] speed = new double[n - 1];
    for (int i = 0; i < n - 1; i++) {
      speed[i] = (origin[i + 1] - origin[i]) / (time[i + 1] - time[i]);
    }
    return speed;
  }


  /**
   * cast String to timestamp.
   *
   * @param s input string
   * @return timestamp
   */
  public static long parseTime(String s) {
    long unit = 0;
    s = s.toLowerCase();
    s = s.replace(" ", "");
    if (s.endsWith("ms")) {
      unit = 1;
      s = s.substring(0, s.length() - 2);
    } else if (s.endsWith("s")) {
      unit = 1000;
      s = s.substring(0, s.length() - 1);
    } else if (s.endsWith("m")) {
      unit = 60 * 1000L;
      s = s.substring(0, s.length() - 1);
    } else if (s.endsWith("h")) {
      unit = 60 * 60 * 1000L;
      s = s.substring(0, s.length() - 1);
    } else if (s.endsWith("d")) {
      unit = 24 * 60 * 60 * 1000L;
      s = s.substring(0, s.length() - 1);
    }
    double v = Double.parseDouble(s);
    return (long) (unit * v);
  }

  public static <Vin> void modifyValue(Vin row, double v, int... opt) throws IOException {

    if (row instanceof GPS && opt.length == 1) {
      setValueAsDouble((GPS) row, v,opt[0]);
    } else if (row instanceof SpeedEvent) {
      setValueAsDouble((SpeedEvent) row, v);
    } else if (row instanceof Stock) {
      setValueAsDouble((Stock) row, v);
    } else if (row instanceof Review) {
      setValueAsDouble((Review) row, v);
    } else if (row instanceof EGC && opt.length == 1) {
      setValueAsDouble((EGC) row, v, opt[0]);
    } else {
      throw new IOException("Fail to get data type in row " + row);
    }
  }

  public static void setValueAsDouble(GPS row, double repaired, int index) {
    if (index==0)
      row.setX(repaired);
    else row.setY(repaired);
  }

  public static void setValueAsDouble(SpeedEvent row, double repaired) {
    row.setSpeed((int) repaired);
  }

  public static void setValueAsDouble(Stock row, double repaired)  {
    row.setValue(repaired);
  }

  public static void setValueAsDouble(Review row, double repaired) {
    row.setValue((int) repaired);
  }

  public static void setValueAsDouble(EGC row, double repaired, int index) {
    if (index==0)
      row.setConsA((long) repaired);
    else row.setConsB((long) repaired);
  }
}
