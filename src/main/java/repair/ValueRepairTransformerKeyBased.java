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

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.io.IOException;
import java.util.*;

/** This function is used to repair the value of the time series.
 * Imported from IoTDB for comparative study.
 * */
public class ValueRepairTransformerKeyBased<K,Vin> implements Transformer<K,Vin, KeyValue<K, Vin>> {
  double minSpeed;
  double maxSpeed;
  private ProcessorContext context;
  private Map<K,Deque<ValueAndTimestamp<Vin>>> windowData;
  Map<K,TimeWindow> currentWindow;
  TimeWindows windowPolicy;
  int optionalAttribute = -1;

  public ValueRepairTransformerKeyBased(double minSpeed, double maxSpeed, TimeWindows windowPolicy) {
    this.minSpeed = minSpeed;
    this.maxSpeed = maxSpeed;
    this.windowData = new HashMap<>();
    this.windowPolicy = windowPolicy;
    this.currentWindow = new HashMap<>();
  }

  public ValueRepairTransformerKeyBased(double minSpeed, double maxSpeed, TimeWindows windowPolicy, int optionalAttribute) {
    this.minSpeed = minSpeed;
    this.maxSpeed = maxSpeed;
    this.windowData = new HashMap<>();
    this.windowPolicy = windowPolicy;
    this.optionalAttribute = optionalAttribute;
    this.currentWindow = new HashMap<>();

  }

  @Override
  public void init(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public KeyValue<K, Vin> transform(K key, Vin value) {

    windowData.computeIfAbsent(key, k -> new ArrayDeque<>());
    if (!currentWindow.containsKey(key)){
      //First record ever for this key
      windowPolicy.windowsFor(Util.getTs(value)).entrySet().stream().min(Comparator.comparingLong(Map.Entry::getKey)).ifPresent(e -> currentWindow.put(key,e.getValue() ));
      windowData.get(key).add(ValueAndTimestamp.make(value, Util.getTs(value)));
      return null;
    } else {
      //from the second record on
      TimeWindow windowOld = currentWindow.get(key);
      windowPolicy.windowsFor(Util.getTs(value)).entrySet().stream().min(Comparator.comparingLong(Map.Entry::getKey)).ifPresent(e -> currentWindow.put(key,e.getValue() ));
      // check if the window has changed, i.e., slided

      if (windowOld.equals(currentWindow.get(key))){
        //same window
        windowData.get(key).add(ValueAndTimestamp.make(value, Util.getTs(value)));
        return null;
      }
    }
    //This code is executed only when the window has slided, thus triggering the repair and forwarding the repaired values

    //repair the values
    ValueRepair vr;
      Screen screen = null;
      try {
          if (optionalAttribute == -1) {
              screen = new Screen(windowData.get(key).iterator());
          } else {
              screen = new Screen(windowData.get(key).iterator(), optionalAttribute);
          }

      } catch (Exception e) {
          throw new RuntimeException(e);
      }
      if (!Double.isNaN(minSpeed)) {
      screen.setSmin(minSpeed);
    }
    if (!Double.isNaN(maxSpeed)) {
      screen.setSmax(maxSpeed);
    }
    vr = screen;

    vr.repair();
    double[] repaired = vr.getRepaired();
    long[] time = vr.getTime();

    //forward the repaired values
    Iterator<ValueAndTimestamp<Vin>> iterator = windowData.get(key).iterator();
    for (int i = 0; i < time.length; i++) {
      Vin value1 = iterator.next().value();
        try {
          if (optionalAttribute == -1) {
            Util.modifyValue(value1, repaired[i]);
          } else {
            Util.modifyValue(value1, repaired[i],optionalAttribute);
          }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        context.forward(key, value1);
    }

    //add the new value to the window
    windowData.get(key).add(ValueAndTimestamp.make(value, Util.getTs(value)));

    //evict old data
    while (!windowData.get(key).isEmpty() && windowData.get(key).peek().timestamp() < currentWindow.get(key).start()) {
      windowData.get(key).poll();
    }

    return null;
  }

  @Override
  public void close() {

  }
}
