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
package org.apache.iotdb;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class SessionExample {

  private static Session session;
  private static final String ROOT_SG1_D1_S1 = "root.sg1.d1.s1";
  private static final String ROOT_SG1_D1_S2 = "root.sg1.d1.s2";
  private static final String ROOT_SG1_D1_S3 = "root.sg1.d1.s3";
  private static final String ROOT_SG1_D1_S4 = "root.sg1.d1.s4";
  private static final String ROOT_SG1_D1 = "root.sg1.d1";


  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException, InterruptedException {
    session = new Session("127.0.0.1", 6667, "root", "root", 1, null);
    session.open(false);

//    insertRecord();
    // 100,000 * 300B = 30,000,000B
    query();
    session.close();
  }

  private static void insertRecord() throws IoTDBConnectionException, StatementExecutionException {

    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      measurements.add("s" + i);
      types.add(TSDataType.INT64);
    }

    for (long time = 0; time < 100; time++) {
      List<Object> values = new ArrayList<>();
      for (int i = 0; i < 1000; i++) {
        values.add(time);
      }
      for (int i = 0; i < 10; i++) {
        session.insertRecord("root.sg1.d" + i, time, measurements, types, values);
      }
      session.executeNonQueryStatement("flush");
    }
  }

  private static void query() throws InterruptedException {
    long startTime = System.nanoTime();
    CountDownLatch countDownLatch = new CountDownLatch(1);
    for (int device = 0; device < 1; device++) {
//      new Thread(new SumTask(device, countDownLatch)).start();
      for (int i = 0; i < 1; i++) {
        new Thread(new GroupByTask(device, 0, 100, countDownLatch)).start();
      }
    }
    countDownLatch.await();
    System.out.println("cost: " + (System.nanoTime() - startTime) / 1_000_000);
  }

  private static class GroupByTask implements Runnable {

    private final int device;
    private final int start;
    private final int end;
    private final CountDownLatch countDownLatch;

    public GroupByTask(int device, int start, int end, CountDownLatch countDownLatch) {
      this.device = device;
      this.start = start;
      this.end = end;
      this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {
      SessionDataSet dataSet;
      try {
        dataSet = session.executeQueryStatement(String.format("select last_value(*) from root.sg1.d%d group by ([%d,%d),10ms)", device, start, end));
        dataSet.setFetchSize(10); // default is 10000
        while (dataSet.hasNext()) {
          dataSet.next();
        }
        dataSet.closeOperationHandle();
        System.out.println("Device" + device + " finished " + start + "-group by task!");
        countDownLatch.countDown();
      } catch (StatementExecutionException | IoTDBConnectionException e) {
        e.printStackTrace();
      }
    }
  }

  private static class SumTask implements Runnable {

    private final int device;
    private final CountDownLatch countDownLatch;


    public SumTask(int device, CountDownLatch countDownLatch) {
      this.device = device;
      this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {
      SessionDataSet dataSet;
      try {
        dataSet = session.executeQueryStatement("select sum(*) from root.sg1.d" + device);
        dataSet.setFetchSize(1); // default is 10000
        while (dataSet.hasNext()) {
          dataSet.next();
        }
        dataSet.closeOperationHandle();
        System.out.println("Device" + device + " finished sum task!");
        countDownLatch.countDown();
      } catch (StatementExecutionException | IoTDBConnectionException e) {
        e.printStackTrace();
      }
    }
  }
}