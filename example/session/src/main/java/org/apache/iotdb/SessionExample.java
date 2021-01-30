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
import java.util.Random;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

public class SessionExample {

  private static Session session;
  private static final String ROOT_SG1_D1 = "root.sg1.d1";


  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open(false);

    //set session fetchSize
    session.setFetchSize(10000);

    long MAX_ROW_NUM = Long.parseLong(args[0]);
    System.out.println("MAX_ROW_NUM: " + MAX_ROW_NUM);
    insertTablet(MAX_ROW_NUM);
    session.close();
  }
  private static void insertTablet(long MAX_ROW_NUM) throws IoTDBConnectionException, StatementExecutionException {
    // The schema of measurements of one device
    // only measurementId and data type in MeasurementSchema take effects in Tablet
    List<MeasurementSchema> schemaList = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      schemaList.add(new MeasurementSchema("s" + i, TSDataType.DOUBLE));
    }

    Tablet tablet = new Tablet(ROOT_SG1_D1, schemaList, 1000);

    Random random = new Random(19876);
    for (long row = 0; row < MAX_ROW_NUM; row++) {
      int rowIndex = tablet.rowSize++;
      long timestamp = random.nextInt((int) MAX_ROW_NUM);
      tablet.addTimestamp(rowIndex, timestamp);
      for (int s = 0; s < 1000; s++) {
        double value = random.nextDouble();
        tablet.addValue(schemaList.get(s).getMeasurementId(), rowIndex, value);
      }
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        session.insertTablet(tablet);
        tablet.reset();
      }
    }

    if (tablet.rowSize != 0) {
      session.insertTablet(tablet);
      tablet.reset();
    }
  }
}
