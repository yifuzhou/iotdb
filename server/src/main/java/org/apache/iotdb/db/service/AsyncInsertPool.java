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
package org.apache.iotdb.db.service;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.serviceSession.pool.SessionPool;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletsReq;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncInsertPool {

  private static final Logger logger = LoggerFactory.getLogger(AsyncInsertPool.class);
  ExecutorService pool;
  SessionPool sessionPool;


  private AsyncInsertPool(){
    sessionPool = new SessionPool("192.168.130.6", 6667, "root", "root", 20);
    pool = IoTDBThreadPoolFactory
        .newFixedThreadPool(10, "async insert pool");
  }


  public void submit(TSInsertTabletsReq req){
    pool.submit(new Runnable() {
      @Override
      public void run() {
        TSInsertTabletsReq transferReq = new TSInsertTabletsReq();
        transferReq.deviceIds = req.deviceIds;
        transferReq.setIsFinal(true);
        transferReq.measurementsList = req.measurementsList;
        transferReq.typesList = req.typesList;
        transferReq.sizeList = new ArrayList<>(req.sizeList);
        List<ByteBuffer> valueBuffer = new ArrayList<>(req.valuesList.size());
        for(ByteBuffer byteBuffer : req.valuesList){
          valueBuffer.add(ByteBuffer.wrap(byteBuffer.array()));
        }
        List<ByteBuffer> timeBuffer = new ArrayList<>(req.timestampsList.size());
        for(ByteBuffer byteBuffer : req.timestampsList){
          timeBuffer.add(ByteBuffer.wrap(byteBuffer.array()));
        }
        transferReq.valuesList = valueBuffer;
        transferReq.timestampsList = timeBuffer;

        try {
          sessionPool.insertTablets(transferReq);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
          logger.error("transfer request failed", e);
        }
      }
    });
  }

  public static AsyncInsertPool getInstance() {
    return AsyncInsertPool.InstanceHolder.INSTANCE;
  }

  static class InstanceHolder {

    private InstanceHolder() {
      // forbidding instantiation
    }

    private static final AsyncInsertPool INSTANCE = new AsyncInsertPool();
  }
}
