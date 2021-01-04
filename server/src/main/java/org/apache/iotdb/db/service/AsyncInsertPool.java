package org.apache.iotdb.db.service;

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
        try {
          sessionPool.insertTablets(req);
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
