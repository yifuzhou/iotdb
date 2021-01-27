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
package org.apache.iotdb.db.engine.flush;

import java.io.IOException;
import java.util.Map;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.IWritableMemChunk;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemTableFlushTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(MemTableFlushTask.class);
  private final RestorableTsFileIOWriter writer;

  private final String storageGroup;

  private final IMemTable memTable;


  /**
   * @param memTable the memTable to flush
   * @param writer the writer where memTable will be flushed to (current tsfile writer or vm writer)
   * @param storageGroup current storage group
   */

  public MemTableFlushTask(IMemTable memTable, RestorableTsFileIOWriter writer, String storageGroup) {
    this.memTable = memTable;
    this.writer = writer;
    this.storageGroup = storageGroup;
    LOGGER.debug("flush task of Storage group {} memtable {} is created ",
        storageGroup, memTable.getVersion());
  }

  /**
   * the function for flushing memtable.
   */
  public void syncFlushMemTable()
      throws InterruptedException, IOException {
    LOGGER.info("The memTable size of SG {} is {}, the avg series points num in chunk is {} ",
        storageGroup,
        memTable.memSize(),
        memTable.getTotalPointsNum() / memTable.getSeriesNumber());
    long start = System.currentTimeMillis();
    long sortTime = 0;
    long encodingTime = 0;
    long ioTime = 0;

    //for map do not use get(key) to iteratate
    for (Map.Entry<String, Map<String, IWritableMemChunk>> memTableEntry : memTable.getMemTableMap().entrySet()) {
      writer.startChunkGroup(memTableEntry.getKey());
      final Map<String, IWritableMemChunk> value = memTableEntry.getValue();
      for (Map.Entry<String, IWritableMemChunk> iWritableMemChunkEntry : value.entrySet()) {
        long startTime = System.currentTimeMillis();
        IWritableMemChunk series = iWritableMemChunkEntry.getValue();
        MeasurementSchema desc = series.getSchema();
        TVList tvList = series.getSortedTVListForFlush();
        long encodingStartTime = System.currentTimeMillis();
        sortTime += encodingStartTime - startTime;
        IChunkWriter seriesWriter = new ChunkWriterImpl(desc);
        writeOneSeries(tvList, seriesWriter, desc.getType());
        seriesWriter.sealCurrentPage();
        seriesWriter.clearPageWriter();
        long ioStartTime = System.currentTimeMillis();
        encodingTime += (ioStartTime - encodingStartTime);
        seriesWriter.writeToFileWriter(this.writer);
        ioTime += (System.currentTimeMillis() - ioStartTime);
      }
      long ioStartTime = System.currentTimeMillis();
      writer.setMinPlanIndex(memTable.getMinPlanIndex());
      writer.setMaxPlanIndex(memTable.getMaxPlanIndex());
      writer.endChunkGroup();
      ioTime += (System.currentTimeMillis() - ioStartTime);
    }

    LOGGER.info(
        "Storage group {} memtable {}, flushing into disk: data sort time cost {} ms.",
        storageGroup, memTable.getVersion(), sortTime);
    LOGGER.info("Storage group {}, flushing memtable {} into disk: Encoding data cost "
            + "{} ms.",
        storageGroup, memTable.getVersion(), encodingTime);
    LOGGER.info("flushing a memtable {} in storage group {}, io cost {}ms", memTable.getVersion(),
        storageGroup, ioTime);

    writer.writeVersion(memTable.getVersion());
    writer.writePlanIndices();

    LOGGER.info(
        "Storage group {} memtable {} flushing a memtable has finished! Time consumption: {}ms",
        storageGroup, memTable, System.currentTimeMillis() - start);


  }

  private void writeOneSeries(TVList tvPairs, IChunkWriter seriesWriterImpl,
      TSDataType dataType) {
    for (int i = 0; i < tvPairs.size(); i++) {
      long time = tvPairs.getTime(i);

      // skip duplicated data
      if ((i + 1 < tvPairs.size() && (time == tvPairs.getTime(i + 1)))) {
        continue;
      }

      switch (dataType) {
        case BOOLEAN:
          seriesWriterImpl.write(time, tvPairs.getBoolean(i));
          break;
        case INT32:
          seriesWriterImpl.write(time, tvPairs.getInt(i));
          break;
        case INT64:
          seriesWriterImpl.write(time, tvPairs.getLong(i));
          break;
        case FLOAT:
          seriesWriterImpl.write(time, tvPairs.getFloat(i));
          break;
        case DOUBLE:
          seriesWriterImpl.write(time, tvPairs.getDouble(i));
          break;
        case TEXT:
          seriesWriterImpl.write(time, tvPairs.getBinary(i));
          break;
        default:
          LOGGER.error("Storage group {} does not support data type: {}", storageGroup,
              dataType);
          break;
      }
    }
  }

}
