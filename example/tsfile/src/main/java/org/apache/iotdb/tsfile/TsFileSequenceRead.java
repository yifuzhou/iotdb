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
package org.apache.iotdb.tsfile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class TsFileSequenceRead {

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static void main(String[] args) throws IOException {
    String filename = "/Users/jackietien/Documents/incubator-iotdb/server/target/iotdb-server-0.11.0-SNAPSHOT/data/data/sequence/root.sg1/0/1600330018342-1-0.tsfile";

//    	[Chunk]
//    position: 13
//    Measurement: s3
//        [Chunk]
//    position: 180455494
//    Measurement: s1
//        [Chunk]
//    position: 360910975
//    Measurement: s2
//    long startTime = System.nanoTime();
//    FileChannel fileChannel = FileChannel.open(Paths.get(filename), StandardOpenOption.READ);
//    ByteBuffer byteBuffer = ByteBuffer.allocate(180000000);
//    fileChannel.read(byteBuffer);
//    byteBuffer.clear();
//    fileChannel.read(byteBuffer, 360000000);
//    long endTime = System.nanoTime();
//    System.out.println(endTime - startTime);

    long startTime = System.nanoTime();
    FileChannel fileChannel = FileChannel.open(Paths.get(filename), StandardOpenOption.READ);
    ByteBuffer byteBuffer = ByteBuffer.allocate(180000000);
    fileChannel.read(byteBuffer);
    byteBuffer.clear();
    fileChannel.read(byteBuffer);
    long endTime = System.nanoTime();
    System.out.println(endTime - startTime);
  }
}
