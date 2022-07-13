/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.mpp.execution.operator.schema;

import org.apache.iotdb.common.rpc.thrift.TSchemaNode;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.common.header.HeaderConstant;
import org.apache.iotdb.db.mpp.execution.driver.SchemaDriverContext;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.source.SourceOperator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import java.util.Set;
import java.util.stream.Collectors;

public class NodePathsSchemaScanOperator implements SourceOperator {
  private final PlanNodeId sourceId;

  private final OperatorContext operatorContext;

  private final PartialPath partialPath;

  private final int level;

  private boolean isFinished;

  public NodePathsSchemaScanOperator(
      PlanNodeId sourceId, OperatorContext operatorContext, PartialPath partialPath, int level) {
    this.sourceId = sourceId;
    this.operatorContext = operatorContext;
    this.partialPath = partialPath;
    this.level = level;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() {
    isFinished = true;
    TsBlockBuilder tsBlockBuilder;
    try {
      if (-1 == level) {
        tsBlockBuilder = new TsBlockBuilder(HeaderConstant.showChildPathsHeader.getRespDataTypes());
        // show child paths
        Set<TSchemaNode> nodePaths =
            ((SchemaDriverContext) operatorContext.getInstanceContext().getDriverContext())
                .getSchemaRegion()
                .getChildNodePathInNextLevel(partialPath);
        nodePaths.forEach(
            node -> {
              tsBlockBuilder.getTimeColumnBuilder().writeLong(0L);
              tsBlockBuilder.getColumnBuilder(0).writeBinary(new Binary(node.getNodeName()));
              tsBlockBuilder
                  .getColumnBuilder(1)
                  .writeBinary(new Binary(String.valueOf(node.getNodeType())));
              tsBlockBuilder.declarePosition();
            });
      } else {
        tsBlockBuilder = new TsBlockBuilder(HeaderConstant.showChildNodesHeader.getRespDataTypes());
        Set<String> childNodes;
        childNodes =
            ((SchemaDriverContext) operatorContext.getInstanceContext().getDriverContext())
                .getSchemaRegion().getNodesListInGivenLevel(partialPath, level, false, null)
                    .stream()
                    .map(PartialPath::getFullPath)
                    .collect(Collectors.toSet());

        childNodes.forEach(
            (path) -> {
              tsBlockBuilder.getTimeColumnBuilder().writeLong(0L);
              tsBlockBuilder.getColumnBuilder(0).writeBinary(new Binary(path));
              tsBlockBuilder.declarePosition();
            });
      }
    } catch (MetadataException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
    return tsBlockBuilder.build();
  }

  @Override
  public boolean hasNext() {
    return !isFinished;
  }

  @Override
  public boolean isFinished() {
    return isFinished;
  }

  @Override
  public PlanNodeId getSourceId() {
    return sourceId;
  }
}
