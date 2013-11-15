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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.parse.sql.transformer.fb;

import java.util.Stack;

import org.apache.hadoop.hive.ql.parse.sql.transformer.QueryInfo;

/**
 * Store some stack for transforming filter block tree.
 * FilterBlockContext.
 *
 */
public class FilterBlockContext {
  Stack<QueryBlock> queryStack = new Stack<QueryBlock>();
  Stack<SubQFilterBlock> subQStack = new Stack<SubQFilterBlock>();
  Stack<TypeFilterBlock> typeStack = new Stack<TypeFilterBlock>();
  QueryInfo qInfo;

  public FilterBlockContext(QueryInfo qInfo) {
    this.qInfo = qInfo;
  }

  public Stack<QueryBlock> getQueryStack() {
    return queryStack;
  }

  public Stack<SubQFilterBlock> getSubQStack() {
    return subQStack;
  }

  public Stack<TypeFilterBlock> getTypeStack() {
    return typeStack;
  }

  public QueryInfo getqInfo() {
    return qInfo;
  }

  public void setqInfo(QueryInfo qInfo) {
    this.qInfo = qInfo;
  }

}
