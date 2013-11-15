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

package org.apache.hadoop.hive.ql.parse.sql;

import java.io.Serializable;

import org.antlr.runtime.Token;
import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.transformer.QueryInfo;

/**
 * SQL AST Node class.
 */
public class SqlASTNode extends CommonTree implements Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * A reference to the QueryInfo object of the containing query.
   */
  private QueryInfo qInfo;

  /**
   * Constructor.
   */
  public SqlASTNode() {

  }

  /**
   * Constructor.
   * 
   * @param token
   *          Token for the CommonTree Node
   * 
   */
  public SqlASTNode(Token token) {
    super(token);
  }

  /**
   * Setter method of the QueryInfo object.
   * 
   * @param qInfo
   *          QueryInfo object of the containing query
   */
  public void setQueryInfo(QueryInfo qInfo) {
    this.qInfo = qInfo;
  }

  /**
   * Getter method of the QueryInfo object.
   * 
   * @return qInfo QueryInfo object of the containing query
   */
  public QueryInfo getQueryInfo() {
    return qInfo;
  }

}
