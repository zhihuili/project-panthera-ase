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

import org.antlr.runtime.tree.CommonTree;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * Exceptions thrown at Translation phase.
 *
 */
public class SqlXlateException extends Exception {

  private static final long serialVersionUID = 1L;
  private String dm = "";
  private static final Log LOG = LogFactory.getLog("hive.ql.parse.sql.SqlParseDriver");

  public CommonTree node = null;
  /**
   * Constructor.
   *
   * @param e the associated exception thrown from Sql Parser
   */
  public SqlXlateException(SqlXlateException e) {
    super(e);
    this.node = e.node;
  }

  public SqlXlateException(CommonTree node, Exception e) {
    super(e);
    this.node = node;
  }

  public SqlXlateException(CommonTree node) {
    super(commonTreeTrace(node));
    this.node = node;
  }

  public SqlXlateException(CommonTree node, String msg) {
    super(msg + ":" + commonTreeTrace(node));
    this.node = node;
  }

  public void outputException(String origin) {
    if (this.node == null) {
      messagePrepare(super.getMessage());
      return ;
    }
    messagePrepare("Error node '"+ this.node.getText().toUpperCase() + "': " + super.getMessage());
    if (this.node.getLine()==0) {
      return ;
    }
    int linenum = this.node.getLine() - 1;

    String[] linetext = origin.split("\n");
    linetext[linetext.length - 1] += " ;";
    String lineptr = linetext[linenum].replaceAll(".", " ");
    int i = 0;
    for(; i <= linenum; i++) {
      messagePrepare(linetext[i].toUpperCase());
    }
    messagePrepare(lineptr.subSequence(0, this.node.getCharPositionInLine()) + "^" +
        lineptr.subSequence(this.node.getCharPositionInLine() + 1, lineptr.length()));
    for(; i < linetext.length; i++) {
      messagePrepare(linetext[i].toUpperCase());
    }
  }

  private void messagePrepare(String str) {
    dm += str + "\n";
  }

  private static String commonTreeTrace(CommonTree node) {
    // TODO Improve this
    if(node == null) {
      return "";
    }
    return "Error(s) near line " + node.getLine() + ":" + node.getCharPositionInLine();
  }

  @Override
  public String getMessage() {
    return dm;
  }

}
