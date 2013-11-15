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
package org.apache.hadoop.hive.ql.parse.sql.generator;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;

/**
 * Generator for SQL92_RESERVED_DATE node.
 *
 * The expected SQL AST structure for a Date value expression is a SQL92_RESERVED_DATE node
 * followed by a sibling CHAR_STRING or CHAR_STRING_PERL or NATIONAL_CHAR_STRING_LIT node.
 *
 */
public class DateGenerator extends BaseHiveASTGenerator {

  @Override
  public boolean generate(ASTNode hiveRoot, CommonTree sqlRoot, ASTNode currentHiveNode,
      CommonTree currentSqlNode, TranslateContext context) throws SqlXlateException {
    //
    // Create a HIVE TOK_FUNCTION node and attach it to the current HIVE node.
    //
    ASTNode func = SqlXlateUtil.newASTNode(HiveParser.TOK_FUNCTION, "TOK_FUNCTION");
    attachHiveNode(hiveRoot, currentHiveNode, func);
    //
    // Create a HIVE TOK_DATE node as the first child of the TOK_FUNCTION node
    //
    ASTNode date = SqlXlateUtil.newASTNode(HiveParser.TOK_DATE, "TOK_DATE");
    attachHiveNode(hiveRoot, func, date);

    assert(currentSqlNode.getChildren() == null || currentSqlNode.getChildren().size() == 0);
    return true;
  }

}
