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

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * Generator for SQL92_RESERVED_BETWEEN and NOT_BETWEEN node.
 *
 */
public class BetweenGenerator extends BaseHiveASTGenerator {

  @Override
  public boolean generate(ASTNode hiveRoot, CommonTree sqlRoot, ASTNode currentHiveNode,
      CommonTree currentSqlNode, TranslateContext context) throws SqlXlateException {
    //
    // Create a HIVE TOK_FUNCTION node and attach it to the current HIVE node.
    //
    ASTNode func = SqlXlateUtil.newASTNode(HiveParser.TOK_FUNCTION, "TOK_FUNCTION");
    attachHiveNode(hiveRoot, currentHiveNode, func);
    //
    // Create a HIVE Identifier node as the first child of the TOK_FUNCTION node.
    //
    ASTNode identifer = SqlXlateUtil.newASTNode(HiveParser.Identifier, "between");
    attachHiveNode(hiveRoot, func, identifer);

    //
    // Create a HIVE node to indicate if it is NOT between as the second child of the TOK_FUNCTION node.
    //

    ASTNode notBetween;
    if (currentSqlNode.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_BETWEEN) {
      notBetween = SqlXlateUtil.newASTNode(HiveParser.KW_FALSE, "KW_FALSE");
    } else {
      notBetween = SqlXlateUtil.newASTNode(HiveParser.KW_TRUE, "KW_TRUE");
    }
    attachHiveNode(hiveRoot, func, notBetween);

    return super.generateChildren(hiveRoot, sqlRoot, func, currentSqlNode, context);
  }

}
