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
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * generate Hive AST for "string" || "string" where "||" represents concatenation operator
 * ConcatenationOpGenerator.
 *
 */
public class ConcatenationOpGenerator extends BaseHiveASTGenerator {

  @Override
  public boolean generate(ASTNode hiveRoot, CommonTree sqlRoot, ASTNode currentHiveNode,
      CommonTree currentSqlNode, TranslateContext context) throws SqlXlateException {

    ASTNode tokFunc = super.newHiveASTNode(HiveParser.TOK_FUNCTION, "TOK_FUNCTION");
    ASTNode funcName = super.newHiveASTNode(HiveParser.Identifier, "concat");

    super.attachHiveNode(hiveRoot, currentHiveNode, tokFunc);
    super.attachHiveNode(hiveRoot, tokFunc, funcName);
    return generateTreeInOrder(hiveRoot, sqlRoot, tokFunc, currentSqlNode, context);
  }

  /**
   * travel the tree in-order.
   * @param hiveRoot
   * @param sqlRoot
   * @param currentHiveNode
   * @param currentSqlNode
   * @param context
   * @return
   * @throws SqlXlateException
   */
  private boolean generateTreeInOrder(ASTNode hiveRoot, CommonTree sqlRoot, ASTNode currentHiveNode,
      CommonTree currentSqlNode, TranslateContext context) throws SqlXlateException {
    assert(currentSqlNode.getChildCount() == 2);
    CommonTree op0 = (CommonTree) currentSqlNode.getChild(0);
    CommonTree op1 = (CommonTree) currentSqlNode.getChild(1);
    if (op0.getType() == PantheraParser_PLSQLParser.CONCATENATION_OP) {
      if (!generateTreeInOrder(hiveRoot, sqlRoot, currentHiveNode, op0, context)) {
        return false;
      }
    } else {
      if (!generateTreeLeaf(hiveRoot, sqlRoot, currentHiveNode, op0, context)) {
        return false;
      }
    }
    if (op1.getType() == PantheraParser_PLSQLParser.CONCATENATION_OP) {
      if (!generateTreeInOrder(hiveRoot, sqlRoot, currentHiveNode, op1, context)) {
        return false;
      }
    } else {
      if (!generateTreeLeaf(hiveRoot, sqlRoot, currentHiveNode, op1, context)) {
        return false;
      }
    }
    return true;
  }

  private boolean generateTreeLeaf(ASTNode hiveRoot, CommonTree sqlRoot, ASTNode currentHiveNode,
      CommonTree leafNode, TranslateContext context) throws SqlXlateException {
    HiveASTGenerator opGenerator = GeneratorFactory.getGenerator(leafNode);
    if (opGenerator == null) {
      throw new SqlXlateException(leafNode, "Untransformed SQL AST node:" + leafNode.getText());
    }
    return opGenerator.generateHiveAST(hiveRoot, sqlRoot, currentHiveNode, leafNode, context);
  }

}
