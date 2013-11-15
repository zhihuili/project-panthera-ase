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

public class StandardFunctionGenerator extends BaseHiveASTGenerator {

  @Override
  public boolean generate(ASTNode hiveRoot, CommonTree sqlRoot, ASTNode currentHiveNode,
      CommonTree currentSqlNode, TranslateContext context) throws SqlXlateException {

    CommonTree node = (CommonTree) currentSqlNode.getChild(0);

    ASTNode tokFunc;

    if (currentSqlNode.getType() == PantheraParser_PLSQLParser.ROUTINE_CALL) {
      tokFunc = SqlXlateUtil.newASTNode(HiveParser.TOK_FUNCTION, "TOK_FUNCTION");
      CommonTree Arguments = (CommonTree) currentSqlNode.getFirstChildWithType(PantheraParser_PLSQLParser.ARGUMENTS);
      if (Arguments != null) {
        CommonTree firstArg = (CommonTree) Arguments.getFirstChildWithType(PantheraParser_PLSQLParser.ARGUMENT);
        if (firstArg != null) {
          CommonTree expr = (CommonTree) firstArg.getFirstChildWithType(PantheraParser_PLSQLParser.EXPR);
          if (expr != null) {
            CommonTree distinct = (CommonTree) expr
                .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_DISTINCT);
            if (distinct != null) {
              tokFunc = SqlXlateUtil.newASTNode(HiveParser.TOK_FUNCTIONDI, "TOK_FUNCTIONDI");
            }
          }
        }
      }

    } else {
      CommonTree arg1 = (CommonTree) node.getChild(0);
      assert (arg1 != null);
      if (arg1.getType() == PantheraParser_PLSQLParser.ASTERISK) {
        if (arg1.getChildCount() > 0) {
          tokFunc = SqlXlateUtil.newASTNode(HiveParser.TOK_FUNCTION, "TOK_FUNCTION");
        } else {
          tokFunc = SqlXlateUtil.newASTNode(HiveParser.TOK_FUNCTIONSTAR, "TOK_FUNCTIONSTAR");
        }
      } else if (SqlXlateUtil.hasNodeTypeInTree(node, PantheraParser_PLSQLParser.SQL92_RESERVED_DISTINCT)) {
        tokFunc = SqlXlateUtil.newASTNode(HiveParser.TOK_FUNCTIONDI, "TOK_FUNCTIONDI");
      } else {
        tokFunc = SqlXlateUtil.newASTNode(HiveParser.TOK_FUNCTION, "TOK_FUNCTION");
      }
    }
    super.attachHiveNode(hiveRoot, currentHiveNode, tokFunc);
    return super.generateChildren(hiveRoot, sqlRoot, tokFunc, currentSqlNode, context);
  }

}
