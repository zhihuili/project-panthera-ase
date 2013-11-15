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
 * To generate SQL AST Node ANY_ELEMENT,there are some crazy situation.
 *
 * AnyElementGenerator.
 *
 */
public class AnyElementGenerator extends BaseHiveASTGenerator {

  @Override
  public boolean generate(ASTNode hiveRoot, CommonTree sqlRoot, ASTNode currentHiveNode,
      CommonTree currentSqlNode, TranslateContext context) throws SqlXlateException {

    if (currentSqlNode.getChildCount() > 1) {
      // two leaf with ANY_ELEMENT
      ASTNode ret = SqlXlateUtil.newASTNode(HiveParser.DOT, ".");
      super.attachHiveNode(hiveRoot, currentHiveNode, ret);
      currentHiveNode = ret;
      ret = SqlXlateUtil.newASTNode(HiveParser.TOK_TABLE_OR_COL, "TOK_TABLE_OR_COL");
      super.attachHiveNode(hiveRoot, currentHiveNode, ret);
      super.generateChildrenExcept(hiveRoot, sqlRoot, ret, currentSqlNode, context, 1);
      return super.generateChildrenExcept(hiveRoot, sqlRoot, currentHiveNode, currentSqlNode,
          context, 0);
    }

    if (currentSqlNode.getChildCount() == 1) {
      CommonTree leaf = (CommonTree) currentSqlNode.getChild(0);
      String leafText = leaf.getText();
      if (leaf.getType() == PantheraParser_PLSQLParser.ID && "null".equals(leafText.toLowerCase())
          || leafText.contains("\"") || leafText.contains("'")) {
        return super.generateChildren(hiveRoot, sqlRoot, currentHiveNode, currentSqlNode, context);
      }
      ASTNode ret = SqlXlateUtil.newASTNode(HiveParser.TOK_TABLE_OR_COL, "TOK_TABLE_OR_COL");
      super.attachHiveNode(hiveRoot, currentHiveNode, ret);
      return super.generateChildren(hiveRoot, sqlRoot, ret, currentSqlNode, context);
    }
    return false;
  }

}
