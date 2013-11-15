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

public class SelectGenerator extends BaseHiveASTGenerator {

  @Override
  public boolean generate(ASTNode hiveRoot, CommonTree sqlRoot, ASTNode currentHiveNode,
      CommonTree currentSqlNode, TranslateContext context) throws SqlXlateException {

    ASTNode ret = SqlXlateUtil.newASTNode(HiveParser.TOK_QUERY, "TOK_QUERY");
    attachHiveNode(hiveRoot,currentHiveNode,ret);

    currentHiveNode = ret;

    CommonTree node = (CommonTree) currentSqlNode.getChild(0);//from node
    if (!GeneratorFactory.getGenerator(node).generateHiveAST(hiveRoot, sqlRoot,
        currentHiveNode, node, context)) {
      return false;
    }

     ret = SqlXlateUtil.newASTNode(HiveParser.TOK_INSERT, "TOK_INSERT");
    super.attachHiveNode(hiveRoot, currentHiveNode, ret);
    currentHiveNode = ret;
    if (currentSqlNode.getParent().getParent().getParent().getType() == PantheraParser_PLSQLParser.SINGLE_TABLE_MODE
        && currentSqlNode.getParent().getParent().getParent().getChild(0).getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_INTO) {
      // insert into <table> select ...
      CommonTree into = (CommonTree) currentSqlNode.getParent().getParent().getParent().getChild(0);
      if (into.getChild(0).getType() != PantheraParser_PLSQLParser.TABLE_REF
          && into.getChild(0).getChild(0).getType() != PantheraParser_PLSQLParser.TABLE_EXPRESSION
          && into.getChild(0).getChild(0).getChild(0).getType() != PantheraParser_PLSQLParser.DIRECT_MODE
          && into.getChild(0).getChild(0).getChild(0).getChild(0).getType() != PantheraParser_PLSQLParser.TABLEVIEW_NAME) {
        throw new SqlXlateException(into, "Unsupported sql AST node:" + into.getText());
      }
      String tableViewName = into.getChild(0).getChild(0).getChild(0).getChild(0).getChild(0).getText();
      if (into.getChildCount() == 2
          && into.getChild(1).getType() == PantheraParser_PLSQLParser.COLUMNS
          && into.getChild(1).getChildCount() != 0) {
        LOG.info("columns for INSERET INTO are not supported, will be ignored!");
      }
      ASTNode insertInto = SqlXlateUtil.newASTNode(HiveParser.TOK_INSERT_INTO, "TOK_INSERT_INTO");
      ASTNode tab = SqlXlateUtil.newASTNode(HiveParser.TOK_TAB, "TOK_TAB");
      insertInto.addChild(tab);
      ASTNode tabName = SqlXlateUtil.newASTNode(HiveParser.TOK_TABNAME, "TOK_TABNAME");
      tab.addChild(tabName);
      ASTNode id = SqlXlateUtil.newASTNode(HiveParser.Identifier, tableViewName);
      tabName.addChild(id);
      ret = insertInto;
    } else {
      ret = super.buildTmpDestinationNode();
    }
    super.attachHiveNode(hiveRoot, currentHiveNode, ret);


    return generateChildrenExcept(hiveRoot,sqlRoot,currentHiveNode,currentSqlNode,context,0);
  }

}
