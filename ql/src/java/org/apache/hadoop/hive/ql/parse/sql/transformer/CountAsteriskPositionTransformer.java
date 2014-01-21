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
package org.apache.hadoop.hive.ql.parse.sql.transformer;

import java.util.ArrayList;
import java.util.List;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.PantheraExpParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockUtil;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * if count(*) is on the right side of the compare operator, transform it
 * to the left side.
 */
public class CountAsteriskPositionTransformer extends BaseSqlASTTransformer {
  SqlASTTransformer tf;

  public CountAsteriskPositionTransformer(SqlASTTransformer tf) {
    this.tf = tf;
  }

  @Override
  public void transform(CommonTree tree, TranslateContext context) throws SqlXlateException {
    tf.transformAST(tree, context);
    this.transformCountAsterisk(tree, context);
  }

  private void transformCountAsterisk(CommonTree tree, TranslateContext context)
      throws SqlXlateException {
    // deep firstly
    for (int i = 0; i < tree.getChildCount(); i++) {
      transformCountAsterisk((CommonTree) (tree.getChild(i)), context);
    }

    // if tree is a filter, need to check if count(*) at right.
    switch (tree.getType()) {
    case PantheraParser_PLSQLParser.EQUALS_OP:
    case PantheraExpParser.EQUALS_NS:
    case PantheraParser_PLSQLParser.NOT_EQUAL_OP:
    case PantheraParser_PLSQLParser.GREATER_THAN_OP:
    case PantheraParser_PLSQLParser.GREATER_THAN_OR_EQUALS_OP:
    case PantheraParser_PLSQLParser.LESS_THAN_OP:
    case PantheraParser_PLSQLParser.LESS_THAN_OR_EQUALS_OP:
      needExchange(tree);
      break;
    default:
      break;
    }
  }

  /**
   * change the tree's 2 children if the right tree contains count(*) but left not contains.
   *
   * @param tree
   * @return
   */
  private boolean needExchange(CommonTree tree){
    if (tree.getChildCount() == 2) {
      // for count(*) at right hand of operator
      CommonTree sf = FilterBlockUtil.createSqlASTNode(tree, PantheraParser_PLSQLParser.STANDARD_FUNCTION, "STANDARD_FUNCTION");
      CommonTree cnt = FilterBlockUtil.createSqlASTNode(tree, PantheraParser_PLSQLParser.COUNT_VK, "count");
      CommonTree ast = FilterBlockUtil.createSqlASTNode(tree, PantheraParser_PLSQLParser.ASTERISK, "*");
      sf.addChild(cnt);
      cnt.addChild(ast);
      List<CommonTree> funcList = new ArrayList<CommonTree>();
      FilterBlockUtil.findSubTree((CommonTree) tree.getChild(1), sf, funcList);
      if (funcList.size() > 0) {
        // right hand of operator contains count(*)
        List<CommonTree> leftFuncList = new ArrayList<CommonTree>();
        FilterBlockUtil.findSubTree((CommonTree) tree.getChild(0), sf, leftFuncList);
        if (leftFuncList.isEmpty()) {
          // left does not contain
          // exchange count(*) to left
          SqlXlateUtil.exchangeChildrenPosition(tree);
          switch (tree.getType()) {
          case PantheraParser_PLSQLParser.GREATER_THAN_OP:
            tree.getToken().setText("<");
            tree.getToken().setType(PantheraParser_PLSQLParser.LESS_THAN_OP);
            break;
          case PantheraParser_PLSQLParser.GREATER_THAN_OR_EQUALS_OP:
            tree.getToken().setText("<=");
            tree.getToken().setType(PantheraParser_PLSQLParser.LESS_THAN_OR_EQUALS_OP);
            break;
          case PantheraParser_PLSQLParser.LESS_THAN_OP:
            tree.getToken().setText(">");
            tree.getToken().setType(PantheraParser_PLSQLParser.GREATER_THAN_OP);
            break;
          case PantheraParser_PLSQLParser.LESS_THAN_OR_EQUALS_OP:
            tree.getToken().setText(">=");
            tree.getToken().setType(PantheraParser_PLSQLParser.GREATER_THAN_OR_EQUALS_OP);
            break;
          default:
            break;
          }
          return true;
        }
      }
    }
    return false;
  }
}