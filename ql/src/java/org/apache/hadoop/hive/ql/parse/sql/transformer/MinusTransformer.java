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

import java.util.List;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.PantheraExpParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockUtil;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * transform MINUS to LEFT JOIN.
 *
 * MinusTransformer.
 *
 */
public class MinusTransformer extends SetOperatorTransformer {
  SqlASTTransformer tf;

  public MinusTransformer(SqlASTTransformer tf) {
    this.tf = tf;
  }

  @Override
  protected void transform(CommonTree tree, TranslateContext context) throws SqlXlateException {
    tf.transformAST(tree, context);
    trans(tree, context);
  }

  private void trans(CommonTree node, TranslateContext context) throws SqlXlateException {
    int childCount = node.getChildCount();
    for (int i = 0; i < childCount; i++) {
      trans((CommonTree) node.getChild(i), context);
    }
    if (node.getType() == PantheraExpParser.PLSQL_RESERVED_MINUS) {
      processIntersect(node, context);
    }
  }

  @Override
  CommonTree makeJoinNode(CommonTree on) {
    CommonTree join = FilterBlockUtil.createSqlASTNode(on, PantheraExpParser.JOIN_DEF, "join");
    join.addChild(FilterBlockUtil.createSqlASTNode(on, PantheraExpParser.LEFT_VK, "left"));
    return join;
  }

  @Override
  CommonTree makeWhere(CommonTree setOperator, CommonTree leftTableRefElement, CommonTree rightTableRefElement,
      List<CommonTree> leftColumnAliasList, List<CommonTree> rightColumnAliasList) {

    CommonTree where = FilterBlockUtil.createSqlASTNode(
        setOperator, PantheraParser_PLSQLParser.SQL92_RESERVED_WHERE, "where");
    CommonTree logicExpr = FilterBlockUtil.createSqlASTNode(setOperator, PantheraParser_PLSQLParser.LOGIC_EXPR,
        "LOGIC_EXPR");
    where.addChild(logicExpr);
    int count = rightColumnAliasList.size();
    String rightAlias = rightTableRefElement.getChild(0).getChild(0).getText();
    for (int i = 0; i < count; i++) {
      CommonTree isNull = FilterBlockUtil.createSqlASTNode(setOperator, PantheraExpParser.IS_NULL, "IS_NULL");
      CommonTree cacatedElement = FilterBlockUtil.createCascatedElementBranch(isNull, rightAlias,
          rightColumnAliasList.get(i).getChild(0).getText());
      isNull.addChild(cacatedElement);
      if (logicExpr.getChildCount() == 0) {
        logicExpr.addChild(isNull);
      } else {
        CommonTree and = FilterBlockUtil.createSqlASTNode(setOperator, PantheraExpParser.SQL92_RESERVED_AND,
            "and");
        CommonTree leftChild = (CommonTree) logicExpr.deleteChild(0);
        and.addChild(leftChild);
        and.addChild(isNull);
        logicExpr.addChild(and);
      }
    }
    return where;

  }

}
