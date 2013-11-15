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
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockUtil;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * process function in ORDER BY
 *
 * OrderByFunctionTransformer.
 *
 */
public class OrderByFunctionTransformer extends BaseSqlASTTransformer {
  SqlASTTransformer tf;

  public OrderByFunctionTransformer(SqlASTTransformer tf) {
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
    if (node.getType() == PantheraExpParser.SQL92_RESERVED_ORDER) {
      processOrder(node, context);
    }
  }

  private void processOrder(CommonTree node, TranslateContext context) {
    CommonTree orderByElements = (CommonTree) node.getChild(0);
    List<CommonTree> functionNodes = new ArrayList<CommonTree>();
    for (int i = 0; i < orderByElements.getChildCount(); i++) {
      CommonTree orderByElement = (CommonTree) orderByElements.getChild(i);
      CommonTree function = FilterBlockUtil.findOnlyNode(orderByElement,
          PantheraExpParser.STANDARD_FUNCTION);
      if (function != null) {
        functionNodes.add(function);
      }
    }
    if (!functionNodes.isEmpty()) {
      processFunction(functionNodes, node, context);
    }
  }

  private void processFunction(List<CommonTree> functionNodes, CommonTree node,
      TranslateContext context) {
    CommonTree selectStatement = (CommonTree) node.getParent();
    CommonTree statements = (CommonTree) selectStatement.getParent();
    statements.deleteChild(selectStatement.childIndex);
    CommonTree select = (CommonTree) selectStatement.getChild(0).getChild(0);
    List<CommonTree> aliasList = FilterBlockUtil.addColumnAlias(select, context);
    for (CommonTree function : functionNodes) {
      CommonTree expr = (CommonTree) function.getParent();
      while (expr.getType() != PantheraParser_PLSQLParser.EXPR) { // for case like "order by 10*avg(grade)"
        expr = (CommonTree) expr.getParent();
      }
      expr.deleteChild(0);
      CommonTree selectItem = FilterBlockUtil.addSelectItem((CommonTree) select
          .getFirstChildWithType(PantheraExpParser.SELECT_LIST), function);
      CommonTree alias = FilterBlockUtil.createAlias(selectItem, context);
      selectItem.addChild(alias);
      CommonTree casctedElement = FilterBlockUtil.createCascatedElement((CommonTree) alias
          .getChild(0));
      expr.addChild(casctedElement);
    }
    CommonTree closingSelectList = FilterBlockUtil.createSelectList(
        (CommonTree) select.getFirstChildWithType(PantheraExpParser.SELECT_LIST),aliasList);
    CommonTree topSelectStatement = createSelectStatement(closingSelectList, context,
        selectStatement);
    statements.addChild(topSelectStatement);
  }

  private CommonTree createSelectStatement(CommonTree selectList, TranslateContext context,
      CommonTree selectStatement) {
    CommonTree topSelectStatement = FilterBlockUtil.dupNode(selectStatement);
    CommonTree subquery = FilterBlockUtil.createSqlASTNode(
        (CommonTree) selectStatement.getChild(0), PantheraExpParser.SUBQUERY, "SUBQUERY");
    topSelectStatement.addChild(subquery);
    CommonTree select = FilterBlockUtil.createSqlASTNode(
        (CommonTree) selectStatement.getChild(0).getChild(0), PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT, "select");
    subquery.addChild(select);
    CommonTree from = FilterBlockUtil.createSqlASTNode(
        (CommonTree) selectStatement.getChild(0).getChild(0).getChild(0), PantheraParser_PLSQLParser.SQL92_RESERVED_FROM, "from");
    select.addChild(from);
    select.addChild(selectList);
    CommonTree tableRef = FilterBlockUtil.createSqlASTNode(from, PantheraParser_PLSQLParser.TABLE_REF,
        "TABLE_REF");
    from.addChild(tableRef);
    CommonTree tableRefElement = FilterBlockUtil.createSqlASTNode(
        tableRef, PantheraParser_PLSQLParser.TABLE_REF_ELEMENT, "TABLE_REF_ELEMENT");
    tableRef.addChild(tableRefElement);
    CommonTree viewAlias = FilterBlockUtil.createAlias(tableRefElement, context);
    tableRefElement.addChild(viewAlias);
    CommonTree tableExpression = FilterBlockUtil.createSqlASTNode(
        tableRefElement, PantheraParser_PLSQLParser.TABLE_EXPRESSION, "TABLE_EXPRESSION");
    tableRefElement.addChild(tableExpression);
    CommonTree selectMode = FilterBlockUtil.createSqlASTNode(
        tableExpression, PantheraParser_PLSQLParser.SELECT_MODE, "SELECT_MODE");
    tableExpression.addChild(selectMode);
    selectMode.addChild(selectStatement);
    return topSelectStatement;
  }
}
