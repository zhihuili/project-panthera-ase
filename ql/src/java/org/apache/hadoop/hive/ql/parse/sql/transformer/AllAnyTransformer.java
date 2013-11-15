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
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockUtil;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * eliminate all() and any() from original query
 * AllAnyTransformer.
 *
 */
public class AllAnyTransformer extends BaseSqlASTTransformer {
  SqlASTTransformer tf;

  public AllAnyTransformer(SqlASTTransformer tf) {
    this.tf = tf;
  }

  @Override
  protected void transform(CommonTree tree, TranslateContext context) throws SqlXlateException {
    tf.transformAST(tree, context);
    trans(tree, context);
  }

  void trans(CommonTree tree, TranslateContext context) throws SqlXlateException {
    // deep firstly
    for (int i = 0; i < tree.getChildCount(); i++) {
      trans((CommonTree) (tree.getChild(i)), context);
    }
    if (tree.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_ALL
        || tree.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_ANY
        || tree.getType() == PantheraParser_PLSQLParser.SOME_VK) {
      if (tree.getParent().getType() != PantheraParser_PLSQLParser.GREATER_THAN_OP && tree.getParent().getType() != PantheraParser_PLSQLParser.GREATER_THAN_OR_EQUALS_OP
          && tree.getParent().getType() != PantheraParser_PLSQLParser.LESS_THAN_OP && tree.getParent().getType() != PantheraParser_PLSQLParser.LESS_THAN_OR_EQUALS_OP
          && tree.getParent().getType() != PantheraParser_PLSQLParser.NOT_EQUAL_OP && tree.getParent().getType() != PantheraParser_PLSQLParser.EQUALS_OP ) {
        return ;
      } else if (tree.getChildIndex() == 0) {
        throw new SqlXlateException(tree, "All/Any/Some sub-query can only show at right operand of a compare operator!");
      } else {
        transAllAny((CommonTree) tree.getParent(), context);
      }
    }
  }

  void transAllAny(CommonTree tree, TranslateContext context) throws SqlXlateException {
    int scopeType = tree.getChild(1).getType();
    int compareType = tree.getType();
    if (scopeType == PantheraParser_PLSQLParser.SQL92_RESERVED_ALL
        && (compareType == PantheraParser_PLSQLParser.GREATER_THAN_OP || compareType == PantheraParser_PLSQLParser.GREATER_THAN_OR_EQUALS_OP)) {// >all
      transForAll(tree, "max", context);
    } else if (scopeType == PantheraParser_PLSQLParser.SQL92_RESERVED_ALL
        && (compareType == PantheraParser_PLSQLParser.LESS_THAN_OP || compareType == PantheraParser_PLSQLParser.LESS_THAN_OR_EQUALS_OP)) {
      transForAll(tree, "min", context);
    } else if (scopeType == PantheraParser_PLSQLParser.SQL92_RESERVED_ALL
        && (compareType == PantheraParser_PLSQLParser.NOT_EQUAL_OP)) {
      transToIn(tree, PantheraParser_PLSQLParser.NOT_IN, "NOT_IN");
    } else if (scopeType == PantheraParser_PLSQLParser.SQL92_RESERVED_ALL
        && (compareType == PantheraParser_PLSQLParser.EQUALS_OP)) {
      transEqualAll(tree, context);
    } else if ((scopeType == PantheraParser_PLSQLParser.SOME_VK || scopeType == PantheraParser_PLSQLParser.SQL92_RESERVED_ANY)
        && (compareType == PantheraParser_PLSQLParser.GREATER_THAN_OP || compareType == PantheraParser_PLSQLParser.GREATER_THAN_OR_EQUALS_OP)) {
      transForAny(tree, "min", context);
    } else if ((scopeType == PantheraParser_PLSQLParser.SOME_VK || scopeType == PantheraParser_PLSQLParser.SQL92_RESERVED_ANY)
        && (compareType == PantheraParser_PLSQLParser.LESS_THAN_OP || compareType == PantheraParser_PLSQLParser.LESS_THAN_OR_EQUALS_OP)) {
      transForAny(tree, "max", context);
    } else if ((scopeType == PantheraParser_PLSQLParser.SOME_VK || scopeType == PantheraParser_PLSQLParser.SQL92_RESERVED_ANY)
        && (compareType == PantheraParser_PLSQLParser.EQUALS_OP)) {
      transToIn(tree, PantheraParser_PLSQLParser.SQL92_RESERVED_IN, "in");
    } else if ((scopeType == PantheraParser_PLSQLParser.SOME_VK || scopeType == PantheraParser_PLSQLParser.SQL92_RESERVED_ANY)
        && (compareType == PantheraParser_PLSQLParser.NOT_EQUAL_OP)) {
      transNotEqualAny(tree, context);
    }
  }

  private void transNotEqualAny(CommonTree tree, TranslateContext context) throws SqlXlateException {
    CommonTree noteq1 = FilterBlockUtil.cloneTree(tree);
    CommonTree noteq2 = FilterBlockUtil.cloneTree(tree);
    transForAny(noteq1, "max", context);
    transForAny(noteq2, "min", context);
    CommonTree t = new CommonTree();
    t.addChild(noteq1);
    t.addChild(noteq2);
    tree.replaceChildren(0, tree.getChildCount()-1, t);
    tree.token.setText("or");
    tree.token.setType(PantheraParser_PLSQLParser.SQL92_RESERVED_OR);
  }

  private void transEqualAll(CommonTree tree, TranslateContext context) throws SqlXlateException {
    CommonTree noteq1 = FilterBlockUtil.cloneTree(tree);
    CommonTree noteq2 = FilterBlockUtil.cloneTree(tree);
    transForAll(noteq1, "max", context);
    transForAll(noteq2, "min", context);
    CommonTree t = new CommonTree();
    t.addChild(noteq1);
    t.addChild(noteq2);
    tree.replaceChildren(0, tree.getChildCount()-1, t);
    tree.token.setText("and");
    tree.token.setType(PantheraParser_PLSQLParser.SQL92_RESERVED_AND);
  }

  private void transForAll(CommonTree tree, String aggregationName, TranslateContext context) throws SqlXlateException {
    CommonTree all = (CommonTree) tree.getChild(1);
    if(all.getChildCount()>1){
      throw new SqlXlateException(all, "not support multi-query in all, try to convert it to single query with subquery.");
    }
    CommonTree subQuery = (CommonTree) all.getChild(0);
    //FIXME add a level will separate correlated filters to separated QueryInfo.
    CommonTree select = addALevel(tree, context);

    //select = (CommonTree) subQuery.getChild(0);
    CommonTree selectItem = (CommonTree) select.getFirstChildWithType(
        PantheraParser_PLSQLParser.SELECT_LIST).getChild(0);//selectItem must be one and only.
    CommonTree expr = (CommonTree) selectItem.getChild(0);

    CommonTree countExpr = FilterBlockUtil.cloneTree(expr);
    CommonTree function = FilterBlockUtil.createFunction(all, aggregationName, (CommonTree) expr
        .deleteChild(0));
    CommonTree tokcase = FilterBlockUtil.createSqlASTNode(all, PantheraParser_PLSQLParser.SEARCHED_CASE, "case");
    expr.addChild(tokcase);
    CommonTree tokwhen = FilterBlockUtil.createSqlASTNode(all, PantheraParser_PLSQLParser.SQL92_RESERVED_WHEN,
        "when");
    tokcase.addChild(tokwhen);
    CommonTree logic = FilterBlockUtil.createSqlASTNode(all, PantheraParser_PLSQLParser.LOGIC_EXPR, "LOGIC_EXPR");
    tokwhen.addChild(logic);
    CommonTree thenExpr = FilterBlockUtil.createSqlASTNode(all, PantheraParser_PLSQLParser.EXPR, "EXPR");
    tokwhen.addChild(thenExpr);
    thenExpr.addChild(function);
    CommonTree equ = FilterBlockUtil.createSqlASTNode(all, PantheraParser_PLSQLParser.EQUALS_OP, "=");
    logic.addChild(equ);
    CommonTree lequ = FilterBlockUtil.createSqlASTNode(all, PantheraParser_PLSQLParser.STANDARD_FUNCTION,
        "STANDARD_FUNCTION");
    equ.addChild(lequ);
    CommonTree requ = FilterBlockUtil.createSqlASTNode(all, PantheraParser_PLSQLParser.STANDARD_FUNCTION,
        "STANDARD_FUNCTION");
    equ.addChild(requ);
    CommonTree lf = FilterBlockUtil.createSqlASTNode(all, PantheraParser_PLSQLParser.COUNT_VK,
        "count");//OR FUNCTION_ENABLING_OVER, "count"
    lequ.addChild(lf);
    CommonTree rf = FilterBlockUtil.createSqlASTNode(all, PantheraParser_PLSQLParser.COUNT_VK,
        "count");//OR FUNCTION_ENABLING_OVER, "count"
    requ.addChild(rf);
    lf.addChild(countExpr);
    CommonTree asterisk = FilterBlockUtil.createSqlASTNode(all, PantheraParser_PLSQLParser.ASTERISK, "*");
    rf.addChild(asterisk);

    // deletes scope node
    tree.deleteChild(1);
    tree.addChild(subQuery);

    // FIXME add condition if all sub query gets empty set, should always be true.
  }

  private static void transForAny(CommonTree tree, String aggregationName, TranslateContext context) throws SqlXlateException {
    CommonTree any = (CommonTree) tree.getChild(1);
    if(any.getChildCount()>1){
      //FIXME what if any/some has more than 1 subQuery node?
      throw new SqlXlateException(any, "we do not support multi-query in any, try to convert it to single query with subquery.");
    }
    CommonTree subQuery = (CommonTree) any.getChild(0);
    CommonTree select = addALevel(tree, context);

    //select = (CommonTree) subQuery.getChild(0);
    CommonTree selectItem = (CommonTree) select.getFirstChildWithType(
        PantheraParser_PLSQLParser.SELECT_LIST).getChild(0);//selectItem must be one and only.
    CommonTree expr = (CommonTree) selectItem.getChild(0);

    CommonTree function = FilterBlockUtil.createFunction((CommonTree) tree.getChild(1),
        aggregationName, (CommonTree) expr.deleteChild(0));
    expr.addChild(function);
    // deletes scope node
    tree.deleteChild(1);
    tree.addChild(subQuery);
  }

  private static CommonTree addALevel(CommonTree tree, TranslateContext context) throws SqlXlateException {
    CommonTree subQuery = (CommonTree) tree.getChild(1).getChild(0);
    CommonTree select = (CommonTree) subQuery.getChild(0);

    List<CommonTree> aliases = FilterBlockUtil.addColumnAlias(select, context);
    CommonTree trueSelect = FilterBlockUtil.dupNode(select);
    subQuery.deleteChild(0);
    subQuery.addChild(trueSelect);
    if(aliases.size() != 1) {
      throw new SqlXlateException(select, "Subquery for all/any/some have more than one element in select-list!");
    }
    CommonTree trueAlias = FilterBlockUtil.cloneTree((CommonTree) aliases.get(0).getChild(0));
    CommonTree from = FilterBlockUtil.createSqlASTNode((CommonTree) select.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM), PantheraParser_PLSQLParser.SQL92_RESERVED_FROM, "from");
    trueSelect.addChild(from);
    CommonTree selectList = FilterBlockUtil.createSqlASTNode(select, PantheraParser_PLSQLParser.SELECT_LIST, "SELECT_LIST");
    trueSelect.addChild(selectList);
    CommonTree selectItem = FilterBlockUtil.createSqlASTNode(select, PantheraParser_PLSQLParser.SELECT_ITEM, "SELECT_ITEM");
    selectList.addChild(selectItem);
    CommonTree expr = FilterBlockUtil.createSqlASTNode(select, PantheraParser_PLSQLParser.EXPR, "EXPR");
    selectItem.addChild(expr);
    CommonTree cascatedElement = FilterBlockUtil.createSqlASTNode(select, PantheraParser_PLSQLParser.CASCATED_ELEMENT, "CASCATED_ELEMENT");
    expr.addChild(cascatedElement);
    CommonTree anyElement = FilterBlockUtil.createSqlASTNode(select, PantheraParser_PLSQLParser.ANY_ELEMENT, "ANY_ELEMENT");
    cascatedElement.addChild(anyElement);
    CommonTree tableRef = FilterBlockUtil.createSqlASTNode(select, PantheraParser_PLSQLParser.TABLE_REF, "TABLE_REF");
    from.addChild(tableRef);
    CommonTree tableRefElement = FilterBlockUtil.createSqlASTNode(select, PantheraParser_PLSQLParser.TABLE_REF_ELEMENT, "TABLE_REF_ELEMENT");
    tableRef.addChild(tableRefElement);
    String tableAliasText = FilterBlockUtil.addTableAlias(tableRefElement, context);
    CommonTree tableAlias = FilterBlockUtil.createSqlASTNode(select, PantheraParser_PLSQLParser.ID, tableAliasText);
    anyElement.addChild(tableAlias);
    anyElement.addChild(trueAlias);
    CommonTree tableExpression = FilterBlockUtil.createSqlASTNode(select, PantheraParser_PLSQLParser.TABLE_EXPRESSION, "TABLE_EXPRESSION");
    tableRefElement.addChild(tableExpression);
    CommonTree selectMode = FilterBlockUtil.createSqlASTNode(select, PantheraParser_PLSQLParser.SELECT_MODE, "SELECT_MODE");
    tableExpression.addChild(selectMode);
    CommonTree selectStatement = FilterBlockUtil.createSqlASTNode(select, PantheraParser_PLSQLParser.SELECT_STATEMENT, "SELECT_STATEMENT");
    selectMode.addChild(selectStatement);
    CommonTree trueSubQuery = FilterBlockUtil.createSqlASTNode(select, PantheraParser_PLSQLParser.SUBQUERY, "SUBQUERY");
    selectStatement.addChild(trueSubQuery);
    trueSubQuery.addChild(select);

    return trueSelect;
  }

  private static void transToIn(CommonTree tree, int type, String text) {
    tree.getToken().setType(type);
    tree.getToken().setText(text);
    CommonTree subQuery = (CommonTree) tree.getChild(1).getChild(0);
    tree.deleteChild(1);
    tree.addChild(subQuery);
  }


}
