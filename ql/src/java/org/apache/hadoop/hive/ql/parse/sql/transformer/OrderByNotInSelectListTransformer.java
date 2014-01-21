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
 * process columns which are not in select list
 *
 * orderByNotInSelectListTransformer.
 *
 */
public class OrderByNotInSelectListTransformer extends BaseSqlASTTransformer {
  SqlASTTransformer tf;

  public OrderByNotInSelectListTransformer(SqlASTTransformer tf) {
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
      processOrderByNotInSelectList(node, context);
    }
  }

  private void processOrderByNotInSelectList(CommonTree order, TranslateContext context)
      throws SqlXlateException {

    CommonTree selectStatement = (CommonTree) order.getParent();
    // order by branch may not under SELECT_STATEMENT node, order by might be in over clause.
    // e.g. over(order by col1).
    if (selectStatement.getType() != PantheraParser_PLSQLParser.SELECT_STATEMENT) {
      return;
    }
    CommonTree select = (CommonTree) ((CommonTree) selectStatement
        .getFirstChildWithType(PantheraParser_PLSQLParser.SUBQUERY))
        .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT);
    CommonTree selectList = (CommonTree) select
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    // check whether there is * or table.* in select-list, do nothing in this transformer if there
    // is.
    if (selectList == null) {// *
      // must include column in order by clause
      // would not do anything if there is * in select-list
      return;
    } else {
      for (int i = 0; i < selectList.getChildCount(); i++) {
        CommonTree expr = (CommonTree) selectList.getChild(i).getChild(0);
        if (expr.getType() == PantheraParser_PLSQLParser.EXPR
            && expr.getChild(0).getType() == PantheraParser_PLSQLParser.DOT_ASTERISK
            && expr.getChild(0).getChild(0).getType() == PantheraParser_PLSQLParser.TABLEVIEW_NAME) {
          // FIXME
          // would not do anything if there is table.* in select-list because this will introduce
          // alias problem
          // e.g. query like "select staff.* from staff join works order by w_empnum" will
          // transformed to
          // "select aa from (select staff.* as aa, w_empnum as bb from staff join works order by bb) t1"
          return;
        }
      }
    }

    CommonTree orderByElements = (CommonTree) order.getChild(0);

    // check whether there is order by column not in select-list
    if (!needInsertSelectList(orderByElements, selectList, context)) {
      // return, no need to transform
      return;
    }

    // need insert column into select-list
    // create column alias in select-list firstly
    int selectListCount = selectList.getChildCount();
    List<CommonTree> aliasList = new ArrayList<CommonTree>();
    for (int i = 0; i < selectList.getChildCount(); i++) {
      CommonTree selectItem = (CommonTree) selectList.getChild(i);
      if (selectItem.getChildCount() == 1) {
        if (selectItem.getChild(0).getType() == PantheraParser_PLSQLParser.EXPR
            && selectItem.getChild(0).getChild(0).getType() == PantheraParser_PLSQLParser.CASCATED_ELEMENT
            && selectItem.getChild(0).getChild(0).getChild(0).getType() == PantheraParser_PLSQLParser.ANY_ELEMENT
            && selectItem.getChild(0).getChild(0).getChild(0).getChildCount() == 1) {
          // if selectItem is only a column name, then alias name should be the same as this column
          CommonTree alias = FilterBlockUtil.createSqlASTNode(selectList, PantheraParser_PLSQLParser.ALIAS, "ALIAS");
          CommonTree aliasName = FilterBlockUtil.createSqlASTNode(alias, PantheraParser_PLSQLParser.ID, selectItem.getChild(0).getChild(0).getChild(0).getChild(0).getText());
          alias.addChild(aliasName);
          selectItem.addChild(alias);
        } else {
          CommonTree alias = FilterBlockUtil.createAlias(selectList, context);
          selectItem.addChild(alias);
        }
      }
      aliasList.add((CommonTree) selectItem.getChild(1));
    }

    for (int i = 0; i < orderByElements.getChildCount(); i++) {
      CommonTree expr = (CommonTree) orderByElements.getChild(i).getChild(0);
      boolean hasSameItem = false;
      for (int j = 0; j < selectList.getChildCount(); j++) {
        if (expr.getChild(0).getType() == PantheraParser_PLSQLParser.CASCATED_ELEMENT
            && expr.getChild(0).getChild(0).getType() == PantheraParser_PLSQLParser.ANY_ELEMENT
            && expr.getChild(0).getChild(0).getChildCount() == 1
            && (CommonTree) selectList.getChild(j).getChild(1) != null
            && selectList.getChild(j).getChild(1).getChild(0).getText()
                .equals(expr.getChild(0).getChild(0).getChild(0).getText())) {
          hasSameItem = true;
          break;
        } else if (FilterBlockUtil.equalsTree((CommonTree) selectList.getChild(j).getChild(0), expr)) {
          expr.deleteChild(0);
          CommonTree columnNode = FilterBlockUtil.createSqlASTNode(expr, PantheraExpParser.ID,
              selectList.getChild(j).getChild(1).getChild(0).getText());
          expr.addChild(FilterBlockUtil.createCascatedElement(columnNode));
          hasSameItem = true;
          break;
        }
      }
      if (hasSameItem) {
        continue;
      }

      CommonTree ct = (CommonTree) orderByElements.getChild(i).getChild(0).getChild(0);
      if (ct.getType() == PantheraParser_PLSQLParser.UNSIGNED_INTEGER) {
        int seq = Integer.valueOf(ct.getText()) - 1;
        if (seq < 0 || seq > selectListCount - 1) {
          throw new SqlXlateException(ct, "Invalid column number in order by clause.");
        }
        // by-Pass the numeric after order by.
        continue;
      }
      // for colname and alias and tablename.colname
      else {
        // FIXME only handle simplest condition: CASCATED_ELEMENT->ANY_ELEMENT->columnName(alias)
        // doesn't handle function or expression or anything else
        if (ct.getType() != PantheraParser_PLSQLParser.CASCATED_ELEMENT
            || ct.getChild(0).getType() != PantheraParser_PLSQLParser.ANY_ELEMENT) {
          // insert into select-list
          expr.deleteChild(0);
          CommonTree selectItem = FilterBlockUtil.addSelectItem((CommonTree) select
              .getFirstChildWithType(PantheraExpParser.SELECT_LIST), ct);
          CommonTree columnAlias = FilterBlockUtil.createAlias(selectList, context);
          selectItem.addChild(columnAlias);
          CommonTree columnNode = FilterBlockUtil.createSqlASTNode(expr, PantheraExpParser.ID,
              columnAlias.getChild(0).getText());
          expr.addChild(FilterBlockUtil.createCascatedElement(columnNode));
          continue;
        }
        if (ct.getChild(0).getChildCount() > 1) { // tablename.colname
          // for tablename.colname, ignore it
          // TODO need to handle tablename.colname
          // HIVE doesn't support tablename.colname in order by while there is only colname in
          // select-list
          // e.g. select s_grade from staff order by staff.s_grade
          // we can insert tablename.colname into select-list here, but may introduce other bugs :
          // "select s_grade, min(s_city) from staff group by s_grade order by staff.s_grade".
          continue;
        } else {
          CommonTree id = (CommonTree) ct.getChild(0).getChild(0);
          String aliasStr = (String) context.getBallFromBasket(id.getText());
          if (aliasStr != null) {
            id.getToken().setText(aliasStr);
            continue;
          }

          expr.deleteChild(0);
          CommonTree selectItem = FilterBlockUtil.addSelectItem((CommonTree) select
              .getFirstChildWithType(PantheraExpParser.SELECT_LIST), ct);
          CommonTree columnAlias = FilterBlockUtil.createAlias(selectList, context);
          selectItem.addChild(columnAlias);
          CommonTree columnNode = FilterBlockUtil.createSqlASTNode(expr, PantheraExpParser.ID,
              columnAlias.getChild(0).getText());
          expr.addChild(FilterBlockUtil.createCascatedElement(columnNode));
          continue;

        }
      }
    }

    CommonTree statementParent = (CommonTree) selectStatement.getParent();
    statementParent.deleteChild(selectStatement.childIndex);

    CommonTree closingSelectList = FilterBlockUtil.createSelectList(
        (CommonTree) select.getFirstChildWithType(PantheraExpParser.SELECT_LIST), aliasList);
    CommonTree topSelectStatement = createSelectStatement(closingSelectList, context,
        selectStatement);
    statementParent.addChild(topSelectStatement);

  }

  private boolean needInsertSelectList(CommonTree orderByElements, CommonTree selectList,
      TranslateContext context) {
    for (int i = 0; i < orderByElements.getChildCount(); i++) {
      CommonTree expr = (CommonTree) orderByElements.getChild(i).getChild(0);
      boolean hasSameItem = false;
      for (int j = 0; j < selectList.getChildCount(); j++) {
        if (expr.getChild(0).getType() == PantheraParser_PLSQLParser.CASCATED_ELEMENT
            && expr.getChild(0).getChild(0).getType() == PantheraParser_PLSQLParser.ANY_ELEMENT
            && expr.getChild(0).getChild(0).getChildCount() == 1
            && (CommonTree) selectList.getChild(j).getChild(1) != null
            && selectList.getChild(j).getChild(1).getChild(0).getText()
                .equals(expr.getChild(0).getChild(0).getChild(0).getText())) {
          hasSameItem = true;
          break;
        } else if (FilterBlockUtil.equalsTree((CommonTree) selectList.getChild(j).getChild(0), expr)) {
          hasSameItem = true;
          break;
        }
      }
      if (hasSameItem) {
        continue;
      }

      CommonTree ct = (CommonTree) orderByElements.getChild(i).getChild(0).getChild(0);
      if (ct.getType() == PantheraParser_PLSQLParser.UNSIGNED_INTEGER) {
        // by-Pass the numeric after order by.
        continue;
      }
      // for colname and alias and tablename.colname
      else {
        if (ct.getType() != PantheraParser_PLSQLParser.CASCATED_ELEMENT
            || ct.getChild(0).getType() != PantheraParser_PLSQLParser.ANY_ELEMENT) {
          return true;
        }
        if (ct.getChild(0).getChildCount() > 1) { // tablename.colname
          // for tablename.colname, ignore it
          // TODO need to handle tablename.colname
          continue;
        } else {
          CommonTree id = (CommonTree) ct.getChild(0).getChild(0);
          String aliasStr = (String) context.getBallFromBasket(id.getText());
          if (aliasStr != null) {
            id.getToken().setText(aliasStr);
            continue;
          }
          return true;
        }
      }
    }
    return false;
  }

  private CommonTree createSelectStatement(CommonTree selectList, TranslateContext context,
      CommonTree selectStatement) {
    CommonTree topSelectStatement = FilterBlockUtil.dupNode(selectStatement);
    CommonTree subquery = FilterBlockUtil.createSqlASTNode(
        (CommonTree) selectStatement.getChild(0), PantheraExpParser.SUBQUERY, "SUBQUERY");
    topSelectStatement.addChild(subquery);
    CommonTree select = FilterBlockUtil.createSqlASTNode(
        (CommonTree) selectStatement.getChild(0).getChild(0),
        PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT, "select");
    subquery.addChild(select);
    CommonTree from = FilterBlockUtil.createSqlASTNode(
        (CommonTree) selectStatement.getChild(0).getChild(0).getChild(0),
        PantheraParser_PLSQLParser.SQL92_RESERVED_FROM, "from");
    select.addChild(from);
    select.addChild(selectList);
    CommonTree tableRef = FilterBlockUtil.createSqlASTNode(from,
        PantheraParser_PLSQLParser.TABLE_REF,
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
