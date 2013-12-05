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
package org.apache.hadoop.hive.ql.parse.sql.transformer.fb.processor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.PantheraExpParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.QueryInfo.Column;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlock;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockUtil;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.PLSQLFilterBlockFactory;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.QueryBlock;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.SubQFilterBlock;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;


public abstract class BaseFilterBlockProcessor implements FilterBlockProcessor {

  QueryBlock bottomQuery;
  QueryBlock topQuery;
  SubQFilterBlock subQ;

  // original node reference
  CommonTree originalTopSelect;
  CommonTree originalBottomSelect;

  // clone node reference
  CommonTree topSelect;
  CommonTree bottomSelect;

  CommonTree subQNode;
  FilterBlockContext fbContext;
  FilterBlock fb;// current normalFilterBlock
  TranslateContext context;
  Map<String, String> tableAliasMap = new HashMap<String, String>();// <tableName,tableAlias>
  // <tableAlias,<columnNam,columnAliase>> TODO how to do duplicated columnName?
  Map<String, Map<String, String>> columnAliasMap = new HashMap<String, Map<String, String>>();
  Boolean hasNotEqualCorrelated = false;
  CommonTree joinTypeNode;
  List<CommonTree> whereFilterColumns;
  List<CommonTree> havingFilterColumns;

  /**
   * template method
   */
  @Override
  public void process(FilterBlockContext fbContext, FilterBlock fb, TranslateContext context)
      throws SqlXlateException {
    bottomQuery = fbContext.getQueryStack().pop();
    bottomQuery.setAggregationList(null);// TODO naive
    topQuery = fbContext.getQueryStack().peek();
    originalBottomSelect = bottomQuery.getASTNode();
    if (topQuery.isProcessHaving()) {
      originalTopSelect = topQuery.getQueryForHaving();
    } else {
      originalTopSelect = topQuery.getASTNode();
    }
    whereFilterColumns = topQuery.getWhereFilterColumns();
    havingFilterColumns = topQuery.getHavingFilterColumns();
    fbContext.getQueryStack().push(bottomQuery);
    subQ = fbContext.getSubQStack().peek();
    subQ.setTransformed();
    if (topQuery.isProcessHaving()) {
      topSelect = topQuery.cloneTransformedQuery();
    } else {
      topSelect = topQuery.cloneSimpleQuery();
    }
    if (topSelect.getType() == PantheraParser_PLSQLParser.SUBQUERY) {
      assert(topSelect.getChildCount() == 1);
      topSelect = (CommonTree) topSelect.getChild(0);
    }
    bottomSelect = bottomQuery.cloneWholeQueryWithoutHaving();
    if (bottomSelect.getType() == PantheraParser_PLSQLParser.SUBQUERY) {
      if (bottomSelect.getChildCount() == 1) {
        bottomSelect = (CommonTree) bottomSelect.getChild(0);
      } else {
        if (!fbContext.getQueryStack().peek().getWhereCFlag() && !fbContext.getQueryStack().peek().getHavingCFlag()) {
          bottomSelect = addLevelForUnion(bottomSelect, context);
        } else {
          // change to UNION in upper level
          throw new SqlXlateException(bottomSelect, "currently not implemented");
        }
      }
    }
    subQNode = subQ.getASTNode();
    this.fbContext = fbContext;
    this.fb = fb;
    this.context = context;
    preProcessAsterisk();
    CommonTree topSelectList = (CommonTree) topSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    CommonTree reservedSelectList = FilterBlockUtil.cloneTree((CommonTree) topSelectList);
    processFB();
    this.rebuildColumnAlias(reservedSelectList, topSelect);
    fb.setTransformedNode(topSelect);
  }

  abstract void processFB() throws SqlXlateException;

  /**
   * add column for top query which is select count(*) or
   *
   * @throws SqlXlateException
   *           *
   */
  void preProcessAsterisk() throws SqlXlateException {
    CommonTree selectList = (CommonTree) topSelect
        .getFirstChildWithType(PantheraExpParser.SELECT_LIST);
    if (selectList != null && selectList.getChildCount() > 0) {
      return;
    }
    List<Column> columnList = fbContext.getqInfo()
        .getRowInfo(
            (CommonTree) originalTopSelect
                .getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_FROM));
    if (columnList != null && columnList.size() > 0) {
      if (selectList == null) {
        if (topSelect.getFirstChildWithType(PantheraExpParser.ASTERISK) != null) {
          // which is *
          CommonTree asterisk = (CommonTree) topSelect.deleteChild(topSelect.getFirstChildWithType(
              PantheraExpParser.ASTERISK).getChildIndex());
          selectList = FilterBlockUtil.createSqlASTNode(asterisk, PantheraExpParser.SELECT_LIST,
              "SELECT_LIST");
          topSelect.addChild(selectList);
        } else {
          throw new SqlXlateException(topSelect, "No select-list nor asterisk in select statement");
        }
      }
      int count = 0;
      for (Column column : columnList) {
        CommonTree selectItem = FilterBlockUtil.createSqlASTNode(selectList,
            PantheraExpParser.SELECT_ITEM,
            "SELECT_ITEM");
        selectList.addChild(selectItem);
        CommonTree expr = FilterBlockUtil.createSqlASTNode(selectList, PantheraExpParser.EXPR,
            "EXPR");
        selectItem.addChild(expr);
        // add alias to avoid duplicate name in multi-table when expanding *
        CommonTree alias = FilterBlockUtil.createSqlASTNode(
            selectList, PantheraExpParser.ALIAS, "ALIAS");
        selectItem.addChild(alias);
        CommonTree aliasId = FilterBlockUtil.createSqlASTNode(selectList, PantheraExpParser.ID,
            "panthera_col_" + count++);
        alias.addChild(aliasId);
        CommonTree cascatedElement = FilterBlockUtil.createSqlASTNode(
            selectList, PantheraExpParser.CASCATED_ELEMENT, "CASCATED_ELEMENT");
        expr.addChild(cascatedElement);
        CommonTree anyElement = FilterBlockUtil.createSqlASTNode(selectList,
            PantheraExpParser.ANY_ELEMENT,
            "ANY_ELEMENT");
        cascatedElement.addChild(anyElement);
        CommonTree tableName = FilterBlockUtil.createSqlASTNode(selectList, PantheraExpParser.ID,
            column.getTblAlias());
        anyElement.addChild(tableName);
        CommonTree columnName = FilterBlockUtil.createSqlASTNode(selectList, PantheraExpParser.ID,
            column.getColAlias());
        anyElement.addChild(columnName);
        // refresh group by
        CommonTree group = topQuery.getGroup();
        if (group != null) {
          int groupCount = group.getChildCount();
          if (group.getChild(groupCount - 1).getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_HAVING) {
            groupCount -= 1;
          }
          refreshGroupOrderForAsterisk(group, groupCount, aliasId, tableName, columnName);
        }

        // refresh order by
        CommonTree order = topQuery.getOrder();
        if (order != null) {
          int orderCount = order.getChildCount();
          refreshGroupOrderForAsterisk(order, orderCount, aliasId, tableName, columnName);
        }
      }
    }
  }

  private void refreshGroupOrderForAsterisk(CommonTree refresh,
      int count, CommonTree aliasId, CommonTree tableName, CommonTree columnName) {
    for (int i = 0; i < count; i++) {
      CommonTree refreshElement = (CommonTree) refresh.getChild(i);
      List<CommonTree> cascated = new ArrayList<CommonTree>();
      FilterBlockUtil.findNode(refreshElement, PantheraParser_PLSQLParser.CASCATED_ELEMENT,
          cascated);
      for (CommonTree cascatedElement : cascated) {
        CommonTree refreshAny = (CommonTree) cascatedElement.getChild(0);
        CommonTree id = FilterBlockUtil.cloneTree(aliasId);
        id.getToken().setCharPositionInLine(refreshAny.getCharPositionInLine());
        id.setTokenStartIndex(refreshAny.getTokenStartIndex());
        id.setTokenStopIndex(refreshAny.getTokenStopIndex());
        if (refreshAny.getChildCount() == 2) {
          if (refreshAny.getChild(1).getText().equals(columnName.getText())
              && refreshAny.getChild(0).getText().equals(tableName.getText())) {
            refreshAny.replaceChildren(0, 1, id);
          }
        } else {
          if (refreshAny.getChild(0).getText().equals(columnName.getText())) {
            refreshAny.replaceChildren(0, 0, id);
          }
        }
      }
    }
    return;
  }

  /**
   * Create TABLE_REF_ELEMENT & attach select node to it.
   *
   * @param select
   * @return alias
   */
  CommonTree createTableRefElement(CommonTree select) {

    CommonTree tableRefElement = FilterBlockUtil.createSqlASTNode(
        select, PantheraParser_PLSQLParser.TABLE_REF_ELEMENT, "TABLE_REF_ELEMENT");
    CommonTree viewAlias = FilterBlockUtil.createAlias(tableRefElement, context);
    this.buildTableAliasMap(viewAlias.getChild(0).getText(), FilterBlockUtil.getTableName(select));
    tableRefElement.addChild(viewAlias);
    CommonTree tableExpression = FilterBlockUtil.createSqlASTNode(
        select, PantheraParser_PLSQLParser.TABLE_EXPRESSION, "TABLE_EXPRESSION");
    tableRefElement.addChild(tableExpression);
    CommonTree selectMode = FilterBlockUtil.createSqlASTNode(
        select, PantheraParser_PLSQLParser.SELECT_MODE, "SELECT_MODE");
    tableExpression.addChild(selectMode);
    CommonTree selectStatement = FilterBlockUtil.createSqlASTNode(
        select, PantheraParser_PLSQLParser.SELECT_STATEMENT, "SELECT_STATEMENT");
    selectMode.addChild(selectStatement);
    CommonTree subQuery = FilterBlockUtil.createSqlASTNode(select,
        PantheraParser_PLSQLParser.SUBQUERY,
        "SUBQUERY");
    selectStatement.addChild(subQuery);
    subQuery.addChild(select);
    return tableRefElement;
  }


  CommonTree createOpBranch(CommonTree viewAlias, CommonTree colAlias) {
    CommonTree subQ = FilterBlockUtil.cloneTree(subQNode);
    for (int i = 0; i < subQ.getChildCount(); i++) {
      if (subQ.getChild(i).getType() == PantheraParser_PLSQLParser.SUBQUERY) {
        subQ.setChild(i, this.createCascatedElement(subQ, (CommonTree) viewAlias.getChild(0),
            (CommonTree) colAlias.getChild(0)));
      }

    }
    return subQ;
  }

  void buildWhereBranch(CommonTree viewAlias, CommonTree colAlias) {
    CommonTree opBranch = this.createOpBranch(viewAlias, colAlias);
    CommonTree where = FilterBlockUtil.createSqlASTNode(
        opBranch, PantheraParser_PLSQLParser.SQL92_RESERVED_WHERE, "where");
    CommonTree logicExpr = FilterBlockUtil.createSqlASTNode(opBranch,
        PantheraParser_PLSQLParser.LOGIC_EXPR,
        "LOGIC_EXPR");
    where.addChild(logicExpr);
    logicExpr.addChild(opBranch);
    topSelect.addChild(where);
  }

  /**
   * create join node & attach to tree
   */
  CommonTree createJoin(CommonTree joinType, CommonTree select) {
    CommonTree from = (CommonTree) select
        .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM);
    CommonTree join = FilterBlockUtil.createSqlASTNode(joinType,
        PantheraParser_PLSQLParser.JOIN_DEF, "join");
    ((CommonTree) from.getChild(0)).addChild(join);// add to table_ref
    return join;
  }

  /**
   * build join node's children without process detail.
   *
   * @param joinTypeNode
   *          join type
   * @return join sub query alias
   */
  CommonTree buildJoin(CommonTree joinTypeNode, CommonTree join, CommonTree select) {
    if (joinTypeNode != null) {
      join.addChild(joinTypeNode);
    }
    CommonTree tableRefElement = this.createTableRefElement(select);
    join.addChild(tableRefElement);

    return (CommonTree) tableRefElement.getFirstChildWithType(PantheraParser_PLSQLParser.ALIAS);
  }

  /**
   * extract join key from filter op node. TODO optimize it.
   *
   * @return false: bottom keys<br>
   *         true: top key
   * @throws SqlXlateException
   */
  Map<Boolean, List<CommonTree>> getFilter(CommonTree filterOp) throws SqlXlateException {

    Map<Boolean, List<CommonTree>> result = new HashMap<Boolean, List<CommonTree>>();
    int minLevel = -1;
    int maxLevel = 0;
    for (int i = 0; i < filterOp.getChildCount(); i++) {
      CommonTree child = (CommonTree) filterOp.getChild(i);
      int level = PLSQLFilterBlockFactory.getInstance().isCorrelated(this.fbContext.getqInfo(), this.fbContext.getSelectStack(), child);
      if (maxLevel < level) {
        maxLevel = level;
      }
      if (level == 0) {
        if (child.getType() == PantheraParser_PLSQLParser.CASCATED_ELEMENT
            || FilterBlockUtil.findOnlyNode(child, PantheraExpParser.CASCATED_ELEMENT) != null) {
          List<CommonTree> uncorrelatedList = result.get(false);
          if (uncorrelatedList == null) {
            uncorrelatedList = new ArrayList<CommonTree>();
            result.put(false, uncorrelatedList);
          }
          // those imm is not applicable to refresh minLevel value.
          if (minLevel < 0) {
            minLevel = level;
          } else if (minLevel > level) {
            minLevel = level;
          }
          uncorrelatedList.add(child);
        }
      } else {
        if (minLevel < 0) {
          minLevel = level;
        } else if (minLevel > level) {
          minLevel = level;
        }
        List<CommonTree> correlatedList = result.get(true);
        if (correlatedList == null) {
          correlatedList = new ArrayList<CommonTree>();
          result.put(true, correlatedList);
        }
        correlatedList.add(child);

      }
      if (maxLevel > 0) {
        // if maxLevel > 0, then minLevel never be -1
        // this case is same as the upper else.
        if (maxLevel - minLevel > 1) {
          throw new SqlXlateException(filterOp, "currently not support correlated level greater than 1");
        } else if (maxLevel == minLevel) {
          // this case it is uncorrelated filter for upper level.
          // though cannot put under ON, the join is OK to be leftsemi-join
          if (minLevel > 1) {
            throw new SqlXlateException(filterOp, "currently not support correlated level greater than 1");
          }
        } else {
          // maxLevel is one more over minLevel
          // maxLevel is not equal to minLevel, means there are at least two level.
          if (filterOp.getType() != PantheraParser_PLSQLParser.EQUALS_OP && filterOp.getType() != PantheraExpParser.EQUALS_NS) {
            this.hasNotEqualCorrelated = true;
            Map<CommonTree, List<CommonTree>> joinMap = (Map<CommonTree, List<CommonTree>>) this.context
                .getBallFromBasket(TranslateContext.JOIN_TYPE_NODE_BALL);
            if (joinMap != null) {
              List<CommonTree> notEqualConditionList = joinMap.get(joinTypeNode);
              if (notEqualConditionList != null) {
                notEqualConditionList.add(filterOp);
              }
            }
          }
          if (minLevel > 0) {
            throw new SqlXlateException(filterOp, "currently not support correlated filter of upper level");
          }
        }
      }
    }

    return result;

  }

  /**
   *
   * @return
   * @throws SqlXlateException
   */
  List<Map<Boolean, List<CommonTree>>> getFilterkey() throws SqlXlateException {
    List<Map<Boolean, List<CommonTree>>> result = new ArrayList<Map<Boolean, List<CommonTree>>>();
    this.getWhereKey(fb.getASTNode(), result);
    return result;
  }

  private void getWhereKey(CommonTree filterOp, List<Map<Boolean, List<CommonTree>>> list)
      throws SqlXlateException {
    if (filterOp == null) {
      return;
    }
    if (FilterBlockUtil.isFilterOp(filterOp)) {
      list.add(getFilter(filterOp));
      return;
    } else if (FilterBlockUtil.isLogicOp(filterOp)) {
      for (int i = 0; i < filterOp.getChildCount(); i++) {
        getWhereKey((CommonTree) filterOp.getChild(i), list);
      }
    } else {
      throw new SqlXlateException(filterOp, "unknow filter operation:" + filterOp.getText());
    }

  }


  /**
   * add cascatedElement(or standardFunction...) branch to bottom SELECT_LIST node
   *
   * @param cascatedElement
   * @return alias
   */
  CommonTree addSelectItem(CommonTree selectList, CommonTree cascatedElement) {
    if (cascatedElement == null || cascatedElement.getChildren() == null) {
      return cascatedElement;
    }
    CommonTree selectItem = addSelectItemBody(selectList, cascatedElement);
    return this.addAlias(selectItem);
  }

  CommonTree addSelectItem(CommonTree selectList, CommonTree cascatedElement, String aliasName) {
    if (cascatedElement == null || cascatedElement.getChildren() == null) {
      return cascatedElement;
    }
    CommonTree selectItem = addSelectItemBody(selectList, cascatedElement);
    return this.addAlias(selectItem, aliasName);
  }

  CommonTree addSelectItemBody(CommonTree selectList, CommonTree cascatedElement) {
    if (cascatedElement.getChild(0).getChildCount() == 2
        && cascatedElement.getChild(0).getType() == PantheraParser_PLSQLParser.ANY_ELEMENT) {
      // TODO just for tpch 20.sql & 21.sql
      cascatedElement.getChild(0).deleteChild(0);
    }
    CommonTree selectItem = FilterBlockUtil.createSqlASTNode(
        cascatedElement, PantheraParser_PLSQLParser.SELECT_ITEM, "SELECT_ITEM");
    selectList.addChild(selectItem);
    CommonTree expr = FilterBlockUtil.createSqlASTNode(cascatedElement,
        PantheraParser_PLSQLParser.EXPR, "EXPR");
    selectItem.addChild(expr);
    expr.addChild(cascatedElement);
    return selectItem;
  }

  CommonTree addSelectItemClosing(CommonTree selectList, CommonTree cascatedElement) {
    if (cascatedElement == null || cascatedElement.getChildren() == null) {
      return cascatedElement;
    }
    CommonTree selectItem = FilterBlockUtil.createSqlASTNode(
        cascatedElement, PantheraParser_PLSQLParser.SELECT_ITEM, "SELECT_ITEM");
    selectList.addChild(selectItem);
    CommonTree expr = FilterBlockUtil.createSqlASTNode(cascatedElement,
        PantheraParser_PLSQLParser.EXPR, "EXPR");
    selectItem.addChild(expr);
    expr.addChild(cascatedElement);
    return this.addAlias(selectItem);
  }

  CommonTree addAlias(CommonTree node) {
    return addAliasBody(node, null);
  }

  CommonTree addAlias(CommonTree node, String aliasName) {
    return addAliasBody(node, aliasName);
  }

  CommonTree addAliasBody(CommonTree node, String aliasName) {
    CommonTree alias;
    alias = (CommonTree) node.getFirstChildWithType(PantheraParser_PLSQLParser.ALIAS);
    if (alias == null) {
      if (aliasName != null) {
        alias = FilterBlockUtil.createAlias(node, aliasName);
      } else {
        alias = FilterBlockUtil.createAlias(node, context);
      }
      node.addChild(alias);
    }
    return alias;
  }


  void buildGroup(CommonTree cascatedElement) {
    if (cascatedElement.getChild(0).getChildCount() == 2) {// FIXME just for tpch 20.sql
      cascatedElement.getChild(0).deleteChild(0);
    }
    CommonTree group = (CommonTree) bottomSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_GROUP);
    if (group == null) {
      group = FilterBlockUtil.createSqlASTNode(cascatedElement,
          PantheraParser_PLSQLParser.SQL92_RESERVED_GROUP,
          "group");
      bottomSelect.addChild(group);
    }
    CommonTree groupByElement = FilterBlockUtil.createSqlASTNode(
        cascatedElement, PantheraParser_PLSQLParser.GROUP_BY_ELEMENT, "GROUP_BY_ELEMENT");
    group.addChild(groupByElement);
    CommonTree expr = FilterBlockUtil.createSqlASTNode(cascatedElement,
        PantheraParser_PLSQLParser.EXPR, "EXPR");
    groupByElement.addChild(expr);
    expr.addChild(cascatedElement);
  }



  /**
   * build on branch
   *
   * @param op
   * @param child0
   * @param child1
   * @return on node
   */
  CommonTree buildOn(CommonTree op, CommonTree child0, CommonTree child1) {
    CommonTree on = FilterBlockUtil.createSqlASTNode(op,
        PantheraParser_PLSQLParser.SQL92_RESERVED_ON,
        "on");
    on.addChild(this.createLogicExpr(op, child0, child1));
    return on;
  }

  CommonTree buildWhere(CommonTree op, CommonTree child0, CommonTree child1) {
    CommonTree where = FilterBlockUtil.createSqlASTNode(
        op, PantheraParser_PLSQLParser.SQL92_RESERVED_WHERE, "where");
    where.addChild(this.createLogicExpr(op, child0, child1));
    return where;
  }

  CommonTree buildWhere(CommonTree op, List<CommonTree> leftChildren,
      List<CommonTree> rightChildren,
      CommonTree leftTableAlias, CommonTree rightTableAlias) throws SqlXlateException {
    if (leftChildren == null || leftChildren.size() != rightChildren.size()
        || leftChildren.size() == 0) {
      throw new SqlXlateException(op, "Illegal condition.");
    }
    CommonTree where = FilterBlockUtil.createSqlASTNode(
        op, PantheraParser_PLSQLParser.SQL92_RESERVED_WHERE, "where");
    CommonTree logicExpr = FilterBlockUtil.createSqlASTNode(op,
        PantheraParser_PLSQLParser.LOGIC_EXPR,
        "LOGIC_EXPR");
    where.addChild(logicExpr);
    CommonTree currentBranch = logicExpr;
    for (int i = 0; i < leftChildren.size(); i++) {
      if (logicExpr.getChildCount() > 0) {
        CommonTree and = FilterBlockUtil.createSqlASTNode(
            op, PantheraParser_PLSQLParser.SQL92_RESERVED_AND, "and");
        and.addChild((CommonTree) logicExpr.deleteChild(0));
        logicExpr.addChild(and);
        currentBranch = and;
      }
      CommonTree operation = FilterBlockUtil.cloneTree(op);
      operation.addChild(this.createCascatedElement(FilterBlockUtil.dupNode((CommonTree) leftTableAlias.getChild(0)),
          FilterBlockUtil.dupNode((CommonTree) leftChildren.get(i).getChild(0))));
      operation.addChild(this.createCascatedElement(FilterBlockUtil.dupNode((CommonTree) rightTableAlias.getChild(0)),
          FilterBlockUtil.dupNode((CommonTree) rightChildren.get(i).getChild(0))));
      currentBranch.addChild(operation);
    }
    return where;
  }

  CommonTree createLogicExpr(CommonTree op, CommonTree child0, CommonTree child1) {
    CommonTree logicExpr = FilterBlockUtil.createSqlASTNode(op,
        PantheraParser_PLSQLParser.LOGIC_EXPR,
        "LOGIC_EXPR");
    logicExpr.addChild(op);
    op.addChild(child0);
    op.addChild(child1);
    return logicExpr;
  }

  void addConditionToWhere(CommonTree where, CommonTree op, CommonTree child0, CommonTree child1) {
    CommonTree logicExpr = (CommonTree) where
        .getFirstChildWithType(PantheraParser_PLSQLParser.LOGIC_EXPR);
    assert (logicExpr.getChildCount() == 1);
    CommonTree current = (CommonTree) logicExpr.deleteChild(0);
    CommonTree and = FilterBlockUtil.createSqlASTNode(
        op, PantheraParser_PLSQLParser.SQL92_RESERVED_AND, "and");
    logicExpr.addChild(and);
    and.addChild(current);
    op.addChild(child0);
    op.addChild(child1);
    and.addChild(op);
  }




  /**
   * add alias for every column & build column alias map
   *
   * @param alias
   *          table alias node
   * @param selectList
   * @return
   * @throws SqlXlateException
   */
  List<CommonTree> buildSelectListAlias(CommonTree alias, CommonTree selectList)
      throws SqlXlateException {
    List<CommonTree> aliasList = new ArrayList<CommonTree>();
    String aliasName = alias == null ? "" : alias.getChild(0).getText();
    Map<String, String> columnMap = this.columnAliasMap.get(aliasName);
    if (columnMap == null) {
      columnMap = new HashMap<String, String>();
      this.columnAliasMap.put(aliasName, columnMap);
    }
    for (int i = 0; i < selectList.getChildCount(); i++) {
      CommonTree selectItem = (CommonTree) selectList.getChild(i);
      CommonTree columnAlias;
      if (selectItem.getChildCount() > 1) {// had alias
        columnAlias = (CommonTree) selectItem.getChild(1);
      } else {
        columnAlias = this.addAlias(selectItem);
      }
      aliasList.add(columnAlias);
      CommonTree anyElement = FilterBlockUtil.findOnlyNode((CommonTree) selectItem,
          PantheraParser_PLSQLParser.ANY_ELEMENT);
      String columnName;
      if (anyElement == null || anyElement.getType() != PantheraParser_PLSQLParser.ANY_ELEMENT) {
        continue;
      }
      if (anyElement.getChildCount() == 2) {
        columnName = anyElement.getChild(1).getText();
      } else {
        columnName = anyElement.getChild(0).getText();
      }
      String columnAliasName = columnAlias.getChild(0).getText();
      columnMap.put(columnName, columnAliasName);
    }
    // this.rebuildGroupOrder(alias);
    return aliasList;
  }

  /**
   * clone subQFilterBlock's non-sub-query branch
   *
   * @return
   * @deprecated
   */
  @Deprecated
  CommonTree cloneSubQOpElement() {
    for (int i = 0; i < subQNode.getChildCount(); i++) {
      if (subQNode.getChild(i).getType() != PantheraParser_PLSQLParser.SUBQUERY) {
        return FilterBlockUtil.cloneTree((CommonTree) subQNode.getChild(i));
      }
    }
    return null;
  }

  CommonTree getSubQOpElement() {
    for (int i = 0; i < subQNode.getChildCount(); i++) {
      if (subQNode.getChild(i).getType() != PantheraParser_PLSQLParser.SUBQUERY) {
        return (CommonTree) subQNode.getChild(i);
      }
    }
    return null;
  }

  void rebuildSubQOpElement(CommonTree subQOpElement, CommonTree columnAlias) {
    CommonTree anyElement = FilterBlockUtil.findOnlyNode(subQOpElement,
        PantheraParser_PLSQLParser.ANY_ELEMENT);
    if (anyElement == null) {// count(*) or whatever
      anyElement = FilterBlockUtil.createSqlASTNode((CommonTree) subQOpElement.getChild(0),
          PantheraExpParser.ANY_ELEMENT, "ANY_ELEMENT");
    } else {
      int count = anyElement.getChildCount();
      for (int i = 0; i < count; i++) {
        anyElement.deleteChild(0);
      }
    }
    anyElement.addChild(FilterBlockUtil.cloneTree((CommonTree) columnAlias
        .getChild(0)));
    CommonTree cascatedElement = (CommonTree) anyElement.getParent();
    if (cascatedElement == null) {// count(*) or whatever
      cascatedElement = FilterBlockUtil.createSqlASTNode(anyElement,
          PantheraExpParser.CASCATED_ELEMENT, "CASCATED_ELEMENT");
      cascatedElement.addChild(anyElement);
    }
    int index = subQOpElement.childIndex;
    subQNode.deleteChild(index);
    SqlXlateUtil.addCommonTreeChild(subQNode, index, cascatedElement);
  }

  CommonTree createClosingSelect(CommonTree oldFrom, CommonTree tableRefElement) {
    CommonTree select = FilterBlockUtil.createSqlASTNode(
        tableRefElement, PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT, "select");
    CommonTree from = FilterBlockUtil.createSqlASTNode(
        oldFrom, PantheraParser_PLSQLParser.SQL92_RESERVED_FROM, "from");
    select.addChild(from);
    CommonTree tableRef = FilterBlockUtil.createSqlASTNode(tableRefElement,
        PantheraParser_PLSQLParser.TABLE_REF, "TABLE_REF");
    from.addChild(tableRef);
    tableRef.addChild(tableRefElement);
    return select;
  }

  CommonTree createCascatedElement(CommonTree child) {
    CommonTree cascatedElement = FilterBlockUtil.createSqlASTNode(
        child, PantheraParser_PLSQLParser.CASCATED_ELEMENT, "CASCATED_ELEMENT");
    CommonTree anyElement = FilterBlockUtil.createSqlASTNode(
        child, PantheraParser_PLSQLParser.ANY_ELEMENT, "ANY_ELEMENT");
    cascatedElement.addChild(anyElement);
    anyElement.addChild(child);
    return cascatedElement;
  }

  CommonTree createCascatedElement(CommonTree tablename, CommonTree child) {
    CommonTree cascatedElement = FilterBlockUtil.createSqlASTNode(
        child, PantheraParser_PLSQLParser.CASCATED_ELEMENT, "CASCATED_ELEMENT");
    CommonTree anyElement = FilterBlockUtil.createSqlASTNode(
        child, PantheraParser_PLSQLParser.ANY_ELEMENT, "ANY_ELEMENT");
    cascatedElement.addChild(anyElement);
    anyElement.addChild(tablename);
    anyElement.addChild(child);
    return cascatedElement;
  }

  /**
   * TODO duplicated with below: CommonTree createCascatedElement(CommonTree child1, CommonTree
   * child2
   *
   * @param tableName
   * @param child
   * @return
   */
  CommonTree createCascatedElementWithTableName(CommonTree root, CommonTree tableName,
      CommonTree child) {
    CommonTree cascatedElement = FilterBlockUtil.createSqlASTNode(
        root, PantheraParser_PLSQLParser.CASCATED_ELEMENT, "CASCATED_ELEMENT");
    CommonTree anyElement = FilterBlockUtil.createSqlASTNode(
        root, PantheraParser_PLSQLParser.ANY_ELEMENT, "ANY_ELEMENT");
    cascatedElement.addChild(anyElement);
    anyElement.addChild(tableName);
    anyElement.addChild(child);
    return cascatedElement;
  }

  CommonTree createCascatedElement(CommonTree root, CommonTree child1, CommonTree child2) {
    CommonTree cascatedElement = FilterBlockUtil.createSqlASTNode(
        root, PantheraParser_PLSQLParser.CASCATED_ELEMENT, "CASCATED_ELEMENT");
    CommonTree anyElement = FilterBlockUtil.createSqlASTNode(
        root, PantheraParser_PLSQLParser.ANY_ELEMENT, "ANY_ELEMENT");
    cascatedElement.addChild(anyElement);
    anyElement.addChild(child1);
    anyElement.addChild(child2);
    return cascatedElement;
  }

  CommonTree createSelectListForClosingSelect(CommonTree topAlias, CommonTree select,
      List<CommonTree> aliasList) throws SqlXlateException {
    CommonTree selectList = FilterBlockUtil.createSqlASTNode(
        select, PantheraParser_PLSQLParser.SELECT_LIST, "SELECT_LIST");
    for (CommonTree alias : aliasList) {
      CommonTree newAlias = this.addSelectItemClosing(selectList, this
          .createCascatedElement(FilterBlockUtil.dupNode((CommonTree) topAlias.getChild(0)),
              FilterBlockUtil.dupNode((CommonTree) alias.getChild(0))));
      //this.reRebuildGroupOrder(alias.getChild(0).getText(), newAlias.getChild(0).getText());
    }
    return selectList;
  }

  /**
   * the difference between this function and createSelectListForClosingSelect
   * is that this function would not refresh Order by.
   */
  CommonTree createSelectListForClosingSelectWithoutRefreshOrder(CommonTree topAlias, CommonTree select,
      List<CommonTree> aliasList) throws SqlXlateException {
    CommonTree selectList = FilterBlockUtil.createSqlASTNode(
        select, PantheraParser_PLSQLParser.SELECT_LIST, "SELECT_LIST");
    for (CommonTree alias : aliasList) {
      CommonTree newAlias = this.addSelectItemClosing(selectList, this
          .createCascatedElement(FilterBlockUtil.dupNode((CommonTree) topAlias.getChild(0)),
              FilterBlockUtil.dupNode((CommonTree) alias.getChild(0))));
    }
    return selectList;
  }

  /**
   * Check if need to add one level for collect set.
   * since collect_set would not co-exist with aggregation function
   * Here we make it that only simple column don't need add one level.
   */
  boolean needAddOneLevel4CollectSet(CommonTree selectList) throws SqlXlateException {
    // collect set is only used for NOT IN uncorrelated subQ, there must be only one select item
    // assert(selectList.getChildCount() == 1);
    for (int i = 0; i < selectList.getChildCount(); i++) {
      CommonTree expr = (CommonTree) selectList.getChild(i).getChild(0);
      if (expr.getChild(0).getType() != PantheraParser_PLSQLParser.CASCATED_ELEMENT
          || expr.getChild(0).getChild(0).getType() != PantheraParser_PLSQLParser.ANY_ELEMENT) {
        return true;
      }
    }
    return false;
  }

  /**
   * add one level select for collect_set function if the parameter for collect_set if not a simple column
   * The reason for doing this is that collect_set can not have a aggregation function parameter
   * e.g. NOT IN (SELECT MAX(col) FROM T1 ...)
   */
  CommonTree addOneLevel4CollectSet(CommonTree select, CommonTree bottomAlias, List<CommonTree> columnAliasList) throws SqlXlateException {
    // create new level select table ref node
    CommonTree newLevelTableRefElement = createTableRefElement(FilterBlockUtil.cloneTree(select));
    // make the new level table ref alias the same with the bottom select table ref alias
    newLevelTableRefElement.setChild(0, FilterBlockUtil.cloneTree(bottomAlias));

    CommonTree oldFrom = (CommonTree) select.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM);
    // create new level select
    CommonTree newLevelSelect = createClosingSelect(oldFrom, newLevelTableRefElement);

    // create new level select list, make new level the select item alias the same with bottom select item alias
    CommonTree selectList = FilterBlockUtil.createSqlASTNode(
        newLevelSelect, PantheraParser_PLSQLParser.SELECT_LIST, "SELECT_LIST");
    for (CommonTree alias : columnAliasList) {
      CommonTree cascatedElement = this
          .createCascatedElement(FilterBlockUtil.dupNode((CommonTree) alias.getChild(0)));
      CommonTree selectItem = FilterBlockUtil.createSqlASTNode(
          cascatedElement, PantheraParser_PLSQLParser.SELECT_ITEM, "SELECT_ITEM");
      selectList.addChild(selectItem);
      CommonTree expr = FilterBlockUtil.createSqlASTNode(cascatedElement,
          PantheraParser_PLSQLParser.EXPR, "EXPR");
      selectItem.addChild(expr);
      expr.addChild(cascatedElement);
      selectItem.addChild(FilterBlockUtil.cloneTree(alias));
    }

    newLevelSelect.addChild(selectList);

    return newLevelSelect;
  }

  /**
   * rebuild select list of bottom select to collect_set function.
   */
  void rebuildCollectSet() {
    CommonTree selectList = (CommonTree) bottomSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    for (int i = 0; i < selectList.getChildCount(); i++) {
      CommonTree expr = (CommonTree) selectList.getChild(i).getChild(0);
      CommonTree element = (CommonTree) expr.deleteChild(0);
      CommonTree cascatedElement = FilterBlockUtil.createSqlASTNode(
          subQNode, PantheraParser_PLSQLParser.CASCATED_ELEMENT, "CASCATED_ELEMENT");
      expr.addChild(cascatedElement);
      CommonTree routineCall = FilterBlockUtil.createSqlASTNode(
          subQNode, PantheraParser_PLSQLParser.ROUTINE_CALL, "ROUTINE_CALL");
      cascatedElement.addChild(routineCall);
      CommonTree routineName = FilterBlockUtil.createSqlASTNode(
          subQNode, PantheraParser_PLSQLParser.ROUTINE_NAME, "ROUTINE_NAME");
      routineCall.addChild(routineName);
      CommonTree collectSet = FilterBlockUtil.createSqlASTNode(subQNode,
          PantheraParser_PLSQLParser.ID, "collect_set");
      routineName.addChild(collectSet);
      CommonTree arguments = FilterBlockUtil.createSqlASTNode(subQNode,
          PantheraParser_PLSQLParser.ARGUMENTS, "ARGUMENTS");
      routineCall.addChild(arguments);
      CommonTree arguement = FilterBlockUtil.createSqlASTNode(subQNode,
          PantheraParser_PLSQLParser.ARGUMENT, "ARGUMENT");
      arguments.addChild(arguement);
      CommonTree newExpr = FilterBlockUtil
          .createSqlASTNode(subQNode, PantheraParser_PLSQLParser.EXPR, "EXPR");
      arguement.addChild(newExpr);
      newExpr.addChild(element);
    }
  }

  /**
   * rebuild LOGIC_EXPR branch to array_contains function.
   *
   * @param logicExpr
   * @throws SqlXlateException
   */
  void rebuildArrayContains(CommonTree logicExpr) throws SqlXlateException {
    for (int k = 0; k < logicExpr.getChildCount(); k++) {
      if (logicExpr.getChild(k).getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_AND) {
        this.rebuildArrayContains((CommonTree) logicExpr.getChild(k));
      } else {
        CommonTree op = (CommonTree) logicExpr.deleteChild(k);

        CommonTree newOp;
        switch (op.getType()) {
        case PantheraParser_PLSQLParser.NOT_IN:
          newOp = FilterBlockUtil.createSqlASTNode(op,
              PantheraParser_PLSQLParser.SQL92_RESERVED_NOT,
              "not");
          // this.attachChild(logicExpr, newOp);
          SqlXlateUtil.addCommonTreeChild(logicExpr, k, newOp);
          break;
        default:
          throw new SqlXlateException(op, "UnProcess logic operator." + op.getText());
        }
        CommonTree cascatedElement = FilterBlockUtil.createSqlASTNode(
            op, PantheraParser_PLSQLParser.CASCATED_ELEMENT, "CASCATED_ELEMENT");
        newOp.addChild(cascatedElement);
        CommonTree routineCall = FilterBlockUtil.createSqlASTNode(
            op, PantheraParser_PLSQLParser.ROUTINE_CALL, "ROUTINE_CALL");
        cascatedElement.addChild(routineCall);
        CommonTree routineName = FilterBlockUtil.createSqlASTNode(
            op, PantheraParser_PLSQLParser.ROUTINE_NAME, "ROUTINE_NAME");
        routineCall.addChild(routineName);
        CommonTree arrayContains = FilterBlockUtil.createSqlASTNode(op,
            PantheraParser_PLSQLParser.ID,
            "array_contains");
        routineName.addChild(arrayContains);
        CommonTree arguments = FilterBlockUtil.createSqlASTNode(
            op, PantheraParser_PLSQLParser.ARGUMENTS, "ARGUMENTS");
        routineCall.addChild(arguments);
        for (int i = 0; i < op.getChildCount(); i++) {
          CommonTree element = (CommonTree) op.getChild(i);
          CommonTree arguement = FilterBlockUtil.createSqlASTNode(
              element, PantheraParser_PLSQLParser.ARGUMENT, "ARGUMENT");
          arguments.addChild(arguement);
          CommonTree expr = FilterBlockUtil.createSqlASTNode(element,
              PantheraParser_PLSQLParser.EXPR,
              "EXPR");
          arguement.addChild(expr);
          expr.addChild(element);
        }

      }
    }

  }

  private void buildTableAliasMap(String alias, Set<String> tableNames) {
    for (String tableName : tableNames) {
      this.tableAliasMap.put(tableName, alias);
    }
  }

  CommonTree rebuildCascatedElement(CommonTree cascatedElement) {
    CommonTree anyElement = (CommonTree) cascatedElement.getChild(0);
    if (anyElement.getChildCount() <= 1) {
      return cascatedElement;
    }
    CommonTree tableName = (CommonTree) anyElement.getChild(0);
    CommonTree columnName = (CommonTree) anyElement.getChild(1);
    String tableAlias = this.tableAliasMap.get(tableName.getText());
    if (tableAlias != null) {
      tableName.getToken().setText(tableAlias);
      Map<String, String> columnMap = this.columnAliasMap.get(tableAlias);
      if (columnMap != null) {
        String columnAlias = columnMap.get(columnName.getText());
        if (columnAlias != null) {
          columnName.getToken().setText(columnAlias);
        }
      }
    }
    return cascatedElement;
  }

  /**
   * only for bottomSelect
   *
   * @param select
   */
  void processSelectAsterisk(CommonTree select) {
    for (int i = 0; i < select.getChildCount(); i++) {
      CommonTree node = (CommonTree) select.getChild(i);
      if (node.getType() == PantheraParser_PLSQLParser.ASTERISK) {
        node.getToken().setType(PantheraParser_PLSQLParser.SELECT_LIST);
        node.getToken().setText("SELECT_LIST");
      }
    }
  }

  void rebuildSelectListByFilter(boolean isLeftJoin, boolean needGroup, CommonTree joinSubAlias,
      CommonTree topAlias) throws SqlXlateException {
    List<Map<Boolean, List<CommonTree>>> joinKeys = this.getFilterkey();
    Set<String> selectKeySet = new HashSet<String>();
    for (int i = 0; i < joinKeys.size(); i++) {
      List<CommonTree> bottomKeys = joinKeys.get(i).get(false);
      List<CommonTree> topKeys = joinKeys.get(i).get(true);
      if (bottomKeys != null) {
        for (CommonTree bottomKey : bottomKeys) {
          String selectKey;
          // FIXME when bottomKey is not CASCATED_ELEMENT
          if (FilterBlockUtil.findOnlyNode(bottomKey, PantheraParser_PLSQLParser.STANDARD_FUNCTION) == null) {
            selectKey = bottomKey.getChild(0).getChildCount() == 2 ? bottomKey.getChild(0)
                .getChild(1).getText() : bottomKey.getChild(0).getChild(0).getText();
            if (needGroup && !selectKeySet.contains(selectKey)) {
              selectKeySet.add(selectKey);
              // group
              this.buildGroup(FilterBlockUtil.cloneTree(bottomKey));
            }
          }
          // add select item
          CommonTree joinKeyAlias = this.addSelectItem((CommonTree) bottomSelect
              .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST), FilterBlockUtil
              .cloneTree(bottomKey));
          // modify filter to alias
          CommonTree anyElement = FilterBlockUtil.findOnlyNode(bottomKey,
              PantheraExpParser.ANY_ELEMENT);
          if (anyElement.getChildCount() == 2) {
            ((CommonTree) anyElement.getChild(0)).getToken().setText(
                joinSubAlias.getChild(0).getText());
            ((CommonTree) anyElement.getChild(1)).getToken().setText(
                joinKeyAlias.getChild(0).getText());
          } else {
            ((CommonTree) anyElement.getChild(0)).getToken().setText(
                joinKeyAlias.getChild(0).getText());
          }
          CommonTree newBottomKey = FilterBlockUtil.
              createCascatedElementBranch(bottomKey, joinSubAlias.getChild(0).getText(), joinKeyAlias.getChild(0).getText());
          if (isLeftJoin && topKeys != null) {
            CommonTree isNull = FilterBlockUtil.createSqlASTNode(
                subQNode.parent, PantheraParser_PLSQLParser.IS_NULL, "IS_NULL");
            isNull.addChild(newBottomKey);
            CommonTree and = FilterBlockUtil.createSqlASTNode(
                subQNode.parent, PantheraParser_PLSQLParser.SQL92_RESERVED_AND, "and");
            and.addChild(fb.getASTNode());
            and.addChild(isNull);
            if (context.getBallFromBasket(isNull) != null) {
              throw new SqlXlateException(isNull, "fatal error: translate context conflict.");
            }
            // for CrossjoinTransformer which should not optimize isNull node in WHERE.
            context.putBallToBasket(isNull, true);
            this.fb.setASTNode(and);
          }
          rebuildWhereKey4rebuildSelectListByFilter(bottomKey, anyElement);
        }
      }

      if (topKeys != null && topAlias != null) {
        CommonTree topKey = topKeys.get(0);// can't more than one topkey.

        // add select item
        CommonTree joinKeyAlias = this.addSelectItem((CommonTree) topSelect
            .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST), FilterBlockUtil
            .cloneTree(topKey));
        insertSelectItemDown((CommonTree) joinKeyAlias.getParent());
        // modify filter to alias
        CommonTree anyElement = FilterBlockUtil.findOnlyNode(topKey, PantheraExpParser.ANY_ELEMENT);
        if (anyElement.getChildCount() == 2) {
          ((CommonTree) anyElement.getChild(0)).getToken().setText(topAlias.getChild(0).getText());
          ((CommonTree) anyElement.getChild(1)).getToken().setText(
              joinKeyAlias.getChild(0).getText());
        } else {
          ((CommonTree) anyElement.getChild(0)).getToken().setText(
              joinKeyAlias.getChild(0).getText());
        }
        rebuildWhereKey4rebuildSelectListByFilter(topKey, anyElement);
      }
    }
  }

  private void insertSelectItemDown(CommonTree selectItem) {
    if (selectItem.getChild(0).getChild(0).getType() != PantheraParser_PLSQLParser.CASCATED_ELEMENT
        && selectItem.getChild(0).getChild(0).getChild(0).getType() != PantheraParser_PLSQLParser.ANY_ELEMENT) {
      return;
    }
    CommonTree localTopSelect = (CommonTree) selectItem.getParent().getParent();
    CommonTree topFrom = (CommonTree) localTopSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM);
    CommonTree fromMode = (CommonTree) ((CommonTree) topFrom.getChild(0).getChild(0))
        .getFirstChildWithType(PantheraParser_PLSQLParser.TABLE_EXPRESSION).getChild(0);
    if (fromMode.getType() == PantheraParser_PLSQLParser.DIRECT_MODE) {
      return;
    }
    if (fromMode.getType() == PantheraParser_PLSQLParser.SELECT_MODE) {
      CommonTree select = FilterBlockUtil.findOnlyNode(fromMode,
          PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT);
      CommonTree selectList = (CommonTree) select
          .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
      CommonTree cascatedElement = (CommonTree) selectItem.getChild(0).getChild(0);
      CommonTree anyElement = (CommonTree) cascatedElement.getChild(0);
      if (!hasSelectItemInSelectList(selectList, anyElement)) {
        String columnName = anyElement.getChild(anyElement.getChildCount() - 1).getText();
        CommonTree joinKeyAlias = this.addSelectItem(selectList,
            FilterBlockUtil.cloneTree((CommonTree) cascatedElement), columnName);
        insertSelectItemDown((CommonTree) joinKeyAlias.getParent());
        // for case like : union, minus, intersect
        if (select.getParent().getChildCount() == 2) {
          CommonTree rightSelect = FilterBlockUtil.findOnlyNode((CommonTree) select.getParent(),
              PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT);
          CommonTree rightJoinKeyAlias = this.addSelectItem(
              (CommonTree) rightSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST),
              FilterBlockUtil.cloneTree((CommonTree) cascatedElement), columnName);
          insertSelectItemDown((CommonTree) rightJoinKeyAlias.getParent());
        }
      }
    }
  }

  private boolean hasSelectItemInSelectList(CommonTree selectList, CommonTree anyElement) {
    for (int i = 0; i < selectList.getChildCount(); i++) {
      CommonTree selectItem = (CommonTree) selectList.getChild(i);
      if (selectItem.getType() != PantheraParser_PLSQLParser.SELECT_ITEM) {
        continue;
      }
      if (selectItem.getChildCount() == 2) {
        if (selectItem.getChild(1).getChild(0).getText().equals(anyElement.getChild(anyElement.getChildCount() - 1).getText())) {
          return true;
        } else {
          continue;
        }
      }
      CommonTree cascatedElement = (CommonTree) selectItem.getChild(0).getChild(0);
      if (cascatedElement.getType() != PantheraParser_PLSQLParser.CASCATED_ELEMENT
          && cascatedElement.getChild(0).getType() != PantheraParser_PLSQLParser.ANY_ELEMENT) {
        continue;
      }
      if (FilterBlockUtil.equalsTree(cascatedElement, (CommonTree) anyElement.getParent())) {
        return true;
      } else {
        continue;
      }
    }
    return false;
  }

  private void rebuildWhereKey4rebuildSelectListByFilter(CommonTree bottomKey, CommonTree anyElement) {
    if (bottomKey.getType() != PantheraExpParser.CASCATED_ELEMENT) {
      CommonTree cascatedElement = (CommonTree) anyElement.getParent();
      int index = bottomKey.childIndex;
      CommonTree whereOp = (CommonTree) bottomKey.getParent();
      whereOp.deleteChild(index);
      SqlXlateUtil.addCommonTreeChild(whereOp, index, cascatedElement);
    }
  }

  CommonTree buildWhereByFB(CommonTree subQCondition, CommonTree compareKeyAlias1,
      CommonTree compareKeyAlias2) {
    CommonTree where = FilterBlockUtil.createSqlASTNode(
        fb.getASTNode(), PantheraParser_PLSQLParser.SQL92_RESERVED_WHERE, "where");
    CommonTree logicExpr = FilterBlockUtil.createSqlASTNode(where,
        PantheraParser_PLSQLParser.LOGIC_EXPR, "LOGIC_EXPR");
    where.addChild(logicExpr);
    logicExpr.addChild(fb.getASTNode());
    if (compareKeyAlias1 != null && compareKeyAlias2 != null) {
      this.addConditionToWhere(where, FilterBlockUtil.dupNode(subQCondition), this
          .createCascatedElement(FilterBlockUtil.cloneTree((CommonTree) compareKeyAlias1
              .getChild(0))), this.createCascatedElement(FilterBlockUtil
          .cloneTree((CommonTree) compareKeyAlias2.getChild(0))));
    }
    topSelect.addChild(where);
    return where;
  }

  void builldSimpleWhere(CommonTree condition) {
    CommonTree where = FilterBlockUtil.createSqlASTNode(
        condition, PantheraParser_PLSQLParser.SQL92_RESERVED_WHERE, "where");
    CommonTree logicExpr = FilterBlockUtil.createSqlASTNode(condition,
        PantheraParser_PLSQLParser.LOGIC_EXPR, "LOGIC_EXPR");
    where.addChild(logicExpr);
    logicExpr.addChild(condition);
    topSelect.addChild(where);
  }

  void rebuildGroupOrder(CommonTree topAlias) throws SqlXlateException {
    // FIXME refresh group in QueryBlock.

    CommonTree order = topQuery.getOrder();

    if (order != null) {
      CommonTree orderByElements = (CommonTree) order.getChild(0);
      for (int i = 0; i < orderByElements.getChildCount(); i++) {
        CommonTree ct = (CommonTree) orderByElements.getChild(i).getChild(0).getChild(0);
        if (ct.getType() == PantheraParser_PLSQLParser.CASCATED_ELEMENT) {// NOT order by 1,2
          CommonTree anyElement = (CommonTree) ct.getChild(0);
          this.rebuildAnyElementAlias(topAlias, anyElement);
        }
        //
        // for order by function, the column name should be update before orderbyFunctionTransformer
        //
        List<CommonTree> funcList = new ArrayList<CommonTree>();
        FilterBlockUtil.findNode((CommonTree) orderByElements.getChild(i),
            PantheraParser_PLSQLParser.STANDARD_FUNCTION, funcList);
        if (!funcList.isEmpty()) {
          for (CommonTree func : funcList) {
            List<CommonTree> anyElementList = new ArrayList<CommonTree>();
            FilterBlockUtil.findNode(func, PantheraParser_PLSQLParser.ANY_ELEMENT, anyElementList);
            for (CommonTree anyElement : anyElementList) {
              this.rebuildAnyElementAlias(topAlias, anyElement);
            }
          }
        }
      }
    }
  }

  private void rebuildHaving(CommonTree havingTree, CommonTree topAlias) {
    if (havingTree.getType() == PantheraParser_PLSQLParser.ANY_ELEMENT) {
      this.rebuildAnyElementAlias(topAlias, havingTree);
      return;
    }
    for (int i = 0; i < havingTree.getChildCount(); i++) {
      rebuildHaving((CommonTree) havingTree.getChild(i), topAlias);
    }
  }

  void rebuildAnyElementAlias(CommonTree topAlias, CommonTree anyElement) {
    CommonTree column;
    if (anyElement.getChildCount() == 2) {
      // ((CommonTree) anyElement.getChild(0)).getToken().setText(topAlias.getChild(0).getText());//
      // TODO
      // remove, needn't table alias.
      anyElement.deleteChild(0);
    }
    column = (CommonTree) anyElement.getChild(0);
    Map<String, String> columnMap = this.columnAliasMap.get(topAlias == null ? "" : topAlias
        .getChild(0).getText());
    String columnAlias = columnMap.get(column.getText());
    if (columnAlias != null) {
      column.getToken().setText(columnAlias);
    }
  }

  /**
   * rebuild order&group with alias when create closing select list
   *
   * @param oldAlias
   * @param newAlias
   */
  void reRebuildGroupOrder(String oldAlias, String newAlias) throws SqlXlateException {

    CommonTree group = topQuery.getGroup();
    CommonTree order = topQuery.getOrder();
    if (group != null) {
      // FIXME should rebuild group alias in query block, use select_list's column replace alias

      // for (int i = 0; i < group.getChildCount(); i++) {
      // CommonTree groupElement = (CommonTree) group.getChild(i);
      // CommonTree anyElement = (CommonTree) groupElement.getChild(0).getChild(0).getChild(0);
      // this.reRebuildAnyElement(oldAlias, newAlias, anyElement);
      // }
    }
    if (order != null) {
      CommonTree orderByElements = (CommonTree) order.getChild(0);
      for (int i = 0; i < orderByElements.getChildCount(); i++) {
        CommonTree orderByElement = (CommonTree) orderByElements.getChild(i);
        CommonTree anyElement = (CommonTree) orderByElement.getChild(0).getChild(0).getChild(0);
        if (anyElement != null && anyElement.getType() == PantheraParser_PLSQLParser.ANY_ELEMENT) {
          this.reRebuildAnyElement(oldAlias, newAlias, anyElement);
        }
        //
        // for order by function, the column name should be update before orderbyFunctionTransformer
        //
        List<CommonTree> funcList = new ArrayList<CommonTree>();
        FilterBlockUtil.findNode(orderByElement, PantheraParser_PLSQLParser.STANDARD_FUNCTION,
            funcList);
        if (!funcList.isEmpty()) {
          for (CommonTree func : funcList) {
            List<CommonTree> anyElementList = new ArrayList<CommonTree>();
            FilterBlockUtil.findNode(func, PantheraParser_PLSQLParser.ANY_ELEMENT, anyElementList);
            for (CommonTree tmpAnyElement : anyElementList) {
              this.reRebuildAnyElement(oldAlias, newAlias, tmpAnyElement);
            }
          }
        }
      }
    }
  }

  private void reRebuildAnyElement(String oldAlias, String newAlias, CommonTree anyElement) {

    if (anyElement.getChildCount() == 2) {
      anyElement.deleteChild(0);
    }
    CommonTree child = (CommonTree) anyElement.getChild(0);
    if (child.getText().equals(oldAlias)) {
      child.getToken().setText(newAlias);
    }

  }

  /**
   * add SELECT_ITEM for subq IN
   *
   * @param select
   * @param subq
   * @return
   * @throws SqlXlateException
   */
  List<CommonTree> addSelectItems4In(CommonTree select, CommonTree subq) throws SqlXlateException {
    CommonTree left = (CommonTree) subq.getChild(0);
    List<CommonTree> result = new ArrayList<CommonTree>();
    if (left.getType() == PantheraParser_PLSQLParser.CASCATED_ELEMENT) {
      CommonTree compareElementAlias = this.addSelectItem((CommonTree) select
          .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST), FilterBlockUtil
          .cloneTree(left));
      result.add(compareElementAlias);
      return result;
    }
    if (left.getType() == PantheraParser_PLSQLParser.VECTOR_EXPR) {
      for (int i = 0; i < left.getChildCount(); i++) {
        CommonTree compareElementAlias = this.addSelectItem((CommonTree) select
            .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST), FilterBlockUtil
            .cloneTree((CommonTree) left.getChild(i).getChild(0)));
        result.add(compareElementAlias);
      }
      return result;
    }
    return null;
  }

  /**
   * rebuild column alias to sequence alias after unnested subquery
   *
   * @param select
   */
  private void rebuildColumnAlias(CommonTree originSelectList, CommonTree select)
      throws SqlXlateException {
    CommonTree selectList = (CommonTree) select
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    if (selectList == null) {
      return;
    }
    for (int i = 0; i < originSelectList.getChildCount(); i++) {
      CommonTree selectItem = (CommonTree) selectList.getChild(i);
      if (selectItem.getChildCount() == 2) {
        CommonTree aliasName = (CommonTree) selectItem.getChild(1).getChild(0);
        String oldColAlias = aliasName.getText();
        if ("panthera".equals(oldColAlias.split("_")[0])) {
          CommonTree originSelectItem = (CommonTree) originSelectList.getChild(i);
          // TODO here ignore select-list select
          CommonTree anyElement = (CommonTree) originSelectItem.getChild(0).getChild(0).getChild(0);
          if (anyElement != null && anyElement.getType() == PantheraParser_PLSQLParser.ANY_ELEMENT) {
            // in case select 'a' or select -3
            String newColAlias = originSelectItem.getChildCount() == 2 ? originSelectItem
                .getChild(1).
                getChild(0).getText() : anyElement.getChild(anyElement.getChildCount() - 1)
                .getText();
            aliasName.getToken().setText(newColAlias);
            //reRebuildGroupOrder(oldColAlias, newColAlias);
          }
        }
      }
    }
  }

  CommonTree createCountAsteriskSelectList() {
    CommonTree not = (CommonTree) subQNode.getParent();
    CommonTree selectList = FilterBlockUtil.createSqlASTNode(
        not, PantheraParser_PLSQLParser.SELECT_LIST, "SELECT_LIST");
    CommonTree selectItem = FilterBlockUtil.createSqlASTNode(
        not, PantheraParser_PLSQLParser.SELECT_ITEM, "SELECT_ITEM");
    selectList.addChild(selectItem);
    CommonTree expr = FilterBlockUtil
        .createSqlASTNode(not, PantheraParser_PLSQLParser.EXPR, "EXPR");
    selectItem.addChild(expr);
    CommonTree standardFunction = FilterBlockUtil.createSqlASTNode(
        not, PantheraParser_PLSQLParser.STANDARD_FUNCTION, "STANDARD_FUNCTION");
    expr.addChild(standardFunction);
    CommonTree count = FilterBlockUtil.createSqlASTNode(not, PantheraParser_PLSQLParser.COUNT_VK,
        "count");
    standardFunction.addChild(count);
    CommonTree asterisk = FilterBlockUtil
        .createSqlASTNode(not, PantheraParser_PLSQLParser.ASTERISK, "*");
    count.addChild(asterisk);
    CommonTree alias = FilterBlockUtil.createSqlASTNode(not, PantheraParser_PLSQLParser.ALIAS,
        "ALIAS");
    selectItem.addChild(alias);
    CommonTree aliasName = FilterBlockUtil.createSqlASTNode(not, PantheraParser_PLSQLParser.ID,
        "panthera_col_0");
    alias.addChild(aliasName);
    return selectList;
  }

  CommonTree reCreateBottomSelect(CommonTree oldSelect, CommonTree tableRefElement,
      CommonTree selectList) {
    CommonTree select = FilterBlockUtil.createSqlASTNode(
        oldSelect, PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT, "select");
    CommonTree oldFrom = (CommonTree) oldSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM);
    CommonTree from = FilterBlockUtil.createSqlASTNode(
        oldFrom, PantheraParser_PLSQLParser.SQL92_RESERVED_FROM, "from");
    select.addChild(from);
    CommonTree oldTableRef = (CommonTree) oldFrom
        .getFirstChildWithType(PantheraParser_PLSQLParser.TABLE_REF);
    CommonTree tableRef = FilterBlockUtil.createSqlASTNode(oldTableRef,
        PantheraParser_PLSQLParser.TABLE_REF,
        "TABLE_REF");
    from.addChild(tableRef);
    tableRef.addChild(tableRefElement);
    select.addChild(selectList);
    return select;
  }

  private CommonTree addLevelForUnion(CommonTree subQuery, TranslateContext context) throws SqlXlateException {
    CommonTree select = FilterBlockUtil.createSqlASTNode(subQuery, PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT, "select");
    CommonTree leftSelect = (CommonTree) subQuery.getChild(0);
    while (leftSelect.getType() != PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT) {
      leftSelect = (CommonTree) leftSelect.getChild(0);
    }
    CommonTree subq = FilterBlockUtil.makeSelectBranch(select, context,
        (CommonTree) leftSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM));
    subq.getParent().replaceChildren(subq.childIndex, subq.childIndex, subQuery);
    CommonTree selectList = FilterBlockUtil.cloneSelectListFromSelect(leftSelect);
    select.addChild(selectList);
    return select;
  }

  void reBuildNotExist4UCWhere(CommonTree not, CommonTree select, CommonTree tabAlias)
      throws SqlXlateException {
    CommonTree where = (CommonTree) select
        .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_WHERE);
    if (where != null) {
      throw new SqlXlateException(where, "Unexpected condition.");
    }
    CommonTree equal = FilterBlockUtil.createSqlASTNode(not, PantheraParser_PLSQLParser.EQUALS_OP,
        "=");
    CommonTree cascatedElement = this.createCascatedElement(equal, tabAlias, FilterBlockUtil
        .createSqlASTNode(not, PantheraParser_PLSQLParser.ID, "panthera_col_0"));
    CommonTree zero = FilterBlockUtil
        .createSqlASTNode(not, PantheraExpParser.UNSIGNED_INTEGER, "0");
    select.addChild(this.buildWhere(equal, cascatedElement, zero));
  }
}
