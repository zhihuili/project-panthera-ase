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
import java.util.List;
import java.util.Map;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.PantheraExpParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockUtil;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.PLSQLFilterBlockFactory;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * provide common process logic for filter block processor. TODO<br>
 * <li>processCompareHavingUC and processEqualsUC didn¡¯t check whether single-row expression
 * returns only one row. It¡¯s ok for these 2 cases because sum and max always return one value.
 * Just a reminder don¡¯t miss the check part. If you didn¡¯t implement yet, add a ¡°TODO¡± remark
 * in code.<br>
 *
 * <li>there are some duplicate code need to be refactor.
 *
 * CommonFilterBlockProcessor.
 *
 */

public abstract class CommonFilterBlockProcessor extends BaseFilterBlockProcessor {

  CommonTree topAlias;
  CommonTree bottomAlias;
  CommonTree topTableRefElement;
  List<CommonTree> topAliasList;
  CommonTree closingSelect;
  CommonTree join;

  /**
   * make branch for top select
   * @throws SqlXlateException
   */
  private void makeTop() throws SqlXlateException {

    // create top select table ref node
    topTableRefElement = super.createTableRefElement(topSelect);
    topAlias = (CommonTree) topTableRefElement.getChild(0);

    // add alias for all top query select item
    topAliasList = super.buildSelectListAlias(topAlias, (CommonTree) topSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST));

    CommonTree topFrom = (CommonTree) topSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM);

    // create closing select
    closingSelect = super.createClosingSelect(topFrom, topTableRefElement);
  }

  /**
   * make branch join
   *
   * @param joinType
   */
  private void makeJoin(CommonTree joinType) {
    // join
    join = super.createJoin(joinType, closingSelect);
    bottomAlias = super.buildJoin(joinType, join, bottomSelect);
  }

  /**
   * make branch for whole sub tree
   * @throws SqlXlateException
   */
  private void makeEnd() throws SqlXlateException {
    // closing select list
    CommonTree selectList = super.createSelectListForClosingSelect(topAlias, closingSelect, topAliasList);
    if (selectList.getChildCount() == 0) {
      selectList = FilterBlockUtil.cloneSelectListFromSelect((CommonTree) topSelect);
      for (int i = 0; i < selectList.getChildCount(); i++) {
        CommonTree selectItem = (CommonTree) selectList.getChild(i);
        CommonTree expr = (CommonTree) selectItem.getChild(0);
        expr.deleteChild(0);
        expr.addChild(super.createCascatedElement(FilterBlockUtil.cloneTree((CommonTree) selectItem
            .getChild(1).getChild(0))));
      }
    }
    closingSelect.addChild(selectList);
    AddAllNeededFilters();

    // set closing select to top select
    topSelect = closingSelect;
  }

  private void AddAllNeededFilters() {
    CommonTree oldTopSelectList = (CommonTree) topSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    List<CommonTree> oldAnyElementList = new ArrayList<CommonTree>();
    FilterBlockUtil.findNode(oldTopSelectList, PantheraParser_PLSQLParser.ANY_ELEMENT, oldAnyElementList);
    String tableName;
    CommonTree oldTopFrom = (CommonTree) topSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM);
    CommonTree oldTopFromFirstTR = (CommonTree) oldTopFrom.getChild(0).getChild(0);
    if (oldTopFromFirstTR.getChildCount() == 2) {
      tableName = oldTopFromFirstTR.getChild(0).getChild(0).getText();
    } else {
      CommonTree tableViewName = (CommonTree) oldTopFromFirstTR.getChild(0).getChild(0).getChild(0);
      tableName = tableViewName.getChild(tableViewName.getChildCount() - 1).getText();
    }
    if (!topQuery.isProcessHaving()) {
      for (CommonTree column : whereFilterColumns) {
        List<CommonTree> nodeList = new ArrayList<CommonTree>();
        FilterBlockUtil.findNode(column, PantheraParser_PLSQLParser.CASCATED_ELEMENT, nodeList);
        for (CommonTree cascated : nodeList) {
          CommonTree cNameNode;
          if (cascated.getChild(0).getChildCount() > 1) {
            cNameNode = (CommonTree) column.getChild(0).getChild(1);
            if (!tableName.startsWith("panthera_")) {
              tableName = column.getChild(0).getChild(0).getText();
            }
          } else {
            cNameNode = (CommonTree) column.getChild(0).getChild(0);
            if (!tableName.startsWith("panthera_")) {
              tableName = "";
            }
          }
          String columnName = cNameNode.getText();
          if (tableName.equals("")) {
            CommonTree singleColumn = FilterBlockUtil.createSqlASTNode(cascated, PantheraParser_PLSQLParser.ID, columnName);
            FilterBlockUtil.addSelectItem(oldTopSelectList, FilterBlockUtil.createCascatedElement(singleColumn));
          } else {
            for (CommonTree oldAny:oldAnyElementList) {
              if (oldAny.getChildCount() == 1 && oldAny.getChild(0).getText().equals(columnName)) {
                CommonTree singleTable = FilterBlockUtil.createSqlASTNode(cascated, PantheraParser_PLSQLParser.ID, tableName);
                SqlXlateUtil.addCommonTreeChild(oldAny, 0, singleTable);
              }
            }
            FilterBlockUtil.addSelectItem(oldTopSelectList, FilterBlockUtil.createCascatedElementBranch(cascated, tableName, columnName));
          }
        }
      }
    } else {
      for (int aggrIndex = 0; aggrIndex < havingFilterColumns.size(); aggrIndex++) {
        CommonTree column = havingFilterColumns.get(aggrIndex);
        CommonTree cascated = FilterBlockUtil.findOnlyNode(column, PantheraParser_PLSQLParser.CASCATED_ELEMENT);
        CommonTree cNameNode;
        if (cascated.getChild(0).getChildCount() > 1) {
          cNameNode = (CommonTree) column.getChild(0).getChild(1);
          if (!tableName.startsWith("panthera_")) {
            tableName = column.getChild(0).getChild(0).getText();
          }
        } else {
          cNameNode = (CommonTree) column.getChild(0).getChild(0);
          if (!tableName.startsWith("panthera_")) {
            tableName = "";
          }
        }
        String columnName = cNameNode.getText();
        if (tableName.equals("")) {
          CommonTree singleColumn = FilterBlockUtil.createSqlASTNode(cascated, PantheraParser_PLSQLParser.ID, columnName);
          FilterBlockUtil.addSelectItem(oldTopSelectList, FilterBlockUtil.createCascatedElement(singleColumn));
        } else {
          for (CommonTree oldAny:oldAnyElementList) {
            if (oldAny.getChildCount() == 1 && oldAny.getChild(0).getText().equals(columnName)) {
              CommonTree singleTable = FilterBlockUtil.createSqlASTNode(cascated, PantheraParser_PLSQLParser.ID, tableName);
              SqlXlateUtil.addCommonTreeChild(oldAny, 0, singleTable);
            }
          }
          FilterBlockUtil.addSelectItem(oldTopSelectList, FilterBlockUtil.createCascatedElementBranch(cascated, tableName, columnName));
        }
      }
    }
  }

  /**
   * process compare operator with correlated
   *
   * @param joinType
   * @throws SqlXlateException
   */
  void processCompareC() throws SqlXlateException {
    CommonTree compareElement = super.getSubQOpElement();
    CommonTree sq = fbContext.getSelectStack().pop();
    if (PLSQLFilterBlockFactory.getInstance().isCorrelated(this.fbContext.getqInfo(),
        this.fbContext.getSelectStack(), compareElement) != 0) {
      throw new SqlXlateException(compareElement, "not support element from outter query compare with sub-query");
    }
    fbContext.getSelectStack().push(sq);

    this.makeTop();
    this.makeJoin(FilterBlockUtil.createSqlASTNode(subQNode, PantheraParser_PLSQLParser.CROSS_VK, "cross"));

    CommonTree compareKeyAlias1 = super.addSelectItem((CommonTree) topSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST), FilterBlockUtil.cloneTree(compareElement));

    // select list
    CommonTree compareKeyAlias2 = super.addAlias((CommonTree) ((CommonTree) bottomSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST)).getChild(0));// childCount==0;
    super.rebuildSelectListByFilter(false, true, bottomAlias, topAlias);

    this.makeEnd();

    // where
    super.buildWhereByFB(subQNode, compareKeyAlias1, compareKeyAlias2);

  }


  /**
   * process exists with correlated
   *
   * @param joinType
   * @throws SqlXlateException
   */
  void processExistsC() throws SqlXlateException {
    this.makeTop();


    this.makeJoin(FilterBlockUtil.createSqlASTNode(subQNode, PantheraParser_PLSQLParser.CROSS_VK,
        PantheraExpParser.LEFTSEMI_STR));


    super.processSelectAsterisk(bottomSelect);
    super.rebuildSelectListByFilter(false, false, bottomAlias, topAlias);

    this.makeEnd();
    super.buildWhereByFB(null, null, null);

    if (super.hasNotEqualCorrelated) {
      // become inner join if there is not equal correlated.
      join.deleteChild(0);
      // add distinct
      SqlXlateUtil.addCommonTreeChild(this.closingSelect, 1, FilterBlockUtil.createSqlASTNode(
          subQNode, PantheraParser_PLSQLParser.SQL92_RESERVED_DISTINCT, "distinct"));
    }
  }

  /**
   * process not exists with correlated
   * not exists will be transform to exists first
   * and use topselect results minus the EXISTS results
   *
   * @param joinType
   * @throws SqlXlateException
   */
  void processNotExistsC() throws SqlXlateException {
    // treat not exists as exists first, and make exists transformation (left semi)
    this.makeTop();
    // restore the original topSelect
    CommonTree cloneOriginTopSelect = FilterBlockUtil.cloneTree(topSelect);
    this.makeJoin(FilterBlockUtil.createSqlASTNode(subQNode, PantheraParser_PLSQLParser.CROSS_VK,
        PantheraExpParser.LEFTSEMI_STR));
    super.processSelectAsterisk(bottomSelect);
    super.rebuildSelectListByFilter(false, false, bottomAlias, topAlias);
    // add alias for all top query select item
    topAliasList = super.buildSelectListAlias(topAlias, (CommonTree) topSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST));

    // clone the topSelect for later use
    CommonTree cloneMinusTopSelect = FilterBlockUtil.cloneTree(topSelect);

    // the following code is copied from makeEnd().
    // only change function "createSelectListForClosingSelect" in to "createSelectListForClosingSelectWithoutRefreshOrder"
    // closing select list
    CommonTree selectList = super.createSelectListForClosingSelectWithoutRefreshOrder(topAlias, closingSelect, topAliasList);
    if (selectList.getChildCount() == 0) {
      selectList = FilterBlockUtil.cloneSelectListFromSelect((CommonTree) topSelect);
      for (int i = 0; i < selectList.getChildCount(); i++) {
        CommonTree selectItem = (CommonTree) selectList.getChild(i);
        CommonTree expr = (CommonTree) selectItem.getChild(0);
        expr.deleteChild(0);
        expr.addChild(super.createCascatedElement(FilterBlockUtil.cloneTree((CommonTree) selectItem
            .getChild(1).getChild(0))));
      }
    }
    closingSelect.addChild(selectList);

    AddAllNeededFilters();
    CommonTree tempTop = topSelect;
    topSelect = cloneMinusTopSelect;
    // add again here
    AddAllNeededFilters();
    topSelect = tempTop;
    CommonTree cloneMinusList = (CommonTree) cloneMinusTopSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    if (cloneMinusList != null) {
      for (int index = 0; index < cloneMinusList.getChildCount(); index++) {
        cloneMinusList.replaceChildren(index, index, FilterBlockUtil.cloneTree((CommonTree) topSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST).getChild(index)));
      }
    }
    // add alias origin
    FilterBlockUtil.addColumnAliasOrigin(cloneMinusTopSelect, context);

    // set closing select to top select
    topSelect = closingSelect;
    // add again here
    AddAllNeededFilters();

    super.buildWhereByFB(null, null, null);

    if (super.hasNotEqualCorrelated) {
      // become inner join if there is not equal correlated.
      join.deleteChild(0);
      // add distinct
      SqlXlateUtil.addCommonTreeChild(this.closingSelect, 1, FilterBlockUtil.createSqlASTNode(
          subQNode, PantheraParser_PLSQLParser.SQL92_RESERVED_DISTINCT, "distinct"));
    }

    // exists subQ transform finished, start to make minus operation
    CommonTree minus = FilterBlockUtil.createSqlASTNode(topSelect,
        PantheraParser_PLSQLParser.PLSQL_RESERVED_MINUS, "minus");
    // topSelect here is EXISTS subQ transformed select
    minus.addChild(topSelect);

    // create top select table ref node
    topTableRefElement = super.createTableRefElement(cloneMinusTopSelect);
    cloneMinusTopSelect.getParent().addChild(minus);
    topAlias = (CommonTree) topTableRefElement.getChild(0);

    // add alias for all top query select item
    topAliasList = super.buildSelectListAlias(topAlias, (CommonTree) cloneOriginTopSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST));

    CommonTree oldFrom = (CommonTree) topSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM);
    // create closing select
    closingSelect = super.createClosingSelect(oldFrom, topTableRefElement);
    selectList = super.createSelectListForClosingSelect(topAlias, closingSelect, topAliasList);
    closingSelect.addChild(selectList);
    topSelect = closingSelect;
  }


  /**
   * process compare operator with uncorrelated
   *
   * @param joinType
   * @throws SqlXlateException
   */
  void processCompareUC() throws SqlXlateException {

    // must have aggregation function in sub query
    this.processAggregationCompareUC();
  }

  private void processAggregationCompareUC() throws SqlXlateException {

    CommonTree compareElement = super.getSubQOpElement();

    CommonTree sq = fbContext.getSelectStack().pop();
    if (PLSQLFilterBlockFactory.getInstance().isCorrelated(this.fbContext.getqInfo(),
        this.fbContext.getSelectStack(), compareElement) != 0) {
      throw new SqlXlateException(compareElement, "not support element from outter query compare with sub-query");
    }
    fbContext.getSelectStack().push(sq);

    this.makeTop();

    // add compare item
    CommonTree cloneCompareElement = FilterBlockUtil.cloneTree(compareElement);

    CommonTree compareElementAlias = super.addSelectItem((CommonTree) topSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST), cloneCompareElement);
    super.rebuildSubQOpElement(compareElement, compareElementAlias);

    this.makeJoin(FilterBlockUtil.createSqlASTNode(subQNode, PantheraParser_PLSQLParser.CROSS_VK, "cross"));

    // compare alias from subq
    CommonTree comparSubqAlias = super.addAlias((CommonTree) ((CommonTree) bottomSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST)).getChild(0));

    this.makeEnd();

    // where
    super.buildWhereBranch(bottomAlias, comparSubqAlias);
  }

  /**
   * process in with uncorrelated
   *
   * @param joinType
   * @throws SqlXlateException
   */
  void processInUC() throws SqlXlateException {

    CommonTree compareElement = super.getSubQOpElement();

    CommonTree sq = fbContext.getSelectStack().pop();
    if (PLSQLFilterBlockFactory.getInstance().isCorrelated(this.fbContext.getqInfo(),
        this.fbContext.getSelectStack(), compareElement) != 0) {
      throw new SqlXlateException(compareElement, "not support element from outter query as IN sub-query node");
    }
    fbContext.getSelectStack().push(sq);

    this.makeTop();

    // add compare item
    // TODO multi parameter in sub query IN
    CommonTree compareElementAlias = super.addSelectItem((CommonTree) topSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST), super.cloneSubQOpElement());

    this.makeJoin(FilterBlockUtil.createSqlASTNode(subQNode, PantheraExpParser.LEFTSEMI_VK, "leftsemi"));

    // compare alias from subq
    if (bottomSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST) == null) {
      throw new SqlXlateException(bottomSelect, "No select-list or select * in subquery!");
    }
    CommonTree comparSubqAlias = super.addAlias((CommonTree) ((CommonTree) bottomSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST)).getChild(0));

    // on
    // FIXME which is first?
    CommonTree equal = FilterBlockUtil.createSqlASTNode(subQNode, PantheraParser_PLSQLParser.EQUALS_OP, "=");
    CommonTree on = super.buildOn(equal, super.createCascatedElementWithTableName(equal,
        (CommonTree) topAlias.getChild(0), (CommonTree) compareElementAlias.getChild(0)), super
        .createCascatedElementWithTableName(equal, (CommonTree) bottomAlias.getChild(0),
            (CommonTree) comparSubqAlias.getChild(0)));
    join.addChild(on);

    this.makeEnd();
  }

  void processInC() throws SqlXlateException {
    CommonTree compareElement = super.getSubQOpElement();

    CommonTree sq = fbContext.getSelectStack().pop();
    if (PLSQLFilterBlockFactory.getInstance().isCorrelated(this.fbContext.getqInfo(),
        this.fbContext.getSelectStack(), compareElement) != 0) {
      throw new SqlXlateException(compareElement, "not support element from outter query as IN sub-query node");
    }
    fbContext.getSelectStack().push(sq);

    this.makeTop();

    // add compare item
    // TODO multi parameter in sub query IN
    CommonTree compareElementAlias = super.addSelectItem((CommonTree) topSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST), super.cloneSubQOpElement());

    this.makeJoin(FilterBlockUtil.createSqlASTNode(subQNode, PantheraExpParser.LEFTSEMI_VK, "leftsemi"));

    // compare alias from subq
    CommonTree comparSubqAlias = super.addAlias((CommonTree) ((CommonTree) bottomSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST)).getChild(0));

    // on
    CommonTree equal = FilterBlockUtil.createSqlASTNode(subQNode, PantheraParser_PLSQLParser.EQUALS_OP, "=");
    CommonTree leftChild = compareElementAlias.getType() == PantheraParser_PLSQLParser.ALIAS ? super
        .createCascatedElementWithTableName(equal, (CommonTree) topAlias.getChild(0),
            (CommonTree) compareElementAlias.getChild(0))
        : compareElementAlias;
    CommonTree on = super.buildOn(equal, leftChild, super
        .createCascatedElementWithTableName(equal, (CommonTree) bottomAlias.getChild(0),
            (CommonTree) comparSubqAlias.getChild(0)));
    join.addChild(on);

    super.rebuildSelectListByFilter(false, false, bottomAlias, topAlias);
    this.makeEnd();
    super.buildWhereByFB(null, null, null);
  }

  /**
   * process not in with uncorrelated
   *
   * @param joinType
   * @throws SqlXlateException
   */
  void processNotInUC() throws SqlXlateException {
    CommonTree compareElement = super.getSubQOpElement();

    CommonTree sq = fbContext.getSelectStack().pop();
    if (PLSQLFilterBlockFactory.getInstance().isCorrelated(this.fbContext.getqInfo(),
        this.fbContext.getSelectStack(), compareElement) != 0) {
      throw new SqlXlateException(compareElement, "not support element from outter query as NOT_IN sub-query node");
    }
    fbContext.getSelectStack().push(sq);

    CommonTree joinType = FilterBlockUtil.createSqlASTNode(subQNode, PantheraParser_PLSQLParser.CROSS_VK,
        "cross");
    this.makeTop();


    // add compare item
    List<CommonTree> compareElementAlias = super.addSelectItems4In(topSelect, super.subQNode);


    this.makeJoin(joinType);

    CommonTree selectList = (CommonTree) bottomSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    // compare alias from subq
    List<CommonTree> comparSubqAlias = null;
    if (!super.needAddOneLevel4CollectSet(selectList)) {
      super.rebuildCollectSet();
      comparSubqAlias = super.buildSelectListAlias(bottomAlias, selectList);
    } else { // need to add one SELECT level for collect_set
      comparSubqAlias = super.buildSelectListAlias(bottomAlias, selectList);
      CommonTree newLevelSelect = super.addOneLevel4CollectSet(bottomSelect, bottomAlias, comparSubqAlias);
      CommonTree subQuery = (CommonTree) bottomSelect.getParent();
      bottomSelect.getParent().replaceChildren(bottomSelect.getChildIndex(), bottomSelect.getChildIndex(), newLevelSelect);
      bottomSelect = newLevelSelect;
      super.rebuildCollectSet();
    }

    this.makeEnd();

    // where
    // FIXME which is first?
    CommonTree where = super.buildWhere(FilterBlockUtil.dupNode(subQNode), comparSubqAlias,
        compareElementAlias, bottomAlias, topAlias);
    closingSelect.addChild(where);
    super.rebuildArrayContains((CommonTree) where.getChild(0));
  }

  /**
   * process not exists with correlated
   *
   * @param joinType
   * @throws SqlXlateException
   */
  void processNotExistsCByLeftJoin() throws SqlXlateException {
    this.makeTop();
    super.joinTypeNode = FilterBlockUtil.createSqlASTNode(subQNode, PantheraParser_PLSQLParser.CROSS_VK,
        PantheraExpParser.LEFT_STR);
    this.makeJoin(joinTypeNode);

    // for optimizing not equal condition
    // Map<joinType node,List<not equal condition node>>
    Map<CommonTree, List<CommonTree>> joinMap = (Map<CommonTree, List<CommonTree>>) super.context
        .getBallFromBasket(TranslateContext.JOIN_TYPE_NODE_BALL);
    if (joinMap == null) {
      joinMap = new HashMap<CommonTree, List<CommonTree>>();
      super.context.putBallToBasket(TranslateContext.JOIN_TYPE_NODE_BALL, joinMap);
    }

    joinMap.put(joinTypeNode, new ArrayList<CommonTree>());

    super.processSelectAsterisk(bottomSelect);
    //for not exists correlated subquery, origin column in select list is meaningless, remove it;
    this.removeAllSelectItem(bottomSelect);
    super.rebuildSelectListByFilter(true, false, bottomAlias, topAlias);
    this.makeEnd();
    super.buildWhereByFB(null, null, null);
  }

  // used for not exist correlated subquery, origin select column is no use
  private void removeAllSelectItem(CommonTree select) {
    CommonTree selectList = (CommonTree) select
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    while (selectList.getChildCount() != 0) {
      selectList.deleteChild(0);
    }
  }

  public void processExistsUC() throws SqlXlateException {
    this.makeTop();
    this.makeJoin(FilterBlockUtil.createSqlASTNode(subQNode, PantheraExpParser.LEFTSEMI_VK, "leftsemi"));
    this.makeEnd();
  }

  public void processNotExistsUC() throws SqlXlateException {
    this.makeTop();
    try{
      CommonTree oldSelect = (CommonTree) subQNode.getChild(0).getChild(0);
      bottomSelect = super.reCreateBottomSelect(oldSelect, super.createTableRefElement(bottomSelect), super
          .createCountAsteriskSelectList());
    } catch (NullPointerException e) {
      throw new SqlXlateException(subQNode, "subquery is not complete.");
    }
    this.makeJoin(FilterBlockUtil.createSqlASTNode(subQNode, PantheraExpParser.CROSS_VK, "cross"));
    this.makeEnd();
    super.reBuildNotExist4UCWhere(subQNode.parent, topSelect, (CommonTree) this.bottomAlias.getChild(0));
  }
}
