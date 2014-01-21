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
import org.apache.hadoop.hive.ql.parse.sql.PantheraConstants;
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
    FilterBlockUtil.AddAllNeededFilters(topSelect, topQuery.isProcessHaving(), whereFilterColumns, havingFilterColumns);

    // set closing select to top select
    topSelect = closingSelect;
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
    assert(bottomSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST).getChildCount() == 1);
    CommonTree compareKeyAlias2 = super.addAlias((CommonTree) ((CommonTree) bottomSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST)).getChild(0));
    super.rebuildSelectListByFilter(false, true, bottomAlias, topAlias);

    this.makeEnd();

    // where
    super.buildWhereByFB(subQNode, compareKeyAlias1, compareKeyAlias2);

  }

  /**
   * process IS_NULL/IS_NOT_NULL operator with correlated<br>
   * (subQuery) is null, this case operator has only one child.
   *
   * @param joinType
   * @throws SqlXlateException
   */
  void processIsIsNotNullC() throws SqlXlateException {

    this.makeTop();
    this.makeJoin(FilterBlockUtil.createSqlASTNode(subQNode, PantheraParser_PLSQLParser.LEFT_VK, "left"));

    // select list
    assert(bottomSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST).getChildCount() == 1);
    CommonTree onlySelectItem = (CommonTree) ((CommonTree) bottomSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST)).getChild(0);
    CommonTree compareKeyAlias = super.addAlias(onlySelectItem);
    boolean needGroup = FilterBlockUtil.isAggrFunc(onlySelectItem);
    // if aggregation, then need group, else no need group
    // TODO is there any requirement for (SUBQUERY is null) filter? Should both be permitted?
    super.rebuildSelectListByFilter(false, needGroup, bottomAlias, topAlias);

    this.makeEnd();

    // where
    CommonTree opBranch = super.buildWhereByFB(subQNode, compareKeyAlias, null);
    // for CrossjoinTransformer which should not optimize isNull/isNotNull node in WHERE.
    context.putBallToBasket(opBranch, true);

  }

  /**
   * process IS_NULL/IS_NOT_NULL operator with uncorrelated<br>
   * (subQuery) is null, this case operator has only one child.
   *
   * @param joinType
   * @throws SqlXlateException
   */
  void processIsIsNotNullUC() throws SqlXlateException {

    this.makeTop();
    this.makeJoin(FilterBlockUtil.createSqlASTNode(subQNode, PantheraParser_PLSQLParser.LEFT_VK, "left"));

    // select list
    assert(bottomSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST).getChildCount() == 1);
    CommonTree compareKeyAlias = super.addAlias((CommonTree) ((CommonTree) bottomSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST)).getChild(0));

    this.makeEnd();

    // where
    CommonTree opBranch = super.buildWhereBranch(bottomAlias, compareKeyAlias);
    // for CrossjoinTransformer which should not optimize isNull/isNotNull node in WHERE.
    context.putBallToBasket(opBranch, true);

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

    // top will become closing after makeEnd
    CommonTree rememberTop = topSelect;
    this.makeEnd();
    // add again here
    FilterBlockUtil.AddAllNeededFilters(cloneMinusTopSelect, topQuery.isProcessHaving(), whereFilterColumns, havingFilterColumns);
    CommonTree cloneMinusList = (CommonTree) cloneMinusTopSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    if (cloneMinusList != null) {
      for (int index = 0; index < cloneMinusList.getChildCount(); index++) {
        cloneMinusList.replaceChildren(index, index, FilterBlockUtil.cloneTree((CommonTree) rememberTop.getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST).getChild(index)));
      }
    }
    // add alias origin
    FilterBlockUtil.addColumnAliasOrigin(cloneMinusTopSelect, context);

    // add again here
    FilterBlockUtil.AddAllNeededFilters(topSelect, topQuery.isProcessHaving(), whereFilterColumns, havingFilterColumns);

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
    CommonTree selectList = super.createSelectListForClosingSelect(topAlias, closingSelect, topAliasList);
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
    // FIXME: this method would not support NULL condition
    if (!super.needAddOneLevel4CollectSet(selectList)) {
      super.rebuildCollectSet();
      comparSubqAlias = super.buildSelectListAlias(bottomAlias, selectList);
    } else { // need to add one SELECT level for collect_set
      comparSubqAlias = super.buildSelectListAlias(bottomAlias, selectList);
      CommonTree newLevelSelect = super.addOneLevel4CollectSet(bottomSelect, bottomAlias, comparSubqAlias);
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

  /**
   * add the subQ node of cascated element with the topSelect's table alias.
   *
   * @param cascated
   * @param select
   */
  void buildAnyElement(CommonTree tree, CommonTree select, boolean isBottom) {
    List<CommonTree> nodelist = new ArrayList<CommonTree>();
    FilterBlockUtil.findNode(tree, PantheraParser_PLSQLParser.CASCATED_ELEMENT, nodelist);
    for (CommonTree cascated:nodelist) {
      CommonTree anyElement = (CommonTree) cascated.getChild(0);
      CommonTree tableName;
      if (anyElement.getChildCount() == 1) {
        CommonTree topFrom = (CommonTree) select
            .getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_FROM);
        CommonTree tableRefElement = (CommonTree) topFrom.getChild(0).getChild(0);
        if (tableRefElement.getChildCount() > 1) {
          tableName = FilterBlockUtil.cloneTree((CommonTree) tableRefElement.getChild(0)
              .getChild(0));
        } else {
          assert(tableRefElement.getChild(0).getChild(0).getType() == PantheraParser_PLSQLParser.DIRECT_MODE);
          tableName = FilterBlockUtil.cloneTree((CommonTree) tableRefElement.getChild(0)
              .getChild(0).getChild(0).getChild(0));
        }
        SqlXlateUtil.addCommonTreeChild(anyElement, 0, tableName);
      } else {
        tableName = (CommonTree) anyElement.getChild(0);
      }
      if (!isBottom) {
        // use a temp alias begin with PantheraConstants.PANTHERA_UPPER as a flag to mark the col is from upper level.
        // and the old name can be restored by removing the prefix.
        // the node is under ON, only influence All/Any/NotInC
        // so need clear ON in ProcessExistsC and ProcessNotExistsC
        // this.upperCols.add(anyElement);
        tableName.getToken().setText(PantheraConstants.PANTHERA_UPPER + tableName.getText());
      }
    }
  }

  /**
   * add the subQ node of cascated element with the topSelect's table alias.
   *
   * @param cascated
   */
  void buildNaiveAnyElement(CommonTree tree) {
    List<CommonTree> nodelist = new ArrayList<CommonTree>();
    FilterBlockUtil.findNode(tree, PantheraParser_PLSQLParser.CASCATED_ELEMENT, nodelist);
    for (CommonTree cascated:nodelist) {
      CommonTree anyElement = (CommonTree) cascated.getChild(0);
      if (anyElement.getType() == PantheraParser_PLSQLParser.ANY_ELEMENT
          && anyElement.getChildCount() == 2) {
        anyElement.deleteChild(0);
      }
    }
  }

  public void processExistsUC() throws SqlXlateException {
    this.makeTop();
    this.makeJoin(FilterBlockUtil.createSqlASTNode(subQNode, PantheraExpParser.LEFTSEMI_VK, "leftsemi"));
    this.makeEnd();
  }

  public void processNotExistsUC() throws SqlXlateException {
    this.makeTop();
    bottomSelect = super.reCreateBottomSelect(bottomSelect, super.createTableRefElement(bottomSelect), super
        .createCountAsteriskSelectList());
    this.makeJoin(FilterBlockUtil.createSqlASTNode(subQNode, PantheraExpParser.CROSS_VK, "cross"));
    this.makeEnd();
    super.reBuildNotExist4UCWhere(subQNode.parent, topSelect, (CommonTree) this.bottomAlias.getChild(0));
  }

  /**
   * for topSelect contains NULL in ALL subQuery
   * if bottomSelect generates nothing, keep this NULL
   * else not keep the NULL
   *
   * @param leftOp
   * @throws SqlXlateException
   */
  public void processAllWindUp(CommonTree leftOp, CommonTree simpleTop) throws SqlXlateException {
    CommonTree cloneAlmostTopSelect = FilterBlockUtil.cloneTree(topSelect);
    // use oldest top select to avoid duplicate calculate
    // if HIVE can cache old subQueries in one query, this would be a bad tradeoff
    topSelect = simpleTop;
    // start to build the exceptional cases query
    this.makeTop();
    // use condition panthera_col_0>0
    bottomSelect = super.reCreateBottomSelect(bottomSelect, super.createTableRefElement(bottomSelect), super
        .createCountAsteriskSelectList());
    this.makeJoin(FilterBlockUtil.createSqlASTNode(subQNode, PantheraExpParser.CROSS_VK, "cross"));
    this.makeEnd();
    // where is (panthera_col_0>0 and leftOp is null)
    // ignore the possible correlated condition, since it is a minus
    // no need to add DISTINCT since the right table only gets one row.
    CommonTree where = super.buildWhereForAll(leftOp);
    closingSelect.addChild(where);

    // exceptional subQ transform finished, start to make minus operation
    CommonTree minus = FilterBlockUtil.createSqlASTNode(topSelect,
        PantheraParser_PLSQLParser.PLSQL_RESERVED_MINUS, "minus");
    // topSelect here is EXISTS subQ transformed select
    minus.addChild(topSelect);
    // add needed filters for cloneAlmostTopSelect
    FilterBlockUtil.AddAllNeededFilters(topSelect, topQuery.isProcessHaving(), whereFilterColumns, havingFilterColumns);

    // create top select table ref node
    topTableRefElement = super.createTableRefElement(cloneAlmostTopSelect);
    cloneAlmostTopSelect.getParent().addChild(minus);
    topAlias = (CommonTree) topTableRefElement.getChild(0);

    // add alias origin
    FilterBlockUtil.addColumnAliasOrigin(cloneAlmostTopSelect, context);

    // add alias for all top query select item
    topAliasList = super.buildSelectListAlias(topAlias, (CommonTree) cloneAlmostTopSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST));

    // add needed filters for cloneAlmostTopSelect
    FilterBlockUtil.AddAllNeededFilters(cloneAlmostTopSelect, topQuery.isProcessHaving(), whereFilterColumns, havingFilterColumns);

    CommonTree oldFrom = (CommonTree) topSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM);
    // create closing select
    closingSelect = super.createClosingSelect(oldFrom, topTableRefElement);
    CommonTree selectList = super.createSelectListForClosingSelect(topAlias, closingSelect, topAliasList);
    closingSelect.addChild(selectList);
    topSelect = closingSelect;
  }

}
