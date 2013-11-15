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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.PantheraExpParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockUtil;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * Support not equal correlated condition in subquery.
 *
 * NotEqualJoinTransformer.
 *
 */
public class NotEqualJoinTransformer extends BaseSqlASTTransformer {
  SqlASTTransformer tf;

  public NotEqualJoinTransformer(SqlASTTransformer tf) {
    this.tf = tf;
  }

  @Override
  public void transform(CommonTree tree, TranslateContext context) throws SqlXlateException {
    tf.transformAST(tree, context);
    this.transformNotEqualJoin(tree, context);
  }

  private void transformNotEqualJoin(CommonTree tree, TranslateContext context)
      throws SqlXlateException {
    Map<CommonTree, List<CommonTree>> joinMap = (Map<CommonTree, List<CommonTree>>) context
        .getBallFromBasket(TranslateContext.JOIN_TYPE_NODE_BALL);
    if (joinMap != null) {
      for (Entry<CommonTree, List<CommonTree>> entry : joinMap.entrySet()) {
        CommonTree joinType = entry.getKey();
        List<CommonTree> notEqualConditionList = entry.getValue();
        if (notEqualConditionList != null && !notEqualConditionList.isEmpty()) {
          if (PantheraExpParser.LEFT_STR.equals(joinType.getText())) {
            processLeftJoin((CommonTree) joinType.getParent(), notEqualConditionList);
          }
          if (PantheraExpParser.LEFTSEMI_STR.equals(joinType.getText())) {
            processLeftSemiJoin((CommonTree) joinType.getParent(), notEqualConditionList);
          }
        }
      }
    }
  }

  /**
   * process not equal in left join(from not exists)
   *
   * @param join
   * @param notEqualConditionList
   * @throws SqlXlateException
   */
  private void processLeftJoin(CommonTree join, List<CommonTree> notEqualConditionList)
      throws SqlXlateException {
    String tableAlias = join.getFirstChildWithType(PantheraExpParser.TABLE_REF_ELEMENT).getChild(0)
        .getChild(0).getText();
    CommonTree select = FilterBlockUtil.findOnlyNode(join, PantheraExpParser.SQL92_RESERVED_SELECT);
    CommonTree selectList = (CommonTree) select
        .getFirstChildWithType(PantheraExpParser.SELECT_LIST);
    CommonTree on = FilterBlockUtil.findOnlyNode(join, PantheraExpParser.SQL92_RESERVED_ON);
    // TODO for some conditions there will be no on branch after subUnnestTransformer, see bug 4413
    if (on == null) {
      throw new SqlXlateException(join, "subquery must have equal condition or filter in where clause"
          + "and can not have different equal condition on two sids of \"or\" operator");
    }
    CommonTree where = FilterBlockUtil.createSqlASTNode(join, PantheraExpParser.SQL92_RESERVED_WHERE,
        "where");
    CommonTree logic = FilterBlockUtil.createSqlASTNode(join, PantheraExpParser.LOGIC_EXPR, "LOGIC_EXPR");
    where.addChild(logic);
    select.addChild(where);
    assert (on != null && selectList != null && tableAlias != null);

    // process join branch
    List<Map<Boolean, List<CommonTree>>> joinKeys = this.getFilterkey(tableAlias, (CommonTree) on
        .getChild(0).getChild(0));
    for (int i = 0; i < joinKeys.size(); i++) {
      List<CommonTree> bottomKeys = joinKeys.get(i).get(false);
      List<CommonTree> topKeys = joinKeys.get(i).get(true);
      // correlated condition with unary operator, e.g. IS NULL, IS NOT NULL
      // TODO is there anything to do here? handle this condition
      if (bottomKeys == null && topKeys != null) {
        continue;
      }
      CommonTree op = (CommonTree) bottomKeys.get(0).getParent();
      // uncorrelated condition,transfer it to WHERE condition and remove it from SELECT_ITEM
      if (bottomKeys != null && topKeys == null) {
        FilterBlockUtil.deleteTheNode(op);
        if (logic.getChildCount() > 0) {
          CommonTree and = FilterBlockUtil
              .createSqlASTNode(join, PantheraExpParser.SQL92_RESERVED_AND, "and");
          and.addChild((CommonTree)logic.deleteChild(0));
          and.addChild(op);
          logic.addChild(and);
        } else {
          logic.addChild(op);
        }
        for (CommonTree bottomkey : bottomKeys) {
          CommonTree anyElement = (CommonTree) bottomkey.getChild(0);
          // must have tablename under ANYELEMENT node
          anyElement.deleteChild(0);
          CommonTree column = (CommonTree) anyElement.getChild(0);
          String columnAlias = column.getText();
          for (int j = 0; j < selectList.getChildCount(); j++) {
            CommonTree selectItem = (CommonTree) selectList.getChild(j);
            CommonTree selectAnyElement = FilterBlockUtil.findOnlyNode(selectItem, PantheraParser_PLSQLParser.ANY_ELEMENT);
            String selectColumn = selectAnyElement.getChild(0).getText();
            String selectAlias = "";
            if (selectItem.getChildCount() > 1) {
              selectAlias = selectItem.getChild(1).getChild(0).getText();
            }
            if (selectAlias.equals(columnAlias)) {
              column.getToken().setText(selectColumn);
              selectList.deleteChild(j);
              break;
            }
          }
        }
      }
      // correlated equal condition,group by it, only process one condition now.
      if (bottomKeys != null && topKeys != null && op.getType() == PantheraExpParser.EQUALS_OP) {
        CommonTree group = (CommonTree) select.getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_GROUP);
        if (group == null) {
          group = FilterBlockUtil.createSqlASTNode(op, PantheraExpParser.SQL92_RESERVED_GROUP,
              "group");
          select.addChild(group);
        }
        // FIXME the GROUP BY element might be the same, then will be duplicated group columns.
        // Currently this issue is handled in RedundantSelectGroupItemTransformer.
        CommonTree groupByElement = FilterBlockUtil.createSqlASTNode(
            op, PantheraExpParser.GROUP_BY_ELEMENT, "GROUP_BY_ELEMENT");
        if (group.getChildCount() != 0
            && group.getChild(group.getChildCount() - 1).getType() == PantheraExpParser.SQL92_RESERVED_HAVING) {
          SqlXlateUtil.addCommonTreeChild(group, group.getChildCount() - 1, groupByElement);
        } else {
          group.addChild(groupByElement);
        }
        CommonTree expr = FilterBlockUtil.createSqlASTNode(op, PantheraExpParser.EXPR, "EXPR");
        groupByElement.addChild(expr);
        CommonTree cascatedElement = FilterBlockUtil.cloneTree((CommonTree) bottomKeys.get(0));
        // must have tablename under ANYELEMENT node
        cascatedElement.getChild(0).deleteChild(0);
        String columnAlias = cascatedElement.getChild(0).getChild(0).getText();
        for (int j = 0; j < selectList.getChildCount(); j++) {
          CommonTree selectItem = (CommonTree) selectList.getChild(j);
          String selectColumn = selectItem.getChild(0).getChild(0).getChild(0).getChild(0)
              .getText();
          String selectAlias = "";
          if (selectItem.getChildCount() > 1) {
            selectAlias = selectItem.getChild(1).getChild(0).getText();
          } else {
            //selectAlias = selectColumn;
          }
          if (selectAlias.equals(columnAlias)) {
            ((CommonTree) cascatedElement.getChild(0).getChild(0)).getToken().setText(selectColumn);
          }
        }
        expr.addChild(cascatedElement);
      }
    }

    // Delete the where branch if no uncorrelated condition under logic node
    if (logic.getChildCount() == 0) {
      where.getParent().deleteChild(where.getChildIndex());
    }

    // process where branch corresponding to outer select
    CommonTree whereLogicExpr = (CommonTree) ((CommonTree) join.getParent().getParent().getParent())
        .getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_WHERE).getChild(0);
    for (CommonTree notEqualNode : notEqualConditionList) {
      Map<Boolean, List<CommonTree>> key = this.getFilter(tableAlias, notEqualNode);
      List<CommonTree> bottomKeyList = key.get(false);
      // if no uncorrelated condition exist, only column in top select exists
      // FIXME the uncorrelated condition will cause wrong results
      // TODO for this condition, move the condition to the where node in left select table instead of current select table
      // e.g. select a from t1 where not exists (select b from t2 where t2.c = t1.c and t1.d is null);
      if (bottomKeyList == null) {
        continue;
      }
      CommonTree bottomKey = bottomKeyList.get(0);
      String bottomKeyAilas = bottomKey.getChild(0).getChild(1).getText();
      String countAliasStr = "";
      // process SELECT LIST
      for (int j = 0; j < selectList.getChildCount(); j++) {
        CommonTree selectItem = (CommonTree) selectList.getChild(j);
        String selectAlias = "";
        if (selectItem.getChildCount() > 1) {
          selectAlias = selectItem.getChild(1).getChild(0).getText();
        }
        if (selectAlias.equals(bottomKeyAilas)) {
          // count
          CommonTree countSelectItem = FilterBlockUtil.cloneTree(selectItem);
          CommonTree countAlias = (CommonTree) countSelectItem.getChild(1).getChild(0);
          countAliasStr = countAlias.getText() + "count";
          countAlias.getToken().setText(countAliasStr);
          CommonTree countCascatedElement = (CommonTree) countSelectItem.getChild(0).deleteChild(0);
          CommonTree countStandardFunction = FilterBlockUtil.createSqlASTNode(
              notEqualNode, PantheraParser_PLSQLParser.STANDARD_FUNCTION, "STANDARD_FUNCTION");
          CommonTree function = FilterBlockUtil.createSqlASTNode(
              notEqualNode, PantheraParser_PLSQLParser.COUNT_VK, "count");
          countStandardFunction.addChild(function);
          CommonTree distinct = FilterBlockUtil.createSqlASTNode(
              notEqualNode, PantheraParser_PLSQLParser.SQL92_RESERVED_DISTINCT, "distinct");
          function.addChild(distinct);
          CommonTree expr = FilterBlockUtil.createSqlASTNode(notEqualNode, PantheraParser_PLSQLParser.EXPR,
              "EXPR");
          function.addChild(expr);
          expr.addChild(countCascatedElement);
          countSelectItem.getChild(0).addChild(countStandardFunction);
          selectList.addChild(countSelectItem);

          // max
          CommonTree maxCascatedElement = (CommonTree) selectItem.getChild(0).deleteChild(0);
          CommonTree standardFunction = FilterBlockUtil.createFunction(notEqualNode, "max", maxCascatedElement);
          selectItem.getChild(0).addChild(standardFunction);
        }
      }
      // process WHERE
      FilterBlockUtil.deleteTheNode(notEqualNode);
      notEqualNode.getToken().setType(PantheraExpParser.EQUALS_OP);
      notEqualNode.getToken().setText("=");

      // handle NULL result
      CommonTree leftTopOr = FilterBlockUtil.createSqlASTNode(notEqualNode, PantheraExpParser.SQL92_RESERVED_OR, "or");
      for (int i = 0; i < 2; i++){
        if (notEqualNode.getChild(i).getType() == PantheraParser_PLSQLParser.CASCATED_ELEMENT
            && notEqualNode.getChild(i).getChild(0).getType() == PantheraParser_PLSQLParser.ANY_ELEMENT) {
          CommonTree isNull = FilterBlockUtil.createSqlASTNode(notEqualNode, PantheraParser_PLSQLParser.IS_NULL, "IS_NULL");
          isNull.addChild(FilterBlockUtil.cloneTree((CommonTree)notEqualNode.getChild(i)));
          leftTopOr.addChild(isNull);
        }
      }
      if (leftTopOr.getChildCount() < 1) {
        throw new SqlXlateException(notEqualNode, "no valid column under notEqualNode:" + notEqualNode.getText());
      }
      if (leftTopOr.getChildCount() == 2) {
        CommonTree leftSubOr = FilterBlockUtil.createSqlASTNode(notEqualNode, PantheraExpParser.SQL92_RESERVED_OR, "or");
        for (int i = 0; i < 2; i++){
          leftSubOr.addChild((CommonTree)leftTopOr.deleteChild(0));
        }
        leftTopOr.addChild(leftSubOr);
      }
      leftTopOr.addChild(notEqualNode);

      CommonTree equal = FilterBlockUtil.createSqlASTNode(notEqualNode, PantheraExpParser.LESS_THAN_OP, "<");
      CommonTree cascated = FilterBlockUtil.createSqlASTNode(notEqualNode, PantheraExpParser.CASCATED_ELEMENT,
          "CASCATED_ELEMENT");
      equal.addChild(cascated);
      CommonTree anyElement = FilterBlockUtil.createSqlASTNode(notEqualNode, PantheraExpParser.ANY_ELEMENT,
          "ANY_ELEMENT");
      cascated.addChild(anyElement);
      CommonTree id = FilterBlockUtil.createSqlASTNode(notEqualNode, PantheraExpParser.ID, countAliasStr);
      anyElement.addChild(id);
      CommonTree one = FilterBlockUtil.createSqlASTNode(notEqualNode, PantheraExpParser.UNSIGNED_INTEGER, "2");
      equal.addChild(one);
      CommonTree and = FilterBlockUtil
          .createSqlASTNode(notEqualNode, PantheraExpParser.SQL92_RESERVED_AND, "and");
      and.addChild(equal);
      and.addChild(leftTopOr);
      CommonTree or = FilterBlockUtil.createSqlASTNode(notEqualNode, PantheraExpParser.SQL92_RESERVED_OR, "or");
      or.addChild(and);
      CommonTree oldCondition = (CommonTree) whereLogicExpr.deleteChild(0);
      or.addChild(oldCondition);
      whereLogicExpr.addChild(or);
    }
  }

  private void processLeftSemiJoin(CommonTree join, List<CommonTree> notEqualConditionList)
      throws SqlXlateException {
    // TODO
  }

  /**
   * extract join key from filter op node.
   *
   * @return false: bottom keys<br>
   *         true: top key
   * @throws SqlXlateException
   */
  Map<Boolean, List<CommonTree>> getFilter(String currentTableAlias, CommonTree filterOp)
      throws SqlXlateException {
    Map<Boolean, List<CommonTree>> result = new HashMap<Boolean, List<CommonTree>>();
    for (int i = 0; i < filterOp.getChildCount(); i++) {
      CommonTree child = (CommonTree) filterOp.getChild(i);
      if (child.getType() == PantheraParser_PLSQLParser.CASCATED_ELEMENT) {
        String tableAlias = child.getChild(0).getChild(0).getText();
        if (currentTableAlias.equals(tableAlias)) {// uncorrelated
          List<CommonTree> uncorrelatedList = result.get(false);
          if (uncorrelatedList == null) {
            uncorrelatedList = new ArrayList<CommonTree>();
            result.put(false, uncorrelatedList);
          }
          uncorrelatedList.add(child);
        }
        if (!currentTableAlias.equals(tableAlias)) {// correlated
          List<CommonTree> correlatedList = result.get(true);
          if (correlatedList == null) {
            correlatedList = new ArrayList<CommonTree>();
            result.put(true, correlatedList);
          }
          correlatedList.add(child);
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
  List<Map<Boolean, List<CommonTree>>> getFilterkey(String currentTableAlias, CommonTree condition)
      throws SqlXlateException {
    List<Map<Boolean, List<CommonTree>>> result = new ArrayList<Map<Boolean, List<CommonTree>>>();
    this.getWhereKey(currentTableAlias, condition, result);
    return result;
  }

  private void getWhereKey(String currentTableAlias, CommonTree filterOp,
      List<Map<Boolean, List<CommonTree>>> list) throws SqlXlateException {
    if (FilterBlockUtil.isFilterOp(filterOp)) {
      list.add(getFilter(currentTableAlias, filterOp));
      return;
    } else if (FilterBlockUtil.isLogicOp(filterOp)) {
      for (int i = 0; i < filterOp.getChildCount(); i++) {
        getWhereKey(currentTableAlias, (CommonTree) filterOp.getChild(i), list);
      }
    } else {
      throw new SqlXlateException(filterOp, "unknow filter operation:" + filterOp.getText());
    }

  }
}
