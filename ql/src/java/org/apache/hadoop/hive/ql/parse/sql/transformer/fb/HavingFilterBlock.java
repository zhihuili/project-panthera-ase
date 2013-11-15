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
package org.apache.hadoop.hive.ql.parse.sql.transformer.fb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.PantheraExpParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;


public class HavingFilterBlock extends TypeFilterBlock {

  @Override
  void execute(FilterBlockContext fbContext, TranslateContext context) throws SqlXlateException {
    //needn't restore queryblock's aggregation function when process HAVING sub query.
    //Tips:WHERE sub query had been processed.
    fbContext.getQueryStack().peek().setAggregationList(null);
  }

  @Override
  void preExecute(FilterBlockContext fbContext, TranslateContext context) throws SqlXlateException {
    QueryBlock topQuery = fbContext.getQueryStack().peek();

    CommonTree group = (CommonTree) topQuery.getASTNode().getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_GROUP);
    List<CommonTree> ss = new ArrayList<CommonTree>();
    FilterBlockUtil.findNode(group, PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT, ss);

    // delete having. DO it before clone.
    FilterBlockUtil.deleteBranch((CommonTree) topQuery.getASTNode().getFirstChildWithType(
        PantheraParser_PLSQLParser.SQL92_RESERVED_GROUP),
        PantheraParser_PLSQLParser.SQL92_RESERVED_HAVING);

    // needn't group after transformed.
    topQuery.setGroup(null);

    topQuery.setHaving(true);

    CommonTree oldSelect = topQuery.cloneTransformedQuery();
    CommonTree oldFrom = (CommonTree) oldSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM);
    CommonTree newSelect = FilterBlockUtil.createSqlASTNode(oldSelect, PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT, "select");
    String tablename = FilterBlockUtil.makeSelectBranch(newSelect, oldSelect, context);
    CommonTree oldSelectList = (CommonTree) oldSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    if (oldSelectList == null) {
      CommonTree oldSelectAsterisk = (CommonTree) oldSelect.getFirstChildWithType(PantheraParser_PLSQLParser.ASTERISK);
      assert(oldSelectAsterisk != null);
      throw new SqlXlateException(oldSelectAsterisk, "not support HAVING with * in SELECT-LIST");
    }
    CommonTree newSelectList = cloneSelectListByAliasFromSelect(oldSelect, tablename, context);
    newSelect.addChild(newSelectList);
    addAllAggrFilter(tablename, oldSelectList, fbContext, context);
    for (CommonTree littleSel : ss) {
      // refresh table name. only can be done after add all AggrFilter
      CommonTree littleFrom = (CommonTree) littleSel.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM);
      List<CommonTree> anyL = new ArrayList<CommonTree>();
      FilterBlockUtil.findNode(littleSel, PantheraParser_PLSQLParser.ANY_ELEMENT, anyL);
      for (CommonTree anyE : anyL) {
        if (anyE.getChildCount() == 2) {
          String tn = anyE.getChild(0).getText();
          if (SqlXlateUtil.containTableName(tn, oldFrom) && !SqlXlateUtil.containTableName(tn, littleFrom)) {
            //FIXME not all should be replaced
            ((CommonTree) anyE.getChild(0)).getToken().setText(tablename);
          }
        }
      }
    }
    CommonTree oldGroup = (CommonTree) oldSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_GROUP);
    assert(oldGroup != null);
    addGroupElement(oldSelectList, oldGroup, fbContext, context);
    topQuery.setQueryForTransfer(newSelect);
    topQuery.setRebuildQueryForTransfer();
    topQuery.setQueryForHaving(newSelect);
  }

  /**
   * add all group element to lower level select list.
   * add all group element to HavingFilterColumns
   *
   * @param tablename
   * @param oldSelectList
   * @param oldGroup
   * @param fbContext
   * @param context
   * @throws SqlXlateException
   */
  private void addGroupElement(CommonTree oldSelectList, CommonTree oldGroup,
      FilterBlockContext fbContext, TranslateContext context) throws SqlXlateException {
    int groupElementCount = oldGroup.getChildCount();
    if (oldGroup.getChild(groupElementCount - 1).getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_HAVING) {
      groupElementCount--;
    }
    for (int i = 0; i < groupElementCount; i++) {
      CommonTree expr = FilterBlockUtil.cloneTree((CommonTree) oldGroup.getChild(i).getChild(0));
      CommonTree groupCascated = (CommonTree) expr.getChild(0);
      if (groupCascated.getType() != PantheraParser_PLSQLParser.CASCATED_ELEMENT) {
        throw new SqlXlateException(groupCascated, "Panthera only support group by column/table.column");
      }
      fbContext.getQueryStack().peek().getHavingFilterColumns().add(FilterBlockUtil.cloneTree(groupCascated));
      CommonTree selectItem = FilterBlockUtil.createSqlASTNode(expr, PantheraParser_PLSQLParser.SELECT_ITEM, "SELECT_ITEM");
      selectItem.addChild(expr);
      oldSelectList.addChild(selectItem);
    }
  }

  /**
   * add all aggr in HavingFilterColumns to lower level select with group
   * replace HavingFilterColumns with newly built alias
   *
   * @param tablename
   * @param selectList
   * @param fbContext
   * @param context
   */
  private void addAllAggrFilter(String tablename, CommonTree selectList, FilterBlockContext fbContext, TranslateContext context) {
    List<CommonTree> hfc = fbContext.getQueryStack().peek().getHavingFilterColumns();
    Iterator<CommonTree> iter = hfc.iterator();
    List<CommonTree> newhfc = new ArrayList<CommonTree>();
    while (iter.hasNext()) {
      CommonTree standard = iter.next();
      CommonTree alias = FilterBlockUtil.createAlias(standard, context);
      CommonTree selectItem = FilterBlockUtil.createSqlASTNode(standard, PantheraParser_PLSQLParser.SELECT_ITEM, "SELECT_ITEM");
      CommonTree expr = FilterBlockUtil.createSqlASTNode(standard, PantheraParser_PLSQLParser.EXPR, "EXPR");
      CommonTree standardParent = (CommonTree) standard.getParent();
      CommonTree renewCol = FilterBlockUtil.createCascatedElementBranch(alias, tablename, alias.getChild(0).getText());
      standardParent.replaceChildren(standard.childIndex, standard.childIndex, renewCol);
      selectItem.addChild(expr);
      expr.addChild(standard);
      selectItem.addChild(alias);
      selectList.addChild(selectItem);
      newhfc.add(FilterBlockUtil.cloneTree(renewCol));
    }
    fbContext.getQueryStack().peek().setHavingFilterColumns(newhfc);
  }

  /**
   * clone SELECT_LIST by SELECT_ITEM ALIAS
   *
   * @param originalSelectList
   * @return
   */
  private CommonTree cloneSelectListByAliasFromSelect(CommonTree originalSelect, String tablename, TranslateContext context) {
    CommonTree selectList;
    CommonTree originalSelectList = (CommonTree) originalSelect
        .getFirstChildWithType(PantheraExpParser.SELECT_LIST);

    int selectNum = originalSelectList.getChildCount();
    selectList = FilterBlockUtil.createSqlASTNode(originalSelectList, PantheraExpParser.SELECT_LIST,
        "SELECT_LIST");
    for (int i = 0; i < selectNum; i++) {
      CommonTree originalSelectItem = (CommonTree) originalSelectList.getChild(i);
      CommonTree originalAlias = (CommonTree) originalSelectItem.getChild(1);
      CommonTree selectItem = FilterBlockUtil.dupNode(originalSelectItem);
      if (originalAlias != null) {
        CommonTree expr = FilterBlockUtil.createSqlASTNode(selectItem, PantheraExpParser.EXPR, "EXPR");
        selectItem.addChild(expr);
        CommonTree cascatedElement = FilterBlockUtil.createSqlASTNode(selectItem,
            PantheraExpParser.CASCATED_ELEMENT, "CASCATED_ELEMENT");
        expr.addChild(cascatedElement);
        CommonTree anyElement = FilterBlockUtil.createSqlASTNode(selectItem, PantheraExpParser.ANY_ELEMENT,
            "ANY_ELEMENT");
        cascatedElement.addChild(anyElement);

        String columnName = originalAlias.getChild(0).getText();
        CommonTree coloumNameSrc = (CommonTree) originalAlias.getChild(0);
        CommonTree columnId = FilterBlockUtil.createSqlASTNode(coloumNameSrc, PantheraExpParser.ID, columnName);
        anyElement.addChild(columnId);
      } else if (FilterBlockUtil.findOnlyNode(originalSelectItem, PantheraParser_PLSQLParser.STANDARD_FUNCTION) != null) {
        CommonTree tempAlias = FilterBlockUtil.createAlias(originalSelectItem, context);
        CommonTree expr = FilterBlockUtil.createSqlASTNode(selectItem, PantheraExpParser.EXPR, "EXPR");
        selectItem.addChild(expr);
        originalSelectItem.addChild(tempAlias);
        CommonTree cascatedElement = FilterBlockUtil.createSqlASTNode(selectItem,
            PantheraExpParser.CASCATED_ELEMENT, "CASCATED_ELEMENT");
        expr.addChild(cascatedElement);
        CommonTree anyElement = FilterBlockUtil.createSqlASTNode(selectItem, PantheraExpParser.ANY_ELEMENT,
            "ANY_ELEMENT");
        cascatedElement.addChild(anyElement);

        String columnName = tempAlias.getChild(0).getText();
        CommonTree coloumNameSrc = (CommonTree) tempAlias.getChild(0);
        CommonTree columnId = FilterBlockUtil.createSqlASTNode(coloumNameSrc, PantheraExpParser.ID, columnName);
        anyElement.addChild(columnId);
      } else {
        selectItem = FilterBlockUtil.cloneTree(originalSelectItem);
      }
      selectList.addChild(selectItem);

      List<CommonTree> anyList = new ArrayList<CommonTree>();
      FilterBlockUtil.findNode(selectItem, PantheraParser_PLSQLParser.ANY_ELEMENT, anyList);
      for (CommonTree any : anyList) {
        CommonTree tableN = FilterBlockUtil.createSqlASTNode(any, PantheraParser_PLSQLParser.ID, tablename);
        if (any.getChildCount() == 2) {
          any.replaceChildren(0, 0, tableN);
        } else {
          SqlXlateUtil.addCommonTreeChild(any, 0, tableN);
        }
      }
    }
    return selectList;
  }
}
