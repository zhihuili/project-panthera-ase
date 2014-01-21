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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.PantheraExpParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;


public class HavingFilterBlock extends TypeFilterBlock {

  private int selectStackSize;
  private Set<CommonTree> notRefreshNode;

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
    CommonTree having = (CommonTree) group.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_HAVING);

    // delete having. DO it before clone.
    FilterBlockUtil.deleteBranch((CommonTree) topQuery.getASTNode().getFirstChildWithType(
        PantheraParser_PLSQLParser.SQL92_RESERVED_GROUP),
        PantheraParser_PLSQLParser.SQL92_RESERVED_HAVING);

    // needn't group after transformed.
    topQuery.setGroup(null);

    topQuery.setHaving(true);

    CommonTree oldSelect = topQuery.cloneTransformedQuery();
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
    selectStackSize = fbContext.getSelectStack().size();
    notRefreshNode = getAllUncorrelatedFilterNodes(this);
    refreshTableAliasInHaving(having, fbContext.getSelectStack(), fbContext, tablename);
    CommonTree oldGroup = (CommonTree) oldSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_GROUP);
    assert(oldGroup != null);
    addGroupElement(oldSelectList, oldGroup, fbContext, context);
    topQuery.setQueryForTransfer(newSelect);
    topQuery.setRebuildQueryForTransfer();
    topQuery.setQueryForHaving(newSelect);
    fbContext.getSelectStack().pop();
    fbContext.getSelectStackForTransfer().pop();
    fbContext.getSelectStack().push(newSelect);
    fbContext.getSelectStackForTransfer().push(newSelect);
  }

  private Set<CommonTree> getAllUncorrelatedFilterNodes(FilterBlock fb) {
    Set<CommonTree> ret = new HashSet<CommonTree>();
    for (FilterBlock childFB : fb.getChildren()) {
      if (childFB instanceof SubQFilterBlock) {
        continue;
      } else if (childFB instanceof UnCorrelatedFilterBlock) {
        CommonTree tree = childFB.getASTNode();
        List<CommonTree> list = new ArrayList<CommonTree>();
        FilterBlockUtil.findNode(tree, PantheraParser_PLSQLParser.ANY_ELEMENT, list);
        ret.addAll(list);
      } else {
        ret.addAll(getAllUncorrelatedFilterNodes(childFB));
      }
    }
    return ret;
  }

  /**
   * TODO not refresh table alias in subQuery's uncorrelated filters.
   * @param having
   * @param fbContext
   * @param context
   * @throws SqlXlateException
   */
  private void refreshTableAliasInHaving(CommonTree tree, Stack<CommonTree> selectStack, FilterBlockContext fbContext, String tablename) throws SqlXlateException {
    switch(tree.getType()) {
    case PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT:
      selectStack.push(tree);
      for (int i = 0; i < tree.getChildCount(); i++) {
        refreshTableAliasInHaving((CommonTree) tree.getChild(i), selectStack, fbContext, tablename);
      }
      selectStack.pop();
      break;
    case PantheraParser_PLSQLParser.SQL92_RESERVED_FROM:
    case PantheraParser_PLSQLParser.SELECT_LIST:
      // FROM and SELECT_LIST do not need a refresh.
      break;
    case PantheraParser_PLSQLParser.ANY_ELEMENT:
      for (CommonTree nf : fbContext.getQueryStack().peek().getHavingFilterColumns()) {
        // here new having filters are all CascatedElement, cloned from true ones
        if (FilterBlockUtil.equalsTree(nf, (CommonTree) tree.getParent())) {
          return;
        }
      }
      int level = PLSQLFilterBlockFactory.getInstance().isAnyElementCorrelated(fbContext.getqInfo(), selectStack, tree);
      if (selectStack.size() - level == this.selectStackSize) {
        // this is a reference to this level.
        if (tree.getChildCount() == 2) {
          if (!notRefreshNode.contains(tree)) {
            ((CommonTree) tree.getChild(0)).getToken().setText(tablename);
          }
          FilterBlock possibleWhereFB = this.getParent().getChildren().get(0);
          if (notRefreshNode.contains(tree) && possibleWhereFB instanceof WhereFilterBlock) {
            CommonTree subT = ((WhereFilterBlock) possibleWhereFB).getSubTable();
            ((CommonTree) tree.getChild(0)).getToken().setText(subT.getText());
          }
        }
      }
      break;
    default:
      for (int i = 0; i < tree.getChildCount(); i++) {
        refreshTableAliasInHaving((CommonTree) tree.getChild(i), selectStack, fbContext, tablename);
      }
      break;
    }
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
