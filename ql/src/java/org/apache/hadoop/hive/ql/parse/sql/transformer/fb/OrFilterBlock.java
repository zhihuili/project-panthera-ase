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
import java.util.List;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.PantheraConstants;
import org.apache.hadoop.hive.ql.parse.sql.PantheraExpParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.QueryInfo.Column;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * transform OR to UNION OrFilterBlock.
 *
 */
public class OrFilterBlock extends LogicFilterBlock {

  @Override
  void processChildren(FilterBlockContext fbContext, TranslateContext context)
      throws SqlXlateException {
    for (FilterBlock fb : this.getChildren()) {
      // store transform context
      CommonTree oldQueryForTransfer = fbContext.getQueryStack().peek().cloneSimpleQuery();
      boolean oldHadRebuildQueryForTransfer = fbContext.getQueryStack().peek().getRebuildQueryForTransfer();
      fb.process(fbContext, context);
      // restore transform context in fbContext since both sides of OR are parallel
      fbContext.getQueryStack().peek().setQueryForTransfer(oldQueryForTransfer);
      if (oldHadRebuildQueryForTransfer) {
        fbContext.getQueryStack().peek().setRebuildQueryForTransfer();
      } else {
        fbContext.getQueryStack().peek().unSetRebuildQueryForTransfer();
      }
    }
  }

  @Override
  public void process(FilterBlockContext fbContext, TranslateContext context)
      throws SqlXlateException {
    QueryBlock topQuery = fbContext.getQueryStack().peek();
    CommonTree goldenSelect;
    if (topQuery.isProcessHaving()) {
      goldenSelect = topQuery.cloneTransformedQuery();
    } else {
      goldenSelect = topQuery.cloneSimpleQuery();
      if (topQuery.getCountAsterisk() != null && topQuery.getCountAsterisk().isOnlyAsterisk) {
        CommonTree aster = topQuery.getCountAsterisk().getSelectItem();
        int position = topQuery.getCountAsterisk().getPosition();
        CommonTree fr = (CommonTree) goldenSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM);
        CommonTree sl = (CommonTree) goldenSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
        List<Column> columnList = fbContext.getqInfo().getRowInfo(fr);
        for (Column cl : columnList) {
          CommonTree cs = FilterBlockUtil.createCascatedElementBranch(aster, cl.getTblAlias(), cl.getColAlias());
          CommonTree si = FilterBlockUtil.createSqlASTNode(aster, PantheraParser_PLSQLParser.SELECT_ITEM, "SELECT_ITEM");
          CommonTree ex = FilterBlockUtil.createSqlASTNode(aster, PantheraParser_PLSQLParser.EXPR, "EXPR");
          si.addChild(ex);
          ex.addChild(cs);
          SqlXlateUtil.addCommonTreeChild(sl, position++, si);
        }
      }
    }

    processChildren(fbContext, context);

    CommonTree leftSelect = this.getChildren().get(0).getTransformedNode();
    CommonTree rightSelect = this.getChildren().get(1).getTransformedNode();
    List<CommonTree> filters;
    if (topQuery.isProcessHaving()) {
      filters = fbContext.getQueryStack().peek().getHavingFilterColumns();
    } else {
      filters = fbContext.getQueryStack().peek().getWhereFilterColumns();
    }
    if (leftSelect.getType() == PantheraParser_PLSQLParser.SUBQUERY && leftSelect.getChildCount() == 1) {
      leftSelect = (CommonTree) leftSelect.getChild(0);
    }
    if (rightSelect.getType() == PantheraParser_PLSQLParser.SUBQUERY && rightSelect.getChildCount() == 1) {
      rightSelect = (CommonTree) rightSelect.getChild(0);
    }
    AddRelatedFilters(leftSelect, filters, topQuery.isProcessHaving());
    AddRelatedFilters(rightSelect, filters, topQuery.isProcessHaving());
    AddRelatedFilters(goldenSelect, filters, topQuery.isProcessHaving());

    CommonTree topSelect = this.buildUnionSelect(leftSelect, rightSelect, context);

    while (leftSelect.getType() == PantheraParser_PLSQLParser.SUBQUERY) {
      leftSelect = (CommonTree) leftSelect.getChild(0);
    }
    FilterBlockUtil.addColumnAliasOrigin(leftSelect, context);
    FilterBlockUtil.addColumnAliasOrigin(goldenSelect, context);
    topSelect.addChild(FilterBlockUtil.cloneSelectListByAliasFromSelect(leftSelect));
    CommonTree closingSelect = makeEqualJoinByLastFewCol(topSelect, goldenSelect, filters.size(), context);
    this.setTransformedNode(closingSelect);
    fbContext.getQueryStack().peek().setQueryForTransfer(closingSelect);
    fbContext.getQueryStack().peek().setRebuildQueryForTransfer();
  }

  private CommonTree makeEqualJoinByLastFewCol(CommonTree topSelect, CommonTree goldenSelect, int size, TranslateContext context) {
    CommonTree topList = (CommonTree) topSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    CommonTree goldenList = (CommonTree) goldenSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    int topNumber = topList.getChildCount();
    assert(topNumber == goldenList.getChildCount());
    CommonTree closingSelect = FilterBlockUtil.createSqlASTNode(topSelect, PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT, "select");
    CommonTree tableRef = FilterBlockUtil.createSqlASTNode(topSelect, PantheraParser_PLSQLParser.TABLE_REF, "TABLE_REF");
    CommonTree topTableAliasId = FilterBlockUtil.addTableRefElement(tableRef, topSelect, context);
    CommonTree join = FilterBlockUtil.createSqlASTNode(goldenSelect, PantheraParser_PLSQLParser.JOIN_DEF, "join");
    tableRef.addChild(join);
    CommonTree goldenTableAliasId = FilterBlockUtil.addTableRefElement(join, goldenSelect, context);
    CommonTree topFrom = (CommonTree) topSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM);
    CommonTree from = FilterBlockUtil.createSqlASTNode(topFrom, PantheraParser_PLSQLParser.SQL92_RESERVED_FROM, "from");
    closingSelect.addChild(from);
    from.addChild(tableRef);
    CommonTree on = FilterBlockUtil.createSqlASTNode(closingSelect, PantheraParser_PLSQLParser.SQL92_RESERVED_ON, "on");
    join.addChild(on);
    CommonTree logicExpr = FilterBlockUtil.createSqlASTNode(closingSelect, PantheraParser_PLSQLParser.LOGIC_EXPR, "LOGIC_EXPR");
    on.addChild(logicExpr);
    for (int i = 0; i < topNumber; i++) {
      // topSelect and goldenSelect is sure to have alias
      CommonTree leftId = (CommonTree) topList.getChild(i).getChild(1).getChild(0);
      CommonTree leftOp = FilterBlockUtil.createCascatedElementBranch(leftId, topTableAliasId.getText(), leftId.getText());
      CommonTree rightId = (CommonTree) goldenList.getChild(i).getChild(1).getChild(0);
      CommonTree rightOp = FilterBlockUtil.createCascatedElementBranch(rightId, goldenTableAliasId.getText(), rightId.getText());
      CommonTree equalns = FilterBlockUtil.createSqlASTNode(closingSelect, PantheraExpParser.EQUALS_NS, "<=>");
      equalns.addChild(leftOp);
      equalns.addChild(rightOp);
      FilterBlockUtil.addConditionToLogicExpr(logicExpr, equalns);
    }
    closingSelect.addChild(makeClosingSelectList(topTableAliasId, topList, topNumber - size));
    return closingSelect;
  }

  private void AddRelatedFilters(CommonTree select, List<CommonTree> filters, boolean isProcessHaving) {
    List<CommonTree> sels = new ArrayList<CommonTree>();
    FilterBlockUtil.findAllSelectForSet(select, sels);
    for (CommonTree sel : sels) {
      CommonTree slsel = (CommonTree) sel.getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
      List<CommonTree> oldAnyElementList = new ArrayList<CommonTree>();
      FilterBlockUtil.findNode(slsel, PantheraParser_PLSQLParser.ANY_ELEMENT, oldAnyElementList);
      String tableName;
      CommonTree from = (CommonTree) sel.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM);
      CommonTree fromFirstTR = (CommonTree) from.getChild(0).getChild(0);
      if (fromFirstTR.getChildCount() == 2) {
        tableName = fromFirstTR.getChild(0).getChild(0).getText();
      } else {
        CommonTree tableViewName = (CommonTree) fromFirstTR.getChild(0).getChild(0).getChild(0);
        tableName = tableViewName.getChild(tableViewName.getChildCount() - 1).getText();
      }
      if (!isProcessHaving) {
        for (CommonTree column : filters) {
          List<CommonTree> nodeList = new ArrayList<CommonTree>();
          FilterBlockUtil.findNode(column, PantheraParser_PLSQLParser.CASCATED_ELEMENT, nodeList);
          for (CommonTree cascated : nodeList) {
            CommonTree cNameNode;
            if (cascated.getChild(0).getChildCount() > 1) {
              cNameNode = (CommonTree) column.getChild(0).getChild(1);
              if (!tableName.startsWith(PantheraConstants.PANTHERA_PREFIX)) {
                tableName = column.getChild(0).getChild(0).getText();
              }
            } else {
              cNameNode = (CommonTree) column.getChild(0).getChild(0);
              if (!tableName.startsWith(PantheraConstants.PANTHERA_PREFIX)) {
                tableName = "";
              }
            }
            String columnName = cNameNode.getText();
            if (tableName.equals("")) {
              CommonTree singleColumn = FilterBlockUtil.createSqlASTNode(cascated, PantheraParser_PLSQLParser.ID, columnName);
              FilterBlockUtil.addSelectItem(slsel, FilterBlockUtil.createCascatedElement(singleColumn));
            } else {
              for (CommonTree oldAny:oldAnyElementList) {
                if (oldAny.getChildCount() == 1 && oldAny.getChild(0).getText().equals(columnName)) {
                  CommonTree singleTable = FilterBlockUtil.createSqlASTNode(cascated, PantheraParser_PLSQLParser.ID, tableName);
                  SqlXlateUtil.addCommonTreeChild(oldAny, 0, singleTable);
                }
              }
              FilterBlockUtil.addSelectItem(slsel, FilterBlockUtil.createCascatedElementBranch(cascated, tableName, columnName));
            }
          }
        }
      } else {
        for (CommonTree column : filters) {
          CommonTree cascated = FilterBlockUtil.findOnlyNode(column, PantheraParser_PLSQLParser.CASCATED_ELEMENT);
          CommonTree cNameNode;
          if (cascated.getChild(0).getChildCount() > 1) {
            cNameNode = (CommonTree) column.getChild(0).getChild(1);
            if (!tableName.startsWith(PantheraConstants.PANTHERA_PREFIX)) {
              tableName = column.getChild(0).getChild(0).getText();
            }
          } else {
            cNameNode = (CommonTree) column.getChild(0).getChild(0);
            if (!tableName.startsWith(PantheraConstants.PANTHERA_PREFIX)) {
              tableName = "";
            }
          }
          String columnName = cNameNode.getText();
          if (tableName.equals("")) {
            CommonTree singleColumn = FilterBlockUtil.createSqlASTNode(cascated, PantheraParser_PLSQLParser.ID, columnName);
            FilterBlockUtil.addSelectItem(slsel, FilterBlockUtil.createCascatedElement(singleColumn));
          } else {
            for (CommonTree oldAny:oldAnyElementList) {
              if (oldAny.getChildCount() == 1 && oldAny.getChild(0).getText().equals(columnName)) {
                CommonTree singleTable = FilterBlockUtil.createSqlASTNode(cascated, PantheraParser_PLSQLParser.ID, tableName);
                SqlXlateUtil.addCommonTreeChild(oldAny, 0, singleTable);
              }
            }
            FilterBlockUtil.addSelectItem(slsel, FilterBlockUtil.createCascatedElementBranch(cascated, tableName, columnName));
          }
        }
      }
    }
  }

  private CommonTree buildUnionSelect(CommonTree leftSelect, CommonTree rightSelect,
      TranslateContext context) {
    CommonTree union = FilterBlockUtil.createSqlASTNode(this.getASTNode(), PantheraExpParser.SQL92_RESERVED_UNION,
        "union");
    union.addChild(rightSelect);
    CommonTree topSelect = FilterBlockUtil.createSqlASTNode(
        leftSelect, PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT, "select");
    CommonTree subquery = FilterBlockUtil.makeSelectBranch(topSelect, context,
        (CommonTree) leftSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM));
    subquery.addChild(leftSelect);
    subquery.addChild(union);
    return topSelect;
  }


  public CommonTree makeClosingSelectList(CommonTree tableAliasId, CommonTree originalSelectList, int selectNum) {
    CommonTree selectList = FilterBlockUtil.createSqlASTNode(originalSelectList, PantheraExpParser.SELECT_LIST, "SELECT_LIST");
    for (int i = 0; i < selectNum; i++) {
      CommonTree originalAlias = (CommonTree) originalSelectList.getChild(i).getChild(1);
      CommonTree selectItem = FilterBlockUtil.dupNode((CommonTree) originalSelectList.getChild(i));
      CommonTree expr = FilterBlockUtil.createSqlASTNode(selectItem, PantheraExpParser.EXPR, "EXPR");
      selectItem.addChild(expr);
      CommonTree cascatedElement = FilterBlockUtil.createSqlASTNode(selectItem,
          PantheraExpParser.CASCATED_ELEMENT, "CASCATED_ELEMENT");
      expr.addChild(cascatedElement);
      CommonTree anyElement = FilterBlockUtil.createSqlASTNode(selectItem, PantheraExpParser.ANY_ELEMENT,
          "ANY_ELEMENT");
      cascatedElement.addChild(anyElement);

      assert(originalAlias != null);
      String columnName = originalAlias.getChild(0).getText();
      CommonTree coloumNameSrc = (CommonTree) originalAlias.getChild(0);

      CommonTree alias = FilterBlockUtil.dupNode(originalAlias);
      CommonTree aliasName = FilterBlockUtil.createSqlASTNode(coloumNameSrc, PantheraParser_PLSQLParser.ID, columnName);
      alias.addChild(aliasName);
      selectItem.addChild(alias);
      CommonTree columnId = FilterBlockUtil.createSqlASTNode(coloumNameSrc, PantheraExpParser.ID, columnName);
      CommonTree tableId = FilterBlockUtil.createSqlASTNode(coloumNameSrc, PantheraExpParser.ID, tableAliasId.getText());
      anyElement.addChild(tableId);
      anyElement.addChild(columnId);
      selectList.addChild(selectItem);
    }
    return selectList;
  }

}
