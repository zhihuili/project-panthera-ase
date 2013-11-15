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
import org.apache.hadoop.hive.ql.parse.sql.PantheraExpParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * transform AND to JOIN(by rebuilding left select).<br>
 * AndFilterBlock.
 *
 */
public class AndFilterBlock extends LogicFilterBlock {

  /**
   * this must have two children.
   *
   * @throws SqlXlateException
   */
  @Override
  public void process(FilterBlockContext fbContext, TranslateContext context)
      throws SqlXlateException {
    // super.processChildren(fbContext, context);
    // FilterBlockProcessorFactory.getAndProcessor().process(fbContext, this, context);
    FilterBlock leftFB = this.getChildren().get(0);
    leftFB.process(fbContext, context);
    fbContext.getQueryStack().peek().setQueryForTransfer(leftFB.getTransformedNode());
    fbContext.getQueryStack().peek().setRebuildQueryForTransfer();
    FilterBlock rightFB = this.getChildren().get(1);

    CommonTree condition = rightFB.getASTNode();
    TypeFilterBlock type = fbContext.getTypeStack().peek();
    if (rightFB instanceof UnCorrelatedFilterBlock) {// simple condition
      if (type instanceof WhereFilterBlock) {
        rebuildWhereCondition(leftFB, condition);
      }
      if (type instanceof HavingFilterBlock) {
        rebuildHavingCondition(leftFB, condition);
      }
      this.setTransformedNode(leftFB.getTransformedNode());
    } else {
      rightFB.process(fbContext, context);
      if (rightFB instanceof CorrelatedFilterBlock) {// Correlated
        fbContext.getTypeStack().pop();
        TypeFilterBlock outType = fbContext.getTypeStack().peek();
        if (outType instanceof WhereFilterBlock) {
          this.rebuildSelectList(rightFB.getTransformedNode());
        }
        fbContext.getTypeStack().push(type);
      }
      this.setTransformedNode(rightFB.getTransformedNode());
    }
  }

  private void rebuildWhereCondition(FilterBlock leftFB, CommonTree condition) {
    CommonTree transformedSelect = leftFB.getTransformedNode();
    rebuildWhereCond(transformedSelect, condition);
  }

  private void rebuildWhereCond(CommonTree transformedSelect, CommonTree condition) {
    if (transformedSelect.getType() == PantheraParser_PLSQLParser.SUBQUERY) {
      for (int i = 0; i < transformedSelect.getChildCount(); i++) {
        rebuildWhereCond((CommonTree) transformedSelect.getChild(i), condition);
      }
    } else if (transformedSelect.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT) {
      rebuildWhereCondition(transformedSelect, condition);
    } else if (transformedSelect.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_UNION) { // UNION node
      rebuildWhereCond((CommonTree) transformedSelect.getChild(0), condition);
    }
  }

  private void rebuildWhereCondition(CommonTree transformedSelect, CommonTree condition) {
    CommonTree tableRefElement = (CommonTree) transformedSelect.getChild(0).getChild(0).getChild(0);
    CommonTree subQuery = (CommonTree) tableRefElement.getChild(tableRefElement.getChildCount() - 1).getChild(0)
        .getChild(0).getChild(0);
    List<List<CommonTree>> selects = new ArrayList<List<CommonTree>>();
    for(int i = 0; i < subQuery.getChildCount(); i++) {
      List<CommonTree> selectLists = new ArrayList<CommonTree>();
      FilterBlockUtil.findNode((CommonTree) subQuery.getChild(i),
          PantheraExpParser.SELECT_LIST, selectLists);
      assert(selectLists != null);
      List<CommonTree> oneSelects = new ArrayList<CommonTree>();
      for (CommonTree sl:selectLists) {
        oneSelects.add((CommonTree) sl.getParent());
      }
      selects.add(oneSelects);
    }
    for(List<CommonTree> sels:selects) {
      CommonTree sel = sels.get(0);
      for (int j = 0; j < sels.size(); j++) {
        sel = sels.get(j);
        if(sel.getCharPositionInLine() == FilterBlockUtil.
            firstAncestorOfType(condition, PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT).getCharPositionInLine()) {
          break;
        }
      }
      CommonTree where = (CommonTree) sel
          .getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_WHERE);
      if (where == null) {
        where = FilterBlockUtil.createSqlASTNode(condition, PantheraExpParser.SQL92_RESERVED_WHERE,
            "where");
        CommonTree group = (CommonTree) sel
            .getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_GROUP);
        if (group != null) {
          int groupIndex = group.getChildIndex();
          SqlXlateUtil.addCommonTreeChild(sel, groupIndex, where);
        } else {
          sel.addChild(where);
        }
        CommonTree logicExpr = FilterBlockUtil.createSqlASTNode(condition,
            PantheraExpParser.LOGIC_EXPR, "LOGIC_EXPR");
        where.addChild(logicExpr);
        logicExpr.addChild(condition);
      } else {
        CommonTree logicExpr = (CommonTree) where.getChild(0);
        CommonTree oldChild = (CommonTree) logicExpr.deleteChild(0);
        CommonTree and = FilterBlockUtil.createSqlASTNode(condition,
            PantheraExpParser.SQL92_RESERVED_AND, "and");
        and.addChild(oldChild);
        and.addChild(condition);
        logicExpr.addChild(and);
      }
    }
  }

  private void rebuildHavingCondition(FilterBlock leftFB, CommonTree condition) {
    CommonTree transformedSelect = leftFB.getTransformedNode();
    rebuildHavingCond(transformedSelect, condition);
  }

  private void rebuildHavingCond(CommonTree transformedSelect, CommonTree condition) {
    if (transformedSelect.getType() == PantheraParser_PLSQLParser.SUBQUERY) {
      for (int i = 0; i < transformedSelect.getChildCount(); i++) {
        rebuildHavingCond((CommonTree) transformedSelect.getChild(i), condition);
      }
    } else if (transformedSelect.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT) {
      rebuildHavingCondition(transformedSelect, condition);
    } else if (transformedSelect.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_UNION) { // UNION node
      rebuildHavingCond((CommonTree) transformedSelect.getChild(0), condition);
    }
  }

  private void rebuildHavingCondition(CommonTree transformedSelect, CommonTree condition) {
    CommonTree tableRefElement = (CommonTree) transformedSelect.getChild(0).getChild(0).getChild(0);
    CommonTree subQuery = (CommonTree) tableRefElement.getChild(tableRefElement.getChildCount() - 1).getChild(0)
        .getChild(0).getChild(0);
    List<List<CommonTree>> groups = new ArrayList<List<CommonTree>>();
    for(int i = 0; i < subQuery.getChildCount(); i++){
      List<CommonTree> oneGroups = new ArrayList<CommonTree>();
      FilterBlockUtil.findNode((CommonTree) subQuery.getChild(i),
          PantheraExpParser.SQL92_RESERVED_GROUP, oneGroups);
      assert(oneGroups != null);
      groups.add(oneGroups);
    }
    for(List<CommonTree> grps:groups) {
      CommonTree group = grps.get(0);
      for (int j = 0; j < grps.size(); j++) {
        group = grps.get(j);
        if(group.getCharPositionInLine() == FilterBlockUtil.
            firstAncestorOfType(condition, PantheraParser_PLSQLParser.SQL92_RESERVED_GROUP).getCharPositionInLine()) {
          break;
        }
      }
      CommonTree having = (CommonTree) group
          .getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_HAVING);
      if (having == null) {
        having = FilterBlockUtil.createSqlASTNode(condition, PantheraExpParser.SQL92_RESERVED_HAVING,
            "having");
        group.addChild(having);
        CommonTree logicExpr = FilterBlockUtil.createSqlASTNode(condition,
            PantheraExpParser.LOGIC_EXPR, "LOGIC_EXPR");
        having.addChild(logicExpr);
        logicExpr.addChild(condition);
      } else {
        CommonTree logicExpr = (CommonTree) having.getChild(0);
        CommonTree oldChild = (CommonTree) logicExpr.deleteChild(0);
        CommonTree and = FilterBlockUtil.createSqlASTNode(condition,
            PantheraExpParser.SQL92_RESERVED_AND, "and");
        and.addChild(oldChild);
        and.addChild(condition);
        logicExpr.addChild(and);
      }
    }
  }

  /**
   * add select item to the most left select
   *
   * @param select
   */
  private void rebuildSelectList(CommonTree select) {
    CommonTree thisSelect = FilterBlockUtil.firstAncestorOfType(this.getASTNode(), PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT);
    List<CommonTree> osList = new ArrayList<CommonTree>();
    FilterBlockUtil.findNode(select, PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT, osList);
    CommonTree outerSelect = FilterBlockUtil.findOnlyNodeWithPosition(select, PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT, thisSelect.getCharPositionInLine());
    CommonTree outerSelectList = (CommonTree) outerSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    List<CommonTree> nodeList = new ArrayList<CommonTree>();
    FilterBlockUtil.findNode(outerSelect, PantheraExpParser.SELECT_LIST, nodeList);
    if (!nodeList.isEmpty() && nodeList.get(0) != outerSelectList) {
      CommonTree innerSelectList = nodeList.get(0);
      for (CommonTree node : nodeList) {
        if (node.getParent().getCharPositionInLine() == thisSelect.getCharPositionInLine()) {
          innerSelectList = node;
          break;
        }
      }
      for (int i = 0; i < outerSelectList.getChildCount(); i++) {
        CommonTree anyElement = (CommonTree) outerSelectList.getChild(i).getChild(0).getChild(0)
            .getChild(0);
        String columnName;
        if (anyElement.getChildCount() == 2) {
          columnName = anyElement.getChild(1).getText();
        } else {
          columnName = anyElement.getChild(0).getText();
        }
        List<CommonTree> nameList = new ArrayList<CommonTree>();
        CommonTree selectItem = null;
        boolean isExist = false;
        for (int j = 0; j < innerSelectList.getChildCount(); j++) {
          selectItem = (CommonTree) innerSelectList.getChild(j);
          FilterBlockUtil.findNodeText(selectItem, columnName, nameList);
          if (!nameList.isEmpty()) {
            isExist = true;
            break;
          }
        }
        if (!isExist) {
          CommonTree newSelectItem = FilterBlockUtil.cloneTree((CommonTree) outerSelectList
              .getChild(i));
          newSelectItem.deleteChild(1);
          innerSelectList.addChild(newSelectItem);
        } else {
          // FIXME maybe the found column is not the expected one
          if (selectItem.getChildCount() == 2) {
            String alias = selectItem.getChild(1).getChild(0).getText();
            // make sure the column is in format of table.column, so that will not cause ambiguous columns
            if (anyElement.getChildCount() == 2) {
              // TODO only delete column name, may have problem
              anyElement.deleteChild(anyElement.getChildCount() - 1);
              CommonTree newColumn = FilterBlockUtil.createSqlASTNode((CommonTree) selectItem
                  .getChild(1).getChild(0), PantheraExpParser.ID, alias);
              anyElement.addChild(newColumn);
            } else {
              CommonTree tableRefElement = FilterBlockUtil.firstAncestorOfType(selectItem,
                  PantheraParser_PLSQLParser.TABLE_REF_ELEMENT);
              if (tableRefElement != null
                  && tableRefElement.getChild(0).getType() == PantheraParser_PLSQLParser.ALIAS) {
                String tableAliasName = tableRefElement.getChild(0).getChild(0).getText();
                anyElement.deleteChild(0);
                anyElement.addChild(FilterBlockUtil.createSqlASTNode((CommonTree) selectItem
                    .getChild(1).getChild(0), PantheraExpParser.ID, tableAliasName));
                anyElement.addChild(FilterBlockUtil.createSqlASTNode((CommonTree) selectItem
                    .getChild(1).getChild(0), PantheraExpParser.ID, alias));
              }
            }
          }
        }
      }
    }
  }
}
