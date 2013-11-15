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
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.QueryInfo.Column;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

public class UnCorrelatedFilterBlock extends NormalFilterBlock {

  @Override
  public void process(FilterBlockContext fbContext, TranslateContext context)
      throws SqlXlateException {

    // this filterBlock do nothing but add back simple condition to original select.
    // the transformation will be done in QueryBlcok.
    if ((fbContext.getSubQStack().size() == fbContext.getQueryStack().size() - 1)) {
      TypeFilterBlock typeFB = fbContext.getTypeStack().peek();
      CommonTree condition = this.getASTNode();
      if (typeFB instanceof WhereFilterBlock) {
        CommonTree topSelect = fbContext.getQueryStack().peek().cloneSimpleQuery();
        restoreAsterisk(topSelect, fbContext);
        putUncorrelatedInWhere(topSelect, condition);
        // to avoid one have alias and another not having alias in both sides of union
        addSelectListAlias(topSelect, context);
        this.setTransformedNode(topSelect);
      }
      if (typeFB instanceof HavingFilterBlock) {
        CommonTree topSelect = fbContext.getQueryStack().peek().cloneTransformedQuery();
        CommonTree tree = FilterBlockUtil.cloneTree(topSelect);
        restoreAsterisk(tree, fbContext);
        handleUncorrelatedInHaving(tree, condition);
        // to avoid one have alias and another not having alias in both sides of union
        addSelectListAlias(tree, context);
        this.setTransformedNode(tree);
      }
      return;
    }

  }

  private void addSelectListAlias(CommonTree topSelect, TranslateContext context) {
    CommonTree selectList = (CommonTree) topSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    for (int i = 0; i < selectList.getChildCount(); i++) {
      CommonTree selectItem = (CommonTree) selectList.getChild(i);
      CommonTree alias = (CommonTree) selectItem.getChild(1);
      CommonTree anyElement = FilterBlockUtil.findOnlyNode(selectItem, PantheraParser_PLSQLParser.ANY_ELEMENT);
      if (anyElement != null && alias == null) {
        alias = FilterBlockUtil.createSqlASTNode(selectItem, PantheraParser_PLSQLParser.ALIAS, "ALIAS");
        selectItem.addChild(alias);
        String aliasStr;
        if (anyElement.getChildCount() == 1) {
          aliasStr = anyElement.getChild(0).getText();
        } else {
          aliasStr = anyElement.getChild(1).getText();
        }
        CommonTree aliasName = FilterBlockUtil.createSqlASTNode(alias,
            PantheraParser_PLSQLParser.ID, aliasStr);
        alias.addChild(aliasName);
      }
    }
  }

  private void restoreAsterisk(CommonTree topSelect, FilterBlockContext fbContext) throws SqlXlateException {
    CommonTree selectList = (CommonTree) topSelect
        .getFirstChildWithType(PantheraExpParser.SELECT_LIST);
    if (selectList != null && selectList.getChildCount() > 0) {
      //FIXME select tablename.* case
      return;
    }
    List<Column> columnList = fbContext.getqInfo().getRowInfo((CommonTree) fbContext.
        getQueryStack().peek().getASTNode().getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_FROM));
    if (columnList != null && columnList.size() > 0) {
      if (selectList == null) {
        if (topSelect.getFirstChildWithType(PantheraExpParser.ASTERISK) != null) {
          //which is *
          CommonTree asterisk = (CommonTree) topSelect.deleteChild(topSelect.getFirstChildWithType(PantheraExpParser.ASTERISK).getChildIndex());
          selectList = FilterBlockUtil.createSqlASTNode(asterisk, PantheraExpParser.SELECT_LIST, "SELECT_LIST");
          topSelect.addChild(selectList);
        } else {
          throw new SqlXlateException(topSelect, "No select-list nor asterisk in select statement");
        }
      }
      int count = 0;
      for (Column column : columnList) {
        CommonTree selectItem = FilterBlockUtil.createSqlASTNode(selectList, PantheraExpParser.SELECT_ITEM,
            "SELECT_ITEM");
        selectList.addChild(selectItem);
        CommonTree expr = FilterBlockUtil.createSqlASTNode(selectList, PantheraExpParser.EXPR, "EXPR");
        selectItem.addChild(expr);
        //add alias to avoid duplicate name in multi-table when expanding *
        CommonTree alias = FilterBlockUtil.createSqlASTNode(selectList, PantheraExpParser.ALIAS, "ALIAS");
        selectItem.addChild(alias);
        CommonTree aliasId = FilterBlockUtil.createSqlASTNode(selectList, PantheraExpParser.ID, "panthera_col_" + count++);
        alias.addChild(aliasId);
        CommonTree cascatedElement = FilterBlockUtil.createSqlASTNode(
            selectList, PantheraExpParser.CASCATED_ELEMENT, "CASCATED_ELEMENT");
        expr.addChild(cascatedElement);
        CommonTree anyElement = FilterBlockUtil.createSqlASTNode(selectList, PantheraExpParser.ANY_ELEMENT,
            "ANY_ELEMENT");
        cascatedElement.addChild(anyElement);
        CommonTree tableName = FilterBlockUtil.createSqlASTNode(selectList, PantheraExpParser.ID, column
            .getTblAlias());
        anyElement.addChild(tableName);
        CommonTree columnName = FilterBlockUtil.createSqlASTNode(selectList, PantheraExpParser.ID, column
            .getColAlias());
        anyElement.addChild(columnName);
      }
    }
  }

  private void handleUncorrelatedInHaving(CommonTree tree, CommonTree condition) {
    CommonTree thisSelect = FilterBlockUtil.firstAncestorOfType(condition, PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT);
    List<CommonTree> selects = new ArrayList<CommonTree>();
    FilterBlockUtil.findNode(tree, PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT, selects);
    int i = 0;
    CommonTree select = tree;
    while (thisSelect.getCharPositionInLine() != select.getCharPositionInLine() && i < selects.size() - 1) {
      select = selects.get(++i);
    }
    if (i < selects.size() - 1) {
      select = selects.get(++i);
    }
    CommonTree group = (CommonTree) select.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_GROUP);
    if (group != null) {
      CommonTree token = (CommonTree) group.getChild(group.getChildCount() - 1);
      if (token.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_HAVING) {
        if (FilterBlockUtil.findOnlyNode(token, PantheraParser_PLSQLParser.SUBQUERY) != null) {
          group.deleteChild(token.childIndex);
        }
      }
      CommonTree having = (CommonTree) group
          .getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_HAVING);
      if (having == null) {
        having = FilterBlockUtil.createSqlASTNode(group, PantheraParser_PLSQLParser.SQL92_RESERVED_HAVING, "having");
        group.addChild(having);
      }
      CommonTree logicExpr = (CommonTree) having
          .getFirstChildWithType(PantheraExpParser.LOGIC_EXPR);
      if (logicExpr == null) {
        logicExpr = FilterBlockUtil.createSqlASTNode(group, PantheraParser_PLSQLParser.LOGIC_EXPR, "LOGIC_EXPR");
        having.addChild(logicExpr);
      }
      if (logicExpr.getChildCount() > 0) {
        CommonTree oldCond = (CommonTree) logicExpr.deleteChild(0);
        CommonTree and = FilterBlockUtil.createSqlASTNode(group, PantheraParser_PLSQLParser.SQL92_RESERVED_AND, "and");
        logicExpr.addChild(and);
        and.addChild(oldCond);
        and.addChild(condition);
      } else {
        logicExpr.addChild(condition);
      }
    } else {
      // no group, means it has underwent transformation.
      // put in where
      putUncorrelatedInWhere(tree, condition);
    }
  }

  private void putUncorrelatedInWhere(CommonTree topSelect, CommonTree condition) {
    CommonTree where = (CommonTree) topSelect.getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_WHERE);
    if (where == null) {
      where = FilterBlockUtil.createSqlASTNode(condition,
        PantheraExpParser.SQL92_RESERVED_WHERE, "where");
      topSelect.addChild(where);
    }
    CommonTree logicExpr = (CommonTree) where.getFirstChildWithType(PantheraExpParser.LOGIC_EXPR);
    if (logicExpr == null) {
      logicExpr = FilterBlockUtil.createSqlASTNode(condition,
          PantheraExpParser.LOGIC_EXPR, "LOGIC_EXPR");
      where.addChild(logicExpr);
    }
    if (logicExpr.getChildCount() == 0) {
      logicExpr.addChild(condition);
    } else {
      CommonTree and = FilterBlockUtil.createSqlASTNode(condition,
          PantheraExpParser.SQL92_RESERVED_AND, "and");
      CommonTree oldCond = (CommonTree) logicExpr.deleteChild(0);
      logicExpr.addChild(and);
      and.addChild(oldCond);
      and.addChild(condition);
    }
  }

}
