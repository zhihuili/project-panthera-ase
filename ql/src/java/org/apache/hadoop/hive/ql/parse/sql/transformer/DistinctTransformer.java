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

import java.util.List;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.QueryInfo.Column;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockUtil;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * delete distinct if there is group node under the select.
 * if there is group node, all rows will not have duplicated ones.
 *
 * DistinctTransformer.
 *
 */
public class DistinctTransformer extends BaseSqlASTTransformer {
  SqlASTTransformer tf;

  public DistinctTransformer(SqlASTTransformer tf) {
    this.tf = tf;
  }

  @Override
  protected void transform(CommonTree tree, TranslateContext context) throws SqlXlateException {
    tf.transformAST(tree, context);
    this.transformQIDistinct(tree, context);
  }

  private void transformQIDistinct(CommonTree tree, TranslateContext context) throws SqlXlateException {
    QueryInfo qInfo = context.getQInfoRoot();
    transformQIDistinct(qInfo, context);
  }

  private void transformQIDistinct(QueryInfo qf, TranslateContext context) throws SqlXlateException {
    for (QueryInfo qinfo : qf.getChildren()) {
      transformQIDistinct(qinfo, context);
    }
    if (!qf.isQInfoTreeRoot()) {
      this.transformDistinct(qf, context);
    }
  }

  private void transformDistinct(QueryInfo qi, TranslateContext context)
      throws SqlXlateException {
    CommonTree select = qi.getSelectKeyForThisQ();
    if (select.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_DISTINCT) == null) {
      return;
    }
    CommonTree distinct = (CommonTree) select.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_DISTINCT);
    CommonTree selectList = (CommonTree) select.getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    if (selectList == null) {
      CommonTree asterisk = (CommonTree) select.getFirstChildWithType(PantheraParser_PLSQLParser.ASTERISK);
      assert(asterisk != null);
      // this select Item will not have function, just expand *
      // alias name keep column name
      List<Column> columnList = qi.getRowInfo((CommonTree) select.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM));
      selectList = FilterBlockUtil.createSqlASTNode(asterisk, PantheraParser_PLSQLParser.SELECT_LIST, "SELECT_LIST");
      select.replaceChildren(asterisk.childIndex, asterisk.childIndex, selectList);
      for (Column column : columnList) {
        String col = column.getColAlias();
        String tab = column.getTblAlias();
        CommonTree alias = FilterBlockUtil.createAlias(asterisk, col);
        CommonTree cas = FilterBlockUtil.createCascatedElementBranch(asterisk, tab, col);
        CommonTree expr = FilterBlockUtil.createSqlASTNode(asterisk, PantheraParser_PLSQLParser.EXPR, "EXPR");
        expr.addChild(cas);
        CommonTree nsi = FilterBlockUtil.createSqlASTNode(asterisk, PantheraParser_PLSQLParser.SELECT_ITEM, "SELECT_ITEM");
        nsi.addChild(expr);
        nsi.addChild(alias);
        selectList.addChild(nsi);
      }
    }
    // if no alias , add original name to it.
    // if alias, keep old alias as new column name and new alias.
    // new table alias use origin, old table alias change to new.
    CommonTree selectParent = (CommonTree) select.getParent();
    CommonTree newSelect = FilterBlockUtil.createSqlASTNode(select, PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT, "select");
    selectParent.replaceChildren(select.childIndex, select.childIndex, newSelect);
    String tb = FilterBlockUtil.makeSelectBranch(newSelect, select, context);
    select.deleteChild(distinct.getChildIndex());
    CommonTree newSelectList = FilterBlockUtil.dupNode(selectList);
    newSelect.addChild(newSelectList);
    CommonTree group = FilterBlockUtil.createSqlASTNode(distinct, PantheraParser_PLSQLParser.SQL92_RESERVED_GROUP, "group");
    newSelect.addChild(group);
    for (int i = 0; i < selectList.getChildCount(); i++) {
      CommonTree selectItem = (CommonTree) selectList.getChild(i);
      CommonTree newSelectAlias = null;
      if (selectItem.getChildCount() == 1) {
        CommonTree oldAny = FilterBlockUtil.findOnlyNode(selectItem, PantheraParser_PLSQLParser.ANY_ELEMENT);
        if (selectItem.getChild(0).getChild(0).getType() == PantheraParser_PLSQLParser.CASCATED_ELEMENT &&
            selectItem.getChild(0).getChild(0).getChild(0).getType() == PantheraParser_PLSQLParser.ANY_ELEMENT) {
          newSelectAlias = (CommonTree) oldAny.getChild(oldAny.getChildCount() - 1);
        } else if (oldAny != null) {
          // this case it is an expression without alias.
          // never cited by outer
          newSelectAlias = (CommonTree) FilterBlockUtil.createAlias(selectItem, context).getChild(0);
        } else {
          // imm number or imm expression
          CommonTree newSelectItem = FilterBlockUtil.cloneTree(selectItem);
          CommonTree groupExpr = FilterBlockUtil.cloneTree((CommonTree) selectItem.getChild(0));
          newSelectList.addChild(newSelectItem);
          CommonTree groupElement = FilterBlockUtil.createSqlASTNode(selectItem, PantheraParser_PLSQLParser.GROUP_BY_ELEMENT, "GROUP_BY_ELEMENT");
          group.addChild(groupElement);
          groupElement.addChild(groupExpr);
          continue;
        }
        CommonTree alias = FilterBlockUtil.createAlias(selectItem, context);
        selectItem.addChild(alias);
      } else {
        newSelectAlias = (CommonTree) selectItem.getChild(1).getChild(0);
      }
      CommonTree tbnode = FilterBlockUtil.createSqlASTNode(selectItem, PantheraParser_PLSQLParser.ID, tb);
      CommonTree colnode = FilterBlockUtil.createSqlASTNode(selectItem, PantheraParser_PLSQLParser.ID, selectItem.getChild(1).getChild(0).getText());

      CommonTree newSelectItem = FilterBlockUtil.createSqlASTNode(selectItem, PantheraParser_PLSQLParser.SELECT_ITEM, "SELECT_ITEM");
      CommonTree newExpr = FilterBlockUtil.createSqlASTNode(selectItem, PantheraParser_PLSQLParser.EXPR, "EXPR");
      CommonTree newCas = FilterBlockUtil.createSqlASTNode(selectItem, PantheraParser_PLSQLParser.CASCATED_ELEMENT, "CASCATED_ELEMENT");
      CommonTree newAny = FilterBlockUtil.createSqlASTNode(selectItem, PantheraParser_PLSQLParser.ANY_ELEMENT, "ANY_ELEMENT");
      newSelectItem.addChild(newExpr);
      if (newSelectAlias != null) {
        CommonTree newAlias = FilterBlockUtil.createSqlASTNode(selectItem, PantheraParser_PLSQLParser.ALIAS, "ALIAS");
        newSelectItem.addChild(newAlias);
        CommonTree alias = FilterBlockUtil.createSqlASTNode(selectItem, PantheraParser_PLSQLParser.ID, newSelectAlias.getText());
        newAlias.addChild(alias);
      }
      newExpr.addChild(newCas);
      newCas.addChild(newAny);
      newAny.addChild(tbnode);
      newAny.addChild(colnode);
      newSelectList.addChild(newSelectItem);
      CommonTree groupExpr = FilterBlockUtil.cloneTree(newExpr);
      CommonTree groupElement = FilterBlockUtil.createSqlASTNode(selectItem, PantheraParser_PLSQLParser.GROUP_BY_ELEMENT, "GROUP_BY_ELEMENT");
      group.addChild(groupElement);
      groupElement.addChild(groupExpr);
    }
  }
}
