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
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.QueryInfo.Column;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockUtil;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * expand select * or select tablename.*<br/>
 * support * coexists with select-list<br/>
 *
 * ExpandAsteriskTransformer.
 *
 */
public class ExpandAsteriskTransformer extends BaseSqlASTTransformer {
  SqlASTTransformer tf;

  public ExpandAsteriskTransformer(SqlASTTransformer tf) {
    this.tf = tf;
  }

  @Override
  protected void transform(CommonTree tree, TranslateContext context) throws SqlXlateException {
    tf.transformAST(tree, context);
    this.transformQIAsterisk(tree, context);
  }

  private void transformQIAsterisk(CommonTree tree, TranslateContext context) throws SqlXlateException {
    QueryInfo qInfo = context.getQInfoRoot();
    transformQIAsterisk(qInfo, context);
  }

  private void transformQIAsterisk(QueryInfo qf, TranslateContext context) throws SqlXlateException {
    for (QueryInfo qinfo : qf.getChildren()) {
      transformQIAsterisk(qinfo, context);
    }
    if (!qf.isQInfoTreeRoot()) {
      this.transformAsterisk(qf, context);
    }
  }

  private void transformAsterisk(QueryInfo qi, TranslateContext context)
      throws SqlXlateException {
    CommonTree select = qi.getSelectKeyForThisQ();
    CommonTree selectList = (CommonTree) select.getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    CommonTree asterisk = (CommonTree) select.getFirstChildWithType(PantheraParser_PLSQLParser.ASTERISK);
    if (asterisk != null) {
      int index = 0;
      if (selectList == null) {
        selectList = FilterBlockUtil.createSqlASTNode(asterisk, PantheraParser_PLSQLParser.SELECT_LIST, "SELECT_LIST");
        select.replaceChildren(asterisk.childIndex, asterisk.childIndex, selectList);
      } else {
        select.deleteChild(asterisk.childIndex);
        if (selectList.childIndex < asterisk.childIndex) {
          index = selectList.getChildCount();
        }
      }
      List<Column> columnList = qi.getRowInfo((CommonTree) select.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM));
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
        SqlXlateUtil.addCommonTreeChild(selectList, index++, nsi);
      }
    }
    for (int i = 0; i < selectList.getChildCount(); i++) {
      CommonTree selectItem = (CommonTree) selectList.getChild(i);
      CommonTree sexpr = (CommonTree) selectItem.getChild(0);
      if (sexpr.getType() == PantheraParser_PLSQLParser.EXPR && sexpr.getChild(0).getType() == PantheraParser_PLSQLParser.DOT_ASTERISK) {
        CommonTree tableView = (CommonTree) sexpr.getChild(0).getChild(0);
        assert(tableView != null && tableView.getType() == PantheraParser_PLSQLParser.TABLEVIEW_NAME);
        String tabname = tableView.getChild(0).getText();
        CommonTree t = new CommonTree();
        List<Column> columnList = qi.getFromRowInfo();
        for (Column column : columnList) {
          String col = column.getColAlias();
          String tab = column.getTblAlias();
          if (tab.equals(tabname)) {
            CommonTree alias = FilterBlockUtil.createAlias(sexpr, col);
            CommonTree cas = FilterBlockUtil.createCascatedElementBranch(sexpr, tab, col);
            CommonTree expr = FilterBlockUtil.createSqlASTNode(sexpr, PantheraParser_PLSQLParser.EXPR, "EXPR");
            expr.addChild(cas);
            CommonTree nsi = FilterBlockUtil.createSqlASTNode(sexpr, PantheraParser_PLSQLParser.SELECT_ITEM, "SELECT_ITEM");
            nsi.addChild(expr);
            nsi.addChild(alias);
            t.addChild(nsi);
          }
        }
        selectList.replaceChildren(i, i, t);
      }
    }
  }
}
