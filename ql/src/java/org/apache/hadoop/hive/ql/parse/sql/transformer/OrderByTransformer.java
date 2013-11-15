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
 * support order by column number. OrderByTransformer.
 *
 */
public class OrderByTransformer extends BaseSqlASTTransformer {

  SqlASTTransformer tf;

  public OrderByTransformer(SqlASTTransformer tf) {
    this.tf = tf;
  }

  @Override
  public void transform(CommonTree tree, TranslateContext context) throws SqlXlateException {
    tf.transformAST(tree, context);
    for (QueryInfo qf : context.getqInfoList()) {
      transformOrderBy(qf, context);
      // Assume no order by clause in subqueries.
    }
  }

  //FIXME if order by something neither a function nor a select-item
  private void transformOrderBy(QueryInfo qf, TranslateContext context) throws SqlXlateException {
    CommonTree select = qf.getSelectKeyForThisQ();
    CommonTree selectStatement = (CommonTree) select.getParent().getParent();
    CommonTree order = (CommonTree) selectStatement
        .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_ORDER);
    if (order != null) {
      List<Column> selectRowInfo = qf.getSelectRowInfo();

      CommonTree orderByElements = (CommonTree) order.getChild(0);
      for (int i = 0; i < orderByElements.getChildCount(); i++) {
        CommonTree ct = (CommonTree) orderByElements.getChild(i).getChild(0).getChild(0);
        if (ct.getType() == PantheraParser_PLSQLParser.UNSIGNED_INTEGER) {// order by column number
          CommonTree expr = (CommonTree) orderByElements.getChild(i).getChild(0);

          int seq = Integer.valueOf(ct.getText()) - 1;
          CommonTree selectList = (CommonTree) ((CommonTree) ((CommonTree) selectStatement
              .getFirstChildWithType(PantheraParser_PLSQLParser.SUBQUERY))
              .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT))
              .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
          if (selectList == null) {// select *
            Column column = null;
            try {
              column = selectRowInfo.get(seq);
            } catch (IndexOutOfBoundsException e) {
              throw new SqlXlateException(ct, "Invalid column number in order by clause.");
            }
            CommonTree cascatedElement = FilterBlockUtil.createSqlASTNode(
                ct, PantheraParser_PLSQLParser.CASCATED_ELEMENT, "CASCATED_ELEMENT");
            CommonTree anyElement = FilterBlockUtil.createSqlASTNode(
                ct, PantheraParser_PLSQLParser.ANY_ELEMENT, "ANY_ELEMENT");
            CommonTree col = FilterBlockUtil.createSqlASTNode(ct, PantheraParser_PLSQLParser.ID, column
                .getColAlias());
            expr.deleteChild(0);
            expr.addChild(cascatedElement);
            cascatedElement.addChild(anyElement);
            anyElement.addChild(col);
          } else { // select SELECT_LIST
            CommonTree item = (CommonTree) selectList.getChild(seq);
            CommonTree alias = (CommonTree) item.getChild(1);
            if (alias == null) {
              // FIXME It should change output column name of result set.
              alias = FilterBlockUtil.createSqlASTNode(item, PantheraParser_PLSQLParser.ALIAS, "ALIAS");
              CommonTree aliasName = FilterBlockUtil.createSqlASTNode(
                  item, PantheraParser_PLSQLParser.ID, "panthera_col_" + seq);
              alias.addChild(aliasName);
              item.addChild(alias);
            }
            CommonTree col = FilterBlockUtil.createSqlASTNode(
                (CommonTree) alias.getChild(0), PantheraParser_PLSQLParser.ID, alias.getChild(0).getText());
            CommonTree cascatedElement = FilterBlockUtil.createSqlASTNode(
                col, PantheraParser_PLSQLParser.CASCATED_ELEMENT, "CASCATED_ELEMENT");
            CommonTree anyElement = FilterBlockUtil.createSqlASTNode(
                col, PantheraParser_PLSQLParser.ANY_ELEMENT, "ANY_ELEMENT");
            cascatedElement.addChild(anyElement);
            anyElement.addChild(col);
            expr.deleteChild(0);
            expr.addChild(cascatedElement);
          }
        }
        // for alias and tablename.colname
        //FIXME maybe there's something else
        else {
          if ( ct.getChild(0).getChildCount()>1 ) {
            CommonTree selectList = (CommonTree) ((CommonTree) ((CommonTree) selectStatement
                .getFirstChildWithType(PantheraParser_PLSQLParser.SUBQUERY))
                .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT))
                .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
            if (selectList == null) {//*
              //FIXME
            } else {//select-list
              boolean flag = false;
              for (int j=0; j< selectList.getChildCount();j++){
                if(isEqualByText((CommonTree) selectList.getChild(j).getChild(0).getChild(0),ct)) {
                  CommonTree colAlias = FilterBlockUtil.createSqlASTNode(
                      (CommonTree) selectList.getChild(j), PantheraParser_PLSQLParser.ALIAS, "ALIAS");
                  CommonTree colAliasName = FilterBlockUtil.createSqlASTNode(
                      colAlias, PantheraParser_PLSQLParser.ID, "panthera_col_" + j);
                  colAlias.addChild(colAliasName);
                  selectList.getChild(j).addChild(colAlias);
                  CommonTree aliasId = FilterBlockUtil.createSqlASTNode(
                      (CommonTree) ct.getChild(0), PantheraParser_PLSQLParser.ID, colAliasName.getText());
                  ct.getChild(0).replaceChildren(0, ct.getChild(0).getChildCount() - 1, aliasId);
                  flag = true;
                  break;
                }
                if(isContainedByText((CommonTree) selectList.getChild(j).getChild(0).getChild(0),ct)) {
                  flag = true;
                  break;
                }
              }
              if (! flag) {
                throw new SqlXlateException(ct, "order by tablename.colname/tablealias.colname can only be used when it shows in select-list");
              }
            }
          } else {
            CommonTree id = (CommonTree) ct.getChild(0).getChild(0);
            String aliasStr = (String) context.getBallFromBasket(id.getText());
            if (aliasStr != null) {
              id.getToken().setText(aliasStr);
            }
          }
        }
      }
    }
  }

  private boolean isContainedByText(CommonTree tree1, CommonTree tree2) {
    if(tree1.getParent().getType() == PantheraParser_PLSQLParser.EXPR
        &&tree1.getType() == PantheraParser_PLSQLParser.DOT_ASTERISK
        &&tree1.getChild(0).getType() == PantheraParser_PLSQLParser.TABLEVIEW_NAME
        &&tree1.getChild(0).getChild(0).getText().equals(tree2.getChild(0).getChild(0).getText())) {
      return true;
    } else {
      return false;
    }
  }

  private boolean isEqualByText(CommonTree tree1, CommonTree tree2) {
    if(!tree1.getText().equals(tree2.getText())){
      return false;
    } else {
       if(tree1.getChildCount() == 0 && tree2.getChildCount() == 0) {
         return true;
       }
       if(tree1.getChildCount() != tree2.getChildCount()) {
         return false;
       }
       for(int i=0; i<tree1.getChildCount();i++){
         if(!isEqualByText((CommonTree) tree1.getChild(i),(CommonTree) tree2.getChild(i))) {
           return false;
         }
       }
       return true;
    }
  }
}
