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
import java.util.List;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.PantheraExpParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockUtil;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * Transform leftsemi join to left join to handle not equal join condition
 *
 * Leftsemi2LeftJoinTransformer.
 *
 */
public class Leftsemi2LeftJoinTransformer extends BaseSqlASTTransformer {
  SqlASTTransformer tf;

  public Leftsemi2LeftJoinTransformer(SqlASTTransformer tf) {
    this.tf = tf;
  }

  @Override
  public void transform(CommonTree tree, TranslateContext context) throws SqlXlateException {
    tf.transformAST(tree, context);
    this.transformLeftsemi2LeftJoin(tree, context);
  }

  private void transformLeftsemi2LeftJoin(CommonTree tree, TranslateContext context)
      throws SqlXlateException {
    int childCount = tree.getChildCount();
    for (int i = 0; i < childCount; i++) {
      transformLeftsemi2LeftJoin((CommonTree) tree.getChild(i), context);
    }
    if (tree.getType() == PantheraExpParser.LEFTSEMI_VK) {
      processLeftsemi2LeftJoin((CommonTree) tree.getParent(), context);
    }
  }

  public void processLeftsemi2LeftJoin(CommonTree leftsemiJoin, TranslateContext context) throws SqlXlateException {
    CommonTree on = (CommonTree) leftsemiJoin.getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_ON);
    if (on == null) {
      return;
    }
    String bottomTableAlias = leftsemiJoin.getFirstChildWithType(PantheraExpParser.TABLE_REF_ELEMENT).getChild(0)
        .getChild(0).getText();
    String topTableAlias = ((CommonTree)leftsemiJoin.getParent()).getChild(leftsemiJoin.getChildIndex() - 1)
        .getChild(0).getChild(0).getText();
    CommonTree bottomSelect = FilterBlockUtil.findOnlyNode(leftsemiJoin, PantheraExpParser.SQL92_RESERVED_SELECT);
    CommonTree bottomSelectList = (CommonTree) bottomSelect
        .getFirstChildWithType(PantheraExpParser.SELECT_LIST);
    CommonTree closingSelect = (CommonTree) leftsemiJoin
        .getAncestor(PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT);
    CommonTree closingWhere = (CommonTree) closingSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_WHERE);
    if (closingWhere != null) {
      QueryInfo qf = null;
      for (QueryInfo tmpQf : context.getqInfoList()) {
        if (closingSelect.equals(tmpQf.getSelectKeyForThisQ())) {
          qf = tmpQf;
        }
      }
      assert (qf != null);
      // check if need to transform leftsemi join to left join. If there is bottom select column
      // in outer where, then need to transform leftsemi join.
      if (!checkWhereHasBottomTableElement(closingWhere, bottomTableAlias, qf, context)) {
        return;
      }
      // need transform leftsemi join to left join.
      List<CommonTree> equalOps = new ArrayList<CommonTree>();
      FilterBlockUtil.findNode(on, PantheraParser_PLSQLParser.EQUALS_OP, equalOps);
      if (equalOps.isEmpty()) {
        throw new SqlXlateException(leftsemiJoin, "SubQuery should have equal join condition.");
      }
      List<CommonTree> joinBottomColumnsCascated = new ArrayList<CommonTree>();
      for (CommonTree equalOp : equalOps) {
        //
        // Check if this is a equality expression between two tables's columns
        //
        if (FilterBlockUtil.IsColumnRef(equalOp.getChild(0)) && FilterBlockUtil.IsColumnRef(equalOp.getChild(1))) {
          String table1 = FilterBlockUtil.getTableName(qf, (CommonTree) equalOp.getChild(0).getChild(0), false);
          String table2 = FilterBlockUtil.getTableName(qf, (CommonTree) equalOp.getChild(1).getChild(0), false);
          //
          // Skip columns not in a src table.
          //
          if (table1 == null || table2 == null) {
            continue;
          }
          if (table1.equals(bottomTableAlias) && table2.equals(bottomTableAlias)) {
            continue;
          } else if (table1.equals(bottomTableAlias) && table2.equals(topTableAlias)) {
            joinBottomColumnsCascated.add((CommonTree) equalOp.getChild(0));
          } else if (table1.equals(topTableAlias) && table2.equals(bottomTableAlias)) {
            joinBottomColumnsCascated.add((CommonTree) equalOp.getChild(1));
          } else {
            continue;
          }
        }
      }
      if (joinBottomColumnsCascated.isEmpty()) {
        throw new SqlXlateException(leftsemiJoin, "SubQuery should have equal join condition.");
      }
      CommonTree left = FilterBlockUtil.createSqlASTNode(leftsemiJoin, PantheraParser_PLSQLParser.LEFT_VK, "left");
      leftsemiJoin.replaceChildren(0, 0, left);
      CommonTree distinct = (CommonTree) bottomSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_DISTINCT);
      if (distinct == null) {
        distinct = FilterBlockUtil.createSqlASTNode(bottomSelect, PantheraParser_PLSQLParser.SQL92_RESERVED_DISTINCT, "distinct");
        SqlXlateUtil.addCommonTreeChild(bottomSelect, bottomSelectList.getChildIndex(), distinct);
      }
      CommonTree condition = (CommonTree) closingWhere.getChild(0).deleteChild(0);
      for (CommonTree columnCascated : joinBottomColumnsCascated) {
        CommonTree isNotNull = FilterBlockUtil.createSqlASTNode(columnCascated, PantheraParser_PLSQLParser.IS_NOT_NULL, "IS_NOT_NULL");
        isNotNull.addChild(FilterBlockUtil.cloneTree(columnCascated));
        CommonTree and = FilterBlockUtil.createSqlASTNode(columnCascated, PantheraParser_PLSQLParser.SQL92_RESERVED_AND, "and");
        and.addChild(condition);
        and.addChild(isNotNull);
        condition = and;
      }
      closingWhere.getChild(0).addChild(condition);
    }
  }


  /**
   * check whether there is bottom table columns under where branch.
   *
   * @param where
   * @param bottomTblAlias
   * @param qf
   * @param context
   * @return
   * @throws SqlXlateException
   */
  private boolean checkWhereHasBottomTableElement(CommonTree where, String bottomTblAlias,
      QueryInfo qf, TranslateContext context) throws SqlXlateException {
    List<CommonTree> anyElements = new ArrayList<CommonTree>();
    FilterBlockUtil.findNode(where, PantheraParser_PLSQLParser.ANY_ELEMENT, anyElements);
    if (anyElements.isEmpty()) {
      return false;
    }
    for (CommonTree anyElement : anyElements) {

      String tableName = FilterBlockUtil.getTableName(qf, anyElement);
      if (tableName == null) {
        continue;
      }
      if (tableName.equals(bottomTblAlias)) {
        return true;
      }
    }
    return false;
  }
}

