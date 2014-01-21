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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.PantheraExpParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockUtil;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * transform USING to ON
 *
 * UsingTransformer.
 *
 */
public class UsingTransformer extends BaseSqlASTTransformer {

  SqlASTTransformer tf;

  public UsingTransformer(SqlASTTransformer tf) {
    this.tf = tf;
  }

  @Override
  public void transform(CommonTree tree, TranslateContext context) throws SqlXlateException {
    tf.transformAST(tree, context);
    trans(tree, context);
  }

  private void trans(CommonTree node, TranslateContext context) throws SqlXlateException {
    int childCount = node.getChildCount();
    for (int i = 0; i < childCount; i++) {
      trans((CommonTree) node.getChild(i), context);
    }
    if (node.getType() == PantheraExpParser.PLSQL_NON_RESERVED_USING) {
      CommonTree join = (CommonTree) node.getParent();
      if (join.getType() != PantheraExpParser.JOIN_DEF) {
        throw new SqlXlateException(join, "Unsupported USING type:" + join.getText());
      }
      processUsing(node, context);
    }
  }

  private void processUsing(CommonTree node, TranslateContext context) throws SqlXlateException {
    CommonTree join = (CommonTree) node.getParent();
    CommonTree tableRef = (CommonTree) join.getParent();
    assert (join.childIndex > 0);
    CommonTree leftTableRefElement = (CommonTree) tableRef.getChild(join.childIndex - 1);
    if (leftTableRefElement.getType() != PantheraExpParser.TABLE_REF_ELEMENT) {
      throw new SqlXlateException(join, "currently only support USING specified in first join of a TABLE_REF");
    }
    // parent of table_ref can also be table_ref_element
    CommonTree select = (CommonTree) node.getAncestor(PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT);
    CommonTree rightTableRefElement = (CommonTree) join
        .getFirstChildWithType(PantheraExpParser.TABLE_REF_ELEMENT);
    String leftTableName = SqlXlateUtil.findTableReferenceName(leftTableRefElement);
    String rightTableName = SqlXlateUtil.findTableReferenceName(rightTableRefElement);
    List<String> leftColumnList = new ArrayList<String>();
    List<String> rightColumnList = new ArrayList<String>();
    for (int i = 0; i < node.getChildCount(); i++) {
      CommonTree columnName = (CommonTree) node.getChild(i);
      String column = columnName.getChild(0).getText();
      leftColumnList.add(column);
      rightColumnList.add(column);
    }
    assert (join.getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_ON) == null);
    // build on to replace USING
    CommonTree on = FilterBlockUtil.makeOn(node, leftTableName, rightTableName, leftColumnList,
        rightColumnList);
    join.replaceChildren(node.childIndex, node.childIndex, on);

    // take INNER as LEFT, because when INNER join on left & right equal, neither cannot be null.
    int joinType = PantheraParser_PLSQLParser.LEFT_VK;
    if (join.getChild(0).getType() == PantheraParser_PLSQLParser.RIGHT_VK
        || join.getChild(0).getType() == PantheraParser_PLSQLParser.FULL_VK) {
      joinType = join.getChild(0).getType();
    }

    // rebuild select-list
    Map<String, CommonTree> commonMap = rebuildSelect(select, node, joinType, leftTableRefElement, rightTableRefElement, context);
    // rebuild others
    FilterBlockUtil.rebuildColumn((CommonTree) select
        .getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_WHERE), commonMap);
    FilterBlockUtil.rebuildColumn((CommonTree) select
        .getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_GROUP), commonMap);
    FilterBlockUtil.deleteAllTableAlias((CommonTree) ((CommonTree) (select.getParent().getParent()))
        .getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_ORDER));
  }

  /**
   *
   * 1. expand select * if exists, omit USING cols, use common colname as alias.<br>
   * 2. replace table alias in select-list
   * 3. replace upper order by x.a with order by a
   * 4. replace filters of a to x.a
   *
   * @param select
   * @param columnSetMap
   * @param joinType
   * @return commonMap
   * @throws SqlXlateException
   */
  private Map<String, CommonTree> rebuildSelect(CommonTree select, CommonTree node,
      int joinType, CommonTree leftTable, CommonTree rightTable, TranslateContext context) throws SqlXlateException {
    CommonTree selectList = (CommonTree) select.getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    CommonTree asterisk = (CommonTree) select.getFirstChildWithType(PantheraParser_PLSQLParser.ASTERISK);
    CommonTree newSelectList = FilterBlockUtil.createSqlASTNode(asterisk != null ? asterisk : selectList,
        PantheraParser_PLSQLParser.SELECT_LIST, "SELECT_LIST");
    Map<String, CommonTree> commonMap = rebuildAsterisk(node, newSelectList, joinType, leftTable, rightTable, context);
    if (asterisk != null) {
      if (selectList == null) {
        assert(asterisk.childIndex == 1);
        select.replaceChildren(asterisk.childIndex, asterisk.childIndex, newSelectList);
        selectList = newSelectList;
      } else if (asterisk.childIndex == 2) {
        for (int i = 0; i < newSelectList.getChildCount(); i++) {
          selectList.addChild(newSelectList.getChild(i));
        }
      } else {
        for (int i = 0; i < newSelectList.getChildCount(); i++) {
          SqlXlateUtil.addCommonTreeChild(selectList, i, (CommonTree) newSelectList.getChild(i));
        }
      }
    }
    for (int i = 0; i < selectList.getChildCount(); i++) {
      CommonTree selectItem = (CommonTree) selectList.getChild(i);
      List<CommonTree> anyList = new ArrayList<CommonTree>();
      FilterBlockUtil.findNode(selectItem, PantheraExpParser.ANY_ELEMENT, anyList);
      if (anyList.size() == 0) {
        continue;
      }
      for (CommonTree anyElement : anyList) {
        String colname = anyElement.getChild(anyElement.getChildCount() - 1).getText();
        if (selectItem.getChildCount() == 1
            && selectItem.getChild(0).getChild(0).getType() == PantheraParser_PLSQLParser.CASCATED_ELEMENT
            && selectItem.getChild(0).getChild(0).getChild(0) == anyElement) {
          // if no alias user defined, rewrite it as col name to ensure dup col be deleted.
          selectItem.addChild(FilterBlockUtil.createAlias(selectItem, colname));
        }
        FilterBlockUtil.rebuildColumn(anyElement, commonMap);
      }
    }
    return commonMap;
  }

  private Map<String, CommonTree> rebuildAsterisk(CommonTree node, CommonTree selectList,
      int joinType, CommonTree leftTable, CommonTree rightTable, TranslateContext context) throws SqlXlateException {
    Map<String, CommonTree> commonMap = new HashMap<String, CommonTree>();
    Set<String> set = new HashSet<String>();
    String leftTableName = SqlXlateUtil.findTableReferenceName(leftTable);
    String rightTableName = SqlXlateUtil.findTableReferenceName(rightTable);
    for (int i = 0; i < node.getChildCount(); i++) {
      String columnName = node.getChild(i).getChild(0).getText();
      CommonTree selectItem = FilterBlockUtil.createSqlASTNode(selectList, PantheraParser_PLSQLParser.SELECT_ITEM, "SELECT_ITEM");
      CommonTree expr = FilterBlockUtil.createSqlASTNode(selectList, PantheraParser_PLSQLParser.EXPR, "EXPR");
      selectItem.addChild(expr);
      if (joinType == PantheraParser_PLSQLParser.LEFT_VK) {
        CommonTree cascated = FilterBlockUtil.createCascatedElementBranch(selectList, leftTableName, columnName);
        expr.addChild(cascated);
      } else if (joinType == PantheraParser_PLSQLParser.RIGHT_VK) {
        CommonTree cascated = FilterBlockUtil.createCascatedElementBranch(selectList, rightTableName, columnName);
        expr.addChild(cascated);
      } else {
        CommonTree leftCascated = FilterBlockUtil.createCascatedElementBranch(selectList, leftTableName, columnName);
        CommonTree rightCascated = FilterBlockUtil.createCascatedElementBranch(selectList, leftTableName, columnName);
        CommonTree search = FilterBlockUtil.createSqlASTNode(selectList, PantheraParser_PLSQLParser.SEARCHED_CASE, "case");
        expr.addChild(search);
        CommonTree tokenWhen = FilterBlockUtil.createSqlASTNode(selectList, PantheraParser_PLSQLParser.SQL92_RESERVED_WHEN, "when");
        CommonTree tokenElse = FilterBlockUtil.createSqlASTNode(selectList, PantheraParser_PLSQLParser.SQL92_RESERVED_ELSE, "else");
        search.addChild(tokenWhen);
        search.addChild(tokenElse);
        CommonTree logicExpr = FilterBlockUtil.createSqlASTNode(selectList, PantheraParser_PLSQLParser.LOGIC_EXPR, "LOGIC_EXPR");
        tokenWhen.addChild(logicExpr);
        CommonTree isNull = FilterBlockUtil.createSqlASTNode(selectList, PantheraParser_PLSQLParser.IS_NULL, "IS_NULL");
        logicExpr.addChild(isNull);
        isNull.addChild(FilterBlockUtil.cloneTree(FilterBlockUtil.cloneTree(rightCascated)));
        CommonTree leftExpr = FilterBlockUtil.createSqlASTNode(selectList, PantheraParser_PLSQLParser.EXPR, "EXPR");
        CommonTree rightExpr = FilterBlockUtil.createSqlASTNode(selectList, PantheraParser_PLSQLParser.EXPR, "EXPR");
        tokenElse.addChild(rightExpr);
        tokenWhen.addChild(leftExpr);
        leftExpr.addChild(leftCascated);
        rightExpr.addChild(rightCascated);
      }
      CommonTree alias = FilterBlockUtil.createAlias(selectList, columnName);
      selectItem.addChild(alias);
      selectList.addChild(selectItem);
      commonMap.put(columnName, selectItem);
      set.add(columnName);
    }
    addRemainingCols(selectList, leftTable, leftTableName, set, context);
    addRemainingCols(selectList, rightTable, rightTableName, set, context);
    return commonMap;
  }

  private void addRemainingCols(CommonTree selectList, CommonTree table, String tableName, Set<String> set, TranslateContext context) throws SqlXlateException {
    Set<String> colSet = FilterBlockUtil.getColumnSet(table, context);
    Iterator<String> colIt = colSet.iterator();
    while (colIt.hasNext()) {
      String col = colIt.next();
      if (set.contains(col)) {
        continue;
      }
      CommonTree selectItem = FilterBlockUtil.createSqlASTNode(selectList, PantheraParser_PLSQLParser.SELECT_ITEM, "SELECT_ITEM");
      CommonTree expr = FilterBlockUtil.createSqlASTNode(selectList, PantheraParser_PLSQLParser.EXPR, "EXPR");
      selectItem.addChild(expr);
      CommonTree cascated = FilterBlockUtil.createCascatedElementBranch(selectList, tableName, col);
      expr.addChild(cascated);
      CommonTree alias = FilterBlockUtil.createAlias(selectList, col);
      selectItem.addChild(alias);
      selectList.addChild(selectItem);
    }

  }

}
