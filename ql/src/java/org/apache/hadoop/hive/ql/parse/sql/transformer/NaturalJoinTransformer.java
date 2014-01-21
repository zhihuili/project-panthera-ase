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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.PantheraExpParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockUtil;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * transform<br>
 * natural join<br>
 * natural left join<br>
 * natural inner join<br>
 * natural right join<br>
 * natural full join<br>
 * to<br>
 * simple join<br>
 * NaturalJoinTransformer.
 *
 */
public class NaturalJoinTransformer extends BaseSqlASTTransformer {

  SqlASTTransformer tf;

  public NaturalJoinTransformer(SqlASTTransformer tf) {
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
      CommonTree child = (CommonTree) node.getChild(i);
      trans(child, context);
    }
    // TODO join can also be 2,3,... child of table_ref, should support more than 2 tables
    if (node.getType() == PantheraExpParser.TABLE_REF && node.getChildCount() > 1
        && node.getChild(1).getType() == PantheraExpParser.JOIN_DEF
        && node.getChild(1).getChild(0).getType() == PantheraExpParser.NATURAL_VK) {
      Map<String, Set<String>> columnSetMap = processNaturalJoin(node, context);
      // take INNER as LEFT, because when INNER join on left & right equal, neither cannot be null.
      int joinType = PantheraParser_PLSQLParser.LEFT_VK;
      if (node.getChild(1).getChild(0).getType() == PantheraParser_PLSQLParser.RIGHT_VK
          || node.getChild(1).getChild(0).getType() == PantheraParser_PLSQLParser.FULL_VK) {
        joinType = node.getChild(1).getChild(0).getType();
      }
      // parent of table_ref can also be table_ref_element
      CommonTree select = (CommonTree) node.getAncestor(PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT);
      Map<String, CommonTree> commonMap = rebuildSelect(select, columnSetMap, joinType);
      FilterBlockUtil.rebuildColumn((CommonTree) select
          .getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_WHERE), commonMap);
      FilterBlockUtil.rebuildColumn((CommonTree) select
          .getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_GROUP), commonMap);
      FilterBlockUtil.deleteAllTableAlias((CommonTree) ((CommonTree) (select.getParent().getParent()))
          .getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_ORDER));
    }
  }

  /**
   *
   * 1. expand select * if exists, omit dup col, use common colname as alias.<br>
   * 2. replace table alias in select-list
   * 3. replace upper order by x.a with order by a
   * 4. replace filters of a to x.a
   *
   * @param select
   * @param columnSetMap
   * @param joinType
   * @return commonMap
   */
  private Map<String, CommonTree> rebuildSelect(CommonTree select, Map<String, Set<String>> columnSetMap, int joinType) {
    CommonTree selectList = (CommonTree) select.getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    CommonTree asterisk = (CommonTree) select.getFirstChildWithType(PantheraParser_PLSQLParser.ASTERISK);
    CommonTree newSelectList = FilterBlockUtil.createSqlASTNode(asterisk != null ? asterisk : selectList, PantheraParser_PLSQLParser.SELECT_LIST, "SELECT_LIST");
    Map<String, CommonTree> commonMap = rebuildAsterisk(newSelectList, columnSetMap, joinType);
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

  private Map<String, CommonTree> rebuildAsterisk(CommonTree selectList, Map<String, Set<String>> columnSetMap,
      int joinType) {
    Iterator<Entry<String, Set<String>>> it = columnSetMap.entrySet().iterator();
    Set<String> set = new HashSet<String>();
    Map<String, CommonTree> index = new HashMap<String, CommonTree>();
    Map<String, CommonTree> commonMap = new HashMap<String, CommonTree>();
    Stack<Entry<String, Set<String>>> entries = new Stack<Entry<String, Set<String>>>();
    int commonCount = 0;
    while (it.hasNext()) {
      entries.push(it.next());
    }
    // use stack to first do right table and then left table
    while (entries.size() > 0) {
      Entry<String, Set<String>> entry = entries.pop();
      String table = entry.getKey();
      Set<String> val = entry.getValue();
      Iterator<String> colIter = val.iterator();
      int independentCount = commonCount;
      while (colIter.hasNext()) {
        String col = colIter.next();
        CommonTree cascated = FilterBlockUtil.createCascatedElementBranch(selectList, table, col);
        CommonTree expr = FilterBlockUtil.createSqlASTNode(cascated, PantheraParser_PLSQLParser.EXPR, "EXPR");
        expr.addChild(cascated);
        CommonTree alias = FilterBlockUtil.createAlias(cascated, col);
        CommonTree selectItem = FilterBlockUtil.createSqlASTNode(expr, PantheraParser_PLSQLParser.SELECT_ITEM, "SELECT_ITEM");
        selectItem.addChild(expr);
        selectItem.addChild(alias);
        if (set.contains(col)) {
          // this is a shared column, put it at first of selectList
          CommonTree existSelectItem = index.get(col);
          // here in the same table will not have two cols with same colname
          // TODO need check
          existSelectItem.getParent().deleteChild(existSelectItem.getChildIndex());
          if (commonMap.get(col) != null) {
            commonCount--;
            independentCount--;
          }
          if (joinType == PantheraParser_PLSQLParser.LEFT_VK) {
            SqlXlateUtil.addCommonTreeChild(selectList, commonCount++, selectItem);
            commonMap.put(col, selectItem);
          } else if (joinType == PantheraParser_PLSQLParser.RIGHT_VK) {
            SqlXlateUtil.addCommonTreeChild(selectList, commonCount++, existSelectItem);
            commonMap.put(col, existSelectItem);
          } else {
            // natural full outer join
            // (case when t1.a is null then t2.a else t1.a end)
            CommonTree compositeSelectItem = FilterBlockUtil.createSqlASTNode(selectList, PantheraParser_PLSQLParser.SELECT_ITEM, "SELECT_ITEM");
            CommonTree compositeExpr = FilterBlockUtil.createSqlASTNode(selectList, PantheraParser_PLSQLParser.EXPR, "EXPR");
            compositeSelectItem.addChild(compositeExpr);
            compositeSelectItem.addChild((CommonTree) selectItem.deleteChild(1));
            CommonTree search = FilterBlockUtil.createSqlASTNode(selectList, PantheraParser_PLSQLParser.SEARCHED_CASE, "case");
            compositeExpr.addChild(search);
            CommonTree tokenWhen = FilterBlockUtil.createSqlASTNode(selectList, PantheraParser_PLSQLParser.SQL92_RESERVED_WHEN, "when");
            CommonTree tokenElse = FilterBlockUtil.createSqlASTNode(selectList, PantheraParser_PLSQLParser.SQL92_RESERVED_ELSE, "else");
            search.addChild(tokenWhen);
            search.addChild(tokenElse);
            CommonTree logicExpr = FilterBlockUtil.createSqlASTNode(selectList, PantheraParser_PLSQLParser.LOGIC_EXPR, "LOGIC_EXPR");
            tokenWhen.addChild(logicExpr);
            CommonTree isNull = FilterBlockUtil.createSqlASTNode(selectList, PantheraParser_PLSQLParser.IS_NULL, "IS_NULL");
            logicExpr.addChild(isNull);
            isNull.addChild(FilterBlockUtil.cloneTree((CommonTree) selectItem.getChild(0).getChild(0)));
            tokenElse.addChild((CommonTree) selectItem.deleteChild(0));
            tokenWhen.addChild((CommonTree) existSelectItem.deleteChild(0));
            SqlXlateUtil.addCommonTreeChild(selectList, commonCount++, compositeSelectItem);
            commonMap.put(col, compositeSelectItem);
          }
          independentCount++;
        } else {
          SqlXlateUtil.addCommonTreeChild(selectList, independentCount++, selectItem);
          set.add(col);
          index.put(col, selectItem);
        }
      }
    }
    return commonMap;
  }

  /**
   * change natural join to equijoin
   *
   * @param node
   * @param context
   * @return return a map for sets of all table_ref_element, key is table alias,
   * val is a set for all cols.
   * @throws SqlXlateException
   */
  private Map<String, Set<String>> processNaturalJoin(CommonTree node, TranslateContext context)
      throws SqlXlateException {
    Map<String, Set<String>> columnSetMap = new LinkedHashMap<String, Set<String>>();
    for (int i = 0; i < node.getChildCount(); i++) {
      CommonTree child = (CommonTree) node.getChild(i);
      if (child.getType() == PantheraExpParser.TABLE_REF_ELEMENT) {
        String leftTable = SqlXlateUtil.findTableReferenceName(child);
        columnSetMap.put(leftTable, FilterBlockUtil.getColumnSet(child, context));
      }
      if (child.getType() == PantheraExpParser.JOIN_DEF
          && child.getChild(0).getType() == PantheraExpParser.NATURAL_VK) {
        // change join type
        CommonTree natural = processNaturalNode(child);
        CommonTree tableRefElement = (CommonTree) child
            .getFirstChildWithType(PantheraExpParser.TABLE_REF_ELEMENT);
        String alias = SqlXlateUtil.findTableReferenceName(tableRefElement);
        Set<String> columnSet = FilterBlockUtil.getColumnSet(tableRefElement, context);
        makeOn(natural, alias, child, columnSet, columnSetMap);
        columnSetMap.put(alias, columnSet);
      }
    }
    return columnSetMap;
  }

  /**
   * make equijoin condition
   *
   * @param njoin
   * @param thisTable
   * @param join
   * @param thisColumnSet
   * @param columnSetMap
   */
  private void makeOn(CommonTree natural, String thisTable, CommonTree join, Set<String> thisColumnSet,
      Map<String, Set<String>> columnSetMap) {
    CommonTree on = FilterBlockUtil.createSqlASTNode(natural,
        PantheraExpParser.SQL92_RESERVED_ON, "on");//njoin.getChild(0) is natural_vk
    CommonTree logicExpr = FilterBlockUtil.createSqlASTNode(on, PantheraExpParser.LOGIC_EXPR,
        "LOGIC_EXPR");
    on.addChild(logicExpr);
    boolean hasCondition = false;
    for (String column : thisColumnSet) {
      for (Entry<String, Set<String>> entry : columnSetMap.entrySet()) {
        Set<String> preColumnSet = entry.getValue();
        String tableAlias = entry.getKey();
        if (preColumnSet.contains(column)) {
          hasCondition = true;
          CommonTree equal = FilterBlockUtil.makeEqualCondition(on, thisTable, tableAlias, column, column);
          FilterBlockUtil.addConditionToLogicExpr(logicExpr, equal);
          break;
        }
      }
    }
    if (hasCondition) {
      join.addChild(on);
    }
  }

  /**
   * build columnSetMap and delete INNER node
   *
   * @param join
   * @return natural join node
   */
  private CommonTree processNaturalNode(CommonTree join) {
    // delete NATURAL node
    CommonTree natural = (CommonTree) join.deleteChild(0);
    if (join.getChild(0).getType() == PantheraExpParser.INNER_VK) {
      // delete INNER node
      join.deleteChild(0);
    }
    return natural;
  }

}
