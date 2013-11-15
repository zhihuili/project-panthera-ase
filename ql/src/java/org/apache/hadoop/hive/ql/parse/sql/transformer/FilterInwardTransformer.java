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
import java.util.List;
import java.util.Map;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.PantheraExpParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.QueryInfo.Column;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockUtil;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * select s_empnum from staff where exists (select * from proj where p_city = s_city and (p_pnum in ('P1 ', 'P3 ', 'P9 ') or p_pname not in('SDP ', 'APPLE ')));
 *
 * This class mainly for leftsemi join condition, in which the filters of bottom select
 * are under where node of the closing select, which will lead to invalid columns under where node.
 * So we need to move the filter to the bottom select.
 *
 * Filters under join on node will also inward to the bottom select or top select.
 *
 * FilterInwardTransformer.
 *
 */
public class FilterInwardTransformer extends BaseSqlASTTransformer {
  SqlASTTransformer tf;

  public FilterInwardTransformer(SqlASTTransformer tf) {
    this.tf = tf;
  }

  @Override
  public void transform(CommonTree tree, TranslateContext context) throws SqlXlateException {
    tf.transformAST(tree, context);
    this.transformFilterInward(tree, context);
  }

  private void transformFilterInward(CommonTree tree, TranslateContext context)
      throws SqlXlateException {
    // deep firstly
    for (int i = 0; i < tree.getChildCount(); i++) {
      transformFilterInward((CommonTree) (tree.getChild(i)), context);
    }

    if (PantheraExpParser.LEFTSEMI_STR.equals(tree.getText())) {
      // TODO the method can only handle one condition:
      // in outer where conditions with "or" node, the columns in the "or" branch must
      // from one table.
      processLeftSemiJoin(context, (CommonTree) tree.getParent());
    }

  }


  private void processLeftSemiJoin(TranslateContext context, CommonTree join)
      throws SqlXlateException {
    String bottomTableAlias = join.getFirstChildWithType(PantheraExpParser.TABLE_REF_ELEMENT).getChild(0)
        .getChild(0).getText();
    CommonTree bottomSelect = FilterBlockUtil.findOnlyNode(join, PantheraExpParser.SQL92_RESERVED_SELECT);
    CommonTree bottomSelectList = (CommonTree) bottomSelect
        .getFirstChildWithType(PantheraExpParser.SELECT_LIST);
    CommonTree on = (CommonTree) join.getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_ON);

    CommonTree bottomWhere = FilterBlockUtil.createSqlASTNode(join, PantheraExpParser.SQL92_RESERVED_WHERE,
        "where");
    CommonTree bottomWhereLogic = FilterBlockUtil.createSqlASTNode(join, PantheraExpParser.LOGIC_EXPR, "LOGIC_EXPR");
    bottomWhere.addChild(bottomWhereLogic);
    bottomSelect.addChild(bottomWhere);
    assert (bottomSelectList != null && bottomTableAlias != null);

    if (on != null) {
      // process join (on) branch
      List<Map<Boolean, List<CommonTree>>> joinKeys = this.getFilterkey(bottomTableAlias,
          (CommonTree) on
              .getChild(0).getChild(0));
      for (int i = 0; i < joinKeys.size(); i++) {
        List<CommonTree> bottomKeys = joinKeys.get(i).get(false);
        List<CommonTree> topKeys = joinKeys.get(i).get(true);
        // correlated condition with unary operator, e.g. IS NULL, IS NOT NULL
        if (bottomKeys == null && topKeys != null) {
          // TODO put the correlated filter to the top select where branch
          continue;
        }
        CommonTree op = (CommonTree) bottomKeys.get(0).getParent();
        // uncorrelated condition,transfer it to WHERE condition and remove it from SELECT_ITEM
        if (bottomKeys != null && topKeys == null) {
          FilterBlockUtil.deleteTheNode(op);
          if (bottomWhereLogic.getChildCount() > 0) {
            CommonTree and = FilterBlockUtil
                .createSqlASTNode(join, PantheraExpParser.SQL92_RESERVED_AND, "and");
            and.addChild((CommonTree) bottomWhereLogic.deleteChild(0));
            and.addChild(op);
            bottomWhereLogic.addChild(and);
          } else {
            bottomWhereLogic.addChild(op);
          }
          for (CommonTree bottomkey : bottomKeys) {
            CommonTree anyElement = (CommonTree) bottomkey.getChild(0);
            anyElement.deleteChild(0);
            CommonTree column = (CommonTree) anyElement.getChild(0);
            String columnAlias = column.getText();
            for (int j = 0; j < bottomSelectList.getChildCount(); j++) {
              CommonTree selectItem = (CommonTree) bottomSelectList.getChild(j);
              String selectColumn = selectItem.getChild(0).getChild(0).getChild(0).getChild(0)
                  .getText();
              String selectAlias = "";
              if (selectItem.getChildCount() > 1) {
                selectAlias = selectItem.getChild(1).getChild(0).getText();
              }
              if (selectAlias.equals(columnAlias)) {
                column.getToken().setText(selectColumn);
                bottomSelectList.deleteChild(j);
                break;
              }
            }
          }
        }
      }
    }

    // process where branch under closing select, inward the "or" branch
    CommonTree closingSelect = (CommonTree) join
        .getAncestor(PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT);
    CommonTree closingWhere = (CommonTree) closingSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_WHERE);
    if (closingWhere != null) {
      Map<String, List<CommonTree>> filterMap = new HashMap<String, List<CommonTree>>();
      getWhereFilter(context, (CommonTree)closingWhere.getChild(0).getChild(0), filterMap);
      if (filterMap.get(bottomTableAlias) != null) {
        for (CommonTree orNode : filterMap.get(bottomTableAlias)) {
          if (bottomWhereLogic.getChildCount() > 0) {
            CommonTree and = FilterBlockUtil
                .createSqlASTNode(join, PantheraExpParser.SQL92_RESERVED_AND, "and");
            and.addChild((CommonTree)bottomWhereLogic.deleteChild(0));
            and.addChild(orNode);
            bottomWhereLogic.addChild(and);
          } else {
            bottomWhereLogic.addChild(orNode);
          }
        }
        Map<String, String> columnAliasMap = getColumnAliasMap((CommonTree)bottomWhere
            .getAncestor(PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT));
        List<CommonTree> anyElements = new ArrayList<CommonTree>();
        FilterBlockUtil.findNode(bottomWhere, PantheraParser_PLSQLParser.ANY_ELEMENT, anyElements);
        for (CommonTree anyElement : anyElements) {
          String id = anyElement.getChild(0).getText();
          if (columnAliasMap.containsKey(id)) {
            ((CommonTree)anyElement.getChild(0)).getToken().setText(columnAliasMap.get(id));
          }
        }
      } else {
        // TODO handle "or" branch which only has column from top select branch
        // FIXME must put this branch to the top select where branch, or this filter will be dropped
      }
    }

    // Delete the where branch if no uncorrelated condition under logic node
    if (bottomWhereLogic.getChildCount() == 0) {
      bottomWhere.getParent().deleteChild(bottomWhere.getChildIndex());
    }
  }


  /**
   * finde the or branches which only has columns from one table
   *
   * @param context
   * @param node
   * @param filterMap
   * @throws SqlXlateException
   */
  private void getWhereFilter(TranslateContext context, CommonTree node, Map<String, List<CommonTree>> filterMap)
      throws SqlXlateException {
    if (node.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_AND) {
      // Transform the left child.
      getWhereFilter(context, (CommonTree) node.getChild(0), filterMap);
      // Transform the right child.
      getWhereFilter(context, (CommonTree) node.getChild(1), filterMap);

      CommonTree leftChild = (CommonTree) node.getChild(0);
      CommonTree rightChild = (CommonTree) node.getChild(1);

      if (leftChild.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_TRUE) {
        //
        // Replace the current node with the right child.
        //
        node.getParent().setChild(node.getChildIndex(), rightChild);
      } else if (rightChild.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_TRUE) {
        //
        // Replace the current node with the left child.
        //
        node.getParent().setChild(node.getChildIndex(), leftChild);
      }
    } else {
      if (node.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_OR) {
        List<String> tables = new ArrayList<String>();
        if (orBranchHasOnlyOneTable(context, node, tables)) {
          String table = tables.get(0);
          if (filterMap.get(table) == null) {
            List<CommonTree> orNodes = new ArrayList<CommonTree>();
            filterMap.put(table, orNodes);
          }
          filterMap.get(table).add(node);
          //
          // Create a new TRUE node and replace the current node with this new node.
          //
          CommonTree trueNode = FilterBlockUtil.createSqlASTNode(
              node, PantheraParser_PLSQLParser.SQL92_RESERVED_TRUE, "true");
          node.getParent().setChild(node.getChildIndex(), trueNode);
        }
      }
    }
    return;
  }


  /**
   * check whether the or branch only has columns from one table
   *
   * @param context
   * @param or
   * @param tableNames
   * @return
   * @throws SqlXlateException
   */
  private boolean orBranchHasOnlyOneTable(TranslateContext context, CommonTree or, List<String> tableNames)
      throws SqlXlateException {
    List<CommonTree> anyElements = new ArrayList<CommonTree>();
    FilterBlockUtil.findNode(or, PantheraParser_PLSQLParser.ANY_ELEMENT, anyElements);
    CommonTree closingSelect = (CommonTree) or.getAncestor(PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT);
    QueryInfo qf = null;
    for (QueryInfo tmpQf : context.getqInfoList()) {
      if (closingSelect.equals(tmpQf.getSelectKeyForThisQ())) {
        qf = tmpQf;
      }
    }
    assert (qf != null);
    if (!anyElements.isEmpty()) {
      String tableName = getTableName(qf, anyElements.get(0));
      if (tableName == null) {
        return false;
      }
      for (CommonTree anyElement : anyElements) {
        String tbName = getTableName(qf, anyElement);
        if (!tbName.equals(tableName)) {
          return false;
        }
      }
      // all columns in the "or" branch are from the same table
      tableNames.add(tableName);
      return true;
    }
    return false;
  }


  /**
   * get the table name of the column.
   *
   * this function is copied from CrossJoinTransformer
   *
   * @param qf
   * @param anyElement
   * @return
   * @throws SqlXlateException
   */
  private String getTableName(QueryInfo qf, CommonTree anyElement) throws SqlXlateException {
    String table = null;

    CommonTree currentSelect = (CommonTree) anyElement
        .getAncestor(PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT);

    if (anyElement.getChildCount() > 1) {
      table = anyElement.getChild(0).getText();
      if (anyElement.getChildCount() > 2) {
        // schema.table
        table += ("." + anyElement.getChild(1).getText());
        // merge schema and table as HIVE does not support schema.table.column in where clause.
        anyElement.deleteChild(1);
        ((CommonTree) anyElement.getChild(0)).getToken().setText(table);
      }
      //
      // Return null table name if it is not a src table.
      //
      if (!qf.getSrcTblAliasForSelectKey(currentSelect).contains(table)) {
        table = null;
      }
    } else {
      String columnName = anyElement.getChild(0).getText();
      List<Column> fromRowInfo = qf.getRowInfo((CommonTree) currentSelect
          .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM));
      for (Column col : fromRowInfo) {
        if (col.getColAlias().equals(columnName)) {
          table = col.getTblAlias();
          break;
        }
      }
    }
    return table;
  }


  /**
   * make a map of the table column name and table column alias
   *
   * @param select
   * @return
   */
  private Map<String, String> getColumnAliasMap(CommonTree select) {
    Map<String, String> columnAliasMap = new HashMap<String, String>();
    CommonTree selectList = (CommonTree)select.getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    for (int i = 0; i < selectList.getChildCount(); i++) {
      CommonTree selectItem = (CommonTree)selectList.getChild(i);
      String alias = null;
      if (selectItem.getChildCount() > 1) { // column has alias
        alias = selectItem.getChild(1).getChild(0).getText();
      }
      CommonTree anyElement = FilterBlockUtil.findOnlyNode(selectItem, PantheraParser_PLSQLParser.ANY_ELEMENT);
      String column = anyElement.getChild(0).getText();
      if (alias != null) {
        columnAliasMap.put(alias, column);
      }
    }
    return columnAliasMap;
  }

  /**
   * extract join key from filter op node.
   *
   * @return false: bottom keys<br>
   *         true: top key
   * @throws SqlXlateException
   */
  Map<Boolean, List<CommonTree>> getFilter(String currentTableAlias, CommonTree filterOp)
      throws SqlXlateException {
    Map<Boolean, List<CommonTree>> result = new HashMap<Boolean, List<CommonTree>>();
    for (int i = 0; i < filterOp.getChildCount(); i++) {
      CommonTree child = (CommonTree) filterOp.getChild(i);
      if (child.getType() == PantheraParser_PLSQLParser.CASCATED_ELEMENT) {
        String tableAlias = child.getChild(0).getChild(0).getText();
        if (currentTableAlias.equals(tableAlias)) {// uncorrelated
          List<CommonTree> uncorrelatedList = result.get(false);
          if (uncorrelatedList == null) {
            uncorrelatedList = new ArrayList<CommonTree>();
            result.put(false, uncorrelatedList);
          }
          uncorrelatedList.add(child);
        }
        if (!currentTableAlias.equals(tableAlias)) {// correlated
          List<CommonTree> correlatedList = result.get(true);
          if (correlatedList == null) {
            correlatedList = new ArrayList<CommonTree>();
            result.put(true, correlatedList);
          }
          correlatedList.add(child);
        }
      }
    }
    return result;
  }

  /**
   *
   * @return
   * @throws SqlXlateException
   */
  List<Map<Boolean, List<CommonTree>>> getFilterkey(String currentTableAlias, CommonTree condition)
      throws SqlXlateException {
    List<Map<Boolean, List<CommonTree>>> result = new ArrayList<Map<Boolean, List<CommonTree>>>();
    this.getWhereKey(currentTableAlias, condition, result);
    return result;
  }

  private void getWhereKey(String currentTableAlias, CommonTree filterOp,
      List<Map<Boolean, List<CommonTree>>> list) throws SqlXlateException {
    if (FilterBlockUtil.isFilterOp(filterOp)) {
      list.add(getFilter(currentTableAlias, filterOp));
      return;
    } else if (FilterBlockUtil.isLogicOp(filterOp)) {
      for (int i = 0; i < filterOp.getChildCount(); i++) {
        getWhereKey(currentTableAlias, (CommonTree) filterOp.getChild(i), list);
      }
    } else {
      throw new SqlXlateException(filterOp, "unknow filter operation:" + filterOp.getText());
    }

  }
}
