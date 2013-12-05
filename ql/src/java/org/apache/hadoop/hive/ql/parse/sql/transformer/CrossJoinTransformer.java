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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.ql.parse.sql.PantheraExpParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.QueryInfo.Column;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockUtil;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * Transformer for multiple-table select.
 *
 */
public class CrossJoinTransformer extends BaseSqlASTTransformer {
  SqlASTTransformer tf;

  private static class JoinPair<T> {
    private final T first;
    private final T second;

    public JoinPair(T first, T second) {
      this.first = first;
      this.second = second;
    }

    public T getFirst() {
      return first;
    }

    public T getSecond() {
      return second;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof JoinPair<?>)) {
        return false;
      }
      JoinPair<T> otherPair = (JoinPair<T>) other;
      return (first.equals(otherPair.first) && second.equals(otherPair.second))
          || (first.equals(otherPair.second) && second.equals(otherPair.first));
    }

    @Override
    public int hashCode() {
      return first.hashCode() ^ second.hashCode();
    }
  }

  private class JoinInfo {
    // we use insertion-ordered LinkedHashMap so that table join order honors the order in the where
    // clause.
    public Map<JoinPair<String>, List<CommonTree>> joinPairInfo = new LinkedHashMap<JoinPair<String>, List<CommonTree>>();
    public Map<String, List<CommonTree>> joinFilterInfo = new HashMap<String, List<CommonTree>>();
  }

  public CrossJoinTransformer(SqlASTTransformer tf) {
    this.tf = tf;
  }

  @Override
  public void transform(CommonTree tree, TranslateContext context) throws SqlXlateException {
    tf.transformAST(tree, context);
    for (QueryInfo qf : context.getqInfoList()) {
      transformQuery(context, qf, qf.getSelectKeyForThisQ());
      // Update the from in the query info in case it was changed by the transformer.
      qf.setFrom((CommonTree) qf.getSelectKeyForThisQ().getFirstChildWithType(
          PantheraParser_PLSQLParser.SQL92_RESERVED_FROM));
    }
  }

  private void transformQuery(TranslateContext context, QueryInfo qf, CommonTree node)
      throws SqlXlateException {
    if (node.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT) {
      CommonTree from = (CommonTree) node
          .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM);
      // After multiplTableSelectTransformer, there must be only one TABLE_REF node under FROM node.
      assert (from.getChildCount() == 1);

      CommonTree join = (CommonTree) ((CommonTree) from.getChild(0))
          .getFirstChildWithType(PantheraParser_PLSQLParser.JOIN_DEF);
      // Skip if there is no join operation in the from clause.
      if (join != null) {
        JoinInfo joinInfo = new JoinInfo();

        //
        // Transform the where condition and generate the join operation info.
        //
        CommonTree where = (CommonTree) node
            .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_WHERE);
        if (where != null) {
          transformWhereCondition(context, qf, (CommonTree) where.getChild(0).getChild(0), joinInfo);
        }

        //
        // make an optimization, avoid tables join without join condition in the beginning
        //
        transformFromClauseOptimize(qf, from, joinInfo);
        //
        // Transform the from clause tree using the generated join operation info.
        //
        transformFromClause(qf, from, joinInfo);
      }
    }

    //
    // Transform subqueries in this query.
    //
    for (int i = 0; i < node.getChildCount(); i++) {
      CommonTree child = (CommonTree) node.getChild(i);
      if (child.getType() != PantheraParser_PLSQLParser.SQL92_RESERVED_FROM) {
        transformQuery(context, qf, child);
      }
    }
  }

  private void transformWhereCondition(TranslateContext context, QueryInfo qf, CommonTree node,
      JoinInfo joinInfo) throws SqlXlateException {
    //
    // We can only transform equality expression between two columns whose ancesotors are all AND
    // operators
    // into JOIN on ...
    //
    if (node.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_AND) {
      // Transform the left child.
      transformWhereCondition(context, qf, (CommonTree) node.getChild(0), joinInfo);
      // Transform the right child.
      transformWhereCondition(context, qf, (CommonTree) node.getChild(1), joinInfo);

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
      // HIVE does not support OR operator in join conditions
      List<CommonTree> OrList = new ArrayList<CommonTree>();
      FilterBlockUtil.findNode(node, PantheraParser_PLSQLParser.SQL92_RESERVED_OR, OrList);
      if (!OrList.isEmpty()) {
        return;
      }

      if (node.getType() == PantheraParser_PLSQLParser.EQUALS_OP) {
        //
        // Check if this is a equality expression between two columns
        //
        if (IsColumnRef(node.getChild(0)) && IsColumnRef(node.getChild(1))) {
          String table1 = getTableName(qf, (CommonTree) node.getChild(0).getChild(0));
          String table2 = getTableName(qf, (CommonTree) node.getChild(1).getChild(0));
          //
          // Skip columns not in a src table.
          //
          if (table1 == null || table2 == null) {
            return;
          }
          //
          // Update join info.
          //
          JoinPair<String> tableJoinPair = new JoinPair<String>(table1, table2);
          List<CommonTree> joinEqualityNodes = joinInfo.joinPairInfo.get(tableJoinPair);
          if (joinEqualityNodes == null) {
            joinEqualityNodes = new ArrayList<CommonTree>();
          }
          joinEqualityNodes.add(node);
          joinInfo.joinPairInfo.put(tableJoinPair, joinEqualityNodes);

          //
          // Create a new TRUE node and replace the current node with this new node.
          //
          CommonTree trueNode = FilterBlockUtil.createSqlASTNode(
              node, PantheraParser_PLSQLParser.SQL92_RESERVED_TRUE, "true");
          node.getParent().setChild(node.getChildIndex(), trueNode);
          return;
        }
      }

      // If there is a hint for keeping the node in the where clause, then skip it
      if (context.getBallFromBasket(node) != null) {
        return;
      }

      //
      // For a where condition that refers any columns from a single table and no subquery, then it
      // can be a join filter.
      //
      List<CommonTree> anyElementList = new ArrayList<CommonTree>();
      FilterBlockUtil.findNode(node, PantheraParser_PLSQLParser.ANY_ELEMENT, anyElementList);

      Set<String> referencedTables = new HashSet<String>();
      String srcTable;
      for (CommonTree anyElement : anyElementList) {
        srcTable = getTableName(qf, (CommonTree) anyElement);
        if (srcTable != null) {
          referencedTables.add(srcTable);
        } else {
          // If the condition refers to a table which is not in the from clause or refers to a
          // column
          // which is not existing, then skip this condition.
          return;
        }
      }

      if (referencedTables.size() == 1
          && !SqlXlateUtil
              .hasNodeTypeInTree(node, PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT)) {
        srcTable = (String) referencedTables.toArray()[0];

        //
        // Update join info.
        //
        List<CommonTree> joinFilterNodes = joinInfo.joinFilterInfo.get(srcTable);
        if (joinFilterNodes == null) {
          joinFilterNodes = new ArrayList<CommonTree>();
        }
        joinFilterNodes.add(node);
        joinInfo.joinFilterInfo.put(srcTable, joinFilterNodes);
        //
        // Create a new TRUE node and replace the current node with this new node.
        //
        CommonTree trueNode = FilterBlockUtil.createSqlASTNode(
            node, PantheraParser_PLSQLParser.SQL92_RESERVED_TRUE, "true");
        node.getParent().setChild(node.getChildIndex(), trueNode);
      }
    }
  }

  private boolean IsColumnRef(Tree node) {
    if (node.getType() == PantheraParser_PLSQLParser.CASCATED_ELEMENT
        && node.getChild(0).getType() == PantheraParser_PLSQLParser.ANY_ELEMENT) {
      return true;
    } else {
      return false;
    }
  }

  private String getTableName(QueryInfo qf, CommonTree anyElement) throws SqlXlateException {
    String table = null;

    CommonTree currentSelect = (CommonTree) anyElement
        .getAncestor(PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT);

    if (anyElement.getChildCount() > 1) {
      table = anyElement.getChild(0).getText();
      if (anyElement.getChildCount() > 2) {
        // schema.table
        //table += ("." + anyElement.getChild(1).getText());
        // merge schema and table as HIVE does not support schema.table.column in where clause.
        //anyElement.deleteChild(1);
        //((CommonTree) anyElement.getChild(0)).getToken().setText(table);
        table = anyElement.getChild(1).getText();
        anyElement.deleteChild(0);
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
          // Add table leaf node because HIVE needs table name for join operation.
          CommonTree tableNameNode = FilterBlockUtil.createSqlASTNode(anyElement, PantheraParser_PLSQLParser.ID,
              table);
          CommonTree columnNode = (CommonTree) anyElement.getChild(0);
          anyElement.setChild(0, tableNameNode);
          anyElement.addChild(columnNode);
          break;
        }
      }
    }
    return table;
  }

  private void transformFromClause(QueryInfo qf, CommonTree oldFrom, JoinInfo joinInfo)
      throws SqlXlateException {
    Set<String> alreadyJoinedTables = new HashSet<String>();

    CommonTree topTableRef = (CommonTree) oldFrom.getChild(0);
    SqlXlateUtil.getSrcTblAlias((CommonTree) topTableRef.getChild(0), alreadyJoinedTables);
    assert (alreadyJoinedTables.size() == 1);
    String firstTable = (String) alreadyJoinedTables.toArray()[0];
    for (int i = 1; i < topTableRef.getChildCount(); i++) {
      CommonTree joinNode = (CommonTree) topTableRef.getChild(i);
      Set<String> srcTables = new HashSet<String>();
      SqlXlateUtil.getSrcTblAlias((CommonTree) joinNode
          .getFirstChildWithType(PantheraParser_PLSQLParser.TABLE_REF_ELEMENT), srcTables);
      assert (srcTables.size() == 1);
      String srcTable = (String) srcTables.toArray()[0];

      // a flag used to identify whether the LEFG/RIGHT JOIN is user defined or Panthera transformed.
      boolean userJoinFlag = false;

      // if any column is referenced in join conditions, add missing table name for HIVE.
      CommonTree OnNode = (CommonTree) joinNode
          .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_ON);
      if (OnNode != null) {
        checkValidOnCondition(qf, (CommonTree) OnNode.getChild(0).getChild(0));
        List<CommonTree> anyElementList = new ArrayList<CommonTree>();
        FilterBlockUtil.findNode(OnNode, PantheraParser_PLSQLParser.ANY_ELEMENT, anyElementList);
        for (CommonTree anyElement : anyElementList) {
          getTableName(qf, anyElement);
        }
        if (joinNode.getChild(0).getType() == PantheraParser_PLSQLParser.LEFT_VK || joinNode
            .getChild(0).getType() == PantheraParser_PLSQLParser.RIGHT_VK
            || joinNode.getChild(0).getType() == PantheraParser_PLSQLParser.FULL_VK) {
          userJoinFlag = true;
        }
      }

      //need to consider pairs belong to same table, so first add it to alreadyJoinedTables
      alreadyJoinedTables.add(srcTable);
      for (String alreadyJoinedTable : alreadyJoinedTables) {
        JoinPair<String> tableJoinPair = new JoinPair<String>(alreadyJoinedTable, srcTable);
        List<CommonTree> JoinEqualityNodes = joinInfo.joinPairInfo.get(tableJoinPair);
        if (JoinEqualityNodes != null) {
          generateJoin(joinNode, JoinEqualityNodes);
          joinInfo.joinPairInfo.remove(tableJoinPair);
        }
      }

      List<CommonTree> joinFilters;
      if (i == 1) {
        // need consider the join filter of the first table
        joinFilters = joinInfo.joinFilterInfo.get(firstTable);
        if (joinFilters != null) {
          handleJoinFilters(joinNode, joinFilters, userJoinFlag);
          joinInfo.joinFilterInfo.remove(firstTable);
        }

        //need to do pairs belong to first table
        JoinPair<String> tableJoinPair = new JoinPair<String>(firstTable, firstTable);
        List<CommonTree> JoinEqualityNodes = joinInfo.joinPairInfo.get(tableJoinPair);
        if (JoinEqualityNodes != null) {
          generateJoin(joinNode, JoinEqualityNodes);
          joinInfo.joinPairInfo.remove(tableJoinPair);
        }
      }

      joinFilters = joinInfo.joinFilterInfo.get(srcTable);
      if (joinFilters != null) {
        handleJoinFilters(joinNode, joinFilters, userJoinFlag);
        joinInfo.joinFilterInfo.remove(srcTable);
      }
    }

    if (!joinInfo.joinPairInfo.isEmpty() || !joinInfo.joinFilterInfo.isEmpty()) {
      throw new SqlXlateException(topTableRef, "Cross join transformer: bad cross join!");
    }
  }

  /**
   * added to optimize from clause, just for test and verification
   * the main purpose of the method is to reorder the tables in from clause
   * so as to fully use the join condition in where clause.
   *
   * FIXME :
   * Only the format "t1,t2,t3" in from expression is considered,
   * the format "t1,t2 join t3 on ..." is not supported.
   *
   * @param qf
   * @param oldFrom
   * @param joinInfoOrig
   * @throws SqlXlateException
   */
  private void transformFromClauseOptimize(QueryInfo qf, CommonTree oldFrom, JoinInfo joinInfoOrig)
      throws SqlXlateException {

    Map<JoinPair<String>, List<CommonTree>> joinPairInfo =
        (Map<JoinPair<String>, List<CommonTree>>)((HashMap)(joinInfoOrig.joinPairInfo)).clone();
    Map<String, List<CommonTree>> joinFilterInfo =
        (Map<String, List<CommonTree>>)((HashMap)(joinInfoOrig.joinFilterInfo)).clone();

    Set<String> alreadyJoinedTables = new HashSet<String>();

    CommonTree topTableRef = (CommonTree) oldFrom.getChild(0);

    CommonTree addedJoinNode = FilterBlockUtil.createSqlASTNode(topTableRef, PantheraParser_PLSQLParser.JOIN_DEF, "join");
    //
    // Create a Cross node and attach it to the join node as the first child.
    //
    CommonTree crossNode = FilterBlockUtil.createSqlASTNode(topTableRef, PantheraParser_PLSQLParser.CROSS_VK, "cross");
    addedJoinNode.addChild(crossNode);

    //
    // add the join node on the first table
    // make all the tableStatement node the same format, make it easy to reorder
    //
    addedJoinNode.addChild(topTableRef.getChild(0));
    topTableRef.setChild(0, addedJoinNode);

    // choose the first table in the from expression
    CommonTree firstJoinNode = chooseFirstJoinNode(topTableRef, qf, joinPairInfo);
    if (firstJoinNode == null) {
      // remove the join and cross node for the first table
      topTableRef.setChild(0, ((CommonTree)topTableRef.getChild(0)).getFirstChildWithType(PantheraParser_PLSQLParser.TABLE_REF_ELEMENT));
      //nothing to do with optimization
      return;
    }
    // put the first table to the first node of the topTableRef
    for (int i = firstJoinNode.getChildIndex(); i > 0; i--) {
      topTableRef.setChild(i, topTableRef.getChild(i - 1));
    }
    topTableRef.setChild(0, firstJoinNode.getFirstChildWithType(PantheraParser_PLSQLParser.TABLE_REF_ELEMENT));

    SqlXlateUtil.getSrcTblAlias((CommonTree)((CommonTree) topTableRef.getChild(0)), alreadyJoinedTables);
    assert (alreadyJoinedTables.size() == 1);
    String firstTable = (String) alreadyJoinedTables.toArray()[0];

    List<CommonTree> noJoinPairJoinList = new LinkedList<CommonTree>();

    // reorder the tables
    for (int i = 1; i < topTableRef.getChildCount(); i++) {
      boolean findTable = false;
      for (int j = i; j < topTableRef.getChildCount(); j++) {
        CommonTree joinNode = (CommonTree) topTableRef.getChild(j);
        Set<String> srcTables = new HashSet<String>();
        SqlXlateUtil.getSrcTblAlias((CommonTree) joinNode
            .getFirstChildWithType(PantheraParser_PLSQLParser.TABLE_REF_ELEMENT), srcTables);
        assert (srcTables.size() == 1);
        String srcTable = (String) srcTables.toArray()[0];

        for (String alreadyJoinedTable : alreadyJoinedTables) {
          JoinPair tableJoinPair = new JoinPair(alreadyJoinedTable, srcTable);
          List<CommonTree> JoinEqualityNodes = joinPairInfo.get(tableJoinPair);
          if (JoinEqualityNodes != null) {
            for (int k = j; k > i; k--) {
              topTableRef.setChild(k, topTableRef.getChild(k - 1));
            }
            topTableRef.setChild(i, joinNode);
            alreadyJoinedTables.add(srcTable);
            findTable = true;
            break;
          }
        }
        if (findTable) {
          break;
        }
      }
      if (!findTable) {
        noJoinPairJoinList.add((CommonTree)topTableRef.getChild(i));
      }
    }

    // FIXME tables in noJoinPairJoinList may also have equal join conditions with each other
    // may also need to be ordered
    if (!noJoinPairJoinList.isEmpty()) {
      List<CommonTree> noFilterJoinList = new LinkedList<CommonTree>();
      for (CommonTree joinNode : noJoinPairJoinList) {
        // remove the child first and add it to last node later
        topTableRef.deleteChild(joinNode.getChildIndex());

        Set<String> tables = new HashSet<String>();
        SqlXlateUtil.getSrcTblAlias((CommonTree) joinNode
            .getFirstChildWithType(PantheraParser_PLSQLParser.TABLE_REF_ELEMENT), tables);
        assert (tables.size() == 1);
        String table = (String) tables.toArray()[0];
        List<CommonTree> joinFilters;
        joinFilters = joinFilterInfo.get(table);
        if (joinFilters != null) {
          topTableRef.addChild(joinNode);
        } else {
          noFilterJoinList.add(joinNode);
        }
      }
      if (!noFilterJoinList.isEmpty()) {
        for (CommonTree joinNode : noFilterJoinList) {
          topTableRef.addChild(joinNode);
        }
      }
    }
  }

  /**
   * just choose the first table that has equal join condition
   *
   * @param topTabRef
   * @param qf
   * @param joinPairInfo
   * @return
   * @throws SqlXlateException
   */
  private CommonTree chooseFirstJoinNode(CommonTree topTabRef, QueryInfo qf, Map<JoinPair<String>, List<CommonTree>> joinPairInfo)
      throws SqlXlateException {

    for (int i = 0; i < topTabRef.getChildCount() - 1; i++) {
      CommonTree leftJoinNode = (CommonTree) topTabRef.getChild(i);
      Set<String> leftJoinTables = new HashSet<String>();
      SqlXlateUtil.getSrcTblAlias((CommonTree) leftJoinNode
          .getFirstChildWithType(PantheraParser_PLSQLParser.TABLE_REF_ELEMENT), leftJoinTables);
      assert (leftJoinTables.size() == 1);
      String leftJoinTable = (String) leftJoinTables.toArray()[0];

      for (int j = i + 1; j < topTabRef.getChildCount(); j++) {
        CommonTree rightJoinNode = (CommonTree) topTabRef.getChild(j);
        Set<String> rightJoinTables = new HashSet<String>();
        SqlXlateUtil.getSrcTblAlias((CommonTree) rightJoinNode
            .getFirstChildWithType(PantheraParser_PLSQLParser.TABLE_REF_ELEMENT), rightJoinTables);
        assert (rightJoinTables.size() == 1);
        String rightJoinTable = (String) rightJoinTables.toArray()[0];

        JoinPair tableJoinPair = new JoinPair<String>(leftJoinTable, rightJoinTable);
        List<CommonTree> JoinEqualityNodes = joinPairInfo.get(tableJoinPair);
        if (JoinEqualityNodes != null) {
          return leftJoinNode;
        }
      }
    }
    return null;
  }

  /**
   * handle Join filters from where condition
   * push back the filters to where branch if join is user defined "LEFT (OUTER) JOIN" or "RIGHT (OUTER) JOIN"
   *
   * @param joinNode
   * @param joinConditionNodes
   */
  private void handleJoinFilters(CommonTree joinNode, List<CommonTree> joinConditionNodes, boolean isUserJoin) {
    CommonTree from = (CommonTree) joinNode.getParent().getParent();
    CommonTree where = (CommonTree) ((CommonTree) from.getParent())
        .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_WHERE);
    assert (where != null);
    if (isUserJoin) {
      pushBackJoinConditionToWhere(where, joinConditionNodes);
    } else {
      generateJoin(joinNode, joinConditionNodes);
    }
  }

  private void pushBackJoinConditionToWhere(CommonTree where, List<CommonTree> ConditionNodes) {
    if (where.getChild(0).getChild(0).getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_TRUE) {
      where.getChild(0).deleteChild(0);
    }
    addCondition(ConditionNodes, (CommonTree) where.getChild(0));
  }

  private void generateJoin(CommonTree joinNode, List<CommonTree> joinConditionNodes) {
    CommonTree OnNode;
    CommonTree logicExprNode;

    if (joinConditionNodes == null) {
      return;
    }

    OnNode = (CommonTree) joinNode
        .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_ON);
    if (OnNode == null) {
      //
      // Generate the join condition sub-tree.
      //
     CommonTree newOnNode = FilterBlockUtil.createSqlASTNode(
          joinNode, PantheraParser_PLSQLParser.SQL92_RESERVED_ON, "on");
      logicExprNode = FilterBlockUtil.createSqlASTNode(joinNode, PantheraParser_PLSQLParser.LOGIC_EXPR,
          "LOGIC_EXPR");
      newOnNode.addChild(logicExprNode);

      if (joinNode.getChild(0).getType() == PantheraParser_PLSQLParser.CROSS_VK) {
        if (joinNode.getChild(0).getText().equals(PantheraExpParser.LEFTSEMI_STR)) {
          ((CommonTree) joinNode.getChild(0)).getToken().setType(PantheraExpParser.LEFTSEMI_VK);
          joinNode.addChild(newOnNode);
        } else if (joinNode.getChild(0).getText().equals(PantheraExpParser.LEFT_STR)) {
          ((CommonTree) joinNode.getChild(0)).getToken().setType(PantheraExpParser.LEFT_VK);
          joinNode.addChild(newOnNode);
        } else {
          // Remove the CROSS node
          joinNode.setChild(0, joinNode.getChild(1));
          joinNode.setChild(1, newOnNode);
        }
      } else {
        joinNode.addChild(newOnNode);
      }
    } else {
      logicExprNode = (CommonTree) OnNode.getChild(0);
    }

    addCondition(joinConditionNodes, logicExprNode);
  }

  private void addCondition(List<CommonTree> joinConditionNodes, CommonTree logicExpr) {
    Iterator<CommonTree> iterator = joinConditionNodes.iterator();
    CommonTree expressionRoot;
    if (logicExpr.getChildCount() == 0) {
      expressionRoot = iterator.next();
    } else {
      expressionRoot = (CommonTree) logicExpr.getChild(0);
    }

    while (iterator.hasNext()) {
      CommonTree andNode = FilterBlockUtil.createSqlASTNode(
          expressionRoot, PantheraParser_PLSQLParser.SQL92_RESERVED_AND, "and");
      andNode.addChild(expressionRoot);
      andNode.addChild(iterator.next());
      expressionRoot = andNode;
    }

    if (logicExpr.getChildCount() == 0) {
      logicExpr.addChild(expressionRoot);
    } else {
      logicExpr.setChild(0, expressionRoot);
    }
  }

  /**
   * check whether there is OR node and non-equal join condition under ON node.
   * if there is, throw exception.
   *
   * @param context
   * @param qf
   * @param node
   * @param joinInfo
   * @throws SqlXlateException
   */
  private void checkValidOnCondition(QueryInfo qf, CommonTree node) throws SqlXlateException {

    if (node.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_AND) {
      // check the left child.
      checkValidOnCondition(qf, (CommonTree) node.getChild(0));
      // check the right child.
      checkValidOnCondition(qf, (CommonTree) node.getChild(1));

    } else {
      // HIVE does not support OR operator in join conditions
      List<CommonTree> OrList = new ArrayList<CommonTree>();
      FilterBlockUtil.findNode(node, PantheraParser_PLSQLParser.SQL92_RESERVED_OR, OrList);
      if (!OrList.isEmpty()) {
        throw new SqlXlateException(OrList.get(0),
            "Panthera don't support OR operation for join ON condition!");
      }

      // if the operation is not equal operation and there are columns from different table, hive would not support
      // this condition.
      if (node.getChildCount() == 2 && node.getType() != PantheraParser_PLSQLParser.EQUALS_OP
          && node.getType() != PantheraExpParser.EQUALS_NS) {
        List<CommonTree> leftAnyElementList = new ArrayList<CommonTree>();
        List<CommonTree> rightAnyElementList = new ArrayList<CommonTree>();
        FilterBlockUtil.findNode((CommonTree) node.getChild(0),
            PantheraParser_PLSQLParser.ANY_ELEMENT, leftAnyElementList);
        FilterBlockUtil.findNode((CommonTree) node.getChild(1),
            PantheraParser_PLSQLParser.ANY_ELEMENT, rightAnyElementList);
        if (!leftAnyElementList.isEmpty() && !rightAnyElementList.isEmpty()) {
          String table = null;
          boolean errFlag = false;
          // concat leftlist and rightlist into one to check whether there are columns from different tables;
          leftAnyElementList.addAll(rightAnyElementList);
          for (CommonTree anyElement : leftAnyElementList) {
            String table1 = getTableName(qf, anyElement);
            if (table1 == null) {
              errFlag = true;
              break;
            }
            if (table == null) {
              table = table1;
              continue;
            }
            if (!table.equals(table1)) {
              errFlag = true;
              break;
            }
          }
          if (errFlag) {
            throw new SqlXlateException(node, "Panthera don't support non-equal join condition!");
          }
        }
      }
    }
  }
}
