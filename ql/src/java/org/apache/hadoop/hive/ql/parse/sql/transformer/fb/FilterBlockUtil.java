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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import org.antlr.runtime.CommonToken;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.sql.PantheraConstants;
import org.apache.hadoop.hive.ql.parse.sql.PantheraExpParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.QueryInfo;
import org.apache.hadoop.hive.ql.parse.sql.transformer.QueryInfo.Column;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.QueryBlock.CountAsterisk;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

public class FilterBlockUtil {
  private static final Log LOG = LogFactory.getLog(FilterBlockUtil.class);

  private FilterBlockUtil() {
  }

  /**
   * dup the node keep same line, charpositioninline, startindex, stopindex, start, stop
   *
   * @param node
   * @return CommonTree
   */
  public static CommonTree dupNode(CommonTree node) {
    if (node == null) {
      return null;
    }
    CommonTree result = null;
    try {
      result = new CommonTree(new CommonToken(node.getToken()));
      result.setTokenStartIndex(node.getTokenStartIndex());
      result.setTokenStopIndex(node.getTokenStopIndex());
    } catch (Exception e) {
      LOG.error("ERROR Node:" + node);
    }
    return result;
  }


  /**
   * clone all branch of tree
   *
   * @param clone
   * @param node
   */
  public static void cloneTree(CommonTree clone, CommonTree node) {
    for (int i = 0; i < node.getChildCount(); i++) {
      CommonTree sub = (CommonTree) node.getChild(i);
      CommonTree cloneSub = dupNode(sub);
      clone.addChild(cloneSub);
      cloneTree(cloneSub, sub);
    }
  }

  /**
   * filter aggregation function from SELECT_LIST
   *
   * @param selectList
   * @return filtered aggregation function list.It's size equals SELECT_ITEM's number.
   */
  static List<CommonTree> filterAggregation(CommonTree selectList, CountAsterisk countAsterisk, CommonTree order) {
    if (selectList == null) {
      return null;
    }
    List<CommonTree> aggregationList = new ArrayList<CommonTree>();
    for (int i = 0; i < selectList.getChildCount(); i++) {
      CommonTree selectItem = (CommonTree) selectList.getChild(i);
      CommonTree expr = (CommonTree) selectItem.getChild(0);
      CommonTree alias = (CommonTree) selectItem.getChild(1);
      List<CommonTree> standardFunctionList = new ArrayList<CommonTree>();
      findNode(expr, PantheraParser_PLSQLParser.STANDARD_FUNCTION, standardFunctionList);
      // TODO only one function supported now, hard to more than one.
      // FIXME support complex expression without function(such as (col*3)/2)
      // TODO these code(and QueryBlock's) need clear
      if (standardFunctionList.size() == 1) {
        CommonTree aliasName;
        if (alias == null) {
          aliasName = createSqlASTNode(selectItem, PantheraParser_PLSQLParser.ID, PantheraConstants.PANTHERA_AGGR + i);
        } else {
          aliasName = (CommonTree) alias.getChild(0);
        }
        if (order != null) {
          CommonTree orderElements = (CommonTree) order.getChild(0);
          for (int j = 0; j < orderElements.getChildCount(); j++) {
            CommonTree orderExpr = (CommonTree) orderElements.getChild(j).getChild(0);
            if (equalsTree((CommonTree) selectItem.getChild(0), orderExpr)) {
              CommonTree newOrderAny = cloneTree(aliasName);
              newOrderAny.getToken().setCharPositionInLine(orderExpr.getCharPositionInLine());
              newOrderAny.setTokenStartIndex(orderExpr.getTokenStartIndex());
              newOrderAny.setTokenStopIndex(orderExpr.getTokenStopIndex());
              orderExpr.replaceChildren(0, 0, createCascatedElement(newOrderAny));
            }
          }
        }
        CommonTree standardFunction = standardFunctionList.get(0);
        // deal with count(*) and count(1) in the same way
        if (standardFunction.getChild(0).getType() == PantheraParser_PLSQLParser.COUNT_VK
            && (standardFunction.getChild(0).getChild(0).getType() == PantheraParser_PLSQLParser.ASTERISK
            || (standardFunction.getChild(0).getChild(0).getType() == PantheraParser_PLSQLParser.EXPR
            && standardFunction.getChild(0).getChild(0).getChild(0).getType() == PantheraParser_PLSQLParser.UNSIGNED_INTEGER))) {
          countAsterisk.setPosition(i);
          countAsterisk.setSelectItem(selectItem);
          if (selectList.getChildCount() == 1) {
            countAsterisk.setOnlyAsterisk(true);
          }
          // store countAsterisk as count(1) here
          CommonTree count = (CommonTree) standardFunction.getChild(0);
          CommonTree cntExpr = FilterBlockUtil.createSqlASTNode((CommonTree) count.deleteChild(0), PantheraParser_PLSQLParser.EXPR, "EXPR");
          count.addChild(cntExpr);
          cntExpr.addChild(FilterBlockUtil.createSqlASTNode(cntExpr, PantheraParser_PLSQLParser.UNSIGNED_INTEGER, "1"));
          continue;
        }
        List<CommonTree> exprList = new ArrayList<CommonTree>();
        findNode(standardFunction, PantheraParser_PLSQLParser.EXPR, exprList);
        CommonTree expr2 = exprList.get(0);
        CommonTree cascatedElement = (CommonTree) expr2.deleteChild(0);
        CommonTree func = cloneTree((CommonTree) expr.getChild(0));
        CommonTree parent = (CommonTree) standardFunction.getParent();
        for (int j = 0; j < parent.getChildCount(); j++) {
          if (parent.getChild(j) == standardFunction) {
            parent.deleteChild(j);
            if (parent.getChildren() == null) {
              parent.addChild(cascatedElement);
            } else {
              parent.getChildren().add(j, cascatedElement);
            }
          }
        }
        aggregationList.add(func);
        expr.deleteChild(0);
        expr.addChild(cascatedElement);
      } else {
        aggregationList.add(null);
      }
    }
    if (countAsterisk.getSelectItem() != null) {
      selectList.deleteChild(countAsterisk.getPosition());
    }
    return aggregationList;
  }

  public static void findAllSelectForSet(CommonTree subQuery, List<CommonTree> nodeList) {
    if (subQuery.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT) {
      nodeList.add(subQuery);
    } else if (subQuery.getType() == PantheraParser_PLSQLParser.SUBQUERY) {
      if (subQuery.getChildCount() == 1) {
        nodeList.add((CommonTree) subQuery.getChild(0));
      } else {
        findAllSelectForSet((CommonTree) subQuery.getChild(0), nodeList);
        findAllSelectForSet((CommonTree) subQuery.getChild(1).getChild(0), nodeList);
      }
    }
  }

  /**
   * find all node which type is input type in the tree which root is node.
   *
   * @param node
   * @param type
   * @param nodeList
   */
  public static void findNode(CommonTree node, int type, List<CommonTree> nodeList) {
    if (node == null) {
      return;
    }
    if (node.getType() == type) {
      nodeList.add(node);
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      findNode((CommonTree) node.getChild(i), type, nodeList);
    }
  }

  /**
   * find all node which structure is same as the other tree.
   *
   * @param node
   * @param criteria
   * @param nodeList
   */
  public static void findSubTree(CommonTree node, CommonTree criteria, List<CommonTree> nodeList) {
    if (node == null) {
      return;
    }
    if (equalsTree(node, criteria)) {
      nodeList.add(node);
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      findSubTree((CommonTree) node.getChild(i), criteria, nodeList);
    }
  }

  public static CommonTree findOnlyNode(CommonTree node, int type) {
    List<CommonTree> nodeList = new ArrayList<CommonTree>();
    findNode(node, type, nodeList);
    return nodeList.isEmpty() ? null : nodeList.get(0);
  }

  public static CommonTree findOnlyNodeWithPosition(CommonTree node, int type, int position) {
    List<CommonTree> nodeList = new ArrayList<CommonTree>();
    findNode(node, type, nodeList);
    CommonTree ret = null;
    for (CommonTree n : nodeList) {
      if (n.getCharPositionInLine() == position){
        ret = n;
        break;
      }
    }
    return ret;
  }

  /**
   * clone tree
   *
   * @param tree
   *          origin tree
   * @return clone tree root
   */
  public static CommonTree cloneTree(CommonTree tree) {
    if (tree == null) {
      return null;
    }
    CommonTree root = dupNode(tree);
    cloneTree(root, tree);
    return root;
  }

  /**
   * get tableName or alias from select node.
   *
   * @param select
   * @return
   */
  public static Set<String> getTableName(CommonTree select) {
    Set<String> result = new HashSet<String>();
    SqlXlateUtil.getSrcTblAndAlias((CommonTree) select
        .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM), result);
    return result;
  }

  /**
   * get table name for column under anyElement. By default, if there is only column
   * under anyElement node, tablename will be add to it.
   */
  public static String getTableName(QueryInfo qf, CommonTree anyElement) throws SqlXlateException {
    return getTableName(qf, anyElement, true);
  }

  /**
   * get the table name of the column via QueryInfo
   *
   * @param qf
   *        input QueryInfo
   * @param anyElement
   *        anyElement AST node, for which column will get the table name
   * @param needAddTable
   *        indicate whether tablename need to add to anyelement node when there is only column under it.
   * @return
   *        return table name, string type
   * @throws SqlXlateException
   */
  public static String getTableName(QueryInfo qf, CommonTree anyElement, boolean needAddTable) throws SqlXlateException {
    String table = null;

    CommonTree currentSelect = (CommonTree) anyElement
        .getAncestor(PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT);

    assert(anyElement.getChildCount() != 0);
    if (anyElement.getChildCount() > 1) {
      assert(anyElement.getChildCount() == 2);
      table = anyElement.getChild(0).getText();
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
          if (needAddTable) {  // whether need to add table under anyelement node
            // Add table leaf node because HIVE needs table name for join operation.
            CommonTree tableNameNode = FilterBlockUtil.createSqlASTNode(anyElement,
                PantheraParser_PLSQLParser.ID,
                table);
            CommonTree columnNode = (CommonTree) anyElement.getChild(0);
            anyElement.setChild(0, tableNameNode);
            anyElement.addChild(columnNode);
          }
          break;
        }
      }
    }
    return table;
  }

  public static CommonTree deleteBranch(CommonTree root, int branchType) {
    if (root == null) {
      return null;
    }
    for (int i = 0; i < root.getChildCount(); i++) {
      if (root.getChild(i).getType() == branchType) {
        return (CommonTree) root.deleteChild(i);
      }
    }
    return null;
  }

  /**
   * the return node remains same line and charpositioninline info with copy node
   * @param copy
   * @param type
   * @param text
   * @return
   */
  public static CommonTree createSqlASTNode(CommonTree copy, int type, String text) {
    CommonTree retTree = dupNode(copy);
    retTree.token.setType(type);
    retTree.token.setText(text);
    return retTree;
  }

  public static CommonTree createAlias(CommonTree srcnode, TranslateContext context) {
    CommonTree alias = createSqlASTNode(srcnode, PantheraParser_PLSQLParser.ALIAS, "ALIAS");
    CommonTree aliasName = createSqlASTNode(alias, PantheraParser_PLSQLParser.ID, context
        .getAliasGen().generateAliasName());
    alias.addChild(aliasName);
    return alias;
  }

  /**
   * create alias with a specified name
   */
  public static CommonTree createAlias(CommonTree srcnode, String aliasName) {
    CommonTree alias = createSqlASTNode(srcnode, PantheraParser_PLSQLParser.ALIAS, "ALIAS");
    alias.addChild(createSqlASTNode(alias, PantheraParser_PLSQLParser.ID, aliasName));
    return alias;
  }

  public static CommonTree createFunction(CommonTree srcNode, String functionName,
      CommonTree element) {
    CommonTree standardFunction = createSqlASTNode(srcNode,
        PantheraParser_PLSQLParser.STANDARD_FUNCTION, "STANDARD_FUNCTION");
    CommonTree function = createSqlASTNode(srcNode,
        PantheraParser_PLSQLParser.FUNCTION_ENABLING_OVER, functionName);
    standardFunction.addChild(function);
    CommonTree arguments = createSqlASTNode(srcNode, PantheraParser_PLSQLParser.ARGUMENTS,
        "ARGUMENTS");
    function.addChild(arguments);
    CommonTree argument = createSqlASTNode(srcNode, PantheraParser_PLSQLParser.ARGUMENT, "ARGUMENT");
    arguments.addChild(argument);
    CommonTree expr = createSqlASTNode(srcNode, PantheraParser_PLSQLParser.EXPR, "EXPR");
    argument.addChild(expr);
    expr.addChild(element);
    return standardFunction;
  }

  public static boolean isFilterOp(CommonTree node) {
    //TODO how about NOT
    //TODO do you have a better idea?
    int type = node.getType();
    switch (type) {
    case PantheraParser_PLSQLParser.EQUALS_OP:
    case PantheraExpParser.EQUALS_NS:
    case PantheraParser_PLSQLParser.GREATER_THAN_OP:
    case PantheraParser_PLSQLParser.GREATER_THAN_OR_EQUALS_OP:
    case PantheraParser_PLSQLParser.LESS_THAN_OP:
    case PantheraParser_PLSQLParser.LESS_THAN_OR_EQUALS_OP:
    case PantheraParser_PLSQLParser.NOT_EQUAL_OP:
    case PantheraExpParser.SQL92_RESERVED_LIKE:
    case PantheraExpParser.NOT_LIKE:
    case PantheraExpParser.SQL92_RESERVED_IN:
    case PantheraExpParser.NOT_IN:
    case PantheraExpParser.IS_NULL:
    case PantheraExpParser.IS_NOT_NULL:
      return true;
    default:
      return false;
    }
  }

  public static boolean isLogicOp(CommonTree node) {
    int type = node.getType();
    switch (type) {
    case PantheraParser_PLSQLParser.SQL92_RESERVED_AND:
    case PantheraParser_PLSQLParser.SQL92_RESERVED_OR:
      return true;
    default:
      return false;
    }
  }

  /**
   * check the input node is a column
   */
  public static boolean IsColumnRef(Tree node) {
    if (node.getType() == PantheraParser_PLSQLParser.CASCATED_ELEMENT
        && node.getChild(0).getType() == PantheraParser_PLSQLParser.ANY_ELEMENT) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * delete the node and re-balance the tree
   *
   * @param op
   */
  public static void deleteTheNode(CommonTree op) {
    int opIndex = op.getChildIndex();
    CommonTree parent = (CommonTree) op.getParent();
    int parentIndex = parent.getChildIndex();
    CommonTree grandpa = (CommonTree) parent.getParent();
    CommonTree brother = (CommonTree) parent.getChild(opIndex == 0 ? 1 : 0);
    grandpa.deleteChild(parentIndex);
    SqlXlateUtil.addCommonTreeChild(grandpa, parentIndex, brother);
  }

  /**
   * @param select
   * @return SUBQUERY node
   */
  public static CommonTree makeSelectBranch(CommonTree select, TranslateContext context, CommonTree oldFrom) {
    CommonTree from = createSqlASTNode(oldFrom, PantheraParser_PLSQLParser.SQL92_RESERVED_FROM,
        "from");
    select.addChild(from);
    CommonTree tableRef = createSqlASTNode(select, PantheraParser_PLSQLParser.TABLE_REF,
        "TABLE_REF");
    from.addChild(tableRef);
    CommonTree tableRefElement = createSqlASTNode(select,
        PantheraParser_PLSQLParser.TABLE_REF_ELEMENT, "TABLE_REF_ELEMENT");
    tableRef.addChild(tableRefElement);
    CommonTree alias = createAlias(tableRefElement, context);
    tableRefElement.addChild(alias);
    CommonTree tableExpression = createSqlASTNode(select,
        PantheraParser_PLSQLParser.TABLE_EXPRESSION, "TABLE_EXPRESSION");
    tableRefElement.addChild(tableExpression);
    CommonTree selectMode = createSqlASTNode(select, PantheraParser_PLSQLParser.SELECT_MODE,
        "SELECT_MODE");
    tableExpression.addChild(selectMode);
    CommonTree selectStatement = createSqlASTNode(select,
        PantheraParser_PLSQLParser.SELECT_STATEMENT, "SELECT_STATEMENT");
    selectMode.addChild(selectStatement);
    CommonTree subquery = createSqlASTNode(select, PantheraExpParser.SUBQUERY, "SUBQUERY");
    selectStatement.addChild(subquery);
    return subquery;
  }

  public static String makeSelectBranch(CommonTree select, CommonTree subQuerySelect, TranslateContext context) {
    CommonTree oldFrom = (CommonTree) subQuerySelect.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM);
    CommonTree from = createSqlASTNode(oldFrom, PantheraParser_PLSQLParser.SQL92_RESERVED_FROM,
        "from");
    select.addChild(from);
    CommonTree tableRef = createSqlASTNode(select, PantheraParser_PLSQLParser.TABLE_REF,
        "TABLE_REF");
    from.addChild(tableRef);
    CommonTree tableRefElement = createSqlASTNode(select,
        PantheraParser_PLSQLParser.TABLE_REF_ELEMENT, "TABLE_REF_ELEMENT");
    tableRef.addChild(tableRefElement);
    CommonTree alias = createAlias(tableRefElement, context);
    tableRefElement.addChild(alias);
    CommonTree tableExpression = createSqlASTNode(select,
        PantheraParser_PLSQLParser.TABLE_EXPRESSION, "TABLE_EXPRESSION");
    tableRefElement.addChild(tableExpression);
    CommonTree selectMode = createSqlASTNode(select, PantheraParser_PLSQLParser.SELECT_MODE,
        "SELECT_MODE");
    tableExpression.addChild(selectMode);
    CommonTree selectStatement = createSqlASTNode(select,
        PantheraParser_PLSQLParser.SELECT_STATEMENT, "SELECT_STATEMENT");
    selectMode.addChild(selectStatement);
    CommonTree subquery = createSqlASTNode(select, PantheraExpParser.SUBQUERY, "SUBQUERY");
    selectStatement.addChild(subquery);
    subquery.addChild(subQuerySelect);
    return alias.getChild(0).getText();
  }

  /**
   * replace column by alias(for clone SELECT_LIST to top select node)
   *
   * @param selectList
   */
  public static void rebuildSelectAlias(CommonTree selectList) {
    for (int i = 0; i < selectList.getChildCount(); i++) {
      CommonTree selectItem = (CommonTree) selectList.getChild(i);
      if (selectItem.getChildCount() == 2) {
        String alias = selectItem.getChild(1).getChild(0).getText();
        // FIXME what if selectItem is not that easy?
        CommonTree column = (CommonTree) selectItem.getChild(0).getChild(0).getChild(0).getChild(0);
        column.getToken().setText(alias);
      }
    }
  }

  /**
   * clone SELECT_LIST. if it is null, return *
   *
   * @param originalSelectList
   * @return
   * @throws SqlXlateException
   */
  public static CommonTree cloneSelectListFromSelect(CommonTree originalSelect)
      throws SqlXlateException {
    CommonTree selectList;
    CommonTree originalSelectList = (CommonTree) originalSelect
        .getFirstChildWithType(PantheraExpParser.SELECT_LIST);
    if (originalSelectList != null) {
      selectList = cloneTree(originalSelectList);
      removeTableNameFromSelectList(selectList);
      rebuildSelectAlias(selectList);
    } else {
      CommonTree originalAsterisk = (CommonTree) originalSelect
          .getFirstChildWithType(PantheraExpParser.ASTERISK);
      if (originalAsterisk == null) {
        throw new SqlXlateException(originalSelect, "Select without select-list or asterisk.");
      }
      selectList = createSqlASTNode(originalAsterisk, PantheraExpParser.ASTERISK, "*");
    }
    return selectList;
  }

  /**
   * clone SELECT_LIST by SELECT_ITEM ALIAS<br>
   * call this method after make sure select item all has alias<br>
   *
   * @param originalSelectList
   * @return new built selectList
   */
  public static CommonTree cloneSelectListByAliasFromSelect(CommonTree originalSelect) {
    CommonTree selectList;
    CommonTree originalSelectList = (CommonTree) originalSelect
        .getFirstChildWithType(PantheraExpParser.SELECT_LIST);
    if (originalSelectList != null) {
      int selectNum = originalSelectList.getChildCount();
      selectList = createSqlASTNode(originalSelectList, PantheraExpParser.SELECT_LIST,
          "SELECT_LIST");
      for (int i = 0; i < selectNum; i++) {
        CommonTree originalAlias = (CommonTree) originalSelectList.getChild(i).getChild(1);
        CommonTree selectItem = dupNode((CommonTree) originalSelectList.getChild(i));
        CommonTree expr = createSqlASTNode(selectItem, PantheraExpParser.EXPR, "EXPR");
        selectItem.addChild(expr);
        CommonTree cascatedElement = createSqlASTNode(selectItem,
            PantheraExpParser.CASCATED_ELEMENT, "CASCATED_ELEMENT");
        expr.addChild(cascatedElement);
        CommonTree anyElement = createSqlASTNode(selectItem, PantheraExpParser.ANY_ELEMENT,
            "ANY_ELEMENT");
        cascatedElement.addChild(anyElement);
        assert(originalAlias != null);
        String columnName = originalAlias.getChild(0).getText();
        CommonTree coloumNameSrc = (CommonTree) originalAlias.getChild(0);
        CommonTree alias = dupNode(originalAlias);
        CommonTree aliasName = createSqlASTNode(coloumNameSrc, PantheraParser_PLSQLParser.ID,
            columnName);
        alias.addChild(aliasName);
        selectItem.addChild(alias);
        CommonTree columnId = createSqlASTNode(coloumNameSrc, PantheraExpParser.ID, columnName);
        anyElement.addChild(columnId);
        selectList.addChild(selectItem);
      }
    } else {
      CommonTree originalAsterisk = (CommonTree) originalSelect
          .getFirstChildWithType(PantheraExpParser.ASTERISK);
      selectList = createSqlASTNode(originalAsterisk, PantheraExpParser.ASTERISK, "*");
    }
    return selectList;
  }

  public static void removeTableNameFromSelectList(CommonTree selectList) {
    for (int i = 0; i < selectList.getChildCount(); i++) {
      CommonTree anyElement = (CommonTree) selectList.getChild(i).getChild(0).getChild(0).getChild(
          0);
      if (anyElement.getChildCount() > 1) {
        anyElement.deleteChild(0);
      }
    }
  }

  /**
   * add sequence alias name in table
   *
   * @param select
   */
  public static List<CommonTree> addColumnAlias(CommonTree select, TranslateContext context) {
    CommonTree selectList = (CommonTree) select
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    if (selectList == null) {
      return null;
    }
    List<CommonTree> result = new ArrayList<CommonTree>();
    int count = 0;
    for (int i = 0; i < selectList.getChildCount(); i++) {
      CommonTree selectItem = (CommonTree) selectList.getChild(i);
      if (selectItem.getChildCount() == 1) {
        CommonTree anyElement = (CommonTree) selectItem.getChild(0).getChild(0).getChild(0);
        String columnStr = null;
        if (anyElement != null && anyElement.getType() == PantheraExpParser.ANY_ELEMENT) {
          columnStr = anyElement.getChildCount() == 1 ? anyElement.getChild(0).getText()
              : anyElement.getChild(1).getText();
        }

        CommonTree alias = createSqlASTNode(selectItem, PantheraParser_PLSQLParser.ALIAS, "ALIAS");
        String aliasStr = PantheraConstants.PANTHERA_COL + count++;
        result.add(alias);
        CommonTree aliasName = createSqlASTNode(selectItem, PantheraParser_PLSQLParser.ID, aliasStr);
        alias.addChild(aliasName);
        selectItem.addChild(alias);

        // for order by & group by
        boolean existInBasket = false;
        for (Entry<Object, Object> e : context.getBasket().entrySet()) {
          Object value = e.getValue();
          if (value instanceof String) {
            String oldAliasStr = value.toString();
            if (oldAliasStr.equals(columnStr)) {
              e.setValue(aliasStr);
              existInBasket = true;
              break;
            }
          }
        }
        if (!existInBasket) {
          context.putBallToBasket(columnStr, aliasStr);
        }
      } else { // originally there is alias
        result.add((CommonTree)selectItem.getChild(1));
      }
    }
    return result;
  }

  /**
   * add origin column name as alias name in table
   *
   * @param select
   */
  public static List<CommonTree> addColumnAliasOrigin(CommonTree select, TranslateContext context) {
    CommonTree selectList = (CommonTree) select
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    if (selectList == null) {
      return null;
    }
    List<CommonTree> result = new ArrayList<CommonTree>();
    for (int i = 0; i < selectList.getChildCount(); i++) {
      CommonTree selectItem = (CommonTree) selectList.getChild(i);
      if (selectItem.getChildCount() == 1) {
        CommonTree anyElement = (CommonTree) selectItem.getChild(0).getChild(0).getChild(0);
        String columnStr = "pantehra_expr_" + i;
        if (anyElement != null && anyElement.getType() == PantheraExpParser.ANY_ELEMENT) {
          columnStr = anyElement.getChildCount() == 1 ? anyElement.getChild(0).getText()
              : anyElement.getChild(1).getText();
        }

        CommonTree alias = createSqlASTNode(selectItem, PantheraParser_PLSQLParser.ALIAS, "ALIAS");
        String aliasStr = columnStr;
        result.add(alias);
        CommonTree aliasName = createSqlASTNode(selectItem, PantheraParser_PLSQLParser.ID, aliasStr);
        alias.addChild(aliasName);
        selectItem.addChild(alias);
      } else { // originally there is alias
        result.add((CommonTree)selectItem.getChild(1));
      }
    }
    return result;
  }

  public static List<CommonTree> addColumnAliasHard(CommonTree select, List<CommonTree> aliasList,
      TranslateContext context) throws SqlXlateException {
    CommonTree selectList = (CommonTree) select
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    if (selectList == null) {
      return null;
    }
    if (selectList.getChildCount() != aliasList.size()) {
      throw new SqlXlateException(selectList, "Right select of union should have a select-list with same number of select-item as left.");
    }
    for (int i = 0; i < selectList.getChildCount(); i++) {
      CommonTree selectItem = (CommonTree) selectList.getChild(i);
      CommonTree anyElement = (CommonTree) selectItem.getChild(0).getChild(0).getChild(0);
      String columnStr = null;
      if (anyElement != null && anyElement.getType() == PantheraExpParser.ANY_ELEMENT) {
        columnStr = anyElement.getChildCount() == 1 ? anyElement.getChild(0).getText()
            : anyElement.getChild(1).getText();
      }

      String aliasStr = aliasList.get(i).getChild(0).getText();
      if (selectItem.getChildCount() == 1) {
        CommonTree alias = createSqlASTNode(selectItem, PantheraParser_PLSQLParser.ALIAS, "ALIAS");
        CommonTree aliasName = createSqlASTNode(selectItem, PantheraParser_PLSQLParser.ID, aliasStr);
        alias.addChild(aliasName);
        selectItem.addChild(alias);
      } else { // originally there is alias
        CommonTree aliasName = (CommonTree) selectItem.getChild(1).getChild(0);
        aliasName.getToken().setText(aliasStr);
      }

      // for order by & group by
      boolean existInBasket = false;
      for (Entry<Object, Object> e : context.getBasket().entrySet()) {
        Object value = e.getValue();
        if (value instanceof String) {
          String oldAliasStr = value.toString();
          if (oldAliasStr.equals(columnStr)) {
            e.setValue(aliasStr);
            existInBasket = true;
            break;
          }
        }
      }
      if (!existInBasket) {
        context.putBallToBasket(columnStr, aliasStr);
      }

    }
    return aliasList;

  }

  /**
   * must not have alias when call
   *
   * @param tableRefElement
   * @param context
   * @return
   * newly added table alias name
   */
  public static String addTableAlias(CommonTree tableRefElement, TranslateContext context) {
    CommonTree alias = createAlias(tableRefElement, context);
    tableRefElement.addChild(alias);
    SqlXlateUtil.exchangeChildrenPosition(tableRefElement);
    return alias.getChild(0).getText();
  }

  public static CommonTree createCascatedElementBranch(CommonTree op, String tableAlias,
      String columnName) {
    CommonTree cascatedElement = createSqlASTNode(op, PantheraParser_PLSQLParser.CASCATED_ELEMENT,
        "CASCATED_ELEMENT");
    CommonTree anyElement = createSqlASTNode(op, PantheraParser_PLSQLParser.ANY_ELEMENT,
        "ANY_ELEMENT");
    cascatedElement.addChild(anyElement);
    CommonTree tableNode = createSqlASTNode(op, PantheraExpParser.ID, tableAlias);
    CommonTree columnNode = createSqlASTNode(op, PantheraExpParser.ID, columnName);
    anyElement.addChild(tableNode);
    anyElement.addChild(columnNode);
    return cascatedElement;
  }

  /**
   * Create TABLE_REF_ELEMENT & attach select node to it.
   *
   * @param select
   * @param context
   * @return
   *        TABLE_REF_ELEMENT node
   */
  public static CommonTree createTableRefElement(CommonTree select, TranslateContext context) {
    CommonTree viewAlias = createAlias(select, context);
    return createTableRefElementWithAlias(select, viewAlias, context);
  }

  /**
   * Create TABLE_REF_ELEMENT & attach select node to it.
   * & attach TABLE_REF_ELEMENT to parent.
   *
   * @param TABLE_REF node to attach TABLE_REF_ELEMENT to.
   * @param select
   * @param context
   * @return
   *        table alias id node
   */
  public static CommonTree addTableRefElement(CommonTree parent, CommonTree select, TranslateContext context) {
    CommonTree viewAlias = createAlias(select, context);
    parent.addChild(createTableRefElementWithAlias(select, viewAlias, context));
    return (CommonTree) viewAlias.getChild(0);
  }

  /**
   * Create TABLE_REF_ELEMENT & attach select node to it.
   *
   * @param select
   * @param ALIAS node
   * @param TranslateContext
   * @return
   *        TABLE_REF_ELEMENT node
   */
  public static CommonTree createTableRefElementWithAlias(CommonTree select, CommonTree viewAlias, TranslateContext context) {
    CommonTree tableRefElement = createSqlASTNode(select,
        PantheraParser_PLSQLParser.TABLE_REF_ELEMENT, "TABLE_REF_ELEMENT");
    tableRefElement.addChild(viewAlias);
    CommonTree tableExpression = createSqlASTNode(select,
        PantheraParser_PLSQLParser.TABLE_EXPRESSION, "TABLE_EXPRESSION");
    tableRefElement.addChild(tableExpression);
    CommonTree selectMode = createSqlASTNode(select, PantheraParser_PLSQLParser.SELECT_MODE,
        "SELECT_MODE");
    tableExpression.addChild(selectMode);
    CommonTree selectStatement = createSqlASTNode(select,
        PantheraParser_PLSQLParser.SELECT_STATEMENT, "SELECT_STATEMENT");
    selectMode.addChild(selectStatement);
    CommonTree subQuery = createSqlASTNode(select, PantheraParser_PLSQLParser.SUBQUERY, "SUBQUERY");
    selectStatement.addChild(subQuery);
    subQuery.addChild(select);
    return tableRefElement;
  }

  /**
   * add alias for every column & build column alias map
   *
   * @param alias
   *          table alias node
   * @param selectList
   * @return
   */
  public static List<CommonTree> buildSelectListAlias(CommonTree selectList,
      TranslateContext context) {
    List<CommonTree> aliasList = new ArrayList<CommonTree>();
    for (int i = 0; i < selectList.getChildCount(); i++) {
      CommonTree selectItem = (CommonTree) selectList.getChild(i);
      CommonTree columnAlias;
      if (selectItem.getChildCount() > 1) {// had alias
        columnAlias = (CommonTree) selectItem.getChild(1);
      } else {
        columnAlias = addAlias(selectItem, context);
      }
      aliasList.add(columnAlias);
    }
    return aliasList;
  }

  public static CommonTree addAlias(CommonTree node, TranslateContext context) {
    CommonTree alias;
    alias = (CommonTree) node.getFirstChildWithType(PantheraParser_PLSQLParser.ALIAS);
    if (alias == null) {
      alias = createAlias(node, context);
      node.addChild(alias);
    }
    return alias;
  }

  public static CommonTree createSelectList(CommonTree srcNode, List<CommonTree> aliasList) {
    CommonTree selectList = createSqlASTNode(srcNode, PantheraParser_PLSQLParser.SELECT_LIST,
        "SELECT_LIST");
    for (CommonTree alias : aliasList) {
      addSelectItem(selectList, createCascatedElement((CommonTree) alias.getChild(0)));
    }
    return selectList;
  }

  public static CommonTree addSelectItem(CommonTree selectList, CommonTree cascatedElement) {
    if (cascatedElement == null || cascatedElement.getChildren() == null) {
      return null;
    }
    CommonTree selectItem = createSqlASTNode(cascatedElement,
        PantheraParser_PLSQLParser.SELECT_ITEM, "SELECT_ITEM");
    selectList.addChild(selectItem);
    CommonTree expr = createSqlASTNode(cascatedElement, PantheraParser_PLSQLParser.EXPR, "EXPR");
    selectItem.addChild(expr);
    expr.addChild(cascatedElement);
    return selectItem;
  }

  public static CommonTree createCascatedElement(CommonTree child) {
    CommonTree cascatedElement = createSqlASTNode(child,
        PantheraParser_PLSQLParser.CASCATED_ELEMENT, "CASCATED_ELEMENT");
    CommonTree anyElement = createSqlASTNode(child, PantheraParser_PLSQLParser.ANY_ELEMENT,
        "ANY_ELEMENT");
    cascatedElement.addChild(anyElement);
    anyElement.addChild(child);
    return cascatedElement;
  }

  /**
   * make equal condition ON
   *
   * @param leftAlias
   * @param rightAlias
   * @param leftColumnAliasList
   * @param rightColumnAliasList
   * @return
   */
  public static CommonTree makeOn(CommonTree nodeUsing, String leftAlias, String rightAlias,
      List<String> leftColumnAliasList, List<String> rightColumnAliasList) {
    CommonTree on = createSqlASTNode(nodeUsing, PantheraParser_PLSQLParser.SQL92_RESERVED_ON, "on");
    CommonTree logicExpr = createSqlASTNode(on, PantheraParser_PLSQLParser.LOGIC_EXPR, "LOGIC_EXPR");
    on.addChild(logicExpr);
    int count = leftColumnAliasList.size();
    for (int i = 0; i < count; i++) {
      CommonTree condition = makeEqualCondition(on, leftAlias, rightAlias, leftColumnAliasList
          .get(i), rightColumnAliasList.get(i));
      FilterBlockUtil.addConditionToLogicExpr(logicExpr, condition);
    }
    return on;
  }

  /**
   * make equal condition as leftTable.column = rightTable.column <br>
   * no need to use <=> here
   *
   * @param on
   * @param leftTable
   * @param rightTable
   * @param leftColumn
   * @param rightColumn
   * @return return equal node
   */
  public static CommonTree makeEqualCondition(CommonTree on, String leftAlias, String rightAlias,
      String leftColumn, String rightColumn) {
    CommonTree equal = createSqlASTNode(on, PantheraExpParser.EQUALS_OP, "=");
    equal.addChild(createCascatedElementBranch(equal, leftAlias, leftColumn));
    equal.addChild(createCascatedElementBranch(equal, rightAlias, rightColumn));
    return equal;

  }

  /**
   * return -1 if contain subQuery,
   * return 0 if uncorrelated,
   * return a positive number if correlated, indicating correlating level
   * @param tree
   * @return
   * @throws SqlXlateException
   */
  public static int getConditionLevel(QueryInfo qi, CommonTree tree, final Stack<CommonTree> selectStack,
      TranslateContext context) throws SqlXlateException{
    if (findOnlyNode(tree, PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT) != null) {
      return -1;
    }
    List<CommonTree> cascatedElementList = new LinkedList<CommonTree>();
    FilterBlockUtil.findNode(tree, PantheraParser_PLSQLParser.CASCATED_ELEMENT, cascatedElementList);
    int mlevel = 0;
    for(CommonTree cascatedE : cascatedElementList) {
      int level = PLSQLFilterBlockFactory.getInstance().isCorrelated(qi, selectStack, cascatedE);
      if (level > mlevel) {
        mlevel = level;
      }
    }
    return mlevel;
  }

  /**
   * check two CommonTree with same structure
   * @param op1
   * @param op2
   * @return
   */
  public static boolean equalsTree(CommonTree op1, CommonTree op2) {
    if (!op1.getText().equals(op2.getText()) || op1.getChildCount() != op2.getChildCount()) {
      return false;
    } else {
      for (int i = 0; i < op1.getChildCount(); i++) {
        if (!equalsTree((CommonTree) op1.getChild(i), (CommonTree) op2.getChild(i))) {
          return false;
        }
      }
      return true;
    }
  }

  /**
   * if only op2's any element has one child, op1 has two child, then op2 will
   * add a table alias as the first child.
   * @param op1
   * @param op2
   * @return
   */
  public static boolean makeEqualsExpandTree(CommonTree op1, CommonTree op2) {
    if (!op1.getText().equals(op2.getText())) {
      return false;
    }
    if (op1.getType() != PantheraExpParser.ANY_ELEMENT) {
      if (op1.getChildCount() != op2.getChildCount()) {
        return false;
      }
    }
    if (op1.getChildCount() < op2.getChildCount()) {
      return false;
    }
    for (int i = 1; i <= op2.getChildCount(); i++) {
      if (!makeEqualsExpandTree((CommonTree) op1.getChild(op1.getChildCount() - i), (CommonTree) op2.getChild(op2.getChildCount() - i))) {
        return false;
      }
    }
    if (op1.getChildCount() > op2.getChildCount()) {
      SqlXlateUtil.addCommonTreeChild(op2, 0, (CommonTree) op1.getChild(0));
    }
    return true;
  }

  /**
   * create a tree for a UDF
   *
   * @param udfName name of the UDF
   * @param exprList a list of the arguments of the UDF
   * @return  return the expr node of the UDF
   */
  public static CommonTree makeUDF(String udfName, List<CommonTree> exprList) {
    CommonTree expr = FilterBlockUtil.createSqlASTNode(
        exprList.get(0), PantheraParser_PLSQLParser.EXPR, "EXPR");
    CommonTree cascatedElement = FilterBlockUtil.createSqlASTNode(
        expr, PantheraParser_PLSQLParser.CASCATED_ELEMENT, "CASCATED_ELEMENT");
    expr.addChild(cascatedElement);
    CommonTree routineCall = FilterBlockUtil.createSqlASTNode(
        expr, PantheraParser_PLSQLParser.ROUTINE_CALL, "ROUTINE_CALL");
    cascatedElement.addChild(routineCall);
    CommonTree routineName = FilterBlockUtil.createSqlASTNode(
        expr, PantheraParser_PLSQLParser.ROUTINE_NAME, "ROUTINE_NAME");
    routineCall.addChild(routineName);
    CommonTree collectSet = FilterBlockUtil.createSqlASTNode(expr,
        PantheraParser_PLSQLParser.ID, udfName);
    routineName.addChild(collectSet);
    CommonTree arguments = FilterBlockUtil.createSqlASTNode(expr,
        PantheraParser_PLSQLParser.ARGUMENTS, "ARGUMENTS");
    routineCall.addChild(arguments);

    for (int i = 0; i < exprList.size(); i++) {
      CommonTree argument = FilterBlockUtil.createSqlASTNode(expr,
          PantheraParser_PLSQLParser.ARGUMENT, "ARGUMENT");
      arguments.addChild(argument);
      argument.addChild(exprList.get(i));
    }
    return expr;
  }

  /**
   * judge whether an expression contains a standard function with aggregation function
   * @param expr
   * @return
   *        if aggr return true else false
   */
  public static boolean isAggrFunc(CommonTree expr) {
    List<CommonTree> sfList = new ArrayList<CommonTree>();
    FilterBlockUtil.findNode(expr, PantheraParser_PLSQLParser.STANDARD_FUNCTION, sfList);
    // TODO ugly hard code
    Set<String> aggregation = new HashSet<String>();
    aggregation.add("max");
    aggregation.add("min");
    aggregation.add("sum");
    aggregation.add("count");
    aggregation.add("avg");
    aggregation.add("stddev_samp");
    aggregation.add("stddev");
    Boolean aggrFlag = false;
    if (sfList.size() != 0) {
      for (CommonTree check : sfList) {
        if (aggregation.contains(check.getChild(0).getText())) {
          aggrFlag = true;
          break;
        }
      }
    }
    return aggrFlag;
  }

  /**
   * Add all needed filters to select-list
   * @param topSelect
   * @param isProcessHaving
   * @param whereFilterColumns
   * @param havingFilterColumns
   */
  public static void AddAllNeededFilters(CommonTree topSelect, boolean isProcessHaving, List<CommonTree> whereFilterColumns, List<CommonTree> havingFilterColumns) {
    CommonTree oldTopSelectList = (CommonTree) topSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    List<CommonTree> oldAnyElementList = new ArrayList<CommonTree>();
    findNode(oldTopSelectList, PantheraParser_PLSQLParser.ANY_ELEMENT, oldAnyElementList);
    String tableName;
    CommonTree oldTopFrom = (CommonTree) topSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM);
    CommonTree oldTopFromFirstTR = (CommonTree) oldTopFrom.getChild(0).getChild(0);
    if (oldTopFromFirstTR.getChildCount() == 2) {
      tableName = oldTopFromFirstTR.getChild(0).getChild(0).getText();
    } else {
      CommonTree tableViewName = (CommonTree) oldTopFromFirstTR.getChild(0).getChild(0).getChild(0);
      tableName = tableViewName.getChild(tableViewName.getChildCount() - 1).getText();
    }
    if (!isProcessHaving) {
      for (CommonTree column : whereFilterColumns) {
        List<CommonTree> nodeList = new ArrayList<CommonTree>();
        findNode(column, PantheraParser_PLSQLParser.CASCATED_ELEMENT, nodeList);
        for (CommonTree cascated : nodeList) {
          CommonTree cNameNode;
          if (cascated.getChild(0).getChildCount() > 1) {
            cNameNode = (CommonTree) column.getChild(0).getChild(1);
            if (!tableName.startsWith(PantheraConstants.PANTHERA_PREFIX)) {
              tableName = column.getChild(0).getChild(0).getText();
            }
          } else {
            cNameNode = (CommonTree) column.getChild(0).getChild(0);
            if (!tableName.startsWith(PantheraConstants.PANTHERA_PREFIX)) {
              tableName = "";
            }
          }
          String columnName = cNameNode.getText();
          if (tableName.equals("")) {
            CommonTree singleColumn = createSqlASTNode(cascated, PantheraParser_PLSQLParser.ID, columnName);
            addSelectItem(oldTopSelectList, createCascatedElement(singleColumn));
          } else {
            for (CommonTree oldAny:oldAnyElementList) {
              if (oldAny.getChildCount() == 1 && oldAny.getChild(0).getText().equals(columnName)) {
                CommonTree singleTable = createSqlASTNode(cascated, PantheraParser_PLSQLParser.ID, tableName);
                SqlXlateUtil.addCommonTreeChild(oldAny, 0, singleTable);
              }
            }
            addSelectItem(oldTopSelectList, createCascatedElementBranch(cascated, tableName, columnName));
          }
        }
      }
    } else {
      for (int aggrIndex = 0; aggrIndex < havingFilterColumns.size(); aggrIndex++) {
        CommonTree column = havingFilterColumns.get(aggrIndex);
        CommonTree cascated = findOnlyNode(column, PantheraParser_PLSQLParser.CASCATED_ELEMENT);
        CommonTree cNameNode;
        if (cascated.getChild(0).getChildCount() > 1) {
          cNameNode = (CommonTree) column.getChild(0).getChild(1);
          if (!tableName.startsWith(PantheraConstants.PANTHERA_PREFIX)) {
            tableName = column.getChild(0).getChild(0).getText();
          }
        } else {
          cNameNode = (CommonTree) column.getChild(0).getChild(0);
          if (!tableName.startsWith(PantheraConstants.PANTHERA_PREFIX)) {
            tableName = "";
          }
        }
        String columnName = cNameNode.getText();
        if (tableName.equals("")) {
          CommonTree singleColumn = createSqlASTNode(cascated, PantheraParser_PLSQLParser.ID, columnName);
          addSelectItem(oldTopSelectList, createCascatedElement(singleColumn));
        } else {
          for (CommonTree oldAny:oldAnyElementList) {
            if (oldAny.getChildCount() == 1 && oldAny.getChild(0).getText().equals(columnName)) {
              CommonTree singleTable = createSqlASTNode(cascated, PantheraParser_PLSQLParser.ID, tableName);
              SqlXlateUtil.addCommonTreeChild(oldAny, 0, singleTable);
            }
          }
          addSelectItem(oldTopSelectList, createCascatedElementBranch(cascated, tableName, columnName));
        }
      }
    }
  }

  /**
   * add additional condition to logic expr node. <br>
   * add "and" node if there is conditions exists under logic expr node.
   *
   * @param logicExpr
   * @param op
   */
  public static void addConditionToLogicExpr(CommonTree logicExpr, CommonTree op) {
    if (logicExpr.getChildCount() > 0) {
      assert (logicExpr.getChildCount() == 1);
      CommonTree and = FilterBlockUtil
          .createSqlASTNode(logicExpr, PantheraExpParser.SQL92_RESERVED_AND, "and");
      and.addChild((CommonTree)logicExpr.deleteChild(0));
      and.addChild(op);
      logicExpr.addChild(and);
    } else {
      logicExpr.addChild(op);
    }
  }

  /**
   * get a set for all column reference names in current tableRefElement.<br>
   * @param tableRefElement
   * @param context
   * @return return a set for all cols in current tableRefElement.
   * @throws SqlXlateException
   */
  public static Set<String> getColumnSet(CommonTree tableRefElement, TranslateContext context)
      throws SqlXlateException {
    // ensure order of columns
    Set<String> result = new LinkedHashSet<String>();
    CommonTree tableViewName = FilterBlockUtil.findOnlyNode(tableRefElement,
        PantheraExpParser.TABLEVIEW_NAME);
    if (tableViewName != null) {
      RowResolver rr = null;
      if (tableViewName.getChildCount() == 1) {
        try{
          rr = context.getMeta().getRRForTbl(tableViewName.getChild(0).getText());
        } catch (SqlXlateException e) {
          throw new SqlXlateException((CommonTree) tableViewName.getChild(0), "HiveException thrown : " + e);
        }
      }
      if (tableViewName.getChildCount() == 2) {
        rr = context.getMeta().getRRForTbl(tableViewName.getChild(0).getText(),
            tableViewName.getChild(1).getText());
      }
      if (rr == null) {
        throw new SqlXlateException((CommonTree) tableViewName.getChild(0),"unknow table name!");
      }
      for (ColumnInfo ci : rr.getColumnInfos()) {
        result.add(ci.getInternalName());
      }
    }
    return result;
  }

  /**
   * delete table alias in tree
   * @param tree
   */
  public static void deleteAllTableAlias(CommonTree tree) {
    if (tree == null) {
      return;
    }
    List<CommonTree> nodeList = new ArrayList<CommonTree>();
    FilterBlockUtil.findNode(tree, PantheraExpParser.ANY_ELEMENT, nodeList);
    for (CommonTree anyElement : nodeList) {
      if (anyElement != null && anyElement.getChildCount() == 2) {
        anyElement.deleteChild(0);
      }
    }
  }

  /**
   * refresh tree with same name in fullMap. not refresh lower level, not refresh correlated columns.<br>
   * FIXME have problem not refresh correlated lower levels.
   *
   * @param tree
   * @param commonMap
   */
  public static void rebuildColumn(CommonTree tree, Map<String, CommonTree> commonMap) {
    if (tree == null
        || tree.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_FROM
        || tree.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT) {
      return;
    }
    for (int i = 0; i < tree.getChildCount(); i++) {
      rebuildColumn((CommonTree) tree.getChild(0), commonMap);
    }
    if (tree.getType() == PantheraParser_PLSQLParser.ANY_ELEMENT && tree.getChildCount() == 1
        && tree.getParent().getType() == PantheraParser_PLSQLParser.CASCATED_ELEMENT) {
      String colname = tree.getChild(0).getText();
      CommonTree item = commonMap.get(colname);
      if (item != null) {
        CommonTree newRef = FilterBlockUtil.cloneTree((CommonTree) item.getChild(0).getChild(0));
        SqlXlateUtil.buildPosition(newRef, tree.getLine(), tree.getCharPositionInLine());
        CommonTree parentparent = (CommonTree) tree.getParent().getParent();
        int position = tree.getParent().getChildIndex();
        parentparent.replaceChildren(position, position, newRef);
      }
    }
  }

  /**
   * return a node with the opposite meaning of the tree node.
   * @param tree
   * @return
   *        a new node that has the opposite function, or null if no appropriate.
   */
  public static CommonTree reverseClone(CommonTree tree) {
    switch (tree.getType()) {
    case PantheraParser_PLSQLParser.EQUALS_OP:
    case PantheraExpParser.EQUALS_NS:
      return createSqlASTNode(tree, PantheraParser_PLSQLParser.NOT_EQUAL_OP, "<>");
    case PantheraExpParser.NOT_EQUAL_OP:
      return createSqlASTNode(tree, PantheraParser_PLSQLParser.EQUALS_OP, "=");
    case PantheraExpParser.GREATER_THAN_OP:
      return createSqlASTNode(tree, PantheraParser_PLSQLParser.LESS_THAN_OR_EQUALS_OP, "<=");
    case PantheraExpParser.GREATER_THAN_OR_EQUALS_OP:
      return createSqlASTNode(tree, PantheraParser_PLSQLParser.LESS_THAN_OP, "<");
    case PantheraExpParser.LESS_THAN_OP:
      return createSqlASTNode(tree, PantheraParser_PLSQLParser.GREATER_THAN_OR_EQUALS_OP, ">=");
    case PantheraExpParser.LESS_THAN_OR_EQUALS_OP:
      return createSqlASTNode(tree, PantheraParser_PLSQLParser.GREATER_THAN_OP, ">");
    }
    return null;
  }

  /**
   * speed up bottomSelect by use select *,
   * or add alias to expression in Select-list to avoid delete
   *
   * @param select
   * @param context
   */
  public static void speedUpSelect(CommonTree select, TranslateContext context) {
    if (select.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_GROUP) == null) {
      // to speed up query
      CommonTree selectList = (CommonTree) select
          .getFirstChildWithType(PantheraExpParser.SELECT_LIST);
      int index = selectList.getChildIndex();
      select.replaceChildren(index, index, createSqlASTNode(
          selectList, PantheraExpParser.ASTERISK, "*"));
    } else {
      // avoid aggr function be deleted by mistake
      addColumnAliasOrigin(select, context);
    }
  }

  /**
   * add condition to where if not exists group, else add to having
   *
   * @param select
   * @param condition
   */
  public static void addConditionToSelect(CommonTree select, CommonTree condition) {
    CommonTree group = (CommonTree) select.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_GROUP);
    CommonTree where;
    if (group == null) {
      where = (CommonTree) select.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_WHERE);
      if (where == null) {
        where = createSqlASTNode(condition, PantheraParser_PLSQLParser.SQL92_RESERVED_WHERE, "where");
        // first child from, second child select-list
        SqlXlateUtil.addCommonTreeChild(select, 2, where);
      }
    } else {
      // here where actually is having...
      // for code laconic
      where = (CommonTree) group.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_HAVING);
      if (where == null) {
        where = createSqlASTNode(condition, PantheraParser_PLSQLParser.SQL92_RESERVED_HAVING, "having");
        group.addChild(where);
      }
    }
    CommonTree logic = (CommonTree) where.getFirstChildWithType(PantheraParser_PLSQLParser.LOGIC_EXPR);
    if (logic == null) {
      logic = createSqlASTNode(condition, PantheraParser_PLSQLParser.LOGIC_EXPR, "LOGIC_EXPR");
      where.addChild(logic);
    }
    addConditionToLogicExpr(logic, condition);
  }

}
