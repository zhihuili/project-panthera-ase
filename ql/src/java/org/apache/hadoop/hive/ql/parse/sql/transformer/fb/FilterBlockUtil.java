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
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import org.antlr.runtime.CommonToken;
import org.antlr.runtime.tree.CommonTree;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.parse.sql.PantheraExpParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.QueryInfo;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.QueryBlock.CountAsterisk;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

public class FilterBlockUtil {
  private static final Log LOG = LogFactory.getLog(FilterBlockUtil.class);
  public static final String PREFIX_COLUMN_ALIAS = "panthera_col_";

  private FilterBlockUtil() {
  }

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
          aliasName = createSqlASTNode(selectItem, PantheraParser_PLSQLParser.ID, "panthera_aggregation_" + i);
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
        if (standardFunction.getChild(0).getType() == PantheraParser_PLSQLParser.COUNT_VK
            && standardFunction.getChild(0).getChild(0).getType() == PantheraParser_PLSQLParser.ASTERISK) {
          countAsterisk.setPosition(i);
          countAsterisk.setSelectItem(selectItem);
          if (selectList.getChildCount() == 1) {
            countAsterisk.setOnlyAsterisk(true);
          }
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

  public static void findNodeText(CommonTree node, String text, List<CommonTree> nodeList) {
    if (node == null) {
      return;
    }
    if (node.getText().equals(text)) {
      nodeList.add(node);
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      findNodeText((CommonTree) node.getChild(i), text, nodeList);
    }
  }

  public static boolean existNodeText(CommonTree node, String text) {
    List<CommonTree> nodeList = new ArrayList<CommonTree>();
    findNodeText(node, text, nodeList);
    return nodeList.size() > 0 ? true : false;
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

  public static CommonTree createSqlASTNode(CommonTree copy, int type, String text) {
    CommonTree retTree = dupNode(copy);
    retTree.token.setType(type);
    retTree.token.setText(text);
    retTree.token.setCharPositionInLine(copy.getCharPositionInLine());
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
   * clone SELECT_LIST by SELECT_ITEM ALIAS
   *
   * @param originalSelectList
   * @return
   */
  public static CommonTree cloneSelectListByAliasFromSelect(CommonTree originalSelect) {
    CommonTree selectList;
    CommonTree originalSelectList = (CommonTree) originalSelect
        .getFirstChildWithType(PantheraExpParser.SELECT_LIST);
    if (originalSelectList != null) {
      int selectNum = originalSelectList.getChildCount();
      selectList = createSqlASTNode(originalSelectList, PantheraExpParser.SELECT_LIST,
          "SELECT_LIST");
      int count = 0;
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

        String columnName;
        CommonTree coloumNameSrc;
        if (originalAlias != null) {
          columnName = originalAlias.getChild(0).getText();
          coloumNameSrc = (CommonTree) originalAlias.getChild(0);

          CommonTree alias = dupNode(originalAlias);
          CommonTree aliasName = createSqlASTNode(coloumNameSrc, PantheraParser_PLSQLParser.ID,
              columnName);
          alias.addChild(aliasName);
          selectItem.addChild(alias);
        } else {
          columnName = PREFIX_COLUMN_ALIAS + count++;
          coloumNameSrc = anyElement;

        }
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
        String aliasStr = PREFIX_COLUMN_ALIAS + count++;
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
   * @return alias
   */
  public static CommonTree createTableRefElement(CommonTree select, TranslateContext context) {
    CommonTree tableRefElement = createSqlASTNode(select,
        PantheraParser_PLSQLParser.TABLE_REF_ELEMENT, "TABLE_REF_ELEMENT");
    CommonTree viewAlias = createAlias(tableRefElement, context);
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
      if (logicExpr.getChildCount() == 0) {
        logicExpr.addChild(condition);
      } else {
        CommonTree and = createSqlASTNode(on, PantheraExpParser.SQL92_RESERVED_AND, "and");
        CommonTree leftChild = (CommonTree) logicExpr.deleteChild(0);
        and.addChild(leftChild);
        and.addChild(condition);
        logicExpr.addChild(and);
      }
    }
    return on;
  }

  private static CommonTree makeEqualCondition(CommonTree on, String leftAlias, String rightAlias,
      String leftColumn, String rightColumn) {
    CommonTree equal = createSqlASTNode(on, PantheraExpParser.EQUALS_OP, "=");
    equal.addChild(createCascatedElementBranch(equal, leftAlias, leftColumn));
    equal.addChild(createCascatedElementBranch(equal, rightAlias, rightColumn));
    return equal;

  }

  public static boolean hasAncestorOfType(CommonTree tree, int type) {
    if (firstAncestorOfType(tree, type) != null) {
      return true ;
    } else {
      return false ;
    }
  }

  public static int getClauseType(CommonTree tree) {
    CommonTree token = tree;
    while(token.getParent() != null) {
      token = (CommonTree) token.getParent();
      if(token.getType() == PantheraParser_PLSQLParser.SELECT_LIST || token.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_FROM
          || token.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_WHERE || token.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_HAVING
          || token.getType() == PantheraParser_PLSQLParser.GROUP_BY_ELEMENT || token.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_ORDER
          || token.getType() == PantheraParser_PLSQLParser.LIMIT_VK) {
        return token.getType();
      }
    }
    return -1;
  }

  public static CommonTree firstAncestorOfType(CommonTree tree, int type) {
    CommonTree token = tree;
    while (token.getParent() != null) {
      token = (CommonTree) token.getParent();
      if (token.getType() == type) {
        return token;
      }
    }
    return null;
  }

  /**
   * return -1 if contain subQuery,
   * return 0 if uncorrelated,
   * return a positive number if correlated, indicating correlating level
   * @param tree
   * @return
   * @throws SqlXlateException
   */
  public static int getConditionLevel(CommonTree tree, final Stack<CommonTree> selectStack,
      TranslateContext context) throws SqlXlateException{
    if (findOnlyNode(tree, PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT) != null) {
      return -1;
    }
    CommonTree botS = selectStack.peek();
    QueryInfo qi = null;
    for (QueryInfo qInfo: context.getqInfoList()) {
      if(qInfo.getSelectKeyForThisQ().getTokenStartIndex() <= botS.getTokenStartIndex()
          && qInfo.getSelectKeyForThisQ().getTokenStopIndex() >= botS.getTokenStopIndex()) {
        qi = qInfo;
        break;
      }
    }
    if (qi == null) {
      throw new SqlXlateException(botS, "select node position is not set right during transform.");
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
    } if (op1.getType() != PantheraExpParser.ANY_ELEMENT) {
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
}
