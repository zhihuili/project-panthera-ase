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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.sql.PantheraExpParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockUtil;

/**
 * transform<br>
 * natural left join<br>
 * natural inner join<br>
 * natural right join<br>
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
    if (node.getType() == PantheraExpParser.TABLE_REF && node.getChildCount() > 1
        && node.getChild(1).getType() == PantheraExpParser.JOIN_DEF
        && node.getChild(1).getChild(0).getType() == PantheraExpParser.NATURAL_VK) {
      Map<String, Set<String>> columnSetMap = processNaturalJoin(node, context);
      CommonTree select = (CommonTree) node.getParent().getParent();
      rebuildSelect((CommonTree) select.getFirstChildWithType(PantheraExpParser.SELECT_LIST),
          columnSetMap);
      rebuildColumn((CommonTree) select
          .getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_WHERE), columnSetMap);
      if (select.getFirstChildWithType(PantheraExpParser.SELECT_LIST) == null) {
        rebuildColumn((CommonTree) ((CommonTree) (select.getParent().getParent()))
            .getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_ORDER), columnSetMap);
      }
    }
  }

  private void rebuildColumn(CommonTree where, Map<String, Set<String>> columnSetMap) {
    if (where == null) {
      return;
    }
    List<CommonTree> nodeList = new ArrayList<CommonTree>();
    FilterBlockUtil.findNode(where, PantheraExpParser.ANY_ELEMENT, nodeList);
    for (CommonTree anyElement : nodeList) {
      rebuildAnyElement(anyElement, columnSetMap);
    }
  }

  private void rebuildSelect(CommonTree selectList, Map<String, Set<String>> columnSetMap) {
    if (selectList == null || selectList.getType() != PantheraExpParser.SELECT_LIST) {
      return;
    }
    for (int i = 0; i < selectList.getChildCount(); i++) {
      CommonTree selectItem = (CommonTree) selectList.getChild(i);
      CommonTree anyElement = FilterBlockUtil.findOnlyNode(selectItem,
          PantheraExpParser.ANY_ELEMENT);
      rebuildAnyElement(anyElement, columnSetMap);
    }
  }

  private void rebuildAnyElement(CommonTree anyElement, Map<String, Set<String>> columnSetMap) {
    if (anyElement != null) {
      if (anyElement.getChildCount() == 1) {
        String column = anyElement.getChild(0).getText();
        String tableAlias = findTableAlias(column, columnSetMap);
        if (tableAlias != null) {
          anyElement.addChild(FilterBlockUtil.createSqlASTNode(
              (CommonTree) anyElement.getChild(0),PantheraExpParser.ID, tableAlias));
          SqlXlateUtil.exchangeChildrenPosition(anyElement);
        }
      }
      if (anyElement.getChildCount() == 2) {
        String column = anyElement.getChild(1).getText();
        String tableAlias = findTableAlias(column, columnSetMap);
        if (tableAlias != null) {
          ((CommonTree) anyElement.getChild(0)).getToken().setText(tableAlias);
        }
      }
    }
  }

  private String findTableAlias(String column, Map<String, Set<String>> columnSetMap) {
    for (Entry<String, Set<String>> entry : columnSetMap.entrySet()) {
      Set<String> columnSet = entry.getValue();
      if (columnSet.contains(column)) {
        return entry.getKey();
      }
    }
    return null;
  }

  private Map<String, Set<String>> processNaturalJoin(CommonTree node, TranslateContext context)
      throws SqlXlateException {
    Map<String, Set<String>> columnSetMap = new HashMap<String, Set<String>>();
    for (int i = 0; i < node.getChildCount(); i++) {
      CommonTree child = (CommonTree) node.getChild(i);
      if (child.getType() == PantheraExpParser.TABLE_REF_ELEMENT) {
        columnSetMap.put(FilterBlockUtil.addTableAlias(child, context),
            getColumnSet(child, context));
      }
      if (child.getType() == PantheraExpParser.JOIN_DEF
          && child.getChild(0).getType() == PantheraExpParser.NATURAL_VK) {
        processNaturalNode(child);
        CommonTree tableRefElement = (CommonTree) child
            .getFirstChildWithType(PantheraExpParser.TABLE_REF_ELEMENT);
        String alias = FilterBlockUtil.addTableAlias(tableRefElement, context);
        Set<String> columnSet = getColumnSet(tableRefElement, context);
        makeOn((CommonTree) node.getChild(1), alias, child, columnSet, columnSetMap);
        columnSetMap.put(alias, columnSet);
      }
    }
    return columnSetMap;
  }

  private void makeOn(CommonTree njoin,String thisTable, CommonTree join, Set<String> thisColumnSet,
      Map<String, Set<String>> columnSetMap) {
    CommonTree on = FilterBlockUtil.createSqlASTNode((CommonTree) njoin.getChild(0),
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
          CommonTree equal = makeEqualCondition(on, thisTable, tableAlias, column);
          if (logicExpr.getChildCount() == 0) {
            logicExpr.addChild(equal);
          } else {
            CommonTree and = FilterBlockUtil.createSqlASTNode(on, PantheraExpParser.SQL92_RESERVED_AND,
                "and");
            CommonTree leftChild = (CommonTree) logicExpr.deleteChild(0);
            and.addChild(leftChild);
            and.addChild(equal);
            logicExpr.addChild(and);
          }
          break;
        }
      }
    }
    if (hasCondition) {
      join.addChild(on);
    }
  }

  private CommonTree makeEqualCondition(CommonTree on, String leftTable, String rightTable, String column) {
    CommonTree equal = FilterBlockUtil.createSqlASTNode(on, PantheraExpParser.EQUALS_OP, "=");
    equal.addChild(FilterBlockUtil.createCascatedElementBranch(equal, leftTable, column));
    equal.addChild(FilterBlockUtil.createCascatedElementBranch(equal, rightTable, column));
    return equal;

  }

  private void processNaturalNode(CommonTree join) {
    // delete NATURAL node
    join.deleteChild(0);
    if (join.getChild(0).getType() == PantheraExpParser.INNER_VK) {
      // delete INNER node
      join.deleteChild(0);
    }
  }

  private Set<String> getColumnSet(CommonTree tableRefElement, TranslateContext context)
      throws SqlXlateException {
    Set<String> result = new HashSet<String>();
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
}
