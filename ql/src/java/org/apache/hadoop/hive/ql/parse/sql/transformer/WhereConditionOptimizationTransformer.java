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
import java.util.Iterator;
import java.util.List;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockUtil;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * Transformer for optimize where conditions in following cases:
 *
 * 1. (a and b) or (a and c) => a and (b or c)
 *
 */
public class WhereConditionOptimizationTransformer extends BaseSqlASTTransformer {
  SqlASTTransformer tf;

  public WhereConditionOptimizationTransformer(SqlASTTransformer tf) {
    this.tf = tf;
  }

  @Override
  public void transform(CommonTree tree, TranslateContext context) throws SqlXlateException {
    tf.transformAST(tree, context);
    for (QueryInfo qf : context.getqInfoList()) {
      transformQuery(qf, qf.getSelectKeyForThisQ());
    }
  }

 private void transformQuery(QueryInfo qf, CommonTree node) throws SqlXlateException {
    if(node.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT){
      CommonTree where = (CommonTree) node.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_WHERE);
      if (where != null) {
        transformWhereCondition((CommonTree) where.getChild(0).getChild(0), null);
      }
    }

    //
    // Transform subqueries in this query.
    //
    for (int i = 0; i < node.getChildCount(); i++) {
      CommonTree child = (CommonTree) node.getChild(i);
      if (child.getType() != PantheraParser_PLSQLParser.SQL92_RESERVED_FROM) {
        transformQuery(qf, child);
      }
    }
  }

  private void transformWhereCondition(CommonTree node, List<CommonTree> andConditionList) throws SqlXlateException {
    // For a AND node, colllect the and conditions below it.
    if (node.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_AND) {
      // Transform the left child.
      transformWhereCondition((CommonTree) node.getChild(0), andConditionList);
      // Transform the right child.
      transformWhereCondition((CommonTree) node.getChild(1), andConditionList);
    } else if (node.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_OR) {
      // For a OR node, create a new AND condition list for each child.
      ArrayList<CommonTree> leftList = new ArrayList<CommonTree>();
      ArrayList<CommonTree> rightList = new ArrayList<CommonTree>();
      // Transform the left child.
      transformWhereCondition((CommonTree) node.getChild(0), leftList);
      // Transform the right child.
      transformWhereCondition((CommonTree) node.getChild(1), rightList);

      // Extract common conditions from the two child.
      ArrayList<CommonTree> commonConditions = new ArrayList<CommonTree>();
      Iterator<CommonTree> leftIterator = leftList.iterator();
      boolean orAlwaysTrue = false;
      while (!orAlwaysTrue && leftIterator.hasNext()) {
        CommonTree leftCondition = leftIterator.next();
        Iterator<CommonTree> rightIterator = rightList.iterator();
        while (rightIterator.hasNext()) {
          CommonTree rightCondition = rightIterator.next();
          if (compareCondition(leftCondition, rightCondition)) {
            commonConditions.add(leftCondition);
            // Remove the common condition from its original position.
            orAlwaysTrue = removeCondition(node, leftCondition);
            if (!orAlwaysTrue) {
              orAlwaysTrue = removeCondition(node, rightCondition);
            }
            // Remove the common condition from the condition lists.
            leftIterator.remove();
            rightIterator.remove();
            break;
          }
        }
      }

      // Generate AND nodes per the common conditions.
      if (!commonConditions.isEmpty()) {
        CommonTree parent = (CommonTree) node.getParent();
        int childIndex = node.getChildIndex();
        CommonTree root;
        Iterator<CommonTree> iterator = commonConditions.iterator();
        if (orAlwaysTrue) {
          root = iterator.next();
        } else {
          root = node;
        }
        while (iterator.hasNext()) {
          CommonTree andNode = FilterBlockUtil.createSqlASTNode(root, PantheraParser_PLSQLParser.SQL92_RESERVED_AND, "and");
          andNode.addChild(root);
          CommonTree commonCondition = iterator.next();
          andNode.addChild(commonCondition);
          root = andNode;
          // Add the new AND condition into the parent's AND condition list.
          if (andConditionList != null) {
            andConditionList.add(commonCondition);
          }
        }
        // replace the current node.
        parent.setChild(childIndex, root);
      }
    } else if (node.getParent().getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_AND) {
      // Add current node into the AND condition list.
      if (andConditionList != null) {
        andConditionList.add(node);
      }
    }
  }

  private boolean compareCondition(CommonTree condition1, CommonTree condition2) {
    if (condition1.getType() != condition2.getType() || !condition1.getText().equals(condition2.getText())) {
      return false;
    }
    if (condition1.getChildCount() != condition2.getChildCount()) {
      return false;
    }
    for (int i = 0; i < condition1.getChildCount(); i++) {
      if (!compareCondition((CommonTree) condition1.getChild(i), (CommonTree) condition2.getChild(i))) {
        return false;
      }
    }
    return true;
  }

  private boolean removeCondition(CommonTree root, CommonTree condition) {
    if ((CommonTree) condition.getParent() == root) {
      return true;
    }

    int siblingIndex = condition.getChildIndex() > 0 ? 0 : 1;
    condition.getParent().getParent().setChild(condition.getParent().getChildIndex(), condition.getParent().getChild(siblingIndex));
    return false;
  }
}
