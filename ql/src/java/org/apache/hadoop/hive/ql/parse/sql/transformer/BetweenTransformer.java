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

/**
 * transform (BETWEEN AND) to (>= ADN <=) <br/>
 * transform (NOT BETWEEN AND) to (< OR >) <br/>
 * BetweenTransformer.
 *
 */
public class BetweenTransformer extends BaseSqlASTTransformer {
  SqlASTTransformer tf;

  public BetweenTransformer(SqlASTTransformer tf) {
    this.tf = tf;
  }

  @Override
  protected void transform(CommonTree tree, TranslateContext context) throws SqlXlateException {
    tf.transformAST(tree, context);
    trans(tree, context);
  }

  void trans(CommonTree tree, TranslateContext context) {
    // deep firstly
    for (int i = 0; i < tree.getChildCount(); i++) {
      trans((CommonTree) (tree.getChild(i)), context);
    }
    if (tree.getType() == PantheraExpParser.SQL92_RESERVED_BETWEEN) {
      transBetween(false, tree, context);
    }
    if (tree.getType() == PantheraExpParser.NOT_BETWEEN) {
      transBetween(true, tree, context);
    }
  }

  /**
   * transform BETWEEN(NOT BETWEEN) branch
   *
   * @param isNot
   *          BETWEEN is false, NOT BETWEEN is true
   * @param tree
   *          BETWEEN(NOT BETWEEN) branch root
   * @param context
   */
  void transBetween(Boolean isNot, CommonTree tree, TranslateContext context) {
    CommonTree leftColumn = (CommonTree) tree.getChild(0);
    CommonTree rightColumn = FilterBlockUtil.cloneTree(leftColumn);
    CommonTree leftVar = (CommonTree) tree.getChild(1);
    List<CommonTree> leftVarList = new ArrayList<CommonTree>();
    if (leftVar.getType() == PantheraExpParser.EXPR) {
      for (int i = 0; i < leftVar.getChildCount(); i++) {
        leftVarList.add((CommonTree) leftVar.getChild(i));
      }
    }
    CommonTree rightVar = (CommonTree) tree.getChild(2);
    List<CommonTree> rightVarList = new ArrayList<CommonTree>();
    if (rightVar.getType() == PantheraExpParser.EXPR) {
      for (int i = 0; i < rightVar.getChildCount(); i++) {
        rightVarList.add((CommonTree) rightVar.getChild(i));
      }
    }
    CommonTree leftOp;
    if (isNot) {
      leftOp = FilterBlockUtil.createSqlASTNode(tree, PantheraExpParser.LESS_THAN_OP, "<");
    } else {
      leftOp = FilterBlockUtil.createSqlASTNode(tree, PantheraExpParser.GREATER_THAN_OR_EQUALS_OP, ">=");
    }
    CommonTree rightOp;
    if (isNot) {
      rightOp = FilterBlockUtil.createSqlASTNode(tree, PantheraExpParser.GREATER_THAN_OP, ">");
    } else {
      rightOp = FilterBlockUtil.createSqlASTNode(tree, PantheraExpParser.LESS_THAN_OR_EQUALS_OP, "<=");
    }
    leftOp.addChild(leftColumn);
    for (CommonTree leftData : leftVarList) {
      leftOp.addChild(leftData);
    }
    rightOp.addChild(rightColumn);
    for (CommonTree rightData : rightVarList) {
      rightOp.addChild(rightData);
    }

    CommonTree op;
    if (isNot) {
      op = FilterBlockUtil.createSqlASTNode(tree, PantheraExpParser.SQL92_RESERVED_OR, "or");
    } else {
      op = FilterBlockUtil.createSqlASTNode(tree, PantheraExpParser.SQL92_RESERVED_AND, "and");
    }
    op.addChild(leftOp);
    op.addChild(rightOp);

    CommonTree parent = (CommonTree) tree.getParent();
    int index = tree.getChildIndex();
    parent.deleteChild(index);
    SqlXlateUtil.addCommonTreeChild(parent, index, op);
  }

}
