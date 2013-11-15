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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Stack;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockUtil;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * reorder all nodes in where and having,
 * pop or to the top of condition, partition conditions into united-by-all conditions
 * build left-most tree for each united-by-all condition
 * move correlated conditions to last.
 * ConditionStructTransformer.
 *
 */
public class ConditionStructTransformer extends BaseSqlASTTransformer {
  SqlASTTransformer tf;

  public ConditionStructTransformer(SqlASTTransformer tf) {
    this.tf = tf;
  }

  @Override
  public void transform(CommonTree tree, TranslateContext context) throws SqlXlateException {
    tf.transformAST(tree, context);
    Stack<CommonTree> selStack = new Stack<CommonTree>();
    this.transformStructDeepFirst(tree, selStack, context);
  }

  public void transformStructDeepFirst(CommonTree tree, Stack<CommonTree> selStack,
      TranslateContext context) throws SqlXlateException {
    if (tree.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT) {
      selStack.push(tree);
    }
    for (int i = 0; i < tree.getChildCount(); i++) {
      transformStructDeepFirst((CommonTree) (tree.getChild(i)), selStack, context);
    }
    if (tree.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT) {
      transformStruct(tree, selStack, context);
      selStack.pop();
    }
  }

  public void transformStruct(CommonTree tree, final Stack<CommonTree> selStack,
      TranslateContext context) throws SqlXlateException {
    CommonTree where = (CommonTree) tree.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_WHERE);
    if (where != null) {
      // pass logic_expr
      transformStructDeMorgan((CommonTree) where.getChild(0), selStack, context);
    }
    CommonTree group = (CommonTree) tree.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_GROUP);
    if (group != null) {
      CommonTree having = (CommonTree) group.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_HAVING);
      if (having != null) {
        // pass logic_expr
        transformStructDeMorgan((CommonTree) having.getChild(0), selStack, context);
      }
    }
  }

  private void transformStructDeMorgan(CommonTree tree, final Stack<CommonTree> selectStack,
      TranslateContext context) throws SqlXlateException {
    Stack<ConditionWithSelectStack> logicCondition = new Stack<ConditionWithSelectStack>();
    ConditionWithSelectStack cwss = new ConditionWithSelectStack();
    cwss.condition = FilterBlockUtil.cloneTree(tree);
    cwss.selStack = selectStack;
    logicCondition.add(cwss);
    List<ConditionWithSelectStack> conditionSet = new ArrayList<ConditionWithSelectStack>();
    partitionByOr(logicCondition, conditionSet, context);
    // remove original conditions structure
    tree.deleteChild(0);
    if (tree.getChildCount() > 0) {
      throw new SqlXlateException(tree, "LOGIC_EXPR contain more than 1 condition, but not connected by AND/OR");
    }
    for (ConditionWithSelectStack conditions : conditionSet) {
      CommonTree transformedBlock = transformStructWithoutOr(conditions.condition, conditions.selStack, context);
      if (tree.getChildCount() == 0) {
        tree.addChild(transformedBlock);
      } else {
        CommonTree oldConds = (CommonTree) tree.deleteChild(0);
        CommonTree or = FilterBlockUtil.createSqlASTNode(transformedBlock,
            PantheraParser_PLSQLParser.SQL92_RESERVED_OR, "or");
        tree.addChild(or);
        or.addChild(oldConds);
        or.addChild(transformedBlock);
      }
    }
  }

  private CommonTree transformStructWithoutOr(CommonTree conditionBlock,
      final Stack<CommonTree> selectStack, final TranslateContext context) {
    List<CommonTree> conditions = BreakUpByAnd(conditionBlock);
    Collections.sort(conditions, new Comparator<CommonTree>() {
      @Override
      public int compare(CommonTree arg0, CommonTree arg1) {
        try {
          return FilterBlockUtil.getConditionLevel(arg0, selectStack, context)
              - FilterBlockUtil.getConditionLevel(arg1, selectStack, context);
        } catch (SqlXlateException e) {
          e.printStackTrace();
          return 0;
        }
      }
    });
    CommonTree ret = null;
    for (CommonTree condition : conditions) {
      if (ret == null) {
        ret = condition;
      } else {
        CommonTree oldCond = ret;
        ret = FilterBlockUtil.createSqlASTNode(condition,
            PantheraParser_PLSQLParser.SQL92_RESERVED_AND, "and");
        ret.addChild(oldCond);
        ret.addChild(condition);
      }
    }
    return ret;
  }

  private List<CommonTree> BreakUpByAnd(CommonTree tree) {
    List<CommonTree> conditions = new ArrayList<CommonTree>();
    if (tree.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_AND) {
      conditions.addAll(BreakUpByAnd((CommonTree) tree.getChild(0)));
      conditions.addAll(BreakUpByAnd((CommonTree) tree.getChild(1)));
      return conditions;
    } else {
      conditions.add(tree);
      return conditions;
    }
  }

  private void partitionByOr(Stack<ConditionWithSelectStack> rootswss,
      List<ConditionWithSelectStack> transformed, TranslateContext context) {
    while (!rootswss.empty()) {
      ConditionWithSelectStack popwss = rootswss.pop();
      CommonTree root = popwss.condition;
      Stack<CommonTree> selectStack = popwss.selStack;
      CommonTree additionalRoot = FilterBlockUtil.cloneTree(root);
      if (partitionRecursive(root, additionalRoot)) {
        ConditionWithSelectStack rootwss = new ConditionWithSelectStack();
        ConditionWithSelectStack additionalRootwss = new ConditionWithSelectStack();
        rootwss.condition = root;
        rootwss.selStack = selectStack;
        additionalRootwss.condition = additionalRoot;
        additionalRootwss.selStack = selectStack;
        rootswss.push(rootwss);
        rootswss.push(additionalRootwss);
      } else {
        ConditionWithSelectStack transformedwss = new ConditionWithSelectStack();
        // root has type of LOGIC_EXPR
        transformedwss.condition = (CommonTree) root.getChild(0);
        transformedwss.selStack = selectStack;
        transformed.add(transformedwss);
      }
    }
  }

  private boolean partitionRecursive(CommonTree root, CommonTree additionalRoot) {
    for (int i = 0; i < root.getChildCount(); i++) {
      CommonTree token = (CommonTree) root.getChild(i);
      CommonTree additionalToken = (CommonTree) additionalRoot.getChild(i);
      if (token.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_OR) {
        //FIXME is it OK to not split OR, if both side of OR is simple and naive?
        if (FilterBlockUtil.findOnlyNode(token, PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT) == null) {
          return false;
        }
        root.replaceChildren(i, i, (CommonTree) token.deleteChild(0));
        additionalRoot.replaceChildren(i, i, (CommonTree) additionalToken.deleteChild(1));
        return true;
      } else if (token.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_AND) {
        if (partitionRecursive(token, additionalToken)) {
          root.replaceChildren(i,i,token);
          additionalRoot.replaceChildren(i,i,additionalToken);
          return true;
        }
      }
    }
    return false;
  }

  /**
   *
   * Inner class
   * ConditionWithSelectStack.
   * to store selectStack for each condition
   *
   */
  private class ConditionWithSelectStack {
    public Stack<CommonTree> selStack;
    public CommonTree condition;
  }
}
