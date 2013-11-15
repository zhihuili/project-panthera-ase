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

import java.util.Stack;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.PantheraExpParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockUtil;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * Transform rownum clause of Oracle.
 *
 * Tranform the form "select ... from ... where rownum (< | <=) <int>" to "select ... from ... limit <int>".
 * note that there is only one condition in the where clause, that is, rownum (< | <=) <int>.
 *
 * Optimization: in the case where "select * from (subquery) limit <int>" then drop the outer query,
 *                     and promote the subquery after attaching the parent's limit token to it.
 *
 */
public class RowNumTransformer  extends BaseSqlASTTransformer  {

  SqlASTTransformer tf;

  public RowNumTransformer(SqlASTTransformer tf) {
    this.tf = tf;
  }

  @Override
  public void transform(CommonTree tree, TranslateContext context) throws SqlXlateException {
    tf.transformAST(tree, context);

    Stack<CommonTree> stack = new Stack<CommonTree>();
    stack.push (null);
    transformRownum (tree, stack);
  }

  private void transformRownum(CommonTree node, Stack<CommonTree> stack) {
    if (node.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT) {
      stack.push (node);
      //
      // If this select has where clause, and the where clause has only one condition: rownum (< | <=) <int>,
      // then transform it to limit.
      //
      CommonTree where = (CommonTree) node.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_WHERE);
      if (where != null) {
        CommonTree operator = (CommonTree) where.getChild(0).getChild(0);
        int operatorType = operator.getType();
        if (operatorType == PantheraParser_PLSQLParser.LESS_THAN_OP ||
            operatorType == PantheraParser_PLSQLParser.LESS_THAN_OR_EQUALS_OP) {
          if (operator.getChild(0).getType() == PantheraParser_PLSQLParser.CASCATED_ELEMENT &&
              operator.getChild(0).getChild(0).getType() == PantheraParser_PLSQLParser.ANY_ELEMENT &&
              operator.getChild(0).getChild(0).getChild(0).getType() == PantheraParser_PLSQLParser.ID &&
              operator.getChild(0).getChild(0).getChild(0).getText().equals("rownum") &&
              operator.getChild(1).getType() == PantheraParser_PLSQLParser.UNSIGNED_INTEGER) {

            int rownum = Integer.parseInt(operator.getChild(1).getText());
            if (operatorType == PantheraParser_PLSQLParser.LESS_THAN_OP && rownum > 0) {
              --rownum;
            }
            //
            // Create a limit token and attach it to the select node as the last child.
            //
            CommonTree limit = FilterBlockUtil.createSqlASTNode(operator, PantheraExpParser.LIMIT_VK, "limit");
            node.addChild(limit);
            //
            // Create a UNSIGNED_INTEGER token and attach it to the limit token.
            //
            CommonTree limitNum = FilterBlockUtil.createSqlASTNode(
                (CommonTree) operator.getChild(1), PantheraExpParser.UNSIGNED_INTEGER, Integer.toString(rownum));
            limit.addChild(limitNum);
            //
            // Delete the where child.
            //
            node.deleteChild(where.getChildIndex());
          }
        }
      }
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      transformRownum((CommonTree) node.getChild(i), stack);
    }

    if (node.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT) {
      //
      // Node may be changed by the subquery. Restore it from the stack.
      //
      node = stack.pop();
      //
      // Optimization: in the case where "select * from (subquery) limit <int>" then drop the outer query,
      //                     and promote the subquery after attaching the parent's limit token to it.
      //

      //
      // Make sure this query is the only child of the parent query.
      //
      CommonTree parentSelect = stack.peek();
      if (parentSelect == null || parentSelect.getChildCount() > 3) {
        return;
      }
      CommonTree parentFrom = (CommonTree) parentSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM);
      if (parentFrom.getChildCount() != 1 || parentFrom.getChild(0).getChildCount() != 1) {
        return;
      }

      //
      // If not select *, return
      //
      if (parentSelect.getFirstChildWithType(PantheraParser_PLSQLParser.ASTERISK) == null) {
        return;
      }

      //
      // Make sure parent select has no group by,order by,having.
      // Note that "order by" is a child of select statement node.
      //

      if (parentSelect.getParent().getParent().getChildCount() > 1) {
        return;
      }
      if (parentSelect.getChildCount() == 3) {
        CommonTree limitParent = (CommonTree) parentSelect.getFirstChildWithType(PantheraExpParser.LIMIT_VK);
        if (limitParent == null) {
          return;
        }
        //
        // Move down the limit token of the parent query to this query. If this query also has a limit,
        // then choose the min limit.
        //
        CommonTree limitThis = (CommonTree) node.getFirstChildWithType(PantheraExpParser.LIMIT_VK);
        if (limitThis == null) {
          node.addChild(limitParent);
        } else {
          int limit1 = Integer.parseInt(limitParent.getChild(0).getText());
          int limit2 = Integer.parseInt(limitThis.getChild(0).getText());
          ((CommonTree) limitThis.getChild(0)).getToken().setText(Integer.toString(limit1 < limit2 ? limit1 : limit2));
        }
      }
      //
      // Replace the parent select statement node with the current select statement node (in case missing order by).
      //
      CommonTree parentSelectStatement = (CommonTree) parentSelect.getParent().getParent();
      parentSelectStatement.getParent().setChild(parentSelectStatement.getChildIndex(), node.getParent().getParent());
      stack.pop();
      stack.push(node);
    }
  }
}
