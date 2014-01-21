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
 * Optimization: in the case where "select * from (subquery) [limit <int>]" then drop the outer query,
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

    transformRownum (tree);
    optimizeSelectAsterisk (tree);
  }

  /**
   * change rownum into limit
   * @param node
   */
  private void transformRownum(CommonTree node) {
    if (node.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT) {
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
      transformRownum((CommonTree) node.getChild(i));
    }
  }

  /**
   * optimize select * from (subQ)
   * @param node
   */
  private void optimizeSelectAsterisk(CommonTree node) {
    //
    // Optimization: in the case where "select * from (subquery) [limit <int>]" then drop the outer
    // query,
    // and promote the subquery after attaching the parent's limit token to it.
    //
    while (true) {
      boolean replacedFlag = false;
      if (node.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT) {
        //
        // if select is not in format "SELECT * FROM ... LIMIT", break.
        //
        if (node.getChildCount() > 3) {
          break;
        }
        //
        // If not select *, break.
        //
        if (node.getFirstChildWithType(PantheraParser_PLSQLParser.ASTERISK) == null) {
          break;
        }
        CommonTree from = (CommonTree) node
            .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM);
        if (from.getChildCount() != 1 || from.getChild(0).getChildCount() != 1) {
          return;
        }
        CommonTree tableRefElement = (CommonTree) from.getChild(0).getChild(0);
        CommonTree mode = (CommonTree) tableRefElement
            .getChild(tableRefElement.getChildCount() - 1).getChild(0);
        //
        // if not select mode in from, break.
        //
        if (mode.getType() != PantheraParser_PLSQLParser.SELECT_MODE) {
          break;
        }
        CommonTree subSubQuery = (CommonTree) mode.getChild(0).getChild(0);
        //
        // if subquery has union/intersect/minus, break.
        //
        if (subSubQuery.getType() != PantheraParser_PLSQLParser.SUBQUERY
            || subSubQuery.getChildCount() != 1) {
          break;
        }
        //
        // Make sure select has no group by,order by,having.
        // Note that "order by" is a child of select statement node.
        //
        if (node.getParent().getParent().getChildCount() > 1) {
          break;
        }
        CommonTree subSelect = (CommonTree) subSubQuery.getChild(0);
        if (node.getChildCount() == 3) {
          CommonTree limit = (CommonTree) node.getFirstChildWithType(PantheraExpParser.LIMIT_VK);
          if (limit == null) {
            break;
          }
          //
          // Move down the limit token of the parent query to this query. If this query also has a
          // limit,
          // then choose the min limit.
          //
          CommonTree subLimit = (CommonTree) subSelect
              .getFirstChildWithType(PantheraExpParser.LIMIT_VK);
          if (subLimit == null) {
            subSelect.addChild(limit);
          } else {
            int limit1 = Integer.parseInt(limit.getChild(0).getText());
            int limit2 = Integer.parseInt(subLimit.getChild(0).getText());
            ((CommonTree) subLimit.getChild(0)).getToken().setText(
                Integer.toString(limit1 < limit2 ? limit1 : limit2));
          }
        }
        // if only select * from (subquery), without limit node, just replace the SELECT.
        //
        // Replace the parent select statement node with the current select statement node (in case
        // missing order by).
        //
        CommonTree subSelectStatement = (CommonTree) mode.getChild(0);
        // subQuery doesn't have order by clause, just replace select tree.
        if (subSelectStatement.getChildCount() == 1) {
          node.getParent().setChild(node.getChildIndex(), subSelect);
          // refresh node to new select.
          node = subSelect;
          replacedFlag = true;
        } else { // subQuery has order by clause, need to replace selectStatement tree.
          if (node.getParent().getType() != PantheraParser_PLSQLParser.SUBQUERY
              || node.getParent().getChildCount() != 1) {
            break;
          }
          // if current select has order by, replace select tree, use current select order by.
          if (node.getParent().getParent().getChildCount() > 1) {
            node.getParent().setChild(node.getChildIndex(), subSelect);
            // refresh node to new select.
            node = subSelect;
            replacedFlag = true;
          } else { // no order by, replace selectStatement tree.
            CommonTree SelectStatement = (CommonTree) node.getParent().getParent();
            SelectStatement.getParent().setChild(SelectStatement.getChildIndex(),
                subSelectStatement);
            // refresh node to new select.
            node = subSelect;
            replacedFlag = true;
          }
        }
      }
      if (replacedFlag) {
        // traverse the replaced node.
        optimizeSelectAsterisk(node);
      }
      break;
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      optimizeSelectAsterisk((CommonTree) node.getChild(i));
    }
  }

}
