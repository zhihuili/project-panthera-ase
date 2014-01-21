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
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockUtil;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * Normalize group by element to make them exactly same as in select-list
 *
 * GroupElementNormalizeTransformer.
 *
 */
public class GroupElementNormalizeTransformer extends BaseSqlASTTransformer {
  SqlASTTransformer tf;

  public GroupElementNormalizeTransformer(SqlASTTransformer tf) {
    this.tf = tf;
  }

  @Override
  protected void transform(CommonTree tree, TranslateContext context) throws SqlXlateException {
    tf.transformAST(tree, context);
    this.transformQIGroup(tree, context);
  }

  private void transformQIGroup(CommonTree tree, TranslateContext context) throws SqlXlateException {
    QueryInfo qInfo = context.getQInfoRoot();
    transformQIGroup(qInfo, context);
  }

  private void transformQIGroup(QueryInfo qf, TranslateContext context) throws SqlXlateException {
    for (QueryInfo qinfo : qf.getChildren()) {
      transformQIGroup(qinfo, context);
    }
    if (!qf.isQInfoTreeRoot()) {
      this.transformGroup(qf, context);
    }
  }

  private void transformGroup(QueryInfo qi, TranslateContext context)
      throws SqlXlateException {
    CommonTree select = qi.getSelectKeyForThisQ();
    CommonTree group = (CommonTree) select.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_GROUP);
    if (group == null) {
      return;
    }
    CommonTree selectList = (CommonTree) select.getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    if (selectList == null) {
      return;
    }
    List<CommonTree> allGroupList = new ArrayList<CommonTree>();
    for (int i = 0; i < group.getChildCount(); i++) {
      CommonTree groupElement = (CommonTree) group.getChild(i);
      if (groupElement.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_HAVING) {
        continue;
      }
      CommonTree cas = (CommonTree) groupElement.getChild(0).getChild(0);
      // for group by position, e.g. group by 1, 2, change the position with select item
      if (cas.getType() == PantheraParser_PLSQLParser.UNSIGNED_INTEGER) {
        try {
          int index = Integer.valueOf(cas.getText());
          if (index > 0 && index <= selectList.getChildCount()) {
            // replace the group node with correspond select item
            int childIdx = cas.getChildIndex();
            CommonTree expr = (CommonTree) cas.getParent();
            expr.replaceChildren(childIdx, childIdx,
                FilterBlockUtil.cloneTree((CommonTree) selectList.getChild(index - 1).getChild(0).getChild(0)));
            cas = (CommonTree) expr.getChild(0);
          }
        } catch (Exception e) {
          // do nothing here
        }
      }
      allGroupList.add(cas);
    }

    normalizeGroup(selectList, allGroupList);
    // normalize twice
    normalizeGroup(selectList, allGroupList);
  }

  /**
   * 1,verify that all non-aggregation select items are in group by list
   * 2,sync the select column and group by column with the same format. (table.column/column)
   */
  private void normalizeGroup(CommonTree selectList, List<CommonTree> allGroupList) throws SqlXlateException {
    for (int i = 0; i < selectList.getChildCount(); i++) {
      CommonTree selectItem = (CommonTree) selectList.getChild(i);
      assert(selectItem.getChild(0).getType() == PantheraParser_PLSQLParser.EXPR);
      CommonTree selectExpr = (CommonTree) selectItem.getChild(0);
      List<CommonTree> caslist = new ArrayList<CommonTree>();
      findNode(selectExpr, caslist);
      for (CommonTree cas:caslist) {
        boolean find = false;
        for (CommonTree groupEx : allGroupList) {
          if (FilterBlockUtil.makeEqualsExpandTree(cas, groupEx)) {
            find = true;
          }
          if (FilterBlockUtil.makeEqualsExpandTree(groupEx, cas)) {
            find = true;
          }
        }
        if (!find) {
          for (CommonTree groupEx : allGroupList) {
            if (FilterBlockUtil.makeEqualsExpandTree((CommonTree) selectExpr.getChild(0), groupEx)) {
              find = true;
            }
            if (FilterBlockUtil.makeEqualsExpandTree(groupEx, (CommonTree) selectExpr.getChild(0))) {
              find = true;
            }
          }
        }
        if (!find) {
          throw new SqlXlateException(cas, "element in select-list is neither in aggregation function nor in group-by list");
        }
      }
    }
  }

  /**
   * find all CascatedElement node not in aggregation parameter.
   *
   * @param node
   * @param nodeList
   */
  public void findNode(CommonTree node, List<CommonTree> nodeList) {
    if (node == null) {
      return;
    }
    if (node.getType() == PantheraParser_PLSQLParser.STANDARD_FUNCTION) {
      if (!node.getChild(0).getText().equals("substring") && !node.getChild(0).getText().equals("substr")) {
        return;
      }
    }
    if (node.getType() == PantheraParser_PLSQLParser.CASCATED_ELEMENT) {
      if (node.getChild(0).getType() == PantheraParser_PLSQLParser.ROUTINE_CALL) {
        List<String> aggrRoutineCall = new ArrayList<String>();
        // TODO ugly hard code here
        aggrRoutineCall.add("collect_set");
        if (!aggrRoutineCall.contains(node.getChild(0).getChild(0).getChild(0).getText())) {
          // such as substring
          findNode((CommonTree) node.getChild(0).getChild(1), nodeList);
        }
        return;
      } else {
        nodeList.add(node);
      }
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      findNode((CommonTree) node.getChild(i), nodeList);
    }
  }
}
