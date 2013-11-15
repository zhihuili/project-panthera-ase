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

import java.util.List;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.PantheraExpParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockUtil;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * transform top level UNION to UNION ALL, and add a level for UNION ALL
 *
 * TopLevelUnionTransformer.
 *
 */
public class TopLevelUnionTransformer extends BaseSqlASTTransformer {

  SqlASTTransformer tf;

  public TopLevelUnionTransformer(SqlASTTransformer tf) {
    this.tf = tf;
  }

  @Override
  public void transform(CommonTree tree, TranslateContext context) throws SqlXlateException {
    tf.transformAST(tree, context);
    trans(tree, context);
  }

  private void trans(CommonTree tree, TranslateContext context) throws SqlXlateException {
    CommonTree subQuery = (CommonTree) tree.getChild(0).getChild(0);
    CommonTree node = (CommonTree) subQuery.getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_UNION);
    if (node != null) {
      processUnion((CommonTree) node.getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_ALL),
          node, context);
    }
  }

  private void processUnion(CommonTree all, CommonTree node, TranslateContext context) throws SqlXlateException {
    int nodeIndex = node.getChildIndex();
    assert (nodeIndex > 0);
    CommonTree parent = (CommonTree) node.getParent();
    CommonTree leftSelect = (CommonTree) parent.getChild(nodeIndex - 1);
    CommonTree rightSelect = (CommonTree) node
        .getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_SELECT);
    if (rightSelect == null) {
      rightSelect = (CommonTree) ((CommonTree) node
          .getFirstChildWithType(PantheraExpParser.SUBQUERY))
          .getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_SELECT);
    }
    if (leftSelect == null || rightSelect == null) {
      return;
    }

    // for different column name in both tables of UNION(Hive don't support it)
    List<CommonTree> aliasList = FilterBlockUtil.addColumnAliasOrigin(leftSelect, context);
    // by this we ensure even we manually write select a1 as a2 union select a2 as a1, the aliases on both sides still match
    FilterBlockUtil.addColumnAliasHard(rightSelect, aliasList, context);

    CommonTree select = FilterBlockUtil.createSqlASTNode(node, PantheraExpParser.SQL92_RESERVED_SELECT,
        "select");
    CommonTree subquery = FilterBlockUtil.makeSelectBranch(select, context,
        (CommonTree) leftSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM));
    subquery.addChild(leftSelect);
    subquery.addChild((CommonTree) parent.deleteChild(nodeIndex));
    CommonTree selectList = FilterBlockUtil.cloneSelectListByAliasFromSelect(leftSelect);
    if (all == null) {
      select.addChild(FilterBlockUtil.createSqlASTNode(node, PantheraExpParser.SQL92_RESERVED_DISTINCT,
          "distinct"));
    }
    select.addChild(selectList);
    parent.deleteChild(nodeIndex - 1);
    SqlXlateUtil.addCommonTreeChild(parent, nodeIndex - 1, select);
  }
}
