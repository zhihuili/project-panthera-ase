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
import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockUtil;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * Transformer for multiple-table select.
 *
 */
public class MultipleTableSelectTransformer extends BaseSqlASTTransformer {
  SqlASTTransformer tf;

  public MultipleTableSelectTransformer(SqlASTTransformer tf) {
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
      //
      // Check if this is a multiple table select.
      //
      Tree from = node.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM);
      if (from.getChildCount() > 1) {
        // Simply transform the multiple table selection into cross join of all tables
        Tree tableRef = from.getChild(0);
        do {
          generateCrossJoin(tableRef, from.getChild(1));
          from.deleteChild(1);
        } while (from.getChildCount() > 1);
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

  private void generateCrossJoin(Tree tableRef, Tree nextTableRef) {
    //
    // Create a Join node and attach it to the tableRef node as the last child.
    //
    CommonTree joinNode = FilterBlockUtil.createSqlASTNode((CommonTree) tableRef, PantheraParser_PLSQLParser.JOIN_DEF, "join");
    //better set at comma, but comma missing in AST
    tableRef.addChild(joinNode);
    //
    // Create a Cross node and attach it to the join node as the first child.
    //
    CommonTree crossNode = FilterBlockUtil.createSqlASTNode(joinNode, PantheraParser_PLSQLParser.CROSS_VK, "cross");
    joinNode.addChild(crossNode);
    //
    // Move the table ref element tree of the nextTableRef node to be the second child of the join node.
    //
    // FIXME nextTableRef.getChild(1) may have join node, will lose here
    joinNode.addChild(nextTableRef.getChild(0));
    nextTableRef.deleteChild(0);
  }
}
