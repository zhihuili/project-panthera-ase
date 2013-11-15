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
import java.util.Stack;

import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlock;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.PLSQLFilterBlockFactory;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * Build FilterBlock tree for every QueryInfo
 * FilterBlockPrepareTransformer.
 *
 */
public class PrepareFilterBlockTransformer extends BaseSqlASTTransformer {
  SqlASTTransformer tf;

  public PrepareFilterBlockTransformer(SqlASTTransformer tf) {
    this.tf = tf;
  }

  @Override
  public void transform(CommonTree tree, TranslateContext context) throws SqlXlateException {
    tf.transformAST(tree, context);
    for (QueryInfo qf : context.getqInfoList()) {
      this.buildFilterBlockTree(qf, context);
    }
  }


  void buildFilterBlockTree(QueryInfo qFNode, TranslateContext context) throws SqlXlateException {
    CommonTree subRoot = qFNode.getSelectKeyForThisQ();
    Stack<CommonTree> selectStack = new Stack<CommonTree>();
    FilterBlock fbRoot = buildFilterBlock(qFNode,selectStack, subRoot);
    qFNode.setFilterBlockTreeRoot(fbRoot);
  }

  FilterBlock buildFilterBlock(QueryInfo qFNode,Stack<CommonTree> selectStack, CommonTree node)
      throws SqlXlateException {
    if (needSkip(node)) {
      return null;
    }
    if (node.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT) {
      selectStack.push(node);
    }
    List<FilterBlock> fbl = new ArrayList<FilterBlock>();
    for (int i = 0; i < node.getChildCount(); i++) {
      CommonTree t = (CommonTree) node.getChild(i);
      FilterBlock fb = buildFilterBlock(qFNode,selectStack, t);
      if (fb != null) {
        fbl.add(fb);
      }
    }
    if (node.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT) {
      selectStack.pop();
    }
    return PLSQLFilterBlockFactory.getInstance().getFilterBlock(qFNode,selectStack, node, fbl);
  }

  boolean needSkip(Tree node) {
    int type = node.getType();
    return type == PantheraParser_PLSQLParser.SQL92_RESERVED_FROM
        || type == PantheraParser_PLSQLParser.SELECT_LIST
        || type == PantheraParser_PLSQLParser.ORDER_BY_ELEMENTS
        || type == PantheraParser_PLSQLParser.GROUP_BY_ELEMENT;
  }
}
