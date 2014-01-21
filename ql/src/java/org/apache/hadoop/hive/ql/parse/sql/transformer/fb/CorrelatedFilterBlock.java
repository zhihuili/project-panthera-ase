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
package org.apache.hadoop.hive.ql.parse.sql.transformer.fb;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;


public class CorrelatedFilterBlock extends NormalFilterBlock {

  @Override
  public void process(FilterBlockContext fbContext, TranslateContext context)
      throws SqlXlateException {
    // this filterBlock do nothing but remove correlated condition to original select.
    // the transformation will be done in QueryBlcok.
    TypeFilterBlock typeFB = fbContext.getTypeStack().peek();
    if (typeFB instanceof WhereFilterBlock) {
      CommonTree topSelect = fbContext.getQueryStack().peek().cloneSimpleQuery();
      this.setTransformedNode(topSelect);
    }
    if (typeFB instanceof HavingFilterBlock) {
      CommonTree topSelect = fbContext.getQueryStack().peek().cloneTransformedQuery();
      this.setTransformedNode(topSelect);
    }
    QueryBlock qb = fbContext.getQueryStack().peek();
    if (fbContext.getTypeStack().peek() instanceof WhereFilterBlock) {
      qb.setWhereCFB(this);
      qb.setWhereCFlag();
    }
    if (fbContext.getTypeStack().peek() instanceof HavingFilterBlock) {
      qb.setHavingCFB(this);
      qb.setHavingCFlag();
    }
  }

  /*
   * Store correlated and uncorrelated columns in QueryBlock. Also for aggregation functions in Having clause.
   */
  @Override
  public void prepare(FilterBlockContext fbContext, TranslateContext context, Stack<CommonTree> selectStack)
      throws SqlXlateException {

    // store columns
    super.prepare(fbContext, context, selectStack);

    // store aggregation functions.
    CommonTree normal = this.getASTNode();
    List<CommonTree> nodeList = new ArrayList<CommonTree>();
    // for aggregation funcs, if it has correlated columns, need to store this function into QueryBlcok.
    FilterBlockUtil.findNode(normal, PantheraParser_PLSQLParser.STANDARD_FUNCTION, nodeList);
    for (CommonTree node : nodeList) {
      // TODO: there might be other non aggregation functions have STANDARD_FUNCTION node.
      if (node.getChild(0).getText().equals("substring") || node.getChild(0).getText().equals("substr")) {
        continue;
      }
      CommonTree cas = FilterBlockUtil.findOnlyNode(node, PantheraParser_PLSQLParser.CASCATED_ELEMENT);
      int level;
      if (cas != null) {
        level = PLSQLFilterBlockFactory.getInstance().isCorrelated(fbContext.getqInfo(), selectStack, cas);
      } else {
        level = 0;
      }
      Stack<QueryBlock> tempQS = new Stack<QueryBlock>();
      Stack<TypeFilterBlock> tempTS = new Stack<TypeFilterBlock>();
      for (int i = 0; i < level; i++) {
        tempQS.push(fbContext.getQueryStack().pop());
        tempTS.push(fbContext.getTypeStack().pop());
      }
      // store aggregation into HavingFilterColumns. Aggregation function can not exists in WHERE clause.
      // so the aggregation founded must be from HAVING clause.
      /*
       * here use "add(node)" instead of "add(FilterBlockUtil.cloneTree(node))", because the column name will be
       * refreshed in HAVING clause
       */
      fbContext.getQueryStack().peek().getHavingFilterColumns().add(node);
      for (int i = 0; i < level; i++) {
        fbContext.getTypeStack().push(tempTS.pop());
        fbContext.getQueryStack().push(tempQS.pop());
      }
    }
  }

}
