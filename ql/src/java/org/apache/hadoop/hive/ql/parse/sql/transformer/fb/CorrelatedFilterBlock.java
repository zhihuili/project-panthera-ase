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

  @Override
  public void prepare(FilterBlockContext fbContext, TranslateContext context, Stack<CommonTree> selectStack)
      throws SqlXlateException {
    CommonTree normal = this.getASTNode();
    List<CommonTree> nodeList = new ArrayList<CommonTree>();

    FilterBlockUtil.findNode(normal, PantheraParser_PLSQLParser.CASCATED_ELEMENT, nodeList);
    for (CommonTree node : nodeList) {
      if (node.getChild(0).getType() == PantheraParser_PLSQLParser.ROUTINE_CALL) {
        continue;
      }
      int level = PLSQLFilterBlockFactory.getInstance().isCorrelated(fbContext.getqInfo(), selectStack, node);
      Stack<QueryBlock> tempQS = new Stack<QueryBlock>();
      Stack<TypeFilterBlock> tempTS = new Stack<TypeFilterBlock>();
      for (int i = 0; i < level; i++) {
        tempQS.push(fbContext.getQueryStack().pop());
        tempTS.push(fbContext.getTypeStack().pop());
      }
      fbContext.getQueryStack().peek().getWhereFilterColumns().add(FilterBlockUtil.cloneTree(node));
      for (int i = 0; i < level; i++) {
        fbContext.getTypeStack().push(tempTS.pop());
        fbContext.getQueryStack().push(tempQS.pop());
      }
    }

    nodeList = new ArrayList<CommonTree>();
    FilterBlockUtil.findNode(normal, PantheraParser_PLSQLParser.STANDARD_FUNCTION, nodeList);
    for (CommonTree node : nodeList) {
      if (node.getChild(0).getText().equals("substring")) {
        continue;
      }
      int level = PLSQLFilterBlockFactory.getInstance().isCorrelated(fbContext.getqInfo(), selectStack, FilterBlockUtil.findOnlyNode(node, PantheraParser_PLSQLParser.CASCATED_ELEMENT));
      Stack<QueryBlock> tempQS = new Stack<QueryBlock>();
      Stack<TypeFilterBlock> tempTS = new Stack<TypeFilterBlock>();
      for (int i = 0; i < level; i++) {
        tempQS.push(fbContext.getQueryStack().pop());
        tempTS.push(fbContext.getTypeStack().pop());
      }
      fbContext.getQueryStack().peek().getHavingFilterColumns().add(node);
      for (int i = 0; i < level; i++) {
        fbContext.getTypeStack().push(tempTS.pop());
        fbContext.getQueryStack().push(tempQS.pop());
      }
    }
  }

}
