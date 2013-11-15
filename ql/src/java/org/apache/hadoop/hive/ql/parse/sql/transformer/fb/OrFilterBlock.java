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

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.PantheraExpParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * transform OR to UNION OrFilterBlock.
 *
 */
public class OrFilterBlock extends LogicFilterBlock {

  @Override
  void processChildren(FilterBlockContext fbContext, TranslateContext context)
      throws SqlXlateException {
    for (FilterBlock fb : this.getChildren()) {
      // store transform context
      CommonTree oldQueryForTransfer = fbContext.getQueryStack().peek().cloneSimpleQuery();
      boolean oldHadRebuildQueryForTransfer = fbContext.getQueryStack().peek().getRebuildQueryForTransfer();
      fb.process(fbContext, context);
      // restore transform context in fbContext since both sides of OR are parallel
      fbContext.getQueryStack().peek().setQueryForTransfer(oldQueryForTransfer);
      if (oldHadRebuildQueryForTransfer) {
        fbContext.getQueryStack().peek().setRebuildQueryForTransfer();
      } else {
        fbContext.getQueryStack().peek().unSetRebuildQueryForTransfer();
      }
    }
  }

  @Override
  public void process(FilterBlockContext fbContext, TranslateContext context)
      throws SqlXlateException {
    processChildren(fbContext, context);
    CommonTree leftSelect = this.getChildren().get(0).getTransformedNode();
    CommonTree rightSelect = this.getChildren().get(1).getTransformedNode();
    if (leftSelect.getType() == PantheraParser_PLSQLParser.SUBQUERY && leftSelect.getChildCount() == 1) {
      leftSelect = (CommonTree) leftSelect.getChild(0);
    }
    if (rightSelect.getType() == PantheraParser_PLSQLParser.SUBQUERY && rightSelect.getChildCount() == 1) {
      rightSelect = (CommonTree) rightSelect.getChild(0);
    }
    CommonTree topSelect = this.buildUnionSelect(leftSelect, rightSelect, context);

    while (leftSelect.getType() == PantheraParser_PLSQLParser.SUBQUERY) {
      leftSelect = (CommonTree) leftSelect.getChild(0);
    }
    topSelect.addChild(FilterBlockUtil.cloneSelectListByAliasFromSelect(leftSelect));
    this.setTransformedNode(topSelect);
  }

  private CommonTree buildUnionSelect(CommonTree leftSelect, CommonTree rightSelect,
      TranslateContext context) {
    CommonTree union = FilterBlockUtil.createSqlASTNode(this.getASTNode(), PantheraExpParser.SQL92_RESERVED_UNION,
        "union");
    union.addChild(rightSelect);
    CommonTree topSelect = FilterBlockUtil.createSqlASTNode(
        leftSelect, PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT, "select");
    CommonTree subquery = FilterBlockUtil.makeSelectBranch(topSelect, context,
        (CommonTree) leftSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM));
    subquery.addChild(leftSelect);
    subquery.addChild(union);
    return topSelect;
  }


}
