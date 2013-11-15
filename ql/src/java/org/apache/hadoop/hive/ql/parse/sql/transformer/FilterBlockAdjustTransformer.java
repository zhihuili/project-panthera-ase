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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlock;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockUtil;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.HavingFilterBlock;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.OrFilterBlock;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.SubQFilterBlock;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.WhereFilterBlock;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * Transform OrFB with one child SubQFB the other child not SubQFB for every QueryInfo.
 *
 * FilterBlockAdjustTransformer.
 *
 */
public class FilterBlockAdjustTransformer extends BaseSqlASTTransformer {

  private static final Log LOG = LogFactory.getLog(FilterBlockAdjustTransformer.class);
  SqlASTTransformer tf;

  public FilterBlockAdjustTransformer(SqlASTTransformer tf) {
    this.tf = tf;
  }

  @Override
  public void transform(CommonTree tree, TranslateContext context) throws SqlXlateException {
    tf.transformAST(tree, context);
    this.transformAllQI(tree, context);
  }

  void transformAllQI(CommonTree tree, TranslateContext context) throws SqlXlateException {
    QueryInfo qInfo = context.getQInfoRoot();
    transformAllQIDeepFirst(qInfo, context);
  }

  void transformAllQIDeepFirst(QueryInfo qf, TranslateContext context) throws SqlXlateException {
    for (QueryInfo qinfo : qf.getChildren()) {
      transformAllQIDeepFirst(qinfo, context);
    }
    if (!qf.isQInfoTreeRoot()) {
      this.transformFB(qf, context);
    }
  }

  private void transformFB(QueryInfo qf, TranslateContext context) throws SqlXlateException {
    FilterBlock fb = qf.getFilterBlockTreeRoot();
    transformFBDeepFirst(qf, fb, context, false);
  }

  private void transformFBDeepFirst(QueryInfo qf, FilterBlock fb, TranslateContext context, boolean inHavingFB)
      throws SqlXlateException {
    for (FilterBlock childfb : fb.getChildren()) {
      if (childfb instanceof HavingFilterBlock) {
        transformFBDeepFirst(qf, childfb, context, true);
      } else if (childfb instanceof WhereFilterBlock) {
        transformFBDeepFirst(qf, childfb, context, false);
      } else {
        transformFBDeepFirst(qf, childfb, context, inHavingFB);
      }
    }
    if (fb instanceof OrFilterBlock) {
      FilterBlock leftFB = fb.getChildren().get(0);
      FilterBlock rightFB = fb.getChildren().get(1);
      if (!(leftFB instanceof SubQFilterBlock && rightFB instanceof SubQFilterBlock)) {
        // need handle
        CommonTree badSubQuery = FilterBlockUtil.firstAncestorOfType(fb.getASTNode(),
            PantheraParser_PLSQLParser.SUBQUERY);
        while(badSubQuery.getParent().getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_UNION) {
          badSubQuery = (CommonTree) badSubQuery.getParent().getParent();
        }
        if (badSubQuery.getParent().getType() == PantheraParser_PLSQLParser.SELECT_STATEMENT) {
          // already in a separated QueryInfo
          CommonTree select = FilterBlockUtil.firstAncestorOfType(fb.getASTNode(), PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT);
          transformBadOrToUnionAndAddLevel(qf, select, fb, context, inHavingFB);
        } else {
          // in a subQuery like IN/COMPARE/EXISTS, NOTIN/NOTEXISTS
          if (isNegativeSubQ(badSubQuery)) {
            transformBadOrToUpperAnd(badSubQuery, fb, context);
          } else {
            transformBadOrToUpperOr(badSubQuery, fb, context);
          }
        }
      }
    }
  }

  private void transformBadOrToUpperOr(CommonTree subQuery, FilterBlock orFB, TranslateContext context) {
    CommonTree splitToken = (CommonTree) subQuery.getParent();
    CommonTree orRoot = orFB.getASTNode();
    CommonTree rightTree = (CommonTree) orRoot.deleteChild(1);
    CommonTree leftTree = (CommonTree) orRoot.deleteChild(0);
    int pos = orRoot.getChildIndex();
    CommonTree orParent = (CommonTree) orRoot.getParent();
    orParent.replaceChildren(pos, pos, leftTree);
    CommonTree leftCondition = FilterBlockUtil.cloneTree(splitToken);
    orParent.replaceChildren(pos, pos, rightTree);
    CommonTree rightCondition = FilterBlockUtil.cloneTree(splitToken);
    CommonTree or = FilterBlockUtil.createSqlASTNode(orRoot,
        PantheraParser_PLSQLParser.SQL92_RESERVED_OR, "or");
    or.addChild(leftCondition);
    or.addChild(rightCondition);
    splitToken.getParent().replaceChildren(splitToken.childIndex, splitToken.childIndex, or);
  }

  private void transformBadOrToUpperAnd(CommonTree subQuery, FilterBlock orFB, TranslateContext context) {
    CommonTree splitToken = (CommonTree) subQuery.getParent();
    if (subQuery.getParent().getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_EXISTS) {
      splitToken = (CommonTree) subQuery.getParent().getParent();
    }
    CommonTree orRoot = orFB.getASTNode();
    CommonTree rightTree = (CommonTree) orRoot.deleteChild(1);
    CommonTree leftTree = (CommonTree) orRoot.deleteChild(0);
    int pos = orRoot.getChildIndex();
    CommonTree orParent = (CommonTree) orRoot.getParent();
    orParent.replaceChildren(pos, pos, leftTree);
    CommonTree leftCondition = FilterBlockUtil.cloneTree(splitToken);
    orParent.replaceChildren(pos, pos, rightTree);
    CommonTree rightCondition = FilterBlockUtil.cloneTree(splitToken);
    CommonTree and = FilterBlockUtil.createSqlASTNode(orRoot,
        PantheraParser_PLSQLParser.SQL92_RESERVED_AND, "and");
    and.addChild(leftCondition);
    and.addChild(rightCondition);
    splitToken.getParent().replaceChildren(splitToken.childIndex, splitToken.childIndex, and);
  }

  private boolean isNegativeSubQ(CommonTree badSubQuery) {
    if (badSubQuery.getParent().getType() == PantheraParser_PLSQLParser.NOT_IN
        || badSubQuery.getParent().getParent().getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_NOT
        || badSubQuery.getParent().getType() == PantheraParser_PLSQLParser.IS_NOT_NULL
        || badSubQuery.getParent().getType() == PantheraParser_PLSQLParser.NOT_LIKE) {
      return true;
    } else {
      return false;
    }
  }

  private void transformBadOrToUnionAndAddLevel(QueryInfo qf, CommonTree select, FilterBlock orFB, TranslateContext context, boolean inHavingFB) throws SqlXlateException {
    // orFB must has SubQFB and UncorrelatedFB as children
    // do nothing
  }

}
