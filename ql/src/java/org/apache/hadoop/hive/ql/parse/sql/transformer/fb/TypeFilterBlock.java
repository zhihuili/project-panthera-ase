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

import java.util.Stack;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.PantheraExpParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * Filter block which process WHERE, SELECT_LIST & HAVING node type.
 * TypeFilterBlock.
 *
 */
public abstract class TypeFilterBlock extends BaseFilterBlock {
  protected int groupCount = 0;

  abstract void preExecute(FilterBlockContext fbContext, TranslateContext context)
      throws SqlXlateException;

  @Override
  public void prepare(FilterBlockContext fbContext, TranslateContext context, Stack<CommonTree> selectStack)
      throws SqlXlateException {
    fbContext.getTypeStack().push(this);
    prepareChildren(fbContext, context, selectStack);
    fbContext.getTypeStack().pop();
  }

  @Override
  public void process(FilterBlockContext fbContext, TranslateContext context)
      throws SqlXlateException {
    fbContext.getTypeStack().push(this);

    preExecute(fbContext, context);
    processChildren(fbContext, context);

    execute(fbContext, context);
    fbContext.getTypeStack().pop();
  }

  abstract void execute(FilterBlockContext fbContext, TranslateContext context)
      throws SqlXlateException;

  /**
   * This newly level should recover select-list without added group by element.
   * @param fbContext
   * @param context
   */
  protected void removeGroupElementFromSelectList(FilterBlockContext fbContext, TranslateContext context) {
    CommonTree select = (CommonTree) this.getASTNode().getAncestor(PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT);
    CommonTree group = (CommonTree) select.getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_GROUP);
    CommonTree transformedNode = this.getTransformedNode();
    if (transformedNode == null || group == null) {
      return ;
    }
    if(select.getCharPositionInLine() != this.getTransformedNode().getCharPositionInLine()) {
      return ;
    }
    removeGroupElement(transformedNode);
  }

  private void removeGroupElement(CommonTree transformed) {
    if (transformed.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT) {
      removeGroupElementFromSelectList(transformed);
    } else if (transformed.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_UNION) {
      removeGroupElement((CommonTree) transformed.getChild(0));
    } else if (transformed.getType() == PantheraParser_PLSQLParser.SUBQUERY) {
      for (int i = 0; i < transformed.getChildCount(); i++) {
        removeGroupElement((CommonTree) transformed.getChild(i));
      }
    }
  }

  private void removeGroupElementFromSelectList(CommonTree transformed) {
    CommonTree selectList = (CommonTree) transformed.
        getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    if (selectList == null) {
      return;
    }
    for (int i = 0; i < groupCount; i++) {
      CommonTree selectItem = (CommonTree) selectList.getChild(selectList.getChildCount() - 1 - i);
      selectList.deleteChild(selectItem.getChildIndex());
    }
  }

  protected int addGroupElementToSelectList(FilterBlockContext fbContext, TranslateContext context) throws SqlXlateException {
    CommonTree group = (CommonTree) fbContext.getQueryStack().peek().getASTNode().
        getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_GROUP);
    if (group == null) {
      return 0;
    } else {
      int originRetCount = group.getChild(group.getChildCount() - 1).
          getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_HAVING ? group.getChildCount() - 1 : group.getChildCount();
      int retCount = 0;
      //CommonTree from = (CommonTree) fbContext.getQueryStack().peek().getASTNode().getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM);
      //List<Column> columnList = fbContext.getqInfo().getRowInfo(from);
      CommonTree selectList = (CommonTree) fbContext.getQueryStack().peek().getASTNode().
          getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
      if (selectList == null) {
        // select * group by a,b,c
        return 0;
      }
      int originSelectCount = selectList.getChildCount();
      for (int i = 0; i < originRetCount; i++) {
        CommonTree groupElement = (CommonTree) group.getChild(i);
        CommonTree expr = FilterBlockUtil.cloneTree((CommonTree) groupElement.getChild(0));
        if (expr.getChild(0).getType() != PantheraParser_PLSQLParser.CASCATED_ELEMENT) {
          throw new SqlXlateException(expr, "Panthera only support group by column/table.column");
        }
        CommonTree alias = FilterBlockUtil.createSqlASTNode(expr, PantheraParser_PLSQLParser.ALIAS, "ALIAS");
        CommonTree any = (CommonTree) expr.getChild(0).getChild(0);
        String str = any.getChildCount() == 2 ? any.getChild(1).getText() : any.getChild(0).getText();
        CommonTree aliasID = FilterBlockUtil.createSqlASTNode(expr, PantheraParser_PLSQLParser.ID, str);
        alias.addChild(aliasID);
        CommonTree selectItem = FilterBlockUtil.createSqlASTNode(expr, PantheraParser_PLSQLParser.SELECT_ITEM, "SELECT_ITEM");
        selectItem.addChild(expr);
        selectItem.addChild(alias);
        CommonTree find = null;
        for (int j = 0; j < originSelectCount + retCount ; j++) {
          CommonTree iterItem = (CommonTree) selectList.getChild(j);
          CommonTree iterExpr = (CommonTree) iterItem.getChild(0);
          if (FilterBlockUtil.makeEqualsExpandTree(expr, iterExpr)) {
            find = iterItem;
            break;
          }
        }
        if (find == null) {
          selectList.addChild(selectItem);
          retCount++;
        }
      }
      return retCount;
    }
  }
}
