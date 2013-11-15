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
import java.util.Map;
import java.util.Stack;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.QueryInfo.Column;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockUtil;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * Build QueryInfo tree for SQL AST tree.
 * PrepareQueryInfoTransformer.
 *
 */
public class PrepareQueryInfoTransformer extends BaseSqlASTTransformer {

  SqlASTTransformer tf;
  private final SqlXlateUtil.AliasGenerator aliasGen = new SqlXlateUtil.AliasGenerator();

  @Override
  public void transform(CommonTree tree, TranslateContext context) throws SqlXlateException {
    this.tf.transformAST(tree, context);
    prepareQueryInfo(tree, context);

  }

  public PrepareQueryInfoTransformer(SqlASTTransformer tf) {
    this.tf = tf;
  }

  void prepareQueryInfo(CommonTree tree, TranslateContext context) throws SqlXlateException {
    Stack<Integer> stack = new Stack<Integer>();
    stack.push(-999);// for first peek;
    List<QueryInfo> qInfoList = new ArrayList<QueryInfo>();
    prepare(tree, null, stack, qInfoList, context);
    context.setQInfoRoot(qInfoList.get(0));
  }

  /**
   * Prepare the Facility data structures for later AST generation.
   *
   * @param ast
   *          SQL AST
   * @param qInfo
   *          the current QueryInfo object
   * @throws SqlXlateException
   */
  protected void prepare(CommonTree ast, QueryInfo qInfo, Stack<Integer> stack,
      List<QueryInfo> qInfoList, TranslateContext context)
      throws SqlXlateException {

    int astType = ast.getType();
    switch (astType) {
    case PantheraParser_PLSQLParser.STATEMENTS:
      // Prepare the root QueryInfo at SQL AST root node
      qInfo = prepareQInfo(ast, qInfo, qInfoList);
      break;
    case PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT: {
      // Prepare a new QueryInfo for each query (including top level queries and subqueries in from
      // clause)
      // subqueries in filters in where clauses will not have new QInfo
      // created. FilterBlocks will be created for them.
      int nodeType = stack.peek();
      if (nodeType == PantheraParser_PLSQLParser.SQL92_RESERVED_FROM || qInfo.isQInfoTreeRoot()) {
        qInfo = prepareQInfo(ast, qInfo, qInfoList);
      } else {
        buildSelectListStr(ast, qInfo.getSelectListForSelectKey(ast));
      }
      // Prepare the top most Filter Blocks
      break;
    }
    case PantheraParser_PLSQLParser.SQL92_RESERVED_FROM: {
      // Prepare the from subtree
      prepareFrom(ast, qInfo);
      stack.push(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM);
      break;
    }
    case PantheraParser_PLSQLParser.SQL92_RESERVED_WHERE: {
      stack.push(PantheraParser_PLSQLParser.SQL92_RESERVED_WHERE);
      break;
    }

    case PantheraParser_PLSQLParser.SELECT_LIST: {
      stack.push(PantheraParser_PLSQLParser.SELECT_LIST);
      break;
    }
    case PantheraParser_PLSQLParser.SQL92_RESERVED_HAVING: {
      stack.push(PantheraParser_PLSQLParser.SQL92_RESERVED_HAVING);
      break;
    }
    default:
    }

    // add reference to qInfo ot each Sql AST node
    // ast.setQueryInfo(qInfo);
    // if do not skip recursion, iterate all the children
    for (int i = 0; i < ast.getChildCount(); i++) {
      prepare((CommonTree) ast.getChild(i), qInfo, stack, qInfoList, context);
    }

    if (astType == stack.peek()) {
      stack.pop();
    }

    if (astType == PantheraParser_PLSQLParser.SQL92_RESERVED_FROM) {
      // Build row info for src tables under the from clause.
      List<Column> fromRowInfo = qInfo.getRowInfo(ast);
      Map<String, String> tblAliasNamePairs = qInfo.getSrcTblAliasNamePairForSelectKey((CommonTree) ast.getParent());

      for (String tblAlias : tblAliasNamePairs.keySet()) {
        String tblName = tblAliasNamePairs.get(tblAlias);
        if (tblName != null) {
          // Get RowResolver for this table.
          RowResolver tblRR = context.getMeta().getRRForTbl(tblName);
          // Dump columns into the fromRR.
          for (ColumnInfo col : tblRR.getColumnInfos()) {
            Column newCol = new Column(tblAlias, col.getInternalName());
            fromRowInfo.add(newCol);
          }
        } else {
          // Propogate the columns from sub-query.
          CommonTree subQSelect = qInfo.GetSuQFromAlias(tblAlias);
          assert(subQSelect != null);
          List<Column> subQRowInfo = qInfo.findChildQueryInfo(subQSelect).getSelectRowInfo();

          for (Column col : subQRowInfo) {
            Column newCol = new Column(tblAlias, col.getColAlias());
            fromRowInfo.add(newCol);
          }
        }
      }
    } else if (astType == PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT) {
      // Build row info for select result.
      List<Column> selectRowInfo = qInfo.getRowInfo(ast);

      List<String> selectList = qInfo.getSelectListForSelectKey(ast);
      for (String selectListItem : selectList) {
        if (selectListItem.equals("*")) {
          List<Column> fromRowInfo = qInfo.getRowInfo((CommonTree)
            ast.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM));
          for (Column col : fromRowInfo) {
            selectRowInfo.add(col);
          }
        } else {
          Column newCol = new Column(null, selectListItem);
          selectRowInfo.add(newCol);
        }
      }
    }
  }

  /**
   * Prepare Query Info object
   *
   * @param ast
   * @param qInfo
   * @return
   */
  private QueryInfo prepareQInfo(CommonTree ast, QueryInfo qInfo, List<QueryInfo> qInfoList) {
    QueryInfo nqi = new QueryInfo();
    qInfoList.add(nqi);
    nqi.setParentQueryInfo(qInfo);
    if (qInfo != null) {
      nqi.setSelectKeyForThisQ(ast);
      buildSelectListStr(ast, nqi.getSelectList());
      qInfo.addChild(nqi);
    }
    return nqi;
  }

  /**
   * Prepare the from clause.
   *
   * @param src
   * @param qInfo
   * @throws SqlXlateException
   */
  private void prepareFrom(CommonTree src, QueryInfo qInfo) throws SqlXlateException {
    // set from clause for this query
    qInfo.setFrom(src);
    // get subquery alias in from
    prepareSubQAliases(src, qInfo);
  }

  /**
   * Prepare SubQuery Aliases
   *
   * @param src
   * @param qInfo
   * @throws SqlXlateException
   */
  private void prepareSubQAliases(CommonTree src, QueryInfo qInfo) throws SqlXlateException {
    // prepare subq alias for each qInfo
    if (src.getType() == PantheraParser_PLSQLParser.TABLE_REF_ELEMENT) {
      CommonTree alias = (CommonTree) src.getFirstChildWithType(PantheraParser_PLSQLParser.ALIAS);
      CommonTree child2 = null;
      CommonTree subquery = null;
      if ((child2 = (CommonTree) src
          .getFirstChildWithType(PantheraParser_PLSQLParser.TABLE_EXPRESSION)) != null) {
        if ((child2 = (CommonTree) child2
            .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_MODE)) != null) {
          if ((child2 = (CommonTree) child2
              .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_STATEMENT)) != null) {
            if ((child2 = (CommonTree) child2
                .getFirstChildWithType(PantheraParser_PLSQLParser.SUBQUERY)) != null) {
              subquery = child2;
            }
          }
        }
      }
      if (subquery != null) {
        ASTNode aliasNode = null;
        if (alias == null) {
          aliasNode = SqlXlateUtil.newASTNode(HiveParser.Identifier, aliasGen
              .generateAliasName());
          // Create a new SqlASTNode for alias for convinient processing later.
          CommonTree sqlAliasNode = FilterBlockUtil.createSqlASTNode(src, PantheraParser_PLSQLParser.ALIAS, "ALIAS");
          sqlAliasNode.addChild(FilterBlockUtil.createSqlASTNode(src, PantheraParser_PLSQLParser.ID, aliasNode.getText()));
          // Attach it to the TABLE_REF_ELEMENT node as the first child.
          assert(src.getChildCount() == 1);
          CommonTree tmpNode = (CommonTree) src.getChild(0);
          src.setChild(0, sqlAliasNode);
          src.addChild(tmpNode);
        } else {
          aliasNode = genForAlias(alias);
        }
        qInfo.setSubQAlias((CommonTree) subquery.getChild(0), aliasNode);
      }
      return;
    }

    for (int i = 0; i < src.getChildCount(); i++) {
      prepareSubQAliases((CommonTree) src.getChild(i), qInfo);
    }
  }

  /**
   * Generate Hive AST for Alias node
   *
   * @param src
   * @return
   * @throws SqlXlateException
   */
  private ASTNode genForAlias(CommonTree src) throws SqlXlateException {
    String text = src.getChild(0).getText();
    ASTNode alias = SqlXlateUtil.newASTNode(HiveParser.Identifier, text);
    // SqlXlateUtil.attachChild(dest, alias);
    return alias;
  }

  private void buildSelectListStr(CommonTree select, List<String> selectListStr) {
    CommonTree selectList = (CommonTree) select
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    if (selectList == null) {
      // select *
      selectListStr.add("*");
      return;
    }
    for (int i = 0; i < selectList.getChildCount(); i++) {
      CommonTree selectItem = (CommonTree) selectList.getChild(i);
      if (selectItem.getChildCount() == 2) {
        // The select item has alias.
        selectListStr.add(selectItem.getChild(1).getChild(0).getText());
        continue;
      }
      if (selectItem.getChild(0).getChild(0).getType() == PantheraParser_PLSQLParser.CASCATED_ELEMENT) {
        // Direct table column reference.
        CommonTree anyElement = (CommonTree) selectItem.getChild(0).getChild(0).getChild(0);
        selectListStr.add(anyElement.getChild(anyElement.getChildCount() - 1).getText());
      } else {
        // Function. since it has no alias, use toStringTree() as its alias.
        selectListStr.add(selectItem.getChild(0).getChild(0).toStringTree());
      }
    }
  }
}
