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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.PantheraExpParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.processor.FilterBlockProcessorFactory;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;


public class QueryBlock extends BaseFilterBlock {

  private Set<String> tableNameSet;
  private CommonTree queryForTransfer;
  private boolean hadRebuildQueryForTransfer;// for AND in HAVING subquery.
  private List<CommonTree> aggregationList;
  private CommonTree group;
  private CommonTree order;
  private CommonTree limit;
  private boolean isHaving = false;
  private final CountAsterisk countAsterisk = new CountAsterisk();
  private final List<CommonTree> preDefinedAliasList = new ArrayList<CommonTree>();
  private CommonTree originalQueryForHaving;

  private boolean whereCFlag;
  private boolean havingCFlag;
  private final List<FilterBlock> whereCFBList = new ArrayList<FilterBlock>();
  private final List<FilterBlock> havingCFBList = new ArrayList<FilterBlock>();

  private final List<CommonTree> whereFilterColumns = new ArrayList<CommonTree>();
  private List<CommonTree> havingFilterColumns = new ArrayList<CommonTree>();

  @Override
  public void prepare(FilterBlockContext fbContext, TranslateContext context, Stack<CommonTree> selectStack)
      throws SqlXlateException {
    fbContext.getQueryStack().push(this);
    selectStack.push(this.getASTNode());
    prepareChildren(fbContext, context, selectStack);
    selectStack.pop();
    fbContext.getQueryStack().pop();
  }

  @Override
  void processChildren(FilterBlockContext fbContext, TranslateContext context)
      throws SqlXlateException {
    for (FilterBlock fb : this.getChildren()) {
      fb.process(fbContext, context);
      this.setQueryForTransfer(fb.getTransformedNode());
      this.setRebuildQueryForTransfer();
    }
    this.setTransformedNode(this.queryForTransfer);
    this.setTransformed();
  }

  @Override
  public void process(FilterBlockContext fbContext, TranslateContext context)
      throws SqlXlateException {

    // store select column alias user defined.
    preProcessQueryBlock();

    fbContext.getQueryStack().push(this);
    fbContext.getSelectStack().push(this.getASTNode());
    fbContext.getSelectStackForTransfer().push(this.getASTNode());

    processChildren(fbContext, context);

    // restore select column alias user defined
    postProcessQueryBlock();

    if (!(this.getParent() instanceof SubQFilterBlock)) {
      // current' is top query block, add transformedNode to origin tree
      // top queryBlock shall not have any correlated filter blocks.
      assert (!whereCFlag && !havingCFlag);
      if (this.getTransformedNode() != null) {
        if (this.getTransformedNode().getType() == PantheraParser_PLSQLParser.SUBQUERY && this.getTransformedNode().getChildCount() == 1) {
          this.setTransformedNode((CommonTree) this.getTransformedNode().getChild(0));
        }
        CommonTree subQueryNode = (CommonTree) this.getASTNode().getParent();// PantheraParser_PLSQLParser.SUBQUERY
        if (this.getTransformedNode().getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT || subQueryNode.getChildCount() > 1) {
          subQueryNode.replaceChildren(this.getASTNode().childIndex, this.getASTNode().childIndex, this.getTransformedNode());
        } else {
          // the transformed node is not SELECT and subQueryNode only has one child
          // FIXME: if subQueryNode is UNION, might cause problem.
          CommonTree nil = new CommonTree();
          nil.addChildren(this.getTransformedNode().getChildren());
          subQueryNode.replaceChildren(0, 0, nil);
        }
      }
    } else { // not top QueryBlock, is a sub Query
      // transfer subQ into JOIN
      // TODO rebuild selectList here
      execute(fbContext, context);
    }

    fbContext.getSelectStack().pop();
    fbContext.getSelectStackForTransfer().pop();
    fbContext.getQueryStack().pop();

    // limit
    if (limit != null && this.getTransformedNode() != null) {
      // what if transformed node is SUBQUERY ?
      this.getTransformedNode().addChild(limit);
    }

    replaceOrder();
  }

  private void replaceOrder() {
    if (this.getTransformedNode() == null) {
      return;
    }
    CommonTree subQuery = (CommonTree) this.getASTNode().getParent();
    if (subQuery == null) {
      return;
    }
    CommonTree statement = (CommonTree) subQuery.getParent();
    if (statement.getType() != PantheraParser_PLSQLParser.SELECT_STATEMENT) {
      return;
    }
    CommonTree oldOrder = (CommonTree) statement.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_ORDER);
    if (oldOrder != null) {
      statement.replaceChildren(oldOrder.getChildIndex(), oldOrder.getChildIndex(), order);
    }
  }

  /**
   * recover user defined column alias
   */
  private void postProcessQueryBlock() {
    CommonTree transformedNode = this.getTransformedNode();
    CommonTree selectList = (CommonTree) this.getASTNode().getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    if (transformedNode == null) {
      return;
    }
    if (selectList == null) {
      return;
    }
    postProcessQB(transformedNode, selectList);
  }

  /**
   * process transformed select list corresponding the current QueryBlock. <br>
   * Process each top SELECT. Top SELECT might be in format of UNION.
   *
   * @param transformed
   * @param selectList
   */
  private void postProcessQB(CommonTree transformed, CommonTree selectList) {
    if (transformed.getType() == PantheraParser_PLSQLParser.SUBQUERY) {
      for (int i = 0; i < transformed.getChildCount(); i++) {
        postProcessQB((CommonTree) transformed.getChild(i), selectList);
      }
    } else if (transformed.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_UNION) {
      postProcessQB((CommonTree) transformed.getChild(0), selectList);
    } else if (transformed.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT) {
      postProcessQueryBlock(transformed, selectList);
    }
  }

  /**
   * recover user defined alias <br>
   * <li>if transformed select item has alias, replace it with user defined alias stored in preDefinedAliasList,
   * <li>if not, add user defined alias
   *
   * @param transformed
   * @param selectList
   */
  private void postProcessQueryBlock(CommonTree transformed, CommonTree selectList) {
    CommonTree recoverList = selectList;
    recoverList = (CommonTree) transformed.getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    for (int i = 0; i < this.preDefinedAliasList.size(); i++) {
      CommonTree recoverItem = (CommonTree) recoverList.getChild(i);
      CommonTree recoverAlias = this.preDefinedAliasList.get(i);
      if (recoverAlias != null) {
        if (recoverItem.getChildCount() == 2) {
          recoverItem.replaceChildren(1, 1, recoverAlias);
        } else {
          recoverItem.addChild(recoverAlias);
        }
      }
    }
  }

  /*
   * store select list columns' alias in proDefineAliasList.
   * if column has alias, then store alias node, if not store null.
   */
  private void preProcessQueryBlock() {
    CommonTree selectList = (CommonTree) this.getASTNode().getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    if (selectList == null) {
      return;
    }
    for (int i = 0; i < selectList.getChildCount(); i++) {
      CommonTree selectItem = (CommonTree) selectList.getChild(i);
      CommonTree selectAlias = null;
      if (selectItem.getChildCount() == 2) {
        selectAlias = (CommonTree) selectItem.deleteChild(1);
      }
      this.preDefinedAliasList.add(selectAlias);
    }
  }

  public void init() {
    recordGroupOrder();
    buildTableNameSet();
    buildQueryForTransfer();
  }

  void recordGroupOrder() {
    group = (CommonTree) this.getASTNode().getFirstChildWithType(
        PantheraParser_PLSQLParser.SQL92_RESERVED_GROUP);
    order = FilterBlockUtil.cloneTree((CommonTree) ((CommonTree) this.getASTNode().getParent().getParent())
        .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_ORDER));
    limit = (CommonTree) this.getASTNode().getFirstChildWithType(PantheraExpParser.LIMIT_VK);
  }


  void buildQueryForTransfer() {
    CommonTree root = this.getASTNode();
    CommonTree cloneRoot = FilterBlockUtil.dupNode(root);
    for (int i = 0; i < root.getChildCount(); i++) {
      CommonTree child = (CommonTree) root.getChild(i);
      int type = child.getType();
      if (type == PantheraParser_PLSQLParser.SQL92_RESERVED_FROM
          || type == PantheraParser_PLSQLParser.SELECT_LIST
          || type == PantheraParser_PLSQLParser.ASTERISK) {
        CommonTree clone = FilterBlockUtil.dupNode(child);
        cloneRoot.addChild(clone);
        FilterBlockUtil.cloneTree(clone, child);
      }
    }
    aggregationList = FilterBlockUtil.filterAggregation((CommonTree) cloneRoot
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST), countAsterisk, order);

    queryForTransfer = cloneRoot;
  }

  /**
   * clone QueryBlock's simple query without where, group...
   *
   * @return
   */
  public CommonTree cloneSimpleQuery() {
    return FilterBlockUtil.cloneTree(queryForTransfer);
  }


  public CommonTree cloneTransformedQuery() {
    if (this.hadRebuildQueryForTransfer) {
      return FilterBlockUtil.cloneTree(queryForTransfer);
    }
    return FilterBlockUtil.cloneTree(this.getASTNode());
  }

  /**
   * clone QueryBlock's query tree and remove having clause
   *
   * @return
   */
  public CommonTree cloneWholeQueryWithoutHaving() {
    CommonTree tree = FilterBlockUtil.cloneTree(this.getASTNode());
    if (this.hadRebuildQueryForTransfer) {
      tree = FilterBlockUtil.cloneTree(queryForTransfer);
    }
    // clone tree including group branch if there is GROUP but excluding HAVING branch
    CommonTree group = (CommonTree) tree.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_GROUP);
    if (group != null) {
      CommonTree token = (CommonTree) group.getChild(group.getChildCount() - 1);
      if (token.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_HAVING && this.isHaving == false) {
        group.deleteChild(token.childIndex);
      }
    }
    return tree;
  }

  /**
   * 1.collect all the correlated filters if there exists for current Query Block.
   * 2.Triggered to process subQuery into join. Which will call FilterBlockProcessor classes.
   *
   * @param fbContext
   * @param context
   * @throws SqlXlateException
   */
  void execute(FilterBlockContext fbContext, TranslateContext context) throws SqlXlateException {
    FilterBlock whereCFB = new CorrelatedFilterBlock();
    FilterBlock havingCFB = new CorrelatedFilterBlock();
    for (FilterBlock oneCFB : whereCFBList) {
      if (whereCFB.getASTNode() == null) {
        whereCFB.setASTNode(oneCFB.getASTNode());
      } else {
        // collect all correlated filters for in WHERE clause in current queryBlock.
        CommonTree and = FilterBlockUtil.createSqlASTNode(whereCFB.getASTNode(), PantheraParser_PLSQLParser.SQL92_RESERVED_AND, "and");
        and.addChild(whereCFB.getASTNode());
        and.addChild(oneCFB.getASTNode());
        whereCFB.setASTNode(and);
      }
    }
    for (FilterBlock oneCFB : havingCFBList) {
      if (havingCFB.getASTNode() == null) {
        havingCFB.setASTNode(oneCFB.getASTNode());
      } else {
        // collect all correlated filters for in HAVING clause in current queryBlock.
        CommonTree and = FilterBlockUtil.createSqlASTNode(havingCFB.getASTNode(), PantheraParser_PLSQLParser.SQL92_RESERVED_AND, "and");
        and.addChild(havingCFB.getASTNode());
        and.addChild(oneCFB.getASTNode());
        havingCFB.setASTNode(and);
      }
    }
    if (this.getASTNode().getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_GROUP) == null) {
      // process subQ in WHERE clause
      if (!whereCFlag) {
        FilterBlockProcessorFactory.getUnCorrelatedProcessor(
            fbContext.getSubQStack().peek().getASTNode()).process(fbContext, this, context);
      } else {
        FilterBlockProcessorFactory.getCorrelatedProcessor(
            fbContext.getSubQStack().peek().getASTNode()).process(fbContext, whereCFB, context);
        this.setTransformed();
        this.setTransformedNode(whereCFB.getTransformedNode());
      }
    } else {
      // process subQ in HAVING clause
      if (!whereCFlag) {
        if (!havingCFlag) {
          FilterBlockProcessorFactory.getUnCorrelatedProcessor(
              fbContext.getSubQStack().peek().getASTNode()).process(fbContext, this, context);
        } else {
          FilterBlockProcessorFactory.getCorrelatedProcessor(
              fbContext.getSubQStack().peek().getASTNode()).process(fbContext, havingCFB, context);
          this.setTransformed();
          this.setTransformedNode(havingCFB.getTransformedNode());
        }
      } else {
        // where contains correlated , and there is a GROUP node.
        throw new SqlXlateException(this.getASTNode(), "Panthera not support GROUP when there is correlated conditions in WHERE-clause currently.");
      }
    }
    return;
  }

  void buildTableNameSet() {
    tableNameSet = new HashSet<String>();
  }

  public Set<String> getTableNameSet() {
    return tableNameSet;
  }

  public CountAsterisk getCountAsterisk() {
    return countAsterisk;
  }

  public void setAggregationList(List<CommonTree> aggregationList) {
    this.aggregationList = aggregationList;
  }

  public List<CommonTree> getAggregationList() {
    return aggregationList;
  }

  public CommonTree getGroup() {
    return group;
  }

  public void setGroup(CommonTree group) {
    this.group = group;
  }

  public CommonTree getOrder() {
    return order;
  }

  /**
   * record count(*) in query<br>
   * CountAsterisk.
   *
   */
  public class CountAsterisk {
    private int position;
    private CommonTree selectItem;
    // only one item count(*) in SELECT_LIST
    boolean isOnlyAsterisk = false;

    public int getPosition() {
      return position;
    }

    public void setPosition(int position) {
      this.position = position;
    }

    public CommonTree getSelectItem() {
      return selectItem;
    }

    public void setSelectItem(CommonTree selectItem) {
      this.selectItem = selectItem;
    }

    public boolean isOnlyAsterisk() {
      return isOnlyAsterisk;
    }

    public void setOnlyAsterisk(boolean isOnlyAsterisk) {
      this.isOnlyAsterisk = isOnlyAsterisk;
    }

  }

  public void setQueryForTransfer(CommonTree queryForTransfer) {
    this.queryForTransfer = queryForTransfer;
  }

  public boolean isProcessHaving() {
    return this.isHaving;
  }

  public void setHaving(boolean isHaving) {
    this.isHaving = isHaving;
  }

  public void setRebuildQueryForTransfer() {
    this.hadRebuildQueryForTransfer = true;
  }

  public void unSetRebuildQueryForTransfer() {
    this.hadRebuildQueryForTransfer = false;
  }

  public boolean getRebuildQueryForTransfer() {
    return this.hadRebuildQueryForTransfer;
  }

  public void setWhereCFlag() {
    whereCFlag = true;
  }

  public void setHavingCFlag() {
    havingCFlag = true;
  }

  public boolean getWhereCFlag() {
    return whereCFlag;
  }

  public boolean getHavingCFlag() {
    return havingCFlag;
  }

  public void setHavingCFB(FilterBlock le) {
    havingCFBList.add(le);
  }

  public void setWhereCFB(FilterBlock le) {
    whereCFBList.add(le);
  }

  public List<CommonTree> getWhereFilterColumns() {
    return whereFilterColumns;
  }

  public List<CommonTree> getHavingFilterColumns() {
    return havingFilterColumns;
  }

  public void setHavingFilterColumns(List<CommonTree> hfc) {
    this.havingFilterColumns = hfc;
  }

  public void setQueryForHaving(CommonTree tree) {
    this.originalQueryForHaving = tree;

  }

  public CommonTree getQueryForHaving() {
    return this.originalQueryForHaving;
  }
}
