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

import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.PantheraExpParser;
import org.apache.hadoop.hive.ql.parse.sql.PantheraMap;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;
import org.apache.hadoop.hive.ql.parse.sql.transformer.QueryInfo;

/**
 * Create filter block by built-in builder.<br>
 * Build the initial FilterBlock Tree for each QInfo<br>
 * Traverse the AST and FB tree is built as a result<br>
 * Tree Traversal: <li>Start Node: the Query root Node of this QInfo <li>Order: Depth-first (from
 * bottom to top) <li>Pruning: For each Query, traverse only the WHERE/HAVING and SELECT_LIST
 * subtree and skip other branches Node processing <li>If current node is logical Op<br>
 * Return none ¨C If none FB is associated to either children and neither child is correlated
 * filter.<br>
 * Return LogicalOpFB ¨C If either of the child is a correlated normal filter. Or any of the child
 * is associate with another FB. <li>If current node is Query Top Node (SELECT or TOK_QUERY)<br>
 * Return QueryBlockFB ¨C Create a new QueryBlockFB and append the FB trees returned from its child
 * where/having and selectexpr. <li>If current node is a operator (/IN/EXISTS) with a subquery as
 * operand<br>
 * Return SubQFB ¨C Create a new SubQFB and append the QueryBlockFB returned from its child. <li>
 * Others<br>
 * Returns either none, or what is returned from its children (e.g. SUBQUERY)<br>
 * Optimize/Adjust Filter Block Tree <li>NOT Digestion: Digest not operator using logic rules (e.g.
 * not (A and B) ? not A or not B ? A' or B' (A', B' are modified filters)) until all the NOT
 * operators are eliminated from the tree <li>LOGIC Fixing: Fix Logic Op according to semantics of
 * SubQFB <li>AND Optimization: Some of the AND operators can be transformed to chaining conditions
 * instead of heavy INTERSECT operation. We mark such AND operator for later processing.<br>
 * Pattern1: SubQFB "AND" Uncorrelated NormalFilter<br>
 *
 * FilterBlockFactory.
 *
 */
public abstract class FilterBlockFactory {

  // Panthera type
  static final int EQUALS = 1;
  static final int NOT_EQUAL = 2;
  static final int GREATER_THAN = 3;
  static final int LESS_THAN = 4;
  static final int LESS_THAN_OR_EQUALS = 5;
  static final int GREATER_THAN_OR_EQUALS = 6;
  static final int NOT_IN = 7;
  static final int IN = 8;
  static final int NOT_LIKE = 9;
  static final int LIKE = 10;
  static final int AND = 11;
  static final int OR = 12;
  static final int NOT = 13;
  static final int SELECT = 14;
  static final int EXISTS = 15;
  static final int SELECT_LIST = 16;
  static final int WHERE = 17;
  static final int HAVING = 18;
  static final int NOT_BETWEEN = 19;
  static final int BETWEEN = 20;
  static final int IS_NULL = 21;
  static final int IS_NOT_NULL = 22;



  // sql type to panthera type, must be initialized by child class
  Map<Integer, Integer> typeMap = new PantheraMap<Integer>();
  // panthera type to FilterBlock
  Map<Integer, FilterBlockBuilder> fbMap = new PantheraMap<FilterBlockBuilder>(50);

  FilterBlockFactory() {
    fbMap.put(EQUALS, new OpBuilder());
    fbMap.put(NOT_EQUAL, new OpBuilder());
    fbMap.put(GREATER_THAN, new OpBuilder());
    fbMap.put(LESS_THAN, new OpBuilder());
    fbMap.put(LESS_THAN_OR_EQUALS, new OpBuilder());
    fbMap.put(GREATER_THAN_OR_EQUALS, new OpBuilder());
    fbMap.put(NOT_IN, new OpBuilder());
    fbMap.put(IN, new OpBuilder());
    fbMap.put(NOT_BETWEEN, new OpBuilder());
    fbMap.put(BETWEEN, new OpBuilder());
    fbMap.put(NOT_LIKE, new OpBuilder());
    fbMap.put(LIKE, new OpBuilder());
    fbMap.put(AND, new AndBuilder());
    fbMap.put(OR, new OrBuilder());
    fbMap.put(SELECT, new QueryBlockBuilder());
    fbMap.put(EXISTS, new OpBuilder());
    fbMap.put(SELECT_LIST, new SelectExprFilterBlockBuilder());
    fbMap.put(WHERE, new WhereFilterBlockBuilder());
    fbMap.put(HAVING, new HavingFilterBlockBuilder());
    fbMap.put(IS_NULL, new OpBuilder());
    fbMap.put(IS_NOT_NULL, new OpBuilder());
    fbMap.put(NOT, new NotBuilder());
  }

  /**
   *
   * @param qFNode
   * @param selectStack
   * @param branch
   * @return true: correlated, false: uncorrelated
   * @throws SqlXlateException
   */
  abstract int isCorrelated(QueryInfo qFNode, Stack<CommonTree> selectStack, CommonTree branch)
      throws SqlXlateException;

  public FilterBlock getFilterBlock(QueryInfo qFNode, Stack<CommonTree> selectStack,
      CommonTree node, List<FilterBlock> fbl) throws SqlXlateException {
    if (typeMap.get(node.getType()) == null) {
      if (fbl.size() == 1) {
        return fbl.get(0);
      }
      if (fbl.size() > 1) {
        if (node.getType() == PantheraExpParser.SUBQUERY) {
          FilterBlock fb = new SubQFilterBlock();
          for (FilterBlock ifb : fbl) {
            fb.addChild(ifb);
          }
          return fb;
        }
        throw new SqlXlateException(node, "Error to prepare filter block: " + node.getType()
            + node.getText());
      }
      return null;
    }
    return fbMap.get(typeMap.get(node.getType())).build(qFNode, selectStack, node, fbl);
  }

  FilterBlock buildLogicOp(CommonTree node, List<FilterBlock> fbl, int type)
      throws SqlXlateException {
    if (fbl == null || fbl.size() != 2) {
      throw new SqlXlateException(node, "Error to prepare filter block: " + node.getType()
          + node.getText());
    }
    FilterBlock fb;
    // merge normalFilterBlock
    if (fbl != null && fbl.size() == 2 && fbl.get(0) instanceof NormalFilterBlock
        && fbl.get(1) instanceof NormalFilterBlock) {
      // both UnCorrelatedFilterBlock
      if (fbl.get(0) instanceof UnCorrelatedFilterBlock
          && fbl.get(1) instanceof UnCorrelatedFilterBlock) {
        fb = new UnCorrelatedFilterBlock();
      } else { // have CorrelatedFilterBlock
        fb = new CorrelatedFilterBlock();
      }
    } else {
      switch (type) {
      case AND:
        fb = new AndFilterBlock();
        break;
      case OR:
        fb = new OrFilterBlock();
        break;
      default:
        throw new SqlXlateException(node, "Error to prepare filter block: " + node.getType()
            + node.getText());
      }

      fb.addAllChildren(fbl);
    }
    fb.setASTNode(node);
    return fb;
  }

  FilterBlock processFilterBlock(Stack<CommonTree> selectStack, CommonTree node,
      List<FilterBlock> fbl, FilterBlock fb) {
    fb.addAllChildren(fbl);
    fb.setASTNode(node);
    if (fb.getChildren() == null || fb.getChildren().size() == 0) {
      return null;
    }
    return fb;
  }

  public interface FilterBlockBuilder {
    FilterBlock build(QueryInfo qFNode, Stack<CommonTree> selectStack, CommonTree node,
        List<FilterBlock> fbl) throws SqlXlateException;
  }

  public class OpBuilder implements FilterBlockBuilder {

    @Override
    public FilterBlock build(QueryInfo qFNode, Stack<CommonTree> selectStack, CommonTree node,
        List<FilterBlock> fbl) throws SqlXlateException {
      FilterBlock fb;
      if (fbl.size() == 1) {
        if (fbl.get(0) instanceof SubQFilterBlock) {
          fb = fbl.get(0);
          fb.setASTNode(node);
          return fb;
        }
        if (fbl.get(0) instanceof QueryBlock) {
          fb = new SubQFilterBlock();
          fb.addChild(fbl.get(0));
          fb.setASTNode(node);
          return fb;
        }
        if (fbl.get(0) instanceof UnCorrelatedFilterBlock) {
          // expression > constant
          // TODO expression > correlated column
          fb = new UnCorrelatedFilterBlock();
          fb.setASTNode(node);
          return fb;
        }
      }
      if (fbl.size() > 0) {
        throw new SqlXlateException(null, "Error to prepare filter block: " + node.getType()
            + node.getText());
      }
      int mcorrelated = 0;
      for (int i = 0; i < node.getChildCount(); i++) {
        int correlated = isCorrelated(qFNode, selectStack, (CommonTree) node.getChild(i));
        if (correlated > mcorrelated) {
          mcorrelated = correlated;
        }
      }
      if (mcorrelated > 0) {
        fb = new CorrelatedFilterBlock();
      } else {
        fb = new UnCorrelatedFilterBlock();
      }
      fb.setASTNode(node);
      return fb;
    }

  }

  public class AndBuilder implements FilterBlockBuilder {

    @Override
    public FilterBlock build(QueryInfo qFNode, Stack<CommonTree> selectStack, CommonTree node,
        List<FilterBlock> fbl) throws SqlXlateException {
      return buildLogicOp(node, fbl, AND);
    }
  }

  public class OrBuilder implements FilterBlockBuilder {

    @Override
    public FilterBlock build(QueryInfo qFNode, Stack<CommonTree> selectStack, CommonTree node,
        List<FilterBlock> fbl) throws SqlXlateException {
      return buildLogicOp(node, fbl, OR);
    }

  }

  public class QueryBlockBuilder implements FilterBlockBuilder {

    @Override
    public FilterBlock build(QueryInfo qFNode, Stack<CommonTree> selectStack, CommonTree node,
        List<FilterBlock> fbl) throws SqlXlateException {
      QueryBlock fb = new QueryBlock();
      processFilterBlock(selectStack, node, fbl, fb);
      fb.init();
      return fb;
    }

  }

  public class SelectExprFilterBlockBuilder implements FilterBlockBuilder {

    @Override
    public FilterBlock build(QueryInfo qFNode, Stack<CommonTree> selectStack, CommonTree node,
        List<FilterBlock> fbl) throws SqlXlateException {
      FilterBlock fb = new SelectExprFilterBlock();
      return processFilterBlock(selectStack, node, fbl, fb);
    }
  }

  public class WhereFilterBlockBuilder implements FilterBlockBuilder {

    @Override
    public FilterBlock build(QueryInfo qFNode, Stack<CommonTree> selectStack, CommonTree node,
        List<FilterBlock> fbl) throws SqlXlateException {
      FilterBlock fb = new WhereFilterBlock();
      return processFilterBlock(selectStack, node, fbl, fb);
    }

  }

  public class HavingFilterBlockBuilder implements FilterBlockBuilder {

    @Override
    public FilterBlock build(QueryInfo qFNode, Stack<CommonTree> selectStack, CommonTree node,
        List<FilterBlock> fbl) throws SqlXlateException {
      FilterBlock fb = new HavingFilterBlock();
      return processFilterBlock(selectStack, node, fbl, fb);
    }

  }

  /**
   * transform NOT logic expression with remove NOT node <br>
   * TODO: should be create LogicExpressTransformer for all logic optimization & transformer
   * FIXME: It's against with design which FilterBlockFactory(and FilterBlock) is unrelated with input AST tree
   */
  public class NotBuilder implements FilterBlockBuilder {
    @Override
    public FilterBlock build(QueryInfo node, Stack<CommonTree> selectStack, CommonTree node2,
        List<FilterBlock> fbl) throws SqlXlateException {
      if (fbl.isEmpty() || fbl.size() > 1) {
        throw new SqlXlateException(null, "Error to prepare filter block:error NOT logic ");
      }
      FilterBlock fb = fbl.get(0);
      CommonTree astNode = (CommonTree) fb.getASTNode();
      // simple NOT EXISTS, do nothing
      if (astNode.getType() == PantheraExpParser.SQL92_RESERVED_EXISTS
          && node2.equals(astNode.getParent())) {
        return fb;
      }
      buildNot(astNode);

      //remove NOT node
      CommonTree child = (CommonTree) node2.getChild(0);
      CommonTree parent = (CommonTree) node2.getParent();
      int position = node2.childIndex;
      parent.deleteChild(position);
      SqlXlateUtil.addCommonTreeChild(parent, position, child);

      return fb;
    }

    public void buildNot(CommonTree astNode) throws SqlXlateException {
      switch (astNode.getType()) {

      // such as NOT(... OR (NOT) EXISTS ...)
      case PantheraExpParser.SQL92_RESERVED_EXISTS:
        CommonTree parent = (CommonTree) astNode.getParent();
        // NOT EXISTS -> EXISTS
        if (parent.getType() == PantheraExpParser.SQL92_RESERVED_NOT) {
          CommonTree grandpa = (CommonTree) parent.getParent();
          int position = parent.childIndex;
          grandpa.deleteChild(position);
          SqlXlateUtil.addCommonTreeChild(grandpa, position, astNode);
        }
        // EXIST -> NOT EXISTS
        else {
          CommonTree notNode = FilterBlockUtil.createSqlASTNode(
              astNode, PantheraExpParser.SQL92_RESERVED_NOT, "not");
          int position = astNode.childIndex;
          parent.deleteChild(position);
          notNode.addChild(astNode);
          SqlXlateUtil.addCommonTreeChild(parent, position, notNode);
        }
        break;
      case PantheraExpParser.SQL92_RESERVED_IN:
        astNode.getToken().setType(PantheraExpParser.NOT_IN);
        astNode.getToken().setText("NOT_IN");
        break;
      case PantheraExpParser.NOT_IN:
        astNode.getToken().setType(PantheraExpParser.SQL92_RESERVED_IN);
        astNode.getToken().setText("in");
        break;
      case PantheraExpParser.LESS_THAN_OP:
        astNode.getToken().setType(PantheraExpParser.GREATER_THAN_OR_EQUALS_OP);
        astNode.getToken().setText(">=");
        break;
      case PantheraExpParser.LESS_THAN_OR_EQUALS_OP:
        astNode.getToken().setType(PantheraExpParser.GREATER_THAN_OP);
        astNode.getToken().setText(">");
        break;
      case PantheraExpParser.GREATER_THAN_OP:
        astNode.getToken().setType(PantheraExpParser.LESS_THAN_OR_EQUALS_OP);
        astNode.getToken().setText("<=");
        break;
      case PantheraExpParser.GREATER_THAN_OR_EQUALS_OP:
        astNode.getToken().setType(PantheraExpParser.LESS_THAN_OP);
        astNode.getToken().setText("<");
        break;
      case PantheraExpParser.NOT_EQUAL_OP:
        astNode.getToken().setType(PantheraExpParser.EQUALS_OP);
        astNode.getToken().setText("=");
        break;
      case PantheraExpParser.EQUALS_OP:
        astNode.getToken().setType(PantheraExpParser.NOT_EQUAL_OP);
        astNode.getToken().setText("<>");
        break;
      case PantheraExpParser.SQL92_RESERVED_OR:
        buildChild(astNode);
        astNode.getToken().setType(PantheraExpParser.SQL92_RESERVED_AND);
        astNode.getToken().setText("and");
        break;
      case PantheraExpParser.SQL92_RESERVED_AND:
        buildChild(astNode);
        astNode.getToken().setType(PantheraExpParser.SQL92_RESERVED_OR);
        astNode.getToken().setText("or");
        break;
      case PantheraExpParser.SQL92_RESERVED_LIKE:
        astNode.getToken().setType(PantheraExpParser.NOT_LIKE);
        astNode.getToken().setText("NOT_LIKE");
        break;
      case PantheraExpParser.SQL92_RESERVED_BETWEEN:
        astNode.getToken().setType(PantheraExpParser.NOT_BETWEEN);
        astNode.getToken().setText("NOT_BETWEEN");
        break;
      case PantheraExpParser.IS_NULL:
        astNode.getToken().setType(PantheraExpParser.IS_NOT_NULL);
        astNode.getToken().setText("IS_NOT_NULL");
        break;
      case PantheraExpParser.IS_NOT_NULL:
        astNode.getToken().setType(PantheraExpParser.IS_NULL);
        astNode.getToken().setText("IS_NULL");
        break;
      default:
        throw new SqlXlateException(astNode, "Unsupported logic express in NOT:" + astNode.getText());
      }

    }

    private void buildChild(CommonTree node) throws SqlXlateException {
      for(int i=0;i<node.getChildCount();i++){
        CommonTree astNode = (CommonTree)node.getChild(i);
        // for NOT EXISTS
        if(astNode.getType()==PantheraExpParser.SQL92_RESERVED_NOT){
          astNode = (CommonTree)astNode.getChild(0);
        }
        buildNot(astNode);
      }
    }
  }


}
