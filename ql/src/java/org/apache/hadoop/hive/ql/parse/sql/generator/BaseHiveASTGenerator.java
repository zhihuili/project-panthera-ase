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
package org.apache.hadoop.hive.ql.parse.sql.generator;

import java.util.Iterator;

import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
/**
 * Base class of HiveASTGenerator.<br>
 * It implement generateHiveAST template method
 *
 * BaseHiveASTGenerator.
 *
 */
public abstract class BaseHiveASTGenerator implements HiveASTGenerator {

  private HiveASTGenerator preGenerator;
  private HiveASTGenerator postGeneator;

  boolean generateChildren(ASTNode hiveRoot, CommonTree sqlRoot, ASTNode currentHiveNode,
      CommonTree currentSqlNode, TranslateContext context) throws SqlXlateException {
    if (currentSqlNode.getChildren() == null) {
      return true;
    }
    Iterator i = currentSqlNode.getChildren().iterator();
    if (i == null) {
      return true;
    }
    while (i.hasNext()) {
      Object o = i.next();
      CommonTree node = (CommonTree) o;
      HiveASTGenerator generator = GeneratorFactory.getGenerator(node);
      if (generator == null) {
        throw new SqlXlateException(node, "Untransformed sql AST node:" + node.getText());
      }
      if (!generator.generateHiveAST(hiveRoot, sqlRoot,
          currentHiveNode, node, context)) {
        return false;
      }
    }
    return true;
  }

  /**
   *
   * @param hiveRoot
   * @param sqlRoot
   * @param currentHiveNode
   * @param currentSqlNode
   * @param context
   * @param idx
   *          generate all children except the id
   * @return
   * @throws Exception
   */
  boolean generateChildrenExcept(ASTNode hiveRoot, CommonTree sqlRoot, ASTNode currentHiveNode,
      CommonTree currentSqlNode, TranslateContext context, int idx) throws SqlXlateException {

    for (int i = 0; i < currentSqlNode.getChildCount(); i++) {
      if (i == idx) {
        continue;
      }
      CommonTree node = (CommonTree) currentSqlNode.getChild(i);
      if (!GeneratorFactory.getGenerator(node).generateHiveAST(hiveRoot, sqlRoot,
          currentHiveNode, node, context)) {
        return false;
      }
    }
    return true;
  }

  void attachHiveNode(ASTNode hiveRoot, ASTNode currentHiveNode,
      ASTNode ret) {
    currentHiveNode.addChild(ret);
    if (hiveRoot != null && (hiveRoot.getChildren() == null || hiveRoot.getChildren().size() == 0)) {
      hiveRoot.addChild(currentHiveNode);
    }
  }

  ASTNode buildTmpDestinationNode() {
    ASTNode desNode = SqlXlateUtil.newASTNode(HiveParser.TOK_DESTINATION, "TOK_DESTINATION");
    ASTNode dirNode = SqlXlateUtil.newASTNode(HiveParser.TOK_DIR, "TOK_DIR");
    desNode.addChild(dirNode);
    ASTNode tmpNode = SqlXlateUtil.newASTNode(HiveParser.TOK_TMP_FILE, "TOK_TMP_FILE");
    dirNode.addChild(tmpNode);
    return desNode;

  }

  ASTNode buildAllColRef() {
    ASTNode select = SqlXlateUtil.newASTNode(HiveParser.TOK_SELECT, "TOK_SELECT");
    ASTNode selExpr = SqlXlateUtil.newASTNode(HiveParser.TOK_SELEXPR, "TOK_SELEXPR");
    select.addChild(selExpr);
    ASTNode allColRef = SqlXlateUtil.newASTNode(HiveParser.TOK_ALLCOLREF, "TOK_ALLCOLREF");
    selExpr.addChild(allColRef);
    return select;
  }

  /**
   * exchange left & right branch<br>
   * if only one branch, no effect.
   *
   * @param branch
   */
  void exchangeChildrenPosition(ASTNode branch) {
    Tree left = (Tree) branch.deleteChild(0);
    branch.addChild(left);
  }

  ASTNode newHiveASTNode(int ttype, String text) {
    return SqlXlateUtil.newASTNode(ttype, text);
  }

  boolean baseProcess(int ttype, String text, ASTNode hiveRoot, CommonTree sqlRoot,
      ASTNode currentHiveNode,
      CommonTree currentSqlNode, TranslateContext context) throws SqlXlateException {
    ASTNode ret = this.newHiveASTNode(ttype, text);
    this.attachHiveNode(hiveRoot, currentHiveNode, ret);
    return this.generateChildren(hiveRoot, sqlRoot, ret, currentSqlNode, context);
  }

  @Override
  public boolean generateHiveAST(ASTNode hiveRoot, CommonTree sqlRoot, ASTNode currentHiveNode,
      CommonTree currentSqlNode, TranslateContext context) throws SqlXlateException {
    if (this.preGenerator != null
        && !this.preGenerator.generateHiveAST(hiveRoot, sqlRoot, currentHiveNode, currentSqlNode,
            context)) {
      return false;
    }
    if (!this.generate(hiveRoot, sqlRoot, currentHiveNode, currentSqlNode, context)) {
      return false;
    }
    if (this.postGeneator != null
        && !this.postGeneator.generateHiveAST(hiveRoot, sqlRoot, currentHiveNode, currentSqlNode,
            context)) {
      return false;
    }
    return true;
  }


  abstract public boolean generate(ASTNode hiveRoot, CommonTree sqlRoot, ASTNode currentHiveNode,
      CommonTree currentSqlNode, TranslateContext context) throws SqlXlateException;

  public void setHivePreGenerator(HiveASTGenerator preGenerator) {
    this.preGenerator = preGenerator;
  }

  public void setHivePostGenerator(HiveASTGenerator postGenerator) {
    this.postGeneator = postGenerator;
  }

  boolean nullOrNotGenerator(boolean isNull, ASTNode hiveRoot, CommonTree sqlRoot,
      ASTNode currentHiveNode,
      CommonTree currentSqlNode, TranslateContext context) throws SqlXlateException {
    ASTNode ret = this.newHiveASTNode(HiveParser.TOK_FUNCTION, "TOK_FUNCTION");
    this.attachHiveNode(hiveRoot, currentHiveNode, ret);
    currentHiveNode = ret;
    if (isNull) {
      ret = this.newHiveASTNode(HiveParser.TOK_ISNULL, "TOK_ISNULL");
    } else {
      ret = this.newHiveASTNode(HiveParser.TOK_ISNOTNULL, "TOK_ISNOTNULL");
    }
    this.attachHiveNode(hiveRoot, currentHiveNode, ret);
    return this.generateChildren(hiveRoot, sqlRoot, currentHiveNode, currentSqlNode, context);
  }
}
