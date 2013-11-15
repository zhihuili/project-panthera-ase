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


public abstract class BaseFilterBlock implements FilterBlock {
  private CommonTree astNode;
  private List<FilterBlock> children = new ArrayList<FilterBlock>();
  private CommonTree transformedNode;
  private FilterBlock parent;
  private boolean hasTransformed = false;//for SubQFilterBlock

  /**
   * TODO evil logic ?
   */
  public void setTransformed() {
    this.hasTransformed = true;
  }

  public boolean hasTransformed() {
    return this.hasTransformed;
  }

  public void setTransformedNode(CommonTree node) {
    this.transformedNode = node;
  }

  public CommonTree getTransformedNode() {
    return this.transformedNode;
  }

  @Override
  public List<FilterBlock> getChildren() {
    return children;
  }

  @Override
  public FilterBlock getParent() {
    return this.parent;
  }

  public void setParent(FilterBlock fb) {
    this.parent = fb;
  }

  public void setASTNode(CommonTree node) {
    this.astNode = node;
  }

  public CommonTree getASTNode() {
    return astNode;
  }

  public void addChild(FilterBlock fb) {
    if (fb != null) {
      children.add(fb);
      fb.setParent(this);
    }
  }

  public void addAllChildren(List<FilterBlock> fbl) {
    this.children = fbl;
    if (fbl != null) {
      for (FilterBlock fb : fbl) {
        fb.setParent(this);
      }
    }
  }

  void processChildren(FilterBlockContext fbContext, TranslateContext context)
      throws SqlXlateException {
    for (FilterBlock fb : this.getChildren()) {
      fb.process(fbContext, context);
      this.setTransformedNode(fb.getTransformedNode());
    }
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "_" + astNode.getText();
  }

  /**
   * Print the entire filter block tree starting from this FB
   *
   * @return The block tree string
   */
  public String toStringTree() {
    if (children == null || children.size() == 0) {
      return this.toString();
    }
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    sb.append(this.toString());
    sb.append(" ");
    for (int i = 0; i < children.size(); i++) {
      sb.append(children.get(i).toStringTree());
      sb.append(" ");
    }
    sb.append("]");
    return sb.toString();
  }

  void prepareChildren(FilterBlockContext fbContext, TranslateContext context, Stack<CommonTree> selectStack)
      throws SqlXlateException {
    for (FilterBlock fb : this.getChildren()) {
      fb.prepare(fbContext, context, selectStack);
    }
  }

  public void prepare(FilterBlockContext fbContext, TranslateContext context, Stack<CommonTree> selectStack)
      throws SqlXlateException {
    prepareChildren(fbContext, context, selectStack);
  }

}
