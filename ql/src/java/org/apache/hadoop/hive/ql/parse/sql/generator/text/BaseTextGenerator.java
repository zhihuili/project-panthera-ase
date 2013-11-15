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
package org.apache.hadoop.hive.ql.parse.sql.generator.text;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.SqlParseException;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;

/**
 * Base class of QueryTextGenerator.<br>
 * It implement QueryTextGenerator template method and common methods
 *
 * BaseTextGenerator.
 *
 */
public abstract class BaseTextGenerator implements QueryTextGenerator {

  @Override
  public String textGenerateQuery(CommonTree root, TranslateContext context) throws Exception {
    String retString = textGenerate(root, context) ;
    return retString ;
  }

  protected String textGenerateChild(CommonTree root, TranslateContext context) throws Exception {
    return textGenerateChild(root, context, " ") ;
  }

  protected String textGenerateChild(CommonTree root, TranslateContext context, String seperate) throws Exception {
    if(root.getChildCount() == 0) {
      return "";
    } else {
      String retString = "" ;
      for(int i = 0; i < root.getChildCount(); i++){
        if (i > 0) {
          retString += seperate;
        }
        if (!(root.getChild(i) instanceof org.antlr.runtime.tree.CommonTree)) {
          throw new SqlParseException("illegal sql AST node:" + root.getChild(i));
        }
        CommonTree tree = (CommonTree) root.getChild(i);
        QueryTextGenerator qtg = TextGeneratorFactory.getTextGenerator(tree);
        retString += qtg.textGenerateQuery(tree, context);
      }
      return retString ;
    }
  }

  protected String textGenerateChild(CommonTree root, TranslateContext context, int count, String seperate) throws Exception {
    if(root.getChildCount() == 0) {
      return "";
    } else {
      String retString = "" ;
      for(int i = 0; i < count; i++){
        if (i > 0) {
          retString += seperate;
        }
        if (!(root.getChild(i) instanceof org.antlr.runtime.tree.CommonTree)) {
          throw new SqlParseException("illegal sql AST node:" + root.getChild(i));
        }
        CommonTree tree = (CommonTree) root.getChild(i);
        QueryTextGenerator qtg = TextGeneratorFactory.getTextGenerator(tree);
        retString += qtg.textGenerateQuery(tree, context);
      }
      return retString ;
    }
  }

  protected String textGenerateChildReverse(CommonTree root, TranslateContext context) throws Exception {
    return textGenerateChildReverse(root, context, " ") ;
  }

  protected String textGenerateChildReverse(CommonTree root, TranslateContext context, String seperate) throws Exception {
    if(root.getChildCount() == 0) {
      return "";
    } else {
      String retString = "" ;
      for(int i = root.getChildCount()-1; i >= 0 ; i--){
        if (i < root.getChildCount()-1 ) {
          retString += seperate;
        }
        if (!(root.getChild(i) instanceof org.antlr.runtime.tree.CommonTree)) {
          throw new SqlParseException("illegal sql AST node:" + root.getChild(i));
        }
        CommonTree tree = (CommonTree) root.getChild(i);
        QueryTextGenerator qtg = TextGeneratorFactory.getTextGenerator(tree);
        retString += qtg.textGenerateQuery(tree, context);
      }
      return retString ;
    }
  }

  protected String textGenerateChildIndex(CommonTree root, TranslateContext context, int index) throws Exception {
    if(root.getChildCount() == 0) {
      throw new SqlXlateException(root, "not enough children number.");
    } else {
      String retString = "" ;
      if (!(root.getChildCount()>index && root.getChild(index) instanceof org.antlr.runtime.tree.CommonTree)) {
        throw new SqlParseException("illegal sql AST node:" + root.getChild(index));
      }
      CommonTree tree = (CommonTree) root.getChild(index);
      QueryTextGenerator qtg = TextGeneratorFactory.getTextGenerator(tree);
      retString += qtg.textGenerateQuery(tree, context);
      return retString ;
    }
  }

  /*
   * generate text of children from root.getChild(beginIndex) to
   * root.getChild(endIndex-1) from left to right.
   *
   * */
  protected String textGenerateChildRange(CommonTree root, TranslateContext context, int beginIndex, int endIndex) throws Exception {
    if(root.getChildCount() == 0) {
      return "";
    } else {
      String retString = "" ;
      if (root.getChildCount() < endIndex) {
        throw new SqlXlateException(root, "not enough children number.");
      }
      for (int index = beginIndex; index < endIndex; index++) {
        if (index > beginIndex ) {
          retString += " ";
        }
        CommonTree tree = (CommonTree) root.getChild(index);
        QueryTextGenerator qtg = TextGeneratorFactory.getTextGenerator(tree);
        retString += qtg.textGenerateQuery(tree, context);
      }
      return retString ;
    }
  }

  protected abstract String textGenerate(CommonTree root, TranslateContext context) throws Exception ;
}
