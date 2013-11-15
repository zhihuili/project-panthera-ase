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
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;

public class SimpleCaseTextGenerator extends BaseTextGenerator {

  @Override
  protected String textGenerate(CommonTree root, TranslateContext context) throws Exception {
    String retString = "(case ";
    if (!(root.getChildCount()>=3)) {
      throw new SqlParseException("Parameters error in simple case expression.");
    }
    if (!(root.getChild(0) instanceof org.antlr.runtime.tree.CommonTree)) {
      throw new SqlParseException("illegal sql AST node:" + root.getChild(0));
    }
    CommonTree cond = (CommonTree)root.getChild(0);
    QueryTextGenerator qtgcond = TextGeneratorFactory.getTextGenerator(cond);
    for (int i = 1; i < root.getChildCount();i++) {
      if (!(root.getChild(i) instanceof org.antlr.runtime.tree.CommonTree)) {
        throw new SqlParseException("illegal sql AST node:" + root.getChild(i));
      }
      CommonTree node = (CommonTree) root.getChild(i);
      if (node.getChildCount()==1) {
        //else clause
        QueryTextGenerator generator = TextGeneratorFactory.getTextGenerator(node);
        if (generator == null) {
          throw new SqlParseException("illegal sql AST node:" + root.getChild(i));
        }
        retString += generator.textGenerateQuery(node, context) + " end)";
      } else {
        //when-then clause
        retString += "when " ;
        retString += qtgcond.textGenerateQuery(cond, context) + " = ";

        QueryTextGenerator generatorR = TextGeneratorFactory.getTextGenerator((CommonTree)node.getChild(0));
        if (generatorR == null) {
          throw new SqlParseException("illegal sql AST node:" + node.getChild(0));
        }
        retString += generatorR.textGenerateQuery((CommonTree)node.getChild(0), context);
        retString += " then ";
        QueryTextGenerator resultgen = TextGeneratorFactory.getTextGenerator((CommonTree)node.getChild(1));
        if (resultgen == null) {
          throw new SqlParseException("illegal sql AST node:" + node.getChild(1));
        }
        retString += resultgen.textGenerateQuery((CommonTree)node.getChild(1), context);
        retString += " " ;
      }
    }
    return retString;
  }

}
