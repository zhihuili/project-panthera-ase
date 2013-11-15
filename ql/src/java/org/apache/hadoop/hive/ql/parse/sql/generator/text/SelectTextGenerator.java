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

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * Deal with select.
 * SelectTextGenerator.
 *
 */
public class SelectTextGenerator extends BaseTextGenerator {

  @Override
  protected String textGenerate(CommonTree root, TranslateContext context) throws Exception {
    String retString = root.getText();
    //select list in child 1;
    //sometimes child 1 is distinct..
    if (root.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_DISTINCT) == null) {
      if (root.getChildCount() < 2) {
        throw new SqlParseException("select parameter number less than 2.");
      }
      if (!(root.getChild(1) instanceof org.antlr.runtime.tree.CommonTree)) {
        throw new SqlParseException("illegal sql AST node:" + root.getChild(1));
      }
      CommonTree selectlist = (CommonTree)root.getChild(1);
      QueryTextGenerator listTG = TextGeneratorFactory.getTextGenerator(selectlist);
      retString += " " + listTG.textGenerateQuery(selectlist, context);
      //from clause in child 0;
      if (!(root.getChild(0) instanceof org.antlr.runtime.tree.CommonTree)) {
        throw new SqlParseException("illegal sql AST node:" + root.getChild(0));
      }
      CommonTree from = (CommonTree)root.getChild(0);
      QueryTextGenerator fromTG = TextGeneratorFactory.getTextGenerator(from);
      retString += " " + fromTG.textGenerateQuery(from, context);
      //other clause
      for (int i = 2; i < root.getChildCount(); i++ ) {
        if (!(root.getChild(i) instanceof org.antlr.runtime.tree.CommonTree)) {
          throw new SqlParseException("illegal sql AST node:" + root.getChild(i));
        }
        CommonTree clause = (CommonTree)root.getChild(i);
        QueryTextGenerator clauseTG = TextGeneratorFactory.getTextGenerator(clause);
        retString += " " + clauseTG.textGenerateQuery(clause, context);
      }
    } else {
      if (root.getChildCount() < 3) {
        throw new SqlParseException("select parameter number less than 3 with \"distinct\".");
      }
      retString += " distinct" ;
      if (!(root.getChild(2) instanceof org.antlr.runtime.tree.CommonTree)) {
        throw new SqlParseException("illegal sql AST node:" + root.getChild(2));
      }
      CommonTree selectlist = (CommonTree)root.getChild(2);
      QueryTextGenerator listTG = TextGeneratorFactory.getTextGenerator(selectlist);
      retString += " " + listTG.textGenerateQuery(selectlist, context);
      //from clause in child 0;
      if (!(root.getChild(0) instanceof org.antlr.runtime.tree.CommonTree)) {
        throw new SqlParseException("illegal sql AST node:" + root.getChild(0));
      }
      CommonTree from = (CommonTree)root.getChild(0);
      QueryTextGenerator fromTG = TextGeneratorFactory.getTextGenerator(from);
      retString += " " + fromTG.textGenerateQuery(from, context);
      //other clause
      for (int i = 3; i < root.getChildCount(); i++ ) {
        if (!(root.getChild(i) instanceof org.antlr.runtime.tree.CommonTree)) {
          throw new SqlParseException("illegal sql AST node:" + root.getChild(i));
        }
        CommonTree clause = (CommonTree)root.getChild(i);
        QueryTextGenerator clauseTG = TextGeneratorFactory.getTextGenerator(clause);
        retString += " " + clauseTG.textGenerateQuery(clause, context);
      }
    }
    return retString ;
  }

}
