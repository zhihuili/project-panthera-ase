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
 * Join may have three child, first cross|left|right|..., second table, third on ...<br>
 * JoinTextGenerator.
 *
 */
public class JoinTextGenerator extends BaseTextGenerator {

  @Override
  protected String textGenerate(CommonTree root, TranslateContext context) throws Exception {
    String retString = "";
    if (root.getFirstChildWithType(PantheraParser_PLSQLParser.TABLE_REF_ELEMENT) == null) {
      throw new SqlParseException("No table for join!");
    }
    if (root.getChild(0).getType() == PantheraParser_PLSQLParser.TABLE_REF_ELEMENT) {
      //no cross|left|right|...
      CommonTree te = (CommonTree) root.getChild(0);
      QueryTextGenerator tg1 = TextGeneratorFactory.getTextGenerator(te);
      retString += " join " + tg1.textGenerateQuery(te, context);
    } else {
      if (!(root.getChild(0) instanceof org.antlr.runtime.tree.CommonTree)) {
        throw new SqlParseException("illegal sql AST node:" + root.getChild(0));
      }
      CommonTree modi = (CommonTree) root.getChild(0);
      QueryTextGenerator tg0 = TextGeneratorFactory.getTextGenerator(modi);
      retString += tg0.textGenerateQuery(modi, context)+" join ";
      CommonTree te = (CommonTree) root.getChild(1);
      QueryTextGenerator tg1 = TextGeneratorFactory.getTextGenerator(te);
      retString += tg1.textGenerateQuery(te, context);
    }
    if (root.getChild(root.getChildCount()-1).getType() == PantheraParser_PLSQLParser.TABLE_REF_ELEMENT) {
      return retString ;
    } else {
      //on clause
      if (!(root.getChild(root.getChildCount()-1) instanceof org.antlr.runtime.tree.CommonTree)) {
        throw new SqlParseException("illegal sql AST node:" + root.getChild(root.getChildCount()-1));
      }
      CommonTree on = (CommonTree) root.getChild(root.getChildCount()-1);
      QueryTextGenerator tg2 = TextGeneratorFactory.getTextGenerator(on);
      return retString + " " + tg2.textGenerateQuery(on, context);
    }
  }

}
