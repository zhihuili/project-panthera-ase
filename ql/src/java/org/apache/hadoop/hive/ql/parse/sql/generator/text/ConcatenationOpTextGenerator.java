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
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * generate query for "string" || "string" where "||" represents concatenation operator
 * ConcatenationOpTextGenerator.
 *
 */
public class ConcatenationOpTextGenerator extends BaseTextGenerator {

  @Override
  protected String textGenerate(CommonTree root, TranslateContext context) throws Exception {

    String ret = "concat(";
    ret += textGenerateTreeInOrder(root, context);
    ret += ")";
    return ret;
  }

  private String textGenerateTreeInOrder(CommonTree root, TranslateContext context) throws Exception {
    if (root.getType() == PantheraParser_PLSQLParser.CONCATENATION_OP) {
      assert (root.getChildCount() == 2);
      return textGenerateTreeInOrder((CommonTree) root.getChild(0), context) + ", " + textGenerateTreeInOrder((CommonTree) root.getChild(1), context);
    } else {
      return textGenerateTreeLeaf(root, context);
    }
  }

  private String textGenerateTreeLeaf(CommonTree root, TranslateContext context) throws Exception {
    QueryTextGenerator tg = TextGeneratorFactory.getTextGenerator(root);
    if (tg == null) {
      throw new SqlXlateException(root, "Untransformed SQL AST node:" + root.getText());
    }
    return tg.textGenerateQuery(root, context);
  }

}
