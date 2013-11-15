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

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;

public class IdGenerator extends BaseHiveASTGenerator {

  @Override
  public boolean generate(ASTNode hiveRoot, CommonTree sqlRoot, ASTNode currentHiveNode,
      CommonTree currentSqlNode, TranslateContext context) throws SqlXlateException {
    ASTNode ret;
    String text = currentSqlNode.getText();
    if (currentSqlNode.getText().contains("\"") || currentSqlNode.getText().contains("'")) {
      ret = super.newHiveASTNode(HiveParser.StringLiteral, currentSqlNode.getText());
    } else if (currentSqlNode.getText().toLowerCase().equals("null")) {
      ret = super.newHiveASTNode(HiveParser.TOK_NULL, "TOK_NULL");
    } else {
      ret = super.newHiveASTNode(HiveParser.Identifier, text);
    }
    super.attachHiveNode(hiveRoot, currentHiveNode, ret);
    return true;// must be leaf;
  }

}
