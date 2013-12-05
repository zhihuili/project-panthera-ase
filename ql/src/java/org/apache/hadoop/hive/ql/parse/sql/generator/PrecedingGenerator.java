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

/**
 * Generator for "preceding" keyword.
 * e.g. select max(s_grade) over(partition by s_empname order by s_city rows unbounded preceding) from staff;
 * PrecedingGenerator.
 */
public class PrecedingGenerator extends BaseHiveASTGenerator {

  @Override
  public boolean generate(ASTNode hiveRoot, CommonTree sqlRoot, ASTNode currentHiveNode,
      CommonTree currentSqlNode, TranslateContext context) throws SqlXlateException {
    ASTNode ret;
    String text = currentSqlNode.getText();
    ret = super.newHiveASTNode(HiveParser.KW_PRECEDING, text);
    if (currentHiveNode.getText().equals("unbounded")) {
      currentHiveNode.getToken().setText(text);
      currentHiveNode.getToken().setType(ret.getType());
      ret = super.newHiveASTNode(HiveParser.KW_UNBOUNDED, "unbounded");
    }
    super.attachHiveNode(hiveRoot, currentHiveNode, ret);
    return super.generateChildren(hiveRoot, sqlRoot, ret, currentSqlNode, context);
  }

}
