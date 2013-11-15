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

import java.util.LinkedList;
import java.util.List;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * left child and then write down root and then right child, with brackets on children.<br>
 * BiOpTextGenerator.
 *
 */
public class BiOpTextGenerator extends BaseTextGenerator {

  @Override
  protected String textGenerate(CommonTree root, TranslateContext context) throws Exception {
    if(root.getChildCount()==2) {
      CommonTree op1 = (CommonTree) root.getChild(0);
      QueryTextGenerator qr1 = TextGeneratorFactory.getTextGenerator(op1);

      CommonTree op2 = (CommonTree) root.getChild(1);
      QueryTextGenerator qr2 = TextGeneratorFactory.getTextGenerator(op2);
      return "(" + qr1.textGenerateQuery(op1, context) + ") " + root.getText() +
          " (" + qr2.textGenerateQuery(op2, context) + ")";
    } else {
      //for date time.
      //FIXME it just keep as it looks like in sqlAST. the rebuilded text can not execute on Hive
      if(root.getChildCount() < 3) {
        throw new SqlXlateException(root, "Calculation expression not supported.");
      }
      List<Integer> positions = new LinkedList<Integer>();
      for (int i = 0; i < root.getChildCount(); i++) {
        if(root.getChild(i).getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_DATE) {
          positions.add(i);
        }
      }
      if(positions.size() > 2 || positions.size() == 0) {
        throw new SqlXlateException(root, "Calculation expression not supported.");
      } else if (positions.size() == 2 || positions.get(0) == 0) {
        //two date item, or one date item as left operand, left operand is always size 2
        return "(" + textGenerateChildRange(root, context, 0, 2) + ") " +
            root.getText() + " (" + textGenerateChildRange(root, context, 2, root.getChildCount()) + ")";
      } else {
        //one date item and at right operand
        return "(" + textGenerateChildRange(root, context, 0, positions.get(0)) + ") " +
            root.getText() + " (" + textGenerateChildRange(root, context, positions.get(0), root.getChildCount()) + ")";
      }
    }
  }

}
