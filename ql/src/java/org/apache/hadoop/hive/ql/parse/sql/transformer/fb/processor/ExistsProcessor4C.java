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
package org.apache.hadoop.hive.ql.parse.sql.transformer.fb.processor;

import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * Process correlated EXISTS in WHERE subquery.<br>
 * ExistsProcessor4C.
 *
 */
public class ExistsProcessor4C extends CommonFilterBlockProcessor {

  @Override
  void processFB() throws SqlXlateException {
    boolean isNot = super.subQNode.getParent().getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_NOT ? true
        : false;
    if (isNot) {
      /**
       * Handle NOT EXISTS correlated subQ with two methods.
       * IMPORTANT: The following two line use the two method to handle NOT EXISTS Correlated subQ
       * IMPORTANT: Here is a switch to use either method. Just Comment one line will use another method.
       * "processNotExistsCByLeftJoin" function use the method like faceBook transformed TPC-H Q21 and Q22.
       * "processNotExistsC" use the method in ASE design document, which will transformed into EXISTS and use MINUS to reduce the result.
       */
      //super.processNotExistsCByLeftJoin();
      super.processNotExistsC();
    } else {
      super.processExistsC();
    }
  }

}
