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

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * build FilterBlockProcessor by subquery node. <br>
 * FilterBlockProcessorFactory.
 *
 */
public class FilterBlockProcessorFactory {

  public static FilterBlockProcessor getUnCorrelatedProcessor(CommonTree subQ)
      throws SqlXlateException {
    int type = subQ.getType();
    switch (type) {
    case PantheraParser_PLSQLParser.EQUALS_OP:
    case PantheraParser_PLSQLParser.NOT_EQUAL_OP:
    case PantheraParser_PLSQLParser.GREATER_THAN_OP:
    case PantheraParser_PLSQLParser.LESS_THAN_OP:
    case PantheraParser_PLSQLParser.LESS_THAN_OR_EQUALS_OP:
    case PantheraParser_PLSQLParser.GREATER_THAN_OR_EQUALS_OP:
      return getCompareSubqueryProcessor4UC(subQ);
    case PantheraParser_PLSQLParser.NOT_IN:
      return new NotInProcessor4UC();
    case PantheraParser_PLSQLParser.SQL92_RESERVED_IN:
      return new InProcessor4UC();
    case PantheraParser_PLSQLParser.SQL92_RESERVED_EXISTS:
      return new ExistsProcessor4UC();
    case PantheraParser_PLSQLParser.IS_NULL:
      return new IsNullProcessor4UC();
    case PantheraParser_PLSQLParser.IS_NOT_NULL:
      return new IsNotNullProcessor4UC();
    default:
      throw new SqlXlateException(subQ, "Unimplement uncorrelated subquery type:" + type);
    }
  }

  public static FilterBlockProcessor getCorrelatedProcessor(CommonTree subQ)
      throws SqlXlateException {
    int type = subQ.getType();
    switch (type) {
    case PantheraParser_PLSQLParser.EQUALS_OP:
    case PantheraParser_PLSQLParser.NOT_EQUAL_OP:
    case PantheraParser_PLSQLParser.GREATER_THAN_OP:
    case PantheraParser_PLSQLParser.LESS_THAN_OP:
    case PantheraParser_PLSQLParser.LESS_THAN_OR_EQUALS_OP:
    case PantheraParser_PLSQLParser.GREATER_THAN_OR_EQUALS_OP:
      return getCompareSubqueryProcessor4C(subQ);
    case PantheraParser_PLSQLParser.SQL92_RESERVED_EXISTS:
      return new ExistsProcessor4C();
    case PantheraParser_PLSQLParser.SQL92_RESERVED_IN:
      return new InProcessor4C();
    case PantheraParser_PLSQLParser.NOT_IN:
      return new NotInProcessor4C();
    case PantheraParser_PLSQLParser.IS_NULL:
      return new IsNullProcessor4C();
    case PantheraParser_PLSQLParser.IS_NOT_NULL:
      return new IsNotNullProcessor4C();
    default:
      throw new SqlXlateException(subQ, "Unimplement correlated subquery type:" + type);
    }
  }

  private static FilterBlockProcessor getCompareSubqueryProcessor4C(CommonTree tree) throws SqlXlateException {
    int scopeType = tree.getChild(1).getType();
    if (scopeType == PantheraParser_PLSQLParser.SQL92_RESERVED_ALL) {
      return new AllProcessor4C();
    } else if (scopeType == PantheraParser_PLSQLParser.SOME_VK || scopeType == PantheraParser_PLSQLParser.SQL92_RESERVED_ANY) {
      return new AnyProcessor4C();
    }
    return new CompareProcessor4C();
  }

  private static FilterBlockProcessor getCompareSubqueryProcessor4UC(CommonTree tree) throws SqlXlateException {
    int scopeType = tree.getChild(1).getType();
    if (scopeType == PantheraParser_PLSQLParser.SQL92_RESERVED_ALL) {
      return new AllProcessor4UC();
    } else if (scopeType == PantheraParser_PLSQLParser.SOME_VK || scopeType == PantheraParser_PLSQLParser.SQL92_RESERVED_ANY) {
      return new AnyProcessor4UC();
    }
    return new CompareProcessor4UC();
  }

  private FilterBlockProcessorFactory() {
  }
}
