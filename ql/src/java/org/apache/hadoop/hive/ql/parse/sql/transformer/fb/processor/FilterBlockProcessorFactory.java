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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * build FilterBlockProcessor by subquery node. <br>
 * FilterBlockProcessorFactory.
 *
 */
public class FilterBlockProcessorFactory {

  private static final Log LOG = LogFactory.getLog(FilterBlockProcessorFactory.class);

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
      return new CompareProcessor4UC();
    case PantheraParser_PLSQLParser.NOT_IN:
      return new NotInProcessor4UC();
    case PantheraParser_PLSQLParser.SQL92_RESERVED_IN:
      return new InProcessor4UC();
    case PantheraParser_PLSQLParser.SQL92_RESERVED_EXISTS:
      return new ExistsProcessor4UC();
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
      return new CompareProcessor4C();
    case PantheraParser_PLSQLParser.SQL92_RESERVED_EXISTS:
      return new ExistsProcessor4C();
    case PantheraParser_PLSQLParser.SQL92_RESERVED_IN:
      return new InProcessor4C();
    case PantheraParser_PLSQLParser.NOT_IN:
      return new NotInProcessor4C();
    default:
      throw new SqlXlateException(subQ, "Unimplement correlated subquery type:" + type);
    }
  }

  private FilterBlockProcessorFactory() {
  }
}
