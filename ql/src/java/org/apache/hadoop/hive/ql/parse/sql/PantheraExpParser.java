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
package org.apache.hadoop.hive.ql.parse.sql;

import org.antlr.runtime.TokenStream;

import br.com.porcelli.parser.plsql.PantheraParser;
import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

public class PantheraExpParser extends PantheraParser_PLSQLParser {

  public static final int PANTHERA_LIMIT = 999;
  public static final int LEFTSEMI_VK = 998;
  public static final int LIMIT_VK = 997;
  public static final int EQUALS_NS = 996;

  public static final String LEFTSEMI_STR = "leftsemi";
  public static final String LEFT_STR="left";

  public PantheraExpParser(TokenStream input, PantheraParser gPantheraParser) {
    super(input, gPantheraParser);
  }

}
