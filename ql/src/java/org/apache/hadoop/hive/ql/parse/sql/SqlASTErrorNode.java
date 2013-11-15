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

import org.antlr.runtime.Token;
import org.antlr.runtime.TokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonErrorNode;

/** 
 * Error counterpart of a normal SqlASTNode.
 * A node representing erroneous token range in 
 * token stream. 
 * Needed by antlr3.
 */
public class SqlASTErrorNode extends CommonErrorNode {
  
  private static final long serialVersionUID = 1L;
  
  /**
   * Constructor.
   * Only used by antlr.
   */
  public SqlASTErrorNode(TokenStream input, Token start, Token stop, RecognitionException e) {
    super(input, start, stop, e);
  }


       
}
