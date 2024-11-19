/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.trinitylake;


import java.util.UUID;

public class BaseTransaction implements Transaction {
  private final String transactionId;

  public BaseTransaction() {
    this.transactionId = UUID.randomUUID().toString();
  }

  @Override
  public String transactionId() {
    return transactionId;
  }

  @Override
  public void commitTransaction() {
    System.out.println("Committing transaction");
  }

  public void rollback() {
    System.out.println("Rolling back transaction");
  }
} 