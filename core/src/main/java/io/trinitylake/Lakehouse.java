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

import io.trinitylake.models.LakehouseDef;
import io.trinitylake.models.NamespaceDef;
import io.trinitylake.models.TableDef;
import java.util.List;

public interface Lakehouse {

  BasicLakehouseVersion beginTransaction(String transaction);

  BasicLakehouseVersion commitTransaction(String transaction, BasicLakehouseVersion version);

  void rollbackTransaction(String transaction);

  List<Long> showLakehouseVersions();

  LakehouseVersion loadLakehouseVersion(long version);

  LakehouseVersion loadLatestLakehouseVersion();

  default LakehouseDef describeLakehouse() {
    return loadLatestLakehouseVersion().describeLakehouse();
  }

  default NamespaceDef describeNamespace(String namespaceName) {
    return loadLatestLakehouseVersion().describeNamespace(namespaceName);
  }

  default TableDef describeTable(String namespaceName, String tableName) {
    return loadLatestLakehouseVersion().describeTable(namespaceName, tableName);
  }

  default List<String> showTables(String namespaceName) {
    return loadLatestLakehouseVersion().showTables(namespaceName);
  }
}
