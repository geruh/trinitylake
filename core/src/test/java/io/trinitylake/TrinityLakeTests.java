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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.substrait.proto.NamedStruct;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.trinitylake.exception.NonEmptyNamespaceException;
import io.trinitylake.exception.ObjectAlreadyExistsException;
import io.trinitylake.exception.ObjectNotFoundException;
import io.trinitylake.models.Column;
import io.trinitylake.models.DataType;
import io.trinitylake.models.FullName;
import io.trinitylake.models.LakehouseDef;
import io.trinitylake.models.NamespaceDef;
import io.trinitylake.models.Schema;
import io.trinitylake.models.TableDef;
import io.trinitylake.models.ViewDef;
import io.trinitylake.storage.LakehouseStorage;
import io.trinitylake.tree.TreeOperations;
import io.trinitylake.tree.TreeRoot;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public abstract class TrinityLakeTests {

  protected static final LakehouseDef LAKEHOUSE_DEF =
      ObjectDefinitions.newLakehouseDefBuilder().build();

  protected static final String NAMESPACE = "ns1";

  protected static final NamespaceDef NAMESPACE_DEF =
      ObjectDefinitions.newNamespaceDefBuilder().putProperties("k1", "v1").build();

  protected static final TableDef TABLE1_DEF =
      ObjectDefinitions.newTableDefBuilder()
          .setSchema(
              Schema.newBuilder()
                  .addColumns(Column.newBuilder().setName("c1").setType(DataType.VARCHAR).build())
                  .build())
          .putProperties("k1", "v1")
          .build();

  protected static final String TABLE1 = "tbl1";

  protected static final TableDef TABLE2_DEF =
      ObjectDefinitions.newTableDefBuilder()
          .setSchema(
              Schema.newBuilder()
                  .addColumns(Column.newBuilder().setName("c2").setType(DataType.INT8).build())
                  .build())
          .putProperties("k2", "v2")
          .build();

  protected static final String TABLE2 = "tbl2";

  protected static final ViewDef VIEW_DEF =
      ObjectDefinitions.newViewDefBuilder()
          .setSchemaBinding(false)
          .setSubstraitReadRel(
              Rel.newBuilder()
                  .setRead(
                      ReadRel.newBuilder()
                          .setBaseSchema(NamedStruct.newBuilder().addNames(TABLE1).build())
                          .build())
                  .build()
                  .toByteString())
          .addReferencedObjectFullNames(
              FullName.newBuilder().setNamespaceName(NAMESPACE).setName(TABLE1).build())
          .putProperties("k1", "v1")
          .build();

  protected static final String VIEW = "view1";

  protected abstract LakehouseStorage storage();

  @Test
  public void testCreateLakehouse() {
    LakehouseStorage storage = storage();

    assertThat(storage.exists(FileLocations.LATEST_VERSION_HINT_FILE_PATH)).isTrue();
    assertThat(TrinityLake.lakehouseExists(storage)).isTrue();
    assertTreeRoot(0);
  }

  @Test
  public void testCreateNamespace() {
    LakehouseStorage storage = storage();
    createNamespaceAndCommit();

    assertTreeRoot(1);
    TreeRoot root = TreeOperations.findLatestRoot(storage);
    String ns1Key = ObjectKeys.namespaceKey(NAMESPACE, LAKEHOUSE_DEF);
    Optional<String> ns1Path = TreeOperations.searchValue(storage, root, ns1Key);
    assertThat(ns1Path.isPresent()).isTrue();
    NamespaceDef readDef = ObjectDefinitions.readNamespaceDef(storage, ns1Path.get());
    assertThat(readDef).isEqualTo(NAMESPACE_DEF);
  }

  @Test
  public void testCreateNamespaceAlreadyExists() {
    LakehouseStorage storage = storage();
    createNamespaceAndCommit();

    RunningTransaction transaction = TrinityLake.beginTransaction(storage);
    assertThatThrownBy(
            () -> TrinityLake.createNamespace(storage, transaction, NAMESPACE, NAMESPACE_DEF))
        .isInstanceOf(ObjectAlreadyExistsException.class);
  }

  @Test
  public void testDescribeNamespace() {
    LakehouseStorage storage = storage();
    createNamespaceAndCommit();

    RunningTransaction transaction = TrinityLake.beginTransaction(storage);
    assertThat(TrinityLake.namespaceExists(storage, transaction, NAMESPACE)).isTrue();

    NamespaceDef ns1DefDescribe = TrinityLake.describeNamespace(storage, transaction, NAMESPACE);
    assertThat(ns1DefDescribe).isEqualTo(NAMESPACE_DEF);
  }

  @Test
  public void testDropNamespace() {
    LakehouseStorage storage = storage();
    createNamespaceAndCommit();

    RunningTransaction transaction = TrinityLake.beginTransaction(storage);
    assertThat(TrinityLake.namespaceExists(storage, transaction, NAMESPACE)).isTrue();
    transaction =
        TrinityLake.dropNamespace(storage, transaction, NAMESPACE, DropNamespaceBehavior.RESTRICT);
    TrinityLake.commitTransaction(storage, transaction);
    transaction = TrinityLake.beginTransaction(storage);

    assertThat(TrinityLake.namespaceExists(storage, transaction, NAMESPACE)).isFalse();
  }

  @Test
  public void testDropNamespaceCascade() {
    LakehouseStorage storage = storage();
    createNamespaceAndTables();

    RunningTransaction transaction = TrinityLake.beginTransaction(storage);
    assertThat(TrinityLake.namespaceExists(storage, transaction, NAMESPACE)).isTrue();
    assertThat(TrinityLake.tableExists(storage, transaction, NAMESPACE, TABLE1)).isTrue();
    assertThat(TrinityLake.tableExists(storage, transaction, NAMESPACE, TABLE2)).isTrue();
    transaction =
        TrinityLake.dropNamespace(storage, transaction, NAMESPACE, DropNamespaceBehavior.CASCADE);
    TrinityLake.commitTransaction(storage, transaction);
    transaction = TrinityLake.beginTransaction(storage);

    assertThat(TrinityLake.namespaceExists(storage, transaction, NAMESPACE)).isFalse();
    assertThat(TrinityLake.tableExists(storage, transaction, NAMESPACE, TABLE1)).isFalse();
    assertThat(TrinityLake.tableExists(storage, transaction, NAMESPACE, TABLE2)).isFalse();
  }

  @Test
  public void testDropNonEmptyNamespace() {
    LakehouseStorage storage = storage();
    createNamespaceAndTables();

    RunningTransaction transaction = TrinityLake.beginTransaction(storage);
    assertThat(TrinityLake.namespaceExists(storage, transaction, NAMESPACE)).isTrue();
    assertThat(TrinityLake.tableExists(storage, transaction, NAMESPACE, TABLE1)).isTrue();
    assertThat(TrinityLake.tableExists(storage, transaction, NAMESPACE, TABLE2)).isTrue();
    assertThatThrownBy(
            () ->
                TrinityLake.dropNamespace(
                    storage, transaction, NAMESPACE, DropNamespaceBehavior.RESTRICT))
        .isInstanceOf(NonEmptyNamespaceException.class)
        .hasMessageContaining(String.format("Namespace %s is not empty", NAMESPACE));
  }

  @Test
  public void testDescribeMissingNamespace() {
    LakehouseStorage storage = storage();
    String ns = "nonexistent";

    RunningTransaction transaction = TrinityLake.beginTransaction(storage);
    assertThat(TrinityLake.namespaceExists(storage, transaction, ns)).isFalse();
    assertThatThrownBy(() -> TrinityLake.describeNamespace(storage, transaction, ns))
        .isInstanceOf(ObjectNotFoundException.class);
  }

  @Test
  public void testAlterDescribeNamespace() {
    LakehouseStorage storage = storage();
    createNamespaceAndCommit();

    RunningTransaction transaction = TrinityLake.beginTransaction(storage);
    assertThat(TrinityLake.namespaceExists(storage, transaction, NAMESPACE)).isTrue();
    NamespaceDef ns1DefDescribe = TrinityLake.describeNamespace(storage, transaction, NAMESPACE);
    assertThat(ns1DefDescribe).isEqualTo(NAMESPACE_DEF);

    NamespaceDef ns1DefAlter =
        ObjectDefinitions.newNamespaceDefBuilder().putProperties("k1", "v2").build();
    transaction = TrinityLake.alterNamespace(storage, transaction, NAMESPACE, ns1DefAlter);
    TrinityLake.commitTransaction(storage, transaction);

    transaction = TrinityLake.beginTransaction(storage);
    assertThat(TrinityLake.namespaceExists(storage, transaction, NAMESPACE)).isTrue();
    NamespaceDef ns1DefAlterDescribe =
        TrinityLake.describeNamespace(storage, transaction, NAMESPACE);
    assertThat(ns1DefAlterDescribe).isEqualTo(ns1DefAlter);
  }

  @Test
  public void testCreateTable() {
    LakehouseStorage storage = storage();
    createNamespaceAndCommit();

    RunningTransaction transaction = TrinityLake.beginTransaction(storage);
    transaction = TrinityLake.createTable(storage, transaction, NAMESPACE, TABLE1, TABLE1_DEF);
    TrinityLake.commitTransaction(storage, transaction);

    TreeRoot root = TreeOperations.findLatestRoot(storage);
    assertTreeRoot(2);
    String t1Key = ObjectKeys.tableKey(NAMESPACE, TABLE1, LAKEHOUSE_DEF);
    Optional<String> t1Path = TreeOperations.searchValue(storage, root, t1Key);
    assertThat(t1Path.isPresent()).isTrue();
    TableDef readDef = ObjectDefinitions.readTableDef(storage, t1Path.get());
    assertThat(readDef).isEqualTo(TABLE1_DEF);
  }

  @Test
  public void testDescribeTable() {
    LakehouseStorage storage = storage();
    createNamespaceAndCommit();

    RunningTransaction transaction = TrinityLake.beginTransaction(storage);
    transaction = TrinityLake.createTable(storage, transaction, NAMESPACE, TABLE1, TABLE1_DEF);
    TrinityLake.commitTransaction(storage, transaction);

    transaction = TrinityLake.beginTransaction(storage);
    assertThat(TrinityLake.tableExists(storage, transaction, NAMESPACE, TABLE1)).isTrue();
    TableDef t1DefDescribe = TrinityLake.describeTable(storage, transaction, NAMESPACE, TABLE1);
    assertThat(t1DefDescribe).isEqualTo(TABLE1_DEF);
  }

  @Test
  public void testAlterDescribeTable() {
    LakehouseStorage storage = storage();
    createNamespaceAndCommit();

    RunningTransaction transaction = TrinityLake.beginTransaction(storage);
    transaction = TrinityLake.createTable(storage, transaction, NAMESPACE, TABLE1, TABLE1_DEF);
    TrinityLake.commitTransaction(storage, transaction);

    transaction = TrinityLake.beginTransaction(storage);
    assertThat(TrinityLake.tableExists(storage, transaction, NAMESPACE, TABLE1)).isTrue();
    TableDef t1DefDescribe = TrinityLake.describeTable(storage, transaction, NAMESPACE, TABLE1);
    assertThat(t1DefDescribe).isEqualTo(TABLE1_DEF);

    TableDef t1DefAlter =
        ObjectDefinitions.newTableDefBuilder()
            .setSchema(
                Schema.newBuilder()
                    .addColumns(Column.newBuilder().setName("c1").setType(DataType.VARCHAR).build())
                    .addColumns(Column.newBuilder().setName("c2").setType(DataType.VARCHAR).build())
                    .build())
            .putProperties("k1", "v2")
            .build();
    transaction = TrinityLake.alterTable(storage, transaction, NAMESPACE, TABLE1, t1DefAlter);
    TrinityLake.commitTransaction(storage, transaction);

    transaction = TrinityLake.beginTransaction(storage);
    assertThat(TrinityLake.tableExists(storage, transaction, NAMESPACE, TABLE1)).isTrue();
    TableDef t1DefAlterDescribe =
        TrinityLake.describeTable(storage, transaction, NAMESPACE, TABLE1);
    assertThat(t1DefAlterDescribe).isEqualTo(t1DefAlter);
  }

  @Test
  public void testDropTable() {
    LakehouseStorage storage = storage();
    createNamespaceAndCommit();

    RunningTransaction transaction = TrinityLake.beginTransaction(storage);
    transaction = TrinityLake.createTable(storage, transaction, NAMESPACE, TABLE1, TABLE1_DEF);
    TrinityLake.commitTransaction(storage, transaction);

    transaction = TrinityLake.beginTransaction(storage);
    assertThat(TrinityLake.tableExists(storage, transaction, NAMESPACE, TABLE1)).isTrue();
    transaction = TrinityLake.dropTable(storage, transaction, NAMESPACE, TABLE1);
    TrinityLake.commitTransaction(storage, transaction);

    transaction = TrinityLake.beginTransaction(storage);
    assertThat(TrinityLake.tableExists(storage, transaction, NAMESPACE, TABLE1)).isFalse();
  }

  @Test
  public void testCreateTableNoNamespace() {
    LakehouseStorage storage = storage();

    RunningTransaction transaction = TrinityLake.beginTransaction(storage);
    assertThatThrownBy(
            () -> TrinityLake.createTable(storage, transaction, NAMESPACE, TABLE1, TABLE1_DEF))
        .isInstanceOf(ObjectNotFoundException.class);
  }

  @Test
  public void testCreateView() {
    LakehouseStorage storage = storage();
    createNamespaceAndCommit();

    RunningTransaction transaction = TrinityLake.beginTransaction(storage);
    transaction = TrinityLake.createView(storage, transaction, NAMESPACE, VIEW, VIEW_DEF);
    TrinityLake.commitTransaction(storage, transaction);

    TreeRoot root = TreeOperations.findLatestRoot(storage);
    assertTreeRoot(2);
    String v1Key = ObjectKeys.viewKey(NAMESPACE, VIEW, LAKEHOUSE_DEF);
    Optional<String> v1Path = TreeOperations.searchValue(storage, root, v1Key);
    assertThat(v1Path.isPresent()).isTrue();
    ViewDef readDef = ObjectDefinitions.readViewDef(storage, v1Path.get());
    assertThat(readDef).isEqualTo(VIEW_DEF);
  }

  @Test
  public void testCreateDescribeView() {
    LakehouseStorage storage = storage();
    createNamespaceAndCommit();

    RunningTransaction transaction = TrinityLake.beginTransaction(storage);
    transaction = TrinityLake.createView(storage, transaction, NAMESPACE, VIEW, VIEW_DEF);
    TrinityLake.commitTransaction(storage, transaction);

    transaction = TrinityLake.beginTransaction(storage);
    assertThat(TrinityLake.viewExists(storage, transaction, NAMESPACE, VIEW)).isTrue();
    ViewDef v1DefDescribe = TrinityLake.describeView(storage, transaction, NAMESPACE, VIEW);
    assertThat(v1DefDescribe).isEqualTo(VIEW_DEF);
  }

  @Test
  public void testCreateDescribeDropDescribeNamespace() {
    LakehouseStorage storage = storage();
    createNamespaceAndCommit();

    RunningTransaction transaction = TrinityLake.beginTransaction(storage);
    transaction = TrinityLake.createView(storage, transaction, NAMESPACE, VIEW, VIEW_DEF);
    TrinityLake.commitTransaction(storage, transaction);

    transaction = TrinityLake.beginTransaction(storage);
    assertThat(TrinityLake.viewExists(storage, transaction, NAMESPACE, VIEW)).isTrue();
    ViewDef v1DefDescribe = TrinityLake.describeView(storage, transaction, NAMESPACE, VIEW);
    assertThat(v1DefDescribe).isEqualTo(VIEW_DEF);

    transaction = TrinityLake.dropView(storage, transaction, NAMESPACE, VIEW);
    TrinityLake.commitTransaction(storage, transaction);

    transaction = TrinityLake.beginTransaction(storage);
    assertThat(TrinityLake.viewExists(storage, transaction, NAMESPACE, VIEW)).isFalse();
  }

  @Test
  public void testDropView() {
    LakehouseStorage storage = storage();
    createNamespaceAndCommit();

    RunningTransaction transaction = TrinityLake.beginTransaction(storage);
    transaction = TrinityLake.createView(storage, transaction, NAMESPACE, VIEW, VIEW_DEF);
    TrinityLake.commitTransaction(storage, transaction);

    transaction = TrinityLake.beginTransaction(storage);
    assertThat(TrinityLake.viewExists(storage, transaction, NAMESPACE, VIEW)).isTrue();
    transaction = TrinityLake.dropView(storage, transaction, NAMESPACE, VIEW);
    TrinityLake.commitTransaction(storage, transaction);

    transaction = TrinityLake.beginTransaction(storage);
    assertThat(TrinityLake.viewExists(storage, transaction, NAMESPACE, VIEW)).isFalse();
  }

  protected void assertTreeRoot(int expectedVersion) {
    LakehouseStorage storage = storage();
    TreeRoot root = TreeOperations.findLatestRoot(storage);
    assertThat(root.path().isPresent()).isTrue();
    assertThat(root.path().get()).isEqualTo(FileLocations.rootNodeFilePath(expectedVersion));
    if (root.previousRootNodeFilePath().isPresent()) {
      assertThat(root.previousRootNodeFilePath().get())
          .isEqualTo(FileLocations.rootNodeFilePath(expectedVersion - 1));
    }
  }

  protected void createNamespaceAndCommit() {
    LakehouseStorage storage = storage();
    RunningTransaction transaction = TrinityLake.beginTransaction(storage);
    transaction = TrinityLake.createNamespace(storage, transaction, NAMESPACE, NAMESPACE_DEF);
    TrinityLake.commitTransaction(storage, transaction);
  }

  protected void createNamespaceAndTables() {
    LakehouseStorage storage = storage();
    RunningTransaction transaction = TrinityLake.beginTransaction(storage);
    transaction = TrinityLake.createNamespace(storage, transaction, NAMESPACE, NAMESPACE_DEF);
    transaction = TrinityLake.createTable(storage, transaction, NAMESPACE, TABLE1, TABLE1_DEF);
    transaction = TrinityLake.createTable(storage, transaction, NAMESPACE, TABLE2, TABLE2_DEF);
    TrinityLake.commitTransaction(storage, transaction);
  }
}
