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
package io.trinitylake.tree;

import static org.assertj.core.api.Assertions.assertThat;

import io.trinitylake.FileLocations;
import io.trinitylake.relocated.com.google.common.collect.Lists;
import io.trinitylake.storage.BasicLakehouseStorage;
import io.trinitylake.storage.LakehouseStorage;
import io.trinitylake.storage.LiteralURI;
import io.trinitylake.storage.local.LocalStorageOps;
import java.io.File;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestTreeOperations {

  @Test
  public void testWriteReadRootNodeFile(@TempDir Path tempDir) {
    LocalStorageOps ops = new LocalStorageOps();
    LakehouseStorage storage = new BasicLakehouseStorage(new LiteralURI("file://" + tempDir), ops);

    TreeRoot treeRoot = new BasicTreeRoot();
    for (int i = 0; i < 10; i++) {
      treeRoot.set("k" + i, "val" + i);
    }
    treeRoot.setPreviousRootNodeFilePath("some/path/to/previous/root");
    treeRoot.setRollbackFromRootNodeFilePath("some/path/to/rollback/from/root");
    treeRoot.setLakehouseDefFilePath("some/path/to/lakehouse/def");

    File file = tempDir.resolve("testWriteReadRootNodeFile.arrow").toFile();

    TreeOperations.writeRootNodeFile(storage, "testWriteReadRootNodeFile.arrow", treeRoot);
    assertThat(file.exists()).isTrue();

    TreeRoot root = TreeOperations.readRootNodeFile(storage, "testWriteReadRootNodeFile.arrow");
    assertThat(
            TreeOperations.getNodeKeyTable(storage, treeRoot).stream()
                .collect(Collectors.toMap(NodeKeyTableRow::key, NodeKeyTableRow::value)))
        .isEqualTo(
            TreeOperations.getNodeKeyTable(storage, root).stream()
                .collect(Collectors.toMap(NodeKeyTableRow::key, NodeKeyTableRow::value)));
  }

  @Test
  public void testFindLatestVersion(@TempDir Path tempDir) {
    LocalStorageOps ops = new LocalStorageOps();
    LakehouseStorage storage = new BasicLakehouseStorage(new LiteralURI("file://" + tempDir), ops);

    TreeRoot treeRootV0 = new BasicTreeRoot();
    treeRootV0.setLakehouseDefFilePath("some/path/to/lakehouse/def");
    String v0Path = FileLocations.rootNodeFilePath(0);
    TreeOperations.writeRootNodeFile(storage, v0Path, treeRootV0);

    TreeRoot treeRootV1 = new BasicTreeRoot();
    treeRootV1.setLakehouseDefFilePath("some/path/to/lakehouse/def");
    treeRootV1.setPreviousRootNodeFilePath(v0Path);
    String v1Path = FileLocations.rootNodeFilePath(1);
    TreeOperations.writeRootNodeFile(storage, v1Path, treeRootV1);

    TreeRoot treeRootV2 = new BasicTreeRoot();
    treeRootV2.setLakehouseDefFilePath("some/path/to/lakehouse/def");
    treeRootV2.setPreviousRootNodeFilePath(v1Path);
    String v2Path = FileLocations.rootNodeFilePath(2);
    TreeOperations.writeRootNodeFile(storage, v2Path, treeRootV2);

    TreeRoot root = TreeOperations.findLatestRoot(storage);
    assertThat(root.path().get()).isEqualTo(v2Path);
  }

  @Test
  public void testTreeRootIterable(@TempDir Path tempDir) {
    LocalStorageOps ops = new LocalStorageOps();
    LakehouseStorage storage = new BasicLakehouseStorage(new LiteralURI("file://" + tempDir), ops);

    TreeRoot treeRootV0 = new BasicTreeRoot();
    treeRootV0.setLakehouseDefFilePath("some/path/to/lakehouse/def");
    String v0Path = FileLocations.rootNodeFilePath(0);
    TreeOperations.writeRootNodeFile(storage, v0Path, treeRootV0);

    TreeRoot treeRootV1 = new BasicTreeRoot();
    treeRootV1.setLakehouseDefFilePath("some/path/to/lakehouse/def");
    treeRootV1.setPreviousRootNodeFilePath(v0Path);
    String v1Path = FileLocations.rootNodeFilePath(1);
    TreeOperations.writeRootNodeFile(storage, v1Path, treeRootV1);

    TreeRoot treeRootV2 = new BasicTreeRoot();
    treeRootV2.setLakehouseDefFilePath("some/path/to/lakehouse/def");
    treeRootV2.setPreviousRootNodeFilePath(v1Path);
    String v2Path = FileLocations.rootNodeFilePath(2);
    TreeOperations.writeRootNodeFile(storage, v2Path, treeRootV2);

    Iterator<TreeRoot> roots = TreeOperations.listRoots(storage).iterator();

    assertThat(roots.hasNext()).isTrue();
    TreeRoot root = roots.next();
    assertThat(root.path().get()).isEqualTo(v2Path);
    assertThat(root.previousRootNodeFilePath().get()).isEqualTo(v1Path);

    assertThat(roots.hasNext()).isTrue();
    root = roots.next();
    assertThat(root.path().get()).isEqualTo(v1Path);
    assertThat(root.previousRootNodeFilePath().get()).isEqualTo(v0Path);

    assertThat(roots.hasNext()).isTrue();
    root = roots.next();
    assertThat(root.path().get()).isEqualTo(v0Path);
    assertThat(root.previousRootNodeFilePath().isPresent()).isFalse();

    assertThat(roots.hasNext()).isFalse();
  }

  @Test
  public void testMergePersistedAndPendingChanges(@TempDir Path tempDir) {
    LocalStorageOps ops = new LocalStorageOps();
    LakehouseStorage storage = new BasicLakehouseStorage(new LiteralURI("file://" + tempDir), ops);

    TreeRoot treeRoot = new BasicTreeRoot();
    treeRoot.setLakehouseDefFilePath("some/path/to/lakehouse/def");
    treeRoot.set("key1", "val1");
    treeRoot.set("key2", "val2");
    treeRoot.set("key3", "val3");
    treeRoot.set("key4", "delete");

    String path = "testMergeChanges.arrow";
    TreeOperations.writeRootNodeFile(storage, path, treeRoot);
    TreeRoot loadedRoot = TreeOperations.readRootNodeFile(storage, path);

    loadedRoot.set("pending_key5", "val5");
    loadedRoot.set("key3", "modified");
    loadedRoot.set("key4", null);

    List<NodeKeyTableRow> mergedTable = TreeOperations.getNodeKeyTable(storage, loadedRoot);
    Map<String, Optional<String>> mergedMap =
        mergedTable.stream()
            .collect(Collectors.toMap(NodeKeyTableRow::key, NodeKeyTableRow::value));

    assertThat(mergedMap).containsKey("key1");
    assertThat(mergedMap.get("key1").get()).isEqualTo("val1");

    assertThat(mergedMap).containsKey("key2");
    assertThat(mergedMap.get("key2").get()).isEqualTo("val2");

    assertThat(mergedMap).containsKey("pending_key5");
    assertThat(mergedMap.get("pending_key5").get()).isEqualTo("val5");

    assertThat(mergedMap).containsKey("key3");
    assertThat(mergedMap.get("key3").get()).isEqualTo("modified");

    assertThat(mergedMap).doesNotContainKey("key4");
  }

  @Test
  public void testKeySortingInNodeKeyTable(@TempDir Path tempDir) {
    LocalStorageOps ops = new LocalStorageOps();
    LakehouseStorage storage = new BasicLakehouseStorage(new LiteralURI("file://" + tempDir), ops);

    TreeRoot treeRoot = new BasicTreeRoot();
    treeRoot.setLakehouseDefFilePath("some/path/to/lakehouse/def");

    treeRoot.set("b_key", "b_value");
    treeRoot.set("a_key", "a_value");
    treeRoot.set("c_key", "c_value");

    String path = "testSorting.arrow";
    TreeOperations.writeRootNodeFile(storage, path, treeRoot);
    TreeRoot loadedRoot = TreeOperations.readRootNodeFile(storage, path);

    // add pending changes
    loadedRoot.set("f_key", "f_value");
    loadedRoot.set("d_key", "d_value");
    loadedRoot.set("e_key", "e_value");

    List<NodeKeyTableRow> keyTable = TreeOperations.getNodeKeyTable(storage, loadedRoot);
    List<String> actualOrder =
        keyTable.stream().map(NodeKeyTableRow::key).collect(Collectors.toList());

    List<String> expectedOrder =
        Lists.newArrayList("a_key", "b_key", "c_key", "d_key", "e_key", "f_key");
    assertThat(actualOrder).isEqualTo(expectedOrder);

    for (int i = 1; i < actualOrder.size(); i++) {
      String prevKey = actualOrder.get(i - 1);
      String currentKey = actualOrder.get(i);
      assertThat(currentKey).isGreaterThan(prevKey);
    }
  }
}
