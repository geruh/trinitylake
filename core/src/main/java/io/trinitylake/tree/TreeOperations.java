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

import io.trinitylake.FileLocations;
import io.trinitylake.ObjectDefinitions;
import io.trinitylake.ObjectKeys;
import io.trinitylake.exception.StorageAtomicSealFailureException;
import io.trinitylake.exception.StorageFileOpenFailureException;
import io.trinitylake.exception.StorageReadFailureException;
import io.trinitylake.exception.StorageWriteFailureException;
import io.trinitylake.models.LakehouseDef;
import io.trinitylake.relocated.com.google.common.collect.Lists;
import io.trinitylake.storage.AtomicOutputStream;
import io.trinitylake.storage.LakehouseStorage;
import io.trinitylake.storage.local.LocalInputStream;
import io.trinitylake.util.FileUtil;
import io.trinitylake.util.ValidationUtil;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.arrow.algorithm.search.VectorSearcher;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TreeOperations {

  private static final Logger LOG = LoggerFactory.getLogger(TreeOperations.class);

  private static final int NODE_FILE_KEY_COLUMN_INDEX = 0;
  private static final String NODE_FILE_KEY_COLUMN_NAME = "key";

  private static final int NODE_FILE_VALUE_COLUMN_INDEX = 1;
  private static final String NODE_FILE_VALUE_COLUMN_NAME = "value";

  private TreeOperations() {}

  public static TreeRoot readRootNodeFile(LakehouseStorage storage, String path) {
    BufferAllocator allocator = storage.getArrowAllocator();
    try (LocalInputStream stream = storage.startReadLocal(path)) {
      TreeRoot root = readRootNodeFile(stream, allocator);
      root.setPath(path);
      return root;
    } catch (IOException e) {
      throw new StorageReadFailureException(e);
    }
  }

  private static TreeRoot readRootNodeFile(LocalInputStream stream, BufferAllocator allocator) {
    TreeRoot treeRoot = new BasicTreeRoot();
    ArrowFileReader reader = new ArrowFileReader(stream.channel(), allocator);
    try {

      for (ArrowBlock arrowBlock : reader.getRecordBlocks()) {
        reader.loadRecordBatch(arrowBlock);
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        VarCharVector keyVector = (VarCharVector) root.getVector(NODE_FILE_KEY_COLUMN_INDEX);
        VarCharVector valueVector = (VarCharVector) root.getVector(NODE_FILE_VALUE_COLUMN_INDEX);
        int numKeys = 0;
        for (int i = 0; i < root.getRowCount(); ++i) {
          Text key = keyVector.getObject(i);
          Text value = valueVector.getObject(i);

          if (ObjectKeys.CREATED_AT_MILLIS.equals(key.toString())) {
            treeRoot.setCreatedAtMillis(Long.parseLong(value.toString()));
          } else if (ObjectKeys.LAKEHOUSE_DEFINITION.equals(key.toString())) {
            treeRoot.setLakehouseDefFilePath(value.toString());
          } else if (ObjectKeys.PREVIOUS_ROOT_NODE.equals(key.toString())) {
            treeRoot.setPreviousRootNodeFilePath(value.toString());
          } else if (ObjectKeys.ROLLBACK_FROM_ROOT_NODE.equals(key.toString())) {
            treeRoot.setRollbackFromRootNodeFilePath(value.toString());
          } else if (ObjectKeys.NUMBER_OF_KEYS.equals(key.toString())) {
            numKeys = Integer.parseInt(value.toString());
          }
        }

        //        ValidationUtil.checkState(
        //            numKeys == treeRoot.numKeys(),
        //            "Recorded number of keys do not match the actual node key table size, the node
        // file might be corrupted");
      }
    } catch (IOException e) {
      throw new StorageReadFailureException(e);
    }

    return treeRoot;
  }

  public static void writeRootNodeFile(LakehouseStorage storage, String path, TreeRoot root) {
    try (AtomicOutputStream stream = storage.startCommit(path)) {
      writeRootNodeFile(stream, root, storage);
    } catch (IOException e) {
      throw new StorageAtomicSealFailureException(e);
    }
  }

  private static void writeRootNodeFile(
      AtomicOutputStream stream, TreeRoot root, LakehouseStorage storage) {
    BufferAllocator allocator = new RootAllocator();
    VarCharVector keyVector = new VarCharVector(NODE_FILE_KEY_COLUMN_NAME, allocator);
    VarCharVector valueVector = new VarCharVector(NODE_FILE_VALUE_COLUMN_NAME, allocator);
    List<NodeKeyTableRow> nodeKeyTable = getNodeKeyTable(storage, root);

    int index = 0;
    long createdAtMillis = System.currentTimeMillis();
    keyVector.setSafe(index, ObjectKeys.CREATED_AT_MILLIS_BYTES);
    valueVector.setSafe(index, Long.toString(createdAtMillis).getBytes(StandardCharsets.UTF_8));

    index++;
    keyVector.setSafe(index, ObjectKeys.LAKEHOUSE_DEFINITION_BYTES);
    valueVector.setSafe(index, root.lakehouseDefFilePath().getBytes(StandardCharsets.UTF_8));

    index++;
    keyVector.setSafe(index, ObjectKeys.NUMBER_OF_KEYS_BYTES);
    valueVector.setSafe(
        index, Integer.toString(nodeKeyTable.size()).getBytes(StandardCharsets.UTF_8));

    if (root.previousRootNodeFilePath().isPresent()) {
      index++;
      keyVector.setSafe(index, ObjectKeys.PREVIOUS_ROOT_NODE_BYTES);
      valueVector.setSafe(
          index, root.previousRootNodeFilePath().get().getBytes(StandardCharsets.UTF_8));
    }

    if (root.rollbackFromRootNodeFilePath().isPresent()) {
      index++;
      keyVector.setSafe(index, ObjectKeys.ROLLBACK_FROM_ROOT_NODE_BYTES);
      valueVector.setSafe(
          index, root.rollbackFromRootNodeFilePath().get().getBytes(StandardCharsets.UTF_8));
    }

    for (NodeKeyTableRow row : nodeKeyTable) {
      if (row.value().isPresent()) {
        index++;
        keyVector.setSafe(index, row.key().getBytes(StandardCharsets.UTF_8));
        valueVector.setSafe(index, row.value().get().getBytes(StandardCharsets.UTF_8));
      }
    }

    index++;
    keyVector.setValueCount(index);
    valueVector.setValueCount(index);

    List<Field> fields = Lists.newArrayList(keyVector.getField(), valueVector.getField());
    List<FieldVector> vectors = Lists.newArrayList(keyVector, valueVector);
    VectorSchemaRoot schema = new VectorSchemaRoot(fields, vectors);
    try (ArrowFileWriter writer = new ArrowFileWriter(schema, null, stream.channel())) {
      writer.start();
      writer.writeBatch();
      writer.end();
    } catch (IOException e) {
      throw new StorageWriteFailureException(e);
    }
  }

  public static void tryWriteRootNodeVersionHintFile(LakehouseStorage storage, long rootVersion) {
    try (OutputStream stream =
        storage.startOverwrite(FileLocations.LATEST_VERSION_HINT_FILE_PATH)) {
      stream.write(Long.toString(rootVersion).getBytes(StandardCharsets.UTF_8));
    } catch (StorageWriteFailureException | IOException e) {
      LOG.error("Failed to write root node version hint file", e);
    }
  }

  public static long findVersionFromRootNode(TreeNode node) {
    ValidationUtil.checkArgument(
        node.path().isPresent(), "Cannot derive version from a node that is not persisted");
    ValidationUtil.checkArgument(
        FileLocations.isRootNodeFilePath(node.path().get()),
        "Cannot derive version from a non-root node");
    return FileLocations.versionFromNodeFilePath(node.path().get());
  }

  public static LakehouseDef findLakehouseDef(LakehouseStorage storage, TreeRoot node) {
    return ObjectDefinitions.readLakehouseDef(storage, node.lakehouseDefFilePath());
  }

  public static TreeRoot findLatestRoot(LakehouseStorage storage) {
    // TODO: this should be improved by adding a minimum version file
    //  so that versions that are too old can be deleted in storage.
    //  Unlike the version hint file, this minimum file must exist if the minimum version is greater
    // than zero,
    //  and the creation of this file should require a global lock
    long latestVersion = 0;
    try (InputStream versionHintStream =
        storage.startRead(FileLocations.LATEST_VERSION_HINT_FILE_PATH)) {
      String versionHintText = FileUtil.readToString(versionHintStream);
      latestVersion = Long.parseLong(versionHintText);
    } catch (StorageFileOpenFailureException | StorageReadFailureException | IOException e) {
      LOG.warn("Failed to read latest version hint file, fallback to search from version 0", e);
    }

    String rootNodeFilePath = FileLocations.rootNodeFilePath(latestVersion);

    while (true) {
      long nextVersion = latestVersion + 1;
      String nextRootNodeFilePath = FileLocations.rootNodeFilePath(latestVersion);
      if (!storage.exists(nextRootNodeFilePath)) {
        break;
      }
      rootNodeFilePath = nextRootNodeFilePath;
      latestVersion = nextVersion;
    }

    TreeRoot root = TreeOperations.readRootNodeFile(storage, rootNodeFilePath);
    return root;
  }

  public static Optional<TreeRoot> findRootForVersion(LakehouseStorage storage, long version) {
    TreeRoot latest = findLatestRoot(storage);
    ValidationUtil.checkArgument(
        latest.path().isPresent(), "latest tree root must be persisted with a path in storage");
    long latestVersion = FileLocations.versionFromNodeFilePath(latest.path().get());
    ValidationUtil.checkArgument(
        version <= latestVersion,
        "Version %d must not be higher than latest version %d",
        version,
        latestVersion);

    TreeRoot current = latest;
    while (current.previousRootNodeFilePath().isPresent()) {
      TreeRoot previous =
          TreeOperations.readRootNodeFile(storage, current.previousRootNodeFilePath().get());
      if (version == FileLocations.versionFromNodeFilePath(previous.path().get())) {
        return Optional.of(previous);
      }
      current = previous;
    }

    return Optional.empty();
  }

  public static TreeRoot findRootBeforeTimestamp(LakehouseStorage storage, long timestampMillis) {
    TreeRoot latest = findLatestRoot(storage);
    ValidationUtil.checkArgument(
        latest.createdAtMillis().isPresent(),
        "latest tree root must be persisted with a timestamp in storage");
    long latestCreatedAtMillis = latest.createdAtMillis().get();
    ValidationUtil.checkArgument(
        timestampMillis <= latestCreatedAtMillis,
        "Timestamp %d must not be higher than latest created version %d",
        timestampMillis,
        latestCreatedAtMillis);

    TreeRoot current = latest;
    while (current.previousRootNodeFilePath().isPresent()) {
      TreeRoot previous =
          TreeOperations.readRootNodeFile(storage, current.previousRootNodeFilePath().get());
      ValidationUtil.checkArgument(
          previous.createdAtMillis().isPresent(),
          "Tree root must be persisted with a timestamp in storage");
      if (timestampMillis > previous.createdAtMillis().get()) {
        return previous;
      }
      current = previous;
    }

    return current;
  }

  public static List<NodeKeyTableRow> getNodeKeyTable(LakehouseStorage storage, TreeNode node) {
    List<NodeKeyTableRow> pendingList = node.pendingChanges();
    if (node.path().isEmpty()) {
      return pendingList;
    }

    List<NodeKeyTableRow> result = Lists.newArrayList();
    int pendingIndex = 0;

    try (LocalInputStream stream = storage.startReadLocal(node.path().get());
        BufferAllocator allocator = storage.getArrowAllocator();
        ArrowFileReader reader = new ArrowFileReader(stream.channel(), allocator)) {
      for (ArrowBlock arrowBlock : reader.getRecordBlocks()) {
        reader.loadRecordBatch(arrowBlock);
        VectorSchemaRoot schemaRoot = reader.getVectorSchemaRoot();
        VarCharVector keysVector = (VarCharVector) schemaRoot.getVector(NODE_FILE_KEY_COLUMN_INDEX);
        VarCharVector valuesVector =
            (VarCharVector) schemaRoot.getVector(NODE_FILE_VALUE_COLUMN_INDEX);

        for (int i = 0; i < keysVector.getValueCount(); i++) {
          String key = new String(keysVector.get(i), StandardCharsets.UTF_8);
          if (isSystemKey(key)) {
            continue;
          }

          while (pendingIndex < pendingList.size()
              && pendingList.get(pendingIndex).key().compareTo(key) < 0) {
            result.add(pendingList.get(pendingIndex));
            pendingIndex++;
          }

          if (pendingIndex < pendingList.size()
              && pendingList.get(pendingIndex).key().equals(key)) {
            if (pendingList.get(pendingIndex).value().isPresent()) {
              String newValue = pendingList.get(pendingIndex).value().get();
              result.add(ImmutableNodeKeyTableRow.builder().key(key).value(newValue).build());
            }
            pendingIndex++;
          } else {
            String value = new String(valuesVector.get(i), StandardCharsets.UTF_8);
            result.add(ImmutableNodeKeyTableRow.builder().key(key).value(value).build());
          }
        }
      }
      while (pendingIndex < pendingList.size()) {
        result.add(pendingList.get(pendingIndex));
        pendingIndex++;
      }
      return result;
    } catch (IOException e) {
      throw new StorageReadFailureException(e);
    }
  }

  public static Iterable<TreeRoot> listRoots(LakehouseStorage storage) {
    return new TreeRootIterable(storage, findLatestRoot(storage));
  }

  private static class TreeRootIterable implements Iterable<TreeRoot> {

    private final LakehouseStorage storage;
    private final TreeRoot latest;

    TreeRootIterable(LakehouseStorage storage, TreeRoot latest) {
      this.storage = storage;
      this.latest = latest;
    }

    @Override
    public Iterator<TreeRoot> iterator() {
      return new LakehouseVersionIterator(storage, latest);
    }
  }

  private static class LakehouseVersionIterator implements Iterator<TreeRoot> {

    private final LakehouseStorage storage;
    private final TreeRoot latest;
    private TreeRoot current;

    LakehouseVersionIterator(LakehouseStorage storage, TreeRoot latest) {
      this.storage = storage;
      this.latest = latest;
      this.current = null;
    }

    @Override
    public boolean hasNext() {
      return current == null || current.previousRootNodeFilePath().isPresent();
    }

    @Override
    public TreeRoot next() {
      if (current == null) {
        this.current = latest;
      } else {
        this.current =
            TreeOperations.readRootNodeFile(storage, current.previousRootNodeFilePath().get());
      }
      return current;
    }
  }

  public static Optional<String> searchValue(
      LakehouseStorage storage, TreeNode startNode, String key) {
    TreeNode currentNode = startNode;
    while (true) {
      NodeSearchResult searchResult = currentNode.search(key);
      if (searchResult.value().isPresent()) {
        return Optional.of(searchResult.value().get());
      }

      if (currentNode.path().isPresent()) {
        searchResult = searchInPersistedNode(storage, currentNode.path().get(), key);
      }

      if (searchResult.value().isPresent()) {
        return Optional.of(searchResult.value().get());
      }

      if (searchResult.nodePointer().isEmpty()) {
        return Optional.empty();
      }

      currentNode = readRootNodeFile(storage, searchResult.nodePointer().get());
    }
  }

  public static void setValue(LakehouseStorage storage, TreeRoot root, String key, String value) {
    // TODO: implement actual algorithm
    root.set(key, value);
  }

  public static void removeKey(LakehouseStorage storage, TreeRoot root, String key) {
    // TODO: implement actual algorithm
    root.set(key, null);
  }

  public static NodeSearchResult searchInPersistedNode(
      LakehouseStorage storage, String nodePath, String key) {
    try (LocalInputStream stream = storage.startReadLocal(nodePath);
        BufferAllocator allocator = storage.getArrowAllocator()) {
      VarCharVector searchKeyVector = createSearchKey(allocator, key);
      try (ArrowFileReader reader = new ArrowFileReader(stream.channel(), allocator)) {

        for (ArrowBlock arrowBlock : reader.getRecordBlocks()) {
          reader.loadRecordBatch(arrowBlock);
          VectorSchemaRoot schemaRoot = reader.getVectorSchemaRoot();
          VarCharVector keyVector =
              (VarCharVector) schemaRoot.getVector(NODE_FILE_KEY_COLUMN_INDEX);
          NodeVarCharComparator comparator = new NodeVarCharComparator();
          comparator.attachVector(keyVector);

          int result =
              VectorSearcher.binarySearch(
                  keyVector, comparator, searchKeyVector, NODE_FILE_KEY_COLUMN_INDEX);
          if (result >= 0) {
            VarCharVector valueVector =
                (VarCharVector) schemaRoot.getVector(NODE_FILE_VALUE_COLUMN_INDEX);
            String value = valueVector.isNull(result) ? null : new String(valueVector.get(result));
            searchKeyVector.close();
            reader.close();
            return ImmutableNodeSearchResult.builder()
                .value(Optional.ofNullable(value))
                .nodePointer(Optional.empty())
                .build();
          }
        }
      } finally {
        searchKeyVector.close();
      }
    } catch (IOException e) {
      throw new StorageReadFailureException(e);
    }

    return ImmutableNodeSearchResult.builder()
        .value(Optional.empty())
        .nodePointer(Optional.empty())
        .build();
  }

  private static VarCharVector createSearchKey(BufferAllocator allocator, String key) {
    VarCharVector searchKeyVector = new VarCharVector("searchKey", allocator);
    searchKeyVector.allocateNew(1);
    searchKeyVector.setSafe(NODE_FILE_KEY_COLUMN_INDEX, key.getBytes(StandardCharsets.UTF_8));
    searchKeyVector.setValueCount(1);
    return searchKeyVector;
  }

  private static boolean isSystemKey(String key) {
    return key.equals(ObjectKeys.CREATED_AT_MILLIS)
        || key.equals(ObjectKeys.NUMBER_OF_KEYS)
        || key.equals(ObjectKeys.LAKEHOUSE_DEFINITION)
        || key.equals(ObjectKeys.PREVIOUS_ROOT_NODE)
        || key.equals(ObjectKeys.ROLLBACK_FROM_ROOT_NODE);
  }
}
