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
package io.trinitylake.storage.s3;

import io.trinitylake.exception.CommitFailureException;
import io.trinitylake.exception.StorageWriteFailureException;
import io.trinitylake.storage.AtomicOutputStream;
import io.trinitylake.storage.CommonStorageOpsProperties;
import io.trinitylake.storage.LiteralURI;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutionException;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

class S3AtomicOutputStream extends AtomicOutputStream {
  private final S3StagingOutputStream delegate;

  S3AtomicOutputStream(
      S3AsyncClient s3,
      LiteralURI uri,
      CommonStorageOpsProperties commonProperties,
      AmazonS3StorageOpsProperties s3Properties) {
    this.delegate =
        new S3StagingOutputStream(s3, uri, commonProperties, s3Properties) {
          @Override
          protected void commit() throws IOException {
            if (isClosed()) {
              return;
            }
            setClosed();
            try {
              getStagingFileStream().close();
              LiteralURI targetURI = getTargetPath();
              getS3Client()
                  .putObject(
                      PutObjectRequest.builder()
                          .bucket(targetURI.authority())
                          .key(targetURI.path())
                          .ifNoneMatch("*")
                          .build(),
                      AsyncRequestBody.fromFile(getStagingFile()))
                  .get();
            } catch (ExecutionException | InterruptedException e) {
              throw new StorageWriteFailureException(
                  e,
                  "Fail to upload to %s from staging file: %s",
                  getTargetPath(),
                  getStagingFile());
            } finally {
              cleanUpStagingFile();
            }
          }
        };
  }

  @Override
  public void flush() throws IOException {
    delegate.flush();
  }

  @Override
  public void write(int b) throws IOException {
    delegate.write(b);
  }

  @Override
  public void write(byte[] bytes) throws IOException {
    delegate.write(bytes);
  }

  @Override
  public void atomicallySeal() throws CommitFailureException, IOException {
    if (delegate.isClosed()) {
      return;
    }
    delegate.close();
  }

  @Override
  public FileChannel channel() {
    // TODO: pass temp file channel here
    return null;
  }
}
