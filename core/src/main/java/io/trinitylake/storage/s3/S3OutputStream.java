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

import com.google.common.io.CountingOutputStream;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import io.trinitylake.exception.StorageWriteFailureException;
import io.trinitylake.exception.StreamOpenFailureException;
import io.trinitylake.storage.PositionOutputStream;
import io.trinitylake.storage.URI;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

class S3OutputStream extends PositionOutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(S3OutputStream.class);

  private final S3AsyncClient s3;
  private final URI uri;

  private CountingOutputStream stagingFileStream;
  private final File stagingDirectory;
  private File stagingFile;

  private long pos = 0;
  private boolean closed = false;

  private RetryPolicy<Object> retryPolicy =
      RetryPolicy.builder()
          .onRetry(
              e ->
                  LOG.warn(
                      "Retrying write to S3, reopening stream (attempt {})", e.getAttemptCount()))
          .onFailure(
              e ->
                  LOG.error(
                      "Failed to read from S3 input stream after exhausting all retries",
                      e.getException()))
          .withMaxRetries(3)
          .build();

  public S3OutputStream(
      S3AsyncClient s3, URI uri, AmazonS3StorageOpsProperties s3StorageProperties) {
    this.s3 = s3;
    this.uri = uri;
    // TODO: given the file size is limited to 1MB by default, is it still worth buffering to
    // staging file?
    //  pending benchmarking to confirm.
    this.stagingDirectory = s3StorageProperties.stagingDirectory();

    try {
      newStream();
    } catch (IOException e) {
      throw new StreamOpenFailureException(e, "Failed to open stream: %s", uri);
    }
  }

  @Override
  public long getPos() {
    return pos;
  }

  @Override
  public void flush() throws IOException {
    stagingFileStream.flush();
  }

  @Override
  public void write(int b) throws IOException {
    stagingFileStream.write(b);
    pos += 1;
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    stagingFileStream.write(b, off, len);
    pos += len;
  }

  private void newStream() throws IOException {
    if (stagingFileStream != null) {
      stagingFileStream.close();
    }

    stagingFile = File.createTempFile("s3-write-", ".tmp", stagingDirectory);
    stagingFile.deleteOnExit();

    OutputStream outputStream = Files.newOutputStream(stagingFile.toPath());
    stagingFileStream = new CountingOutputStream(new BufferedOutputStream(outputStream));
  }

  @Override
  public void close() throws IOException {
    close(true);
  }

  private void close(boolean completeUploads) throws IOException {
    if (closed) {
      return;
    }

    super.close();
    closed = true;

    try {
      stagingFileStream.close();
      if (completeUploads) {
        s3.putObject(
                PutObjectRequest.builder()
                    .bucket(uri.authority())
                    .key(uri.path())
                    .ifNoneMatch("*")
                    .build(),
                AsyncRequestBody.fromFile(stagingFile))
            .get();
      }
    } catch (ExecutionException | InterruptedException e) {
      throw new StorageWriteFailureException(
          e, "Fail to upload to %s from staging file: %s", uri, stagingFile);
    } finally {
      cleanUpStagingFile();
    }
  }

  private void cleanUpStagingFile() {
    try {
      Failsafe.with(retryPolicy).run(stagingFile::delete);
    } catch (FailsafeException e) {
      LOG.warn("Failed to delete staging file: {}", stagingFile, e);
    }
  }

  @SuppressWarnings({"checkstyle:NoFinalizer", "Finalize"})
  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    if (!closed) {
      close(false);
    }
  }
}