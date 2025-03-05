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
package io.trinitylake.util;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.substrait.proto.ReadRel;
import io.trinitylake.exception.InvalidArgumentException;

public class SubstraitUtil {

  private SubstraitUtil() {}

  public static ReadRel loadSubstraitReadReal(ByteString substraitReadRelBytes) {
    ValidationUtil.checkArgument(
        substraitReadRelBytes != null && !substraitReadRelBytes.isEmpty(),
        "substraitReadRelBytes cannot be null or empty.");

    try {
      return ReadRel.parseFrom(substraitReadRelBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new InvalidArgumentException(
          "Invalid Substrait read relation: Unable to parse bytes.", e);
    }
  }
}
