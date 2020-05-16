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
package org.apache.parquet.format;

import java.io.IOException;
import java.io.InputStream;

public interface BlockCipher{
  
  // TODO doc AAD

  public interface Encryptor{
    /**
     * Encrypts the plaintext.
     * Make sure the returned contents starts at offset 0 and fills up the byte array.
     * Input plaintext starts at offset 0, and has a length of plaintext.length.
     * @param plaintext
     * @return ciphertext
     * @throws IOException
     */
    public byte[] encrypt(byte[] plaintext, byte[] AAD) throws IOException;
  }


  public interface Decryptor{  
    /**
     * Decrypts the ciphertext. 
     * Make sure the returned plaintext starts at offset 0 and and fills up the byte array.
     * Input ciphertext starts at offset 0, and has a length of ciphertext.length.
     * @param lengthAndCiphertext
     * @return plaintext
     * @throws IOException
     */
    public byte[] decrypt(byte[] lengthAndCiphertext, byte[] AAD) throws IOException;

    public byte[] decryptInputStream(InputStream from, byte[] AAD) throws IOException;
  }
}


