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
package com.facebook.presto.parquet.crypto.retriever;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Assembly 4 parts: IV, key version, encrypted working key, master key name into byte array
 *
 * DeAssembly array back to the 4 parts.
 *
 * The byte array is constructed with a variable length encoding format.
 *
 * The assembled byte array follows this format:
 * V - (T - LLLL - DD...D) - (T - LLLL - DD...D) - (T - LLLL - DD...D) - (T - LLLL - DD...D)
 *     |_________________|
 *      Encoded metadata
 *
 * Legend:
 * V = Assembler version -> 1 byte to denote the version of the assembler used to generate the byte array.
 *                          This allows for future backward compatibility if encoding changes.
 * T = MetadataType -> 1 byte representing the type of byte data that follows: IV, key version,
 *                     encrypted working key or master key name.
 * L = Length byte -> 4 bytes to indicate the number of data bytes that follow for this encoding.
 * D = Data byte -> length number of bytes which hold the encoded data.
 *
 */

public class KeyMetadataAssembler
{
    static final byte ASSEMBLER_VERSION = 1;
    static final byte LENGTH_BYTES = 4;

    public enum MetadataType
    {
        IV((byte) 1),
        KEYVERSION((byte) 2),
        EEK((byte) 3),
        MKNAME((byte) 4);

        private byte val;

        MetadataType(byte val)
        {
            this.val = val;
        }

        public byte getVal()
        {
            return val;
        }
    }

    private KeyMetadataAssembler()
    {
    }

    public static byte[] assembly(String name, byte[] iv, int version, byte[] eek)
            throws IOException
    {
        if (iv == null || eek == null) {
            throw new IllegalArgumentException("iv and eek must not be null");
        }
        if (iv.length != 16) {
            throw new IllegalArgumentException("IV length must be 16 bytes");
        }
        if (version < 0) {
            throw new IllegalArgumentException("version must be greater than equal to 0");
        }
        if (eek.length != 16 && eek.length != 24 && eek.length != 32) {
            throw new IllegalArgumentException("eek length must be either 16, 24 or 32 bytes");
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write(new byte[]{ASSEMBLER_VERSION});

        addTypeData(baos, MetadataType.IV, iv);
        addTypeData(baos, MetadataType.KEYVERSION, ByteBuffer.allocate(4).putInt(version).array());
        addTypeData(baos, MetadataType.EEK, eek);
        addTypeData(baos, MetadataType.MKNAME, name.getBytes(StandardCharsets.UTF_8));

        return baos.toByteArray();
    }

    /**
     * Given a metadata byte array and the offset of the first length byte,
     * returns the number of bytes of data that follows.
     * @param metadata -> a byte array, generated by the assembly function,
     *                 which holds the variable length encoding of key metadata.
     * @param offset -> an int representing the offset of the first length byte in the metadata byte array.
     * @return the length representing the number of bytes of data that follows.
     * @throws IOException
     */
    protected static int getDataLength(byte[] metadata, int offset)
            throws IOException
    {
        int remainingSpace = metadata.length - offset;
        if (remainingSpace < LENGTH_BYTES) {
            throw new IndexOutOfBoundsException("Not enough space in metadata array after offset to copy" +
                    " length bytes.");
        }
        byte[] lengthBytes = new byte[LENGTH_BYTES];
        System.arraycopy(metadata, offset, lengthBytes, 0, LENGTH_BYTES);
        return ByteBuffer.wrap(lengthBytes).getInt();
    }

    /**
     * Given a metadata byte array and the offset of the first length byte,
     * returns a String holding the master key name.
     * Use for MetadataType: MKNAME
     * Note: Caller must iterate through metadata byte array and validate the MetadataType
     * preceding the input offset value.
     * @param metadata -> a byte array, generated by the assembly function,
     *                 which holds the variable length encoding of key metadata.
     * @param offset -> an int representing the offset of the first length byte in the metadata byte array.
     * @return a String representing master key name.
     * @throws IOException
     */
    public static String getName(byte[] metadata, int offset)
            throws IOException
    {
        byte[] nameBytes = getDataByteArray(metadata, offset);
        return new String(nameBytes, StandardCharsets.UTF_8);
    }

    /**
     * Given a metadata byte array and the offset of the first length byte,
     * returns a byte array holding the IV.
     * Use for MetadataType: IV
     * Note: Caller must iterate through metadata byte array and validate the MetadataType
     * preceding the input offset value.
     * @param metadata -> a byte array, generated by the assembly function,
     *                 which holds the variable length encoding of key metadata.
     * @param offset -> an int representing the offset of the first length byte in the metadata byte array.
     * @return a byte array representing the IV (initialization vector)
     * @throws IOException
     */
    public static byte[] getIv(byte[] metadata, int offset)
            throws IOException
    {
        return getDataByteArray(metadata, offset);
    }

    /**
     * Given a metadata byte array and the offset of the first length byte,
     * returns an int representing the version of the master key.
     * Use for MetadataType: KEYVERSION
     * Note: Caller must iterate through metadata byte array and validate the MetadataType
     * preceding the input offset value.
     * @param metadata -> a byte array, generated by the assembly function,
     *                 which holds the variable length encoding of key metadata.
     * @param offset -> an int representing the offset of the first length byte in the metadata byte array.
     * @return an int representing the version of the master key.
     * @throws IOException
     */
    public static int getVersion(byte[] metadata, int offset)
            throws IOException
    {
        byte[] versionBytes = getDataByteArray(metadata, offset);
        return ByteBuffer.wrap(versionBytes).getInt();
    }

    /**
     * Given a metadata byte array and the offset of the first length byte,
     * returns a byte array holding the EEK.
     * Use for MetadataType: EEK
     * Note: Caller must iterate through metadata byte array and validate the MetadataType
     * preceding the offset value input.
     * @param metadata -> a byte array, generated by the assembly function,
     *                 which holds the variable length encoding of key metadata.
     * @param offset -> an int representing the offset of the first length byte in the metadata byte array.
     * @return a byte array representing the encrypted working key.
     * @throws IOException
     */
    public static byte[] getEEK(byte[] metadata, int offset)
            throws IOException
    {
        return getDataByteArray(metadata, offset);
    }

    /**
     * Writes 1 byte for MetadataType, 4 bytes to represent length of data to follow
     * and finally length bytes of actual data.
     * @param baos ByteArrayOutputStream used to build the byte array during assembly.
     * @param type MetadataType enum which represents the type of the data being written.
     * @param data the data to be written to the byte array.
     * @throws IOException
     */
    private static void addTypeData(ByteArrayOutputStream baos, MetadataType type, byte[] data)
            throws IOException
    {
        int length = data.length;
        baos.write(new byte[]{type.getVal()});
        baos.write(ByteBuffer.allocate(LENGTH_BYTES).putInt(length).array());
        baos.write(data);
    }

    /**
     * Given metadata byte array and offset of first length byte,
     * Returns a byte array containing the data bytes of the variable length code.
     * @param metadata variable length encoding generated through the assembly function.
     * @param offset index of the first length byte of an encoded metadata in the metadata byte array.
     * @return byte array of the data bytes
     * @throws IOException
     */
    private static byte[] getDataByteArray(byte[] metadata, int offset)
            throws IOException
    {
        int length = getDataLength(metadata, offset);
        int dataStart = offset + LENGTH_BYTES;
        byte[] dataBytes = new byte[length];
        System.arraycopy(metadata, dataStart, dataBytes, 0, length);
        return dataBytes;
    }
}
