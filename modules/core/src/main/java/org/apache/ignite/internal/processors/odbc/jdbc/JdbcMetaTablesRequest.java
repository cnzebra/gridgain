/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.odbc.jdbc;

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * JDBC tables metadata request.
 */
public class JdbcMetaTablesRequest extends JdbcRequest {
    /** Schema search pattern. */
    private String schemaName;

    /** Table search pattern. */
    private String tblName;

    /** Table types. */
    private String[] tblTypes;

    /**
     * Default constructor is used for deserialization.
     */
    JdbcMetaTablesRequest() {
        super(META_TABLES);
    }

    /**
     * @param schemaName Schema search pattern.
     * @param tblName Table search pattern.
     * @param tblTypes Table types.
     */
    public JdbcMetaTablesRequest(String schemaName, String tblName, String[] tblTypes) {
        super(META_TABLES);

        this.schemaName = schemaName;
        this.tblName = tblName;
        this.tblTypes = tblTypes;
    }

    /**
     * @return Schema search pattern.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * @return Table search pattern.
     */
    public String tableName() {
        return tblName;
    }

    /**
     * @return Table types.
     */
    public String[] tableTypes() {
        return tblTypes;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer,
        JdbcProtocolContext protoCtx) throws BinaryObjectException {
        super.writeBinary(writer, protoCtx);

        writer.writeString(schemaName);
        writer.writeString(tblName);

        if (protoCtx.isTableTypesSupported())
            writer.writeStringArray(tblTypes);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader,
        JdbcProtocolContext protoCtx) throws BinaryObjectException {
        super.readBinary(reader, protoCtx);

        schemaName = reader.readString();
        tblName = reader.readString();

        try {
            if (protoCtx.isTableTypesSupported())
                tblTypes = reader.readStringArray();
        }
        catch (Exception ignored) {
            // TODO: GG-25595 remove when version 8.7.X support ends
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcMetaTablesRequest.class, this);
    }
}