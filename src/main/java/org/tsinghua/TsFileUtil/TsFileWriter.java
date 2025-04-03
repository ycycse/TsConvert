/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.tsinghua.TsFileUtil;

import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.ColumnSchemaBuilder;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.v4.ITsFileWriter;
import org.apache.tsfile.write.v4.TsFileWriterBuilder;
import org.tsinghua.JsonToTsFile;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class TsFileWriter {

    public static void writeToTsFile(TableSchema tableSchema, List<String> itemList, List<Long> timestamp, List<Double> valueList, String targetFilePath) throws IOException {
        System.out.println("Writing to tsfile: " + targetFilePath);
        File tsfile = new File(targetFilePath);
        try(ITsFileWriter writer =
                new TsFileWriterBuilder()
                        .file(tsfile)
                        .tableSchema(tableSchema)
                        .build()){

            Tablet tablet = new Tablet(Arrays.asList("item_id", "value"), Arrays.asList(TSDataType.STRING, TSDataType.DOUBLE), timestamp.size());
            for (int i = 0; i < timestamp.size(); i++) {
                tablet.addTimestamp(i, timestamp.get(i));
                tablet.addValue(i, 0, itemList.get(i));
                tablet.addValue(i, 1, valueList.get(i));
            }
            writer.write(tablet);
        } catch (WriteProcessException e) {
            throw new RuntimeException(e);
        }
    }

    public static void writeToTsFile(List<JsonToTsFile.Dataset> data, String targetTsFilePath) throws IOException, WriteProcessException {
        File tsfile = new File(targetTsFilePath);
        String tableName = "utsd";
        TableSchema tableSchema =
                new TableSchema(
                        tableName,
                        Arrays.asList(
                                new ColumnSchemaBuilder()
                                        .name("device_id")
                                        .dataType(TSDataType.STRING)
                                        .category(Tablet.ColumnCategory.TAG)
                                        .build(),
                                new ColumnSchemaBuilder()
                                        .name("value")
                                        .dataType(TSDataType.DOUBLE)
                                        .category(Tablet.ColumnCategory.FIELD)
                                        .build()
                        )
                );

        long time = 0;
        try (ITsFileWriter writer =
                     new TsFileWriterBuilder()
                             .file(tsfile)
                             .tableSchema(tableSchema)
                             .build()) {
            for (JsonToTsFile.Dataset dataset : data) {
                String itemId = dataset.item_id;
                List<Double> target = dataset.target;
                Tablet tablet = new Tablet(Arrays.asList("item_id", "value"), Arrays.asList(TSDataType.STRING, TSDataType.DOUBLE), target.size());
                for (int i = 0; i < target.size(); i++) {
                    tablet.addTimestamp(i, time);
                    time++;
                    tablet.addValue(i, 0, itemId);
                    tablet.addValue(i, 1, target.get(i));
                }
                writer.write(tablet);
            }
        }
    }
}
