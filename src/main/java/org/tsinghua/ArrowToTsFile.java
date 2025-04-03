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

package org.tsinghua;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.ColumnSchemaBuilder;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.write.record.Tablet;
import org.tsinghua.TsFileUtil.TsFileWriter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class ArrowToTsFile {

    public static void parseTsFileFromJson(){
        String jsonPath = "/Volumes/ycy/tsfileExp/UTSD-12G/data/json/utsd.json";
        ObjectMapper objectMapper = new ObjectMapper();
        TableSchema tableSchema = new TableSchema("dataset", Arrays.asList(
                new ColumnSchemaBuilder()
                        .name("item_id")
                        .dataType(TSDataType.STRING)
                        .category(Tablet.ColumnCategory.TAG)
                        .build(),
                new ColumnSchemaBuilder()
                        .name("value")
                        .dataType(TSDataType.DOUBLE)
                        .category(Tablet.ColumnCategory.FIELD)
                        .build()
        ));
        Random random = new Random();
        final int MAX_LINE_NUMBERS = 1000000;
        try(BufferedReader reader = new BufferedReader(new FileReader(jsonPath))){
            String line;
            List<Long> timestampList = new ArrayList<>();
            List<Double> targetList = new ArrayList<>();
            List<String> itemList = new ArrayList<>();
            int tsFileIndex = 0;
            while((line = reader.readLine()) != null){
                JsonNode jsonNode = objectMapper.readTree(line);

                String itemId = jsonNode.path("item_id").asText();
                JsonNode timestamp = jsonNode.path("timestamp");
                JsonNode target = jsonNode.path("target");

                assert timestamp.isArray();
                assert target.isArray();
                assert timestamp.size() == target.size();

                for (int i = 0; i < timestamp.size(); i++) {
                    itemList.add(itemId);
                }

                for(int i = 0; i < timestamp.size(); i++){
                    timestampList.add(timestamp.get(i).asLong() + random.nextInt(5));
                }

                for(int i = 0; i < target.size(); i++){
                    targetList.add(target.get(i).asDouble());
                }

                if(timestampList.size() > MAX_LINE_NUMBERS){
                    TsFileWriter.writeToTsFile(tableSchema, itemList, timestampList, targetList, "/Volumes/ycy/tsfileExp/data/tsfile/" + "utsd" + "-" + "100w" + ".tsfile");
                    timestampList.clear();
                    targetList.clear();
                    itemList.clear();
                    tsFileIndex++;
                    return;
                }
            }
            if(!timestampList.isEmpty()) {
                TsFileWriter.writeToTsFile(tableSchema, itemList, timestampList, targetList, "/Volumes/ycy/tsfileExp/data/tsfile/diff/" + "utsd" + "-" + tsFileIndex + ".tsfile");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        parseTsFileFromJson();
    }

}
