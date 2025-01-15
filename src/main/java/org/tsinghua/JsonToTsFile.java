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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static org.tsinghua.TsFileUtil.TsFileWriter.writeToTsFile;

public class JsonToTsFile {

    public static class Dataset{
        @JsonProperty
        public String item_id;
        @JsonProperty
        public String start;
        @JsonProperty
        public String end;
        @JsonProperty
        public String freq;
        @JsonProperty
        public List<Double> target;
    }

    private static void preProcess() throws IOException {
        String filePath = "/Users/ycycse/WorkSpace/TsFileTransformer/utsd.json";
        String json = new String(Files.readAllBytes(Paths.get(filePath)));
        String validJson = "[" + json.replaceAll("}\\s*\\{", "},{") + "]";

        Files.write(Paths.get("valid_ustd.json"), validJson.getBytes());
        System.out.println("Preprocess done!");
    }

    public static void main(String[] args) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            List<Dataset> datasetList = objectMapper.readValue(new File("/Users/ycycse/WorkSpace/TsFileTransformer/valid_ustd.json"), objectMapper.getTypeFactory().constructCollectionType(List.class, Dataset.class));
            writeToTsFile(datasetList, "/Users/ycycse/WorkSpace/TsFileTransformer/utsd.tsfile");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
