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

import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.tsfile.read.v4.ITsFileReader;
import org.apache.tsfile.read.v4.TsFileReaderBuilder;

import java.io.File;
import java.sql.ResultSet;
import java.util.List;

public class TsFileReader {

    public static void main(String[] args) {

        File f = FSFactoryProducer.getFSFactory().getFile("/Users/ycycse/WorkSpace/TsFileTransformer/utsd.tsfile");

        try(ITsFileReader reader = new TsFileReaderBuilder().file(f).build()){
            List<TableSchema> tableSchemaList = reader.getAllTableSchema();
            for(TableSchema tableSchema : tableSchemaList){
                System.out.println(tableSchema);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
