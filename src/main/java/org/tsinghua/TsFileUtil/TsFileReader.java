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
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.v4.ITsFileReader;
import org.apache.tsfile.read.v4.TsFileReaderBuilder;

import java.io.File;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class TsFileReader {

    public static void main(String[] args) {
        long startTime = 1739460644422L;
        File f = FSFactoryProducer.getFSFactory().getFile("/Volumes/ycy/tsfileExp/UTSD-12G/data/tsfile/tsfileutsd-0.tsfile");
        int index = 0;
        long start = System.currentTimeMillis();
        long min_time = 1739460644422L;
        long max_time = 1739460648534L;
        long minus = max_time - min_time;
        Random random = new Random();
        int length = 2000;
        Set<String> device_id = new HashSet<>();
        int MAX_LENGTH = 500;
        int count = 0;
        try(ITsFileReader reader = new TsFileReaderBuilder().file(f).build()) {
            while (count < MAX_LENGTH) {
                long start_time = random.nextLong() % minus + min_time;
                long end_time = start_time + length;
                ResultSet resultSet = reader.query("dataset", Arrays.asList("item_id", "value"), start_time, end_time);
                while (resultSet.next()) {
                    long time = resultSet.getLong("Time");
                    String item_id = resultSet.getString("item_id");
                    Double value = resultSet.getDouble("value");
                    index++;
                }
                count++;
            }
            System.out.println(index);
            System.out.println(count);
            System.out.println("Time: " + (System.currentTimeMillis() - start));

        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
