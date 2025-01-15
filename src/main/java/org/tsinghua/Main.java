package org.tsinghua;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class Main {
    private static long time = 0;
    private static void processNumberLine(ArrayList<Long> timestampList, ArrayList<Double> valuesList, String curLine) {
        String[] values = curLine.split("\\s+");
        for (String value : values) {
            if (value.equals("")) {
                continue;
            }
            timestampList.add(time);
            valuesList.add(Double.parseDouble(value));
            time += 1;
        }
    }
    public static void main(String[] args) throws IOException {
        String csvFilePath = "/Users/ycycse/WorkSpace/tsConvert/UTSD/utsd.csv";
        try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {
            String header = br.readLine();
            System.out.println(header);
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");

                String numbers = values[4];
                ArrayList<Long> timestampsList = new ArrayList<>();
                ArrayList<Double> valuesList = new ArrayList<>();
                String curLine = numbers.substring(2);
                System.out.println(curLine);
                while(!curLine.contains("]")){
                    processNumberLine(timestampsList, valuesList, curLine.trim());
                    curLine = br.readLine();
                    System.out.println(curLine);
                }
                processNumberLine(timestampsList, valuesList, curLine.substring(0, curLine.length() - 2));
                time = 0;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
//        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
//        String arrow_path = "/Users/ycycse/WorkSpace/UTSD/UTSD-1G/data-00001-of-00080.arrow";
//
//        try (FileInputStream fileInputStream = new FileInputStream(arrow_path)) {
//            ArrowFileReader arrowFileReader = new ArrowFileReader(fileInputStream.getChannel(), allocator);
//
//            VectorSchemaRoot root = arrowFileReader.getVectorSchemaRoot();
//
//            arrowFileReader.loadNextBatch();
//
//            System.out.println(root.getRowCount());
//
//            arrowFileReader.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }
}