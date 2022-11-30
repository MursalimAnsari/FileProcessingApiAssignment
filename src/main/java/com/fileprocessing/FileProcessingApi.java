package com.fileprocessing;


import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class FileProcessingApi {


      final static String INPUT_FILE_ONE_PATH =       "F:\\file-processing-test-files\\file_one.csv";
      final static String INPUT_FILE_TWO_PATH =       "F:\\file-processing-test-files\\file_two.csv";
      final static String FILE_OUTPUT_ADDED_PATH =    "F:\\file-processing-test-files\\file_output_added.csv";
      final static String FILE_OUTPUT_REMOVED_PATH =  "F:\\file-processing-test-files\\file_output_removed.csv";
      final static String FILE_OUTPUT_MODIFIED_PATH = "F:\\file-processing-test-files\\file_output_modified.csv";


//      // test cases::
//      final static String INPUT_FILE_ONE_PATH =       "F:\\testing-file-processing-api\\test_file_one.csv";
//      final static String INPUT_FILE_TWO_PATH =       "F:\\testing-file-processing-api\\test_file_two.csv";
//      final static String FILE_OUTPUT_ADDED_PATH =    "F:\\testing-file-processing-api\\test_file_output_added.csv";
//      final static String FILE_OUTPUT_REMOVED_PATH =  "F:\\testing-file-processing-api\\test_file_output_removed.csv";
//      final static String FILE_OUTPUT_MODIFIED_PATH = "F:\\testing-file-processing-api\\test_file_output_modified.csv";







    public static void main(String[] args) throws IOException {

        BufferedWriter bw1 = null;
        BufferedWriter bw2 = null;
        BufferedWriter bw3 = null;


           try{

              final Map<String,String> recordsFileOneMap= readFileRecordsToMap(INPUT_FILE_ONE_PATH);

              final Map<String,String> recordsFileTwoMap= readFileRecordsToMap(INPUT_FILE_TWO_PATH);


               bw1 = new BufferedWriter(new FileWriter(FILE_OUTPUT_ADDED_PATH));
               bw2 = new BufferedWriter(new FileWriter(FILE_OUTPUT_REMOVED_PATH));
               bw3 = new BufferedWriter(new FileWriter(FILE_OUTPUT_MODIFIED_PATH));


               processFileData(recordsFileOneMap,recordsFileTwoMap,bw1,bw2, bw3);

        } catch (IOException e){
            e.printStackTrace();
        } finally {

            if(bw1!=null){
                bw1.close();
            }

            if(bw2!=null){
                bw2.close();
            }

            if(bw3!=null){
                bw3.close();
            }

        }

    }


    private static void processFileData(Map<String,String> recordsFileOneMap, Map<String,String> recordsFileTwoMap, BufferedWriter bw1, BufferedWriter bw2, BufferedWriter bw3){

        System.out.println("File Processing started...");

        recordsFileTwoMap.entrySet().stream().forEach(record_key_file_two_entry->{
            String key = record_key_file_two_entry.getKey();
            String value = record_key_file_two_entry.getValue();

            String keyValue = key + "," +value;

            // if records in file2 but not in file1
            if(!recordsFileOneMap.containsKey(key)) {
                try {
                    bw1.write(keyValue);
                    bw1.newLine();
                    bw1.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            } else {

                    // records of file2 which exists in file1(common records) but value is no same
                    if(!value.contentEquals(recordsFileOneMap.get(key))){
                        try{

                            bw3.write(keyValue);
                            bw3.newLine();
                            bw3.flush();

                        }catch (IOException e){
                            e.printStackTrace();
                        }
                    }
                }
        });

        recordsFileOneMap.entrySet().stream().forEach(
                record_key_file_one_entry->{
                    String key   = record_key_file_one_entry.getKey();
                    String value = record_key_file_one_entry.getValue();
                    String keyValue = key +","+value;

                    if(!recordsFileTwoMap.containsKey(key)){
                        try {
                            bw2.write(keyValue);
                            bw2.newLine();
                            bw2.flush();
                        }catch (IOException e){
                            e.printStackTrace();
                        }
                    }
                }
        );


            System.out.println("File processing completed...");

       }


    static Map<String,String> readFileRecordsToMap(String fileName) throws FileNotFoundException,IOException{

            Map<String,String> map = new HashMap<>();

            BufferedReader br = new BufferedReader(new FileReader(fileName));

            String line =null;

            while((line =br.readLine())!=null){

                String[] arr= line.split(",");

                map.put(arr[0], arr[1]);
            }
            return map;
        }

}
