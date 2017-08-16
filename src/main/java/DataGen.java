import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.ArrayList;
import java.util.Random;

/**
 * Created by root on 8/4/17.
 */
public class DataGen {
    private static ArrayList<String> country = new ArrayList<>();
    private static ArrayList<String> phone = new ArrayList<>();
    private static int clen = 0, plen =0;
    static {
        try {
            BufferedReader cbr = new BufferedReader(new FileReader("country"));
            BufferedReader pbr = new BufferedReader(new FileReader("phone"));
            String line = cbr.readLine();
            while(line != null){
                if(line.length()>0){
                    country.add(line);
                    clen++;
                }
                line = cbr.readLine();
            }
            line = pbr.readLine();
            while(line != null){
                if(line.length()>0){
                    phone.add(line);
                    plen++;
                }
                line = pbr.readLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void main(String args[]){
        long expectedRows = 1000000000;
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://bdpe822n3:9000");

        try {
            FileSystem hdfs = FileSystem.get(conf);
            Path targetPath = new Path("/user/hive/metastore/vectorizationdata");
            hdfs.delete(targetPath,true);
            OutputStream os = hdfs.create(targetPath);
            BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os), 32768);
            Random rd = new Random();
            for (long i = 0; i < expectedRows; i++) {
                br.write(country.get(rd.nextInt(clen)) + "|" + phone.get(rd.nextInt(plen)) + "|" + Integer.toString(rd.nextInt(7))+ "\n");
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
