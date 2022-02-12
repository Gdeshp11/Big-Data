import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.security.KeyStore.Entry;
import java.util.*;

import javax.swing.plaf.basic.BasicInternalFrameTitlePane.CloseAction;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import java.text.DecimalFormat;

import myMap.myMap;
// import Map.Map;
import Reduce.Reduce;

public class stockMarket {
    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("Usage: stockMarket <in_dir> <out_dir> <number of matrix multiplications>");
            System.exit(2);
        }

        HashMap<String, Float[][]> closing_prices = myMap.readData("../data/");

        for (java.util.Map.Entry<String, Float[][]> item : closing_prices.entrySet()) {
            File f = new File(args[0] + "/" + item.getKey().substring(0, item.getKey().indexOf(".")));

            if (f.createNewFile()) {
                System.out.println("created file " + item.getKey());
                FileWriter fwrite = new FileWriter(f);

                for (int i = 0; i < 3; i++) {
                    for (int j = 0; j < 3; j++) {
                        Float matValue = item.getValue()[i][j];
                        if (matValue != 0) {
                            // M,i,j,Mij
                            fwrite.write(i + "," + j + "," + matValue + "\n");
                        }
                    }
                }
                fwrite.close();
            } else {
                System.err.println("error creating file..");
                System.exit(-1);
            }
        }
        String out_dir = args[1];
        String in_dir = "HDFS";
        Integer num_multiplication = Integer.parseInt(args[2]);

        for (int i = 1; i <= num_multiplication; ++i) {

            Configuration conf = new Configuration();
            // M is an m-by-n matrix; N is an n-by-p matrix.
            conf.set("m", "3");
            conf.set("n", "3");
            conf.set("p", "3");
            // conf.set("X", args[2]);

            @SuppressWarnings("deprecation")
            Job job = new Job(conf, "stockMarket");
            job.setJarByClass(stockMarket.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setMapperClass(myMap.class);
            job.setReducerClass(Reduce.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job, new Path(in_dir));
            FileOutputFormat.setOutputPath(job, new Path(out_dir));

            job.waitForCompletion(true);
            in_dir = out_dir;
            out_dir = args[1] + "/" + i;
        }
        System.exit(0);
    }

}
