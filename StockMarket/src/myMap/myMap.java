package myMap;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.*;
import java.io.File;
import java.io.FileReader;

public class myMap
        extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        try{
        Configuration conf = context.getConfiguration();
        int m = Integer.parseInt(conf.get("m"));
        int p = Integer.parseInt(conf.get("p"));
        String line = value.toString();
        // (M, i, j, Mij);
        String[] indicesAndValue = line.split(",");
        Text outputKey = new Text();
        Text outputValue = new Text();

        for (int k = 0; k < p; k++) {
            outputKey.set(indicesAndValue[0] + "," + k);
            // outputKey.set(i,k);
            outputValue.set("M" + "," + indicesAndValue[1]
                    + "," + indicesAndValue[2]);
            // outputValue.set(M,j,Mij);
            context.write(outputKey, outputValue);
        }

        for (int i = 0; i < m; i++) {
            outputKey.set(i + "," + indicesAndValue[1]);
            outputValue.set("N," + indicesAndValue[0] + ","
                    + indicesAndValue[2]);
            context.write(outputKey, outputValue);
        }
    }
    catch (Exception e) {
        e.printStackTrace();
    }
    }

    public static HashMap<String, Float[][]> readData(String input_file_dir) {
        HashMap<String, Float[][]> ClosingPricesMap = new HashMap<String, Float[][]>();

        try {
            File f = new File(input_file_dir);

            for (File i : f.listFiles()) {
                if (i.isDirectory())
                    continue;
                else if (i.getName().matches(".*\\.csv")) {

                    List<Float> closingPrices = new ArrayList<>();
                    Float[][] transMatrix = new Float[3][3];

                    System.out.println(i.getName());
                    Scanner scanner = new Scanner(i);
                    scanner.nextLine(); // skip first line
                    while (scanner.hasNextLine()) {
                        closingPrices.add(Float.parseFloat(getClosingPrice(scanner.nextLine())));
                    }
                    transMatrix = getTransMatrix(getAvgPriceMap(closingPrices));
                    ClosingPricesMap.put(i.getName(), transMatrix);
                    scanner.close();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        // System.out.println(closing_prices);
        return ClosingPricesMap;
    }

    private static String getClosingPrice(String line) {
        List<String> values = new ArrayList<String>();
        try (Scanner rowScanner = new Scanner(line)) {
            rowScanner.useDelimiter(",");
            while (rowScanner.hasNext()) {
                values.add(rowScanner.next());
            }
        }
        return values.get(1).substring(1); // get closing price only and skip $ sign
    }

    private static java.util.Map<Integer, Float> getAvgPriceMap(List<Float> closing_prices) {
        java.util.Map<Integer, Float> avgPriceMap = new HashMap<Integer, Float>();
        Integer week = 1;
        for (int i = 0; i < closing_prices.size() - 5; i += 5) {
            Float week_avg = (closing_prices.get(i) + closing_prices.get(i + 1) + closing_prices.get(i + 2)
                    + closing_prices.get(i + 3) + closing_prices.get(i + 4)) / 5;
            // avg.add(week_avg);
            avgPriceMap.put(week++, week_avg);
        }
        return avgPriceMap;
    }

    private static Float[][] getTransMatrix(java.util.Map<Integer, Float> mp) {
        Integer bullTobulltrans = 0;
        Integer bullTobeartrans = 0;
        Integer bullTostagnanttrans = 0;
        Integer bearTobulltrans = 0;
        Integer bearTobeartrans = 0;
        Integer bearTostagnanttrans = 0;
        Integer stagnantTobulltrans = 0;
        Integer stagnantTobeartrans = 0;
        Integer stagnantTostagnanttrans = 0;

        List<String> transitions = new ArrayList<>();
        Float curr_week_price = 0.0F, prev_week_price = 0.0F;
        for (java.util.Map.Entry<Integer, Float> item : mp.entrySet()) {
            curr_week_price = item.getValue();

            if (prev_week_price == 0) {
                prev_week_price = curr_week_price;
                continue;
            } else if (curr_week_price - prev_week_price > 1.0f) {
                transitions.add("bullish");
            } else if (curr_week_price - prev_week_price < 0.0f) {

                transitions.add("bearish");
            } else if (Math.abs(curr_week_price - prev_week_price) <= 0.8f) {
                transitions.add("stagnant");
            }

            prev_week_price = curr_week_price;
        }

        for (int i = 0; i < transitions.size() - 1; ++i) {
            String curr = transitions.get(i);
            String next = transitions.get(i + 1);

            switch (curr) {
                case "bullish": {
                    if (next == "bullish") {
                        bullTobulltrans++;
                    }
                    if (next == "bearish") {
                        bullTobeartrans++;
                    }
                    if (next == "stagnant") {
                        bullTostagnanttrans++;
                    }
                    break;
                }
                case "bearish": {
                    if (next == "bullish") {
                        bearTobulltrans++;
                    }
                    if (next == "bearish") {
                        bearTobeartrans++;
                    }
                    if (next == "stagnant") {
                        bearTostagnanttrans++;
                    }
                    break;
                }
                case "stagnant": {
                    if (next == "bullish") {
                        stagnantTobulltrans++;
                    }
                    if (next == "bearish") {
                        stagnantTobeartrans++;
                    }
                    if (next == "stagnant") {
                        stagnantTostagnanttrans++;
                    }
                    break;
                }
                default:
                    throw new IllegalArgumentException("Invalid type of transition: " + curr);
            }
        }

        Float[][] transMatrix = { { bullTobulltrans / 49F, bullTobeartrans / 49F, bullTostagnanttrans / 49F },
                { bearTobulltrans / 49F, bearTobeartrans / 49F, bearTostagnanttrans / 49F },
                { stagnantTobulltrans / 49F, stagnantTobeartrans / 49F, stagnantTostagnanttrans / 49F } };

        return transMatrix;
    }

}
