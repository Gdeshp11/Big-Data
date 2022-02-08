import java.io.File;
import java.io.FileReader;
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

import Map.*;
// import Map.Map;
import Reduce.*;

public class stockMarket {
    public static void main(String[] args) throws Exception {
        // System.out.println("Hello, World!");
        HashMap<String, Integer[][]> closing_prices = readData("../data/");
        for (java.util.Map.Entry<String, Integer[][]> item : closing_prices.entrySet()) {
            System.out.println("\n\n+++Key: " + item.getKey());
            Integer[][] tmp = item.getValue();
            for (int i = 0; i < 3; i++) {
                for (int j = 0; j < 3; j++)
                    System.out.println("\n---value mat[" + i + "][" + j + "]: " + tmp[i][j]);
            }
        }
        // System.out.println(closing_prices);
        // System.out.format("closing prices size: %d%n", closing_prices.size());
        // System.out.println(closing_prices);
        // java.util.Map<Integer, Float> weekly_avg_prices =
        // getAvgPriceMap(closing_prices);
        // System.out.format("avg prices size: %d%n", weekly_avg_prices.size());
        // System.out.println(weekly_avg_prices);
        // getTransMatrix(weekly_avg_prices);
    }

    public static HashMap<String, Integer[][]> readData(String input_file_dir) {
        HashMap<String, Integer[][]> ClosingPricesMap = new HashMap<String, Integer[][]>();

        try {
            File f = new File(input_file_dir);

            // String[] files = f.list();
            // for(int i=0;i<files.length;i++)
            // {
            // System.out.println(f.list()[i]);
            // }

            for (File i : f.listFiles()) {
                if (i.isDirectory())
                    continue;
                else if (i.getName().matches(".*\\.csv")) {

                    List<Float> closingPrices = new ArrayList<>();
                    Integer[][] transMatrix = new Integer[3][3];

                    System.out.println(i.getName());
                    Scanner scanner = new Scanner(i);
                    scanner.nextLine(); // skip first line
                    while (scanner.hasNextLine()) {
                        // System.out.println( getClosingPrice(scanner.nextLine()));
                        closingPrices.add(Float.parseFloat(getClosingPrice(scanner.nextLine())));
                    }
                    transMatrix = getTransMatrix(getAvgPriceMap(closingPrices));
                    ClosingPricesMap.put(i.getName(), transMatrix);

                    // for(int i_=0;i_<3;i_++){
                    // for(int j=0;j<3;j++)
                    // System.out.println("\n---transmat["+i_+"]["+j+"]: "+transMatrix[i_][j]);
                    // }

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

    private static Integer[][] getTransMatrix(java.util.Map<Integer, Float> mp) {
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
        // Integer curr_week;
        Float curr_week_price = 0.0F, prev_week_price = 0.0F;
        for (java.util.Map.Entry<Integer, Float> item : mp.entrySet()) {
            // curr_week = item.getKey();
            curr_week_price = item.getValue();
            // System.out.format("%n[ prev week price: %f, curr week price:%f ]
            // %n",prev_week_price,curr_week_price);
            if (prev_week_price == 0) {
                prev_week_price = curr_week_price;
                continue;
            } else if (curr_week_price - prev_week_price > 1.0f) {
                // bullTobulltrans++;
                // System.out.println("bullish week");
                transitions.add("bullish");
            } else if (curr_week_price - prev_week_price < 0.0f) {

                // System.out.println("bearish week");
                transitions.add("bearish");
            } else if (Math.abs(curr_week_price - prev_week_price) <= 0.8f) {
                // System.out.println("stagnant week");
                transitions.add("stagnant");
            }

            prev_week_price = curr_week_price;
        }
        // System.out.println(transitions);

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

        // System.out.println(transitions);

        // System.out.println("bulltobull: "+bullTobulltrans);
        // System.out.println("bulltobear: "+bullTobeartrans);
        // System.out.println("bulltostagnant: "+bullTostagnanttrans);

        // System.out.println("beartobull: "+bearTobulltrans);
        // System.out.println("beartobear: "+bearTobeartrans);
        // System.out.println("beartostagnant: "+bearTostagnanttrans);

        // System.out.println("stagnanttobull: "+stagnantTobulltrans);
        // System.out.println("stagnanttobear: "+stagnantTobeartrans);
        // System.out.println("stagnanttostagnant: "+stagnantTostagnanttrans);

        Integer[][] transMatrix = { { bullTobulltrans, bullTobeartrans, bullTostagnanttrans },
                { bearTobulltrans, bearTobeartrans, bearTostagnanttrans },
                { stagnantTobulltrans, stagnantTobeartrans, stagnantTostagnanttrans } };

        return transMatrix;
    }
}