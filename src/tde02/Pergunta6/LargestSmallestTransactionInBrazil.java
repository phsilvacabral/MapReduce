package tde02.Pergunta6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

import javax.naming.Context;

public class LargestSmallestTransactionInBrazil {

    public static void main(String args[]) throws IOException,
            ClassNotFoundException,
            InterruptedException {

        BasicConfigurator.configure();

        Configuration c = new Configuration();

        Path input = new Path("in/operacoes_comerciais_inteira.csv"); // arquivo de entrada
        Path output = new Path("output/pergunta_6"); // arquivo de saida

        Job j = new Job(c, "LargestSmallestTransactionInBrazil");

        j.setJarByClass(MapForLargestSmallestTransactionInBrazil.class);
        j.setMapperClass(MapForLargestSmallestTransactionInBrazil.class);
        j.setReducerClass(ReducerForLargestSmallestTransactionInBrazil.class);
        j.setCombinerClass(CombinerForLargestSmallestTransactionInBrazil.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(LargestSmallestTransaction.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true)?0:1);

    }


    public static class MapForLargestSmallestTransactionInBrazil extends Mapper<LongWritable, Text, Text, LargestSmallestTransaction> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {


            String line = value.toString();

            if (line.startsWith("country_or_area")) {
                return;
            }

            String[] fields = line.split(";");

            if (fields.length < 6) {
                return;
            }

            String country = fields[0].trim().toLowerCase();
            String year = fields [1].trim();
            String valueString = fields[5].trim();

            if (!country.equalsIgnoreCase("brazil")) {
                return;
            }

            if (!year.equalsIgnoreCase("2016")) {
                return;
            }

            float valueFloat;
            try {
                valueFloat = Float.parseFloat(valueString);
            } catch (NumberFormatException e) {
                return;
            }

            con.write(new Text("Brasil_2016"), new LargestSmallestTransaction(valueFloat, valueFloat));
        }
    }


    public static class CombinerForLargestSmallestTransactionInBrazil extends Reducer<Text, LargestSmallestTransaction, Text, LargestSmallestTransaction> {
        public void reduce(Text key, Iterable<LargestSmallestTransaction> values, Context con)
                throws IOException, InterruptedException {

            float maxValue = Float.MIN_VALUE;
            float minValue = Float.MAX_VALUE;

            for (LargestSmallestTransaction val : values) {

                if (val.getLargest() > maxValue){
                    maxValue = val.getLargest();
                }

                if (val.getSmallest() < minValue){
                    minValue = val.getSmallest();
                }
            }

            con.write(key, new LargestSmallestTransaction(maxValue, minValue));
        }
    }



    public static class ReducerForLargestSmallestTransactionInBrazil extends Reducer<Text, LargestSmallestTransaction, Text, Text> {
        public void reduce(Text key, Iterable<LargestSmallestTransaction> values, Context con)
                throws IOException, InterruptedException {

            float maxValue = Float.MIN_VALUE;
            float minValue = Float.MAX_VALUE;

            for (LargestSmallestTransaction val : values) {

                if (val.getLargest() > maxValue){
                    maxValue = val.getLargest();
                }

                if (val.getSmallest() < minValue){
                    minValue = val.getSmallest();
                }
            }

            con.write(new Text("Maior valor"), new Text(String.valueOf(maxValue)));
            con.write(new Text("Menor valor"), new Text(String.valueOf(minValue)));

        }
    }

}