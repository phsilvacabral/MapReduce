package tde02.question2;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;


public class TransactionsPerYearCount {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();

        Path input = new Path("in/operacoes_comerciais_inteira.csv");

        Path output = new Path("output/question2");

        Job j = new Job(c, "TransactionsPerYearCount");

        j.setJarByClass(TransactionsPerYearCount.class);
        j.setMapperClass(MapForTransactionsPerYearCount.class);
        j.setCombinerClass(CombineForTransactionsPerYearCount.class);
        j.setReducerClass(ReduceForTransactionsPerYearCount.class);


        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true)?0:1);
    }


    public static class MapForTransactionsPerYearCount extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            if (key.get() == 0) {
                return;
            }

            String linha = value.toString();

            String[] colunas = linha.split(";");

            if (colunas.length > 1) {
                String ano = colunas[1].trim();

                if (!ano.isEmpty() && ano.matches("\\d{4}")) {
                    con.write(new Text(ano), new IntWritable(1));
                }
            }
        }
    }


    public static class CombineForTransactionsPerYearCount extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            int cont = 0;

            for(IntWritable valores: values){
                cont += valores.get();
            }

            con.write(key, new IntWritable(cont));

        }
    }


    public static class ReduceForTransactionsPerYearCount extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            int cont = 0;

            for(IntWritable valores: values){
                cont += valores.get();
            }

            con.write(key, new IntWritable(cont));

        }
    }

}