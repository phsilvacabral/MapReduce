package tde02.Pergunta3;

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

public class TransacaoPorCategoria {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration c = new Configuration();

        Path input = new Path("in/operacoes_comerciais_inteira.csv");
        Path output = new Path("output/Pergunta_3");

        Job j = new Job(c, "TransacaoPorCategoria");

        j.setJarByClass(TransacaoPorCategoria.class);
        j.setMapperClass(TransacaoPorCategoria.MapTransacaoPorCategoria.class);
        j.setCombinerClass(TransacaoPorCategoria.CombinerTransacaoPorCategoria.class);
        j.setReducerClass(TransacaoPorCategoria.ReducerTransacaoPorCategoria.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapTransacaoPorCategoria extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();

            if (linha.startsWith("country_or_area")) {
                return;
            }

            String[] colunas = linha.split(";");

            if (colunas.length == 10) {
                String categoria = colunas[9].trim().toLowerCase();

                if (!categoria.isEmpty()) {
                    con.write(new Text(categoria), new IntWritable(1));
                }
            }
        }
    }

    public static class CombinerTransacaoPorCategoria extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            int cont = 0;
            for (IntWritable v : values) {
                cont += v.get();
            }
            con.write(key, new IntWritable(cont));
        }
    }

    public static class ReducerTransacaoPorCategoria extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            int cont = 0;
            for (IntWritable v : values) {
                cont += v.get();
            }
            con.write(key, new IntWritable(cont));
        }
    }
}
