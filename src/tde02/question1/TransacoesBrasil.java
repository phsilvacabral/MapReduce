package tde02.question1;

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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class TransacoesBrasil {
    public static void main(String args[]) throws IOException,
            ClassNotFoundException,
            InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/operacoes_comerciais_inteira.csv");

        // arquivo de saida
        Path output = new Path("output/question1");

        // criacao do job e seu nome
        Job j = new Job(c, "qtde_transacoes_brasil");

        // 1. registrar classes
        j.setJarByClass(TransacoesBrasil.class);
        j.setMapperClass(MapForTransacoes.class);
        j.setReducerClass(ReduceForTransacoes.class);

        // 2. tipos de saída
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        // 3. arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true)?0:1);

    }


    public static class MapForTransacoes extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // entrada Afghanistan;2016;010410;Sheep, live;Export;6088;2339;Number of items;51;01_live_animals
            String linha = value.toString();

            String[] valores = linha.split(";");

            String pais = valores[0];
            if (pais.equals("Brazil")) {
                // <key, value> --- <pais, (brasil, 1)>
                con.write(new Text("Brazil"), new IntWritable(1));
            }
        }
    }

    public static class ReduceForTransacoes extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            // soma de todas as transacoes do brasil
            int soma = 0;

            for (IntWritable v : values) {
                soma += v.get();
            }

            con.write(key, new IntWritable(soma));
        }
    }

}
