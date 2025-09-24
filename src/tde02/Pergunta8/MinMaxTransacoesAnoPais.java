package tde02.Pergunta8;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class MinMaxTransacoesAnoPais {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path("in/operacoes_comerciais_inteira.csv");
        Path output = new Path("output/Pergunta_8");

        Job job = new Job(c, "MinMaxTransacoes");


        job.setJarByClass(MinMaxTransacoesAnoPais.class);
        job.setMapperClass(MinMaxMapper.class);
        job.setCombinerClass(MinMaxReducer.class);
        job.setReducerClass(MinMaxReducer.class);

        job.setMapOutputKeyClass(PaisAnoWritable.class);
        job.setMapOutputValueClass(MinMaxWritable.class);

        job.setOutputKeyClass(PaisAnoWritable.class);
        job.setOutputValueClass(MinMaxWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class MinMaxMapper extends Mapper<LongWritable, Text, PaisAnoWritable, MinMaxWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String linha = value.toString();

            if (linha.startsWith("country_or_area")) {
                return;
            }

            String[] dados = linha.split(";");

            if (dados.length > 5) {
                String pais = dados[0].trim().toUpperCase();
                String ano = dados[1];

                if (!ano.matches("\\d{4}")) {
                    return;
                }

                try {
                    long valor = Long.parseLong(dados[5]);
                    PaisAnoWritable outputKey = new PaisAnoWritable(pais, ano);
                    MinMaxWritable outputValue = new MinMaxWritable(valor, valor);
                    con.write(outputKey, outputValue);
                } catch (NumberFormatException e) {
                    System.err.println("Ignorando linha com valor inválido: " + linha);
                }
            }
        }
    }

    public static class MinMaxReducer extends Reducer<PaisAnoWritable, MinMaxWritable, PaisAnoWritable, MinMaxWritable> {
        private final MinMaxWritable result = new MinMaxWritable();

        public void reduce(PaisAnoWritable key, Iterable<MinMaxWritable> values, Context con)
                throws IOException, InterruptedException {

            long min = Long.MAX_VALUE;
            long max = Long.MIN_VALUE;

            for (MinMaxWritable val : values) {
                if (val.getMin() < min) {
                    min = val.getMin();
                }
                if (val.getMax() > max) {
                    max = val.getMax();
                }
            }

            result.setMin(min);
            result.setMax(max);

            con.write(key, result);
        }
    }
}