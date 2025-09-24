package tde02.Pergunta7;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class MediaTransacaoPorAno {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration c = new Configuration();

        Path input = new Path("in/operacoes_comerciais_inteira.csv");
        Path output = new Path("output/Pergunta_7");

        Job j = new Job(c, "MediaTransacaoPorAno");

        j.setJarByClass(MediaTransacaoPorAno.class);
        j.setMapperClass(MapMediaTransacao.class);
        j.setReducerClass(ReducerMediaTransacao.class);
        j.setCombinerClass(CombinerMediaTransacao.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(AvgTransactionValue.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }


    public static class MapMediaTransacao extends Mapper<LongWritable, Text, Text, AvgTransactionValue> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();

            if (linha.startsWith("country_or_area")) {
                return;
            }

            String[] valores = linha.split(";");

            if (valores.length < 6) {
                return;
            }

            String pais = valores[0].trim().toLowerCase();
            String ano = valores[1].trim();
            String tipo = valores[4].trim().toLowerCase();
            String valorString = valores[5].trim();

            if (!pais.equalsIgnoreCase("brazil")) {
                return;
            }

            if (!tipo.equalsIgnoreCase("export")) {
                return;
            }

            if (!ano.matches("\\d{4}")) {
                return;
            }

            float valor;
            try {
                valor = Float.parseFloat(valorString);
            } catch (NumberFormatException e) {
                return;
            }

            con.write(new Text(ano), new AvgTransactionValue(valor, 1));
        }
    }


    public static class CombinerMediaTransacao extends Reducer<Text, AvgTransactionValue, Text, AvgTransactionValue> {
        public void reduce(Text key, Iterable<AvgTransactionValue> values, Context con)
                throws IOException, InterruptedException {

            int somaFrequencia = 0;
            float somaValores = 0;

            for (AvgTransactionValue val : values) {
                somaValores += val.getValue();
                somaFrequencia += val.getFreq();
            }

            con.write(key, new AvgTransactionValue(somaValores, somaFrequencia));
        }
    }


    public static class ReducerMediaTransacao extends Reducer<Text, AvgTransactionValue, Text, FloatWritable> {
        public void reduce(Text key, Iterable<AvgTransactionValue> values, Context con)
                throws IOException, InterruptedException {

            int somaFrequencia = 0;
            float somaValores = 0;

            for (AvgTransactionValue val : values) {
                somaValores += val.getValue();
                somaFrequencia += val.getFreq();
            }

            float media = somaValores / somaFrequencia;

            con.write(key, new FloatWritable(media));
        }
    }

}