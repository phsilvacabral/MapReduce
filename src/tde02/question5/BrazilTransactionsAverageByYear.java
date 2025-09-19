package tde02.question5;

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
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class BrazilTransactionsAverageByYear {

    public static void main(String args[]) throws IOException,
            ClassNotFoundException,
            InterruptedException {

        BasicConfigurator.configure();

        Configuration c = new Configuration();

        Path input = new Path("in/operacoes_comerciais_inteira.csv");

        Path output = new Path("output/question5");

        // criacao do job e seu nome
        Job j = new Job(c, "BrazilTransactionsAverageByYear");

        // 1. registrar classes
        j.setJarByClass(BrazilTransactionsAverageByYear.class);
        j.setMapperClass(MapForBrazilTransactionsAverageByYear.class);
        j.setReducerClass(ReduceForBrazilTransactionsAverageByYear.class);
        j.setCombinerClass(CombineForBrazilTransactionsAverageByYear.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(AvgTransactionValue.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true)?0:1);

    }


    public static class MapForBrazilTransactionsAverageByYear extends Mapper<LongWritable, Text, Text, AvgTransactionValue> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            if (key.get() == 0) {
                return;
            }

            String linha = value.toString();
            String[] valores = linha.split(";");

            if (valores.length < 6) {
                return;
            }

            String pais = valores[0].trim();
            String ano = valores[1].trim();
            String valorString = valores[5].trim();

            if (!pais.equalsIgnoreCase("Brazil")) {
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


    public static class CombineForBrazilTransactionsAverageByYear extends Reducer<Text, AvgTransactionValue, Text, AvgTransactionValue> {
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


    public static class ReduceForBrazilTransactionsAverageByYear extends Reducer<Text, AvgTransactionValue, Text, FloatWritable> {
        public void reduce(Text key, Iterable<AvgTransactionValue> values, Context con)
                throws IOException, InterruptedException {

            int somaFrequencia = 0;
            float somaValores = 0;

            for (AvgTransactionValue val : values) {
                somaValores += val.getValue();
                somaFrequencia += val.getFreq();
            }

            float media = somaValores/somaFrequencia;

            con.write(key, new FloatWritable(media));
        }
    }

}