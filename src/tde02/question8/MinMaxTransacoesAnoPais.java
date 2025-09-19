package tde02.question8;

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
        // arquivo de entrada
        Path input = new Path("in/operacoes_comerciais_inteira.csv");

        // arquivo de saida
        Path output = new Path("output/question8");

        Job job = new Job(c, "MinMaxTransacoes");

        // 1. Registrar as classes
        job.setJarByClass(MinMaxTransacoesAnoPais.class);
        job.setMapperClass(MinMaxMapper.class);
        job.setCombinerClass(MinMaxReducer.class);
        job.setReducerClass(MinMaxReducer.class);

        // 2. Definir os tipos de saída do Map
        job.setMapOutputKeyClass(PaisAnoWritable.class);
        job.setMapOutputValueClass(MinMaxWritable.class);

        // 3. Definir os tipos de saída final (do Reduce)
        job.setOutputKeyClass(PaisAnoWritable.class);
        job.setOutputValueClass(MinMaxWritable.class);

        // 4. Definir os arquivos de entrada e saída
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        // 5. Executar o job e esperar a conclusão
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    // Mapper: Lê cada linha do CSV, extrai (ano;país) como chave e o valor da transação como valor.
    public static class MinMaxMapper extends Mapper<LongWritable, Text, PaisAnoWritable, MinMaxWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String linha = value.toString();

            // Ignorar o cabeçalho do CSV
            if (linha.startsWith("country_or_area")) {
                return;
            }

            String[] dados = linha.split(";");

            // Verificar se a linha tem o número esperado de colunas
            if (dados.length > 5) {
                String pais = dados[0];
                String ano = dados[1];
                try {
                    long valor = Long.parseLong(dados[5]);

                    // Chave composta: (país, ano) usando a classe Writable customizada
                    PaisAnoWritable outputKey = new PaisAnoWritable(pais, ano);
                    // Valor: para cada valor, é criado um objeto onde min e max são o próprio valor
                    MinMaxWritable outputValue = new MinMaxWritable(valor, valor);
                    con.write(outputKey, outputValue);
                } catch (NumberFormatException e) {
                    // Ignorar linhas com valor de transação malformado
                    System.err.println("Ignorando linha com valor inválido: " + linha);
                }
            }
        }
    }

    // Reducer: Para cada chave (ano    país), itera sobre todos os valores de transação e encontra o mínimo e o máximo.
    public static class MinMaxReducer extends Reducer<PaisAnoWritable, MinMaxWritable, PaisAnoWritable, MinMaxWritable> {
        private final MinMaxWritable result = new MinMaxWritable();

        public void reduce(PaisAnoWritable key, Iterable<MinMaxWritable> values, Context con)
                throws IOException, InterruptedException {

            long min = Long.MAX_VALUE;
            long max = Long.MIN_VALUE;

            // Itera sobre todos os valores parciais (MinMaxWritable) para a chave atual
            for (MinMaxWritable val : values) {
                // Compara e atualiza o mínimo
                if (val.getMin() < min) {
                    min = val.getMin();
                }
                // Compara e atualiza o máximo
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
