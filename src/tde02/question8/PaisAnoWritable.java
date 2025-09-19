package tde02.question8;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class PaisAnoWritable implements WritableComparable<PaisAnoWritable> {

    private Text pais;
    private Text ano;

    // Construtor padrão é obrigatório para o framework
    public PaisAnoWritable() {
        this.pais = new Text();
        this.ano = new Text();
    }

    public PaisAnoWritable(String pais, String ano) {
        this.pais = new Text(pais);
        this.ano = new Text(ano);
    }

    // Getters
    public Text getPais() { return pais; }
    public Text getAno() { return ano; }

    @Override
    public void write(DataOutput out) throws IOException {
        pais.write(out);
        ano.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        pais.readFields(in);
        ano.readFields(in);
    }

    @Override
    public int compareTo(PaisAnoWritable o) {
        // Compara primeiro pelo país
        int cmp = this.pais.compareTo(o.pais);
        if (cmp != 0) {
            return cmp;
        }
        // Se os países forem iguais, compara pelo ano
        return this.ano.compareTo(o.ano);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PaisAnoWritable that = (PaisAnoWritable) o;
        return Objects.equals(pais, that.pais) && Objects.equals(ano, that.ano);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pais, ano);
    }

    @Override
    public String toString() {
        // Define como a chave será escrita no arquivo de saída final
        return pais.toString() + "\t" + ano.toString();
    }
}