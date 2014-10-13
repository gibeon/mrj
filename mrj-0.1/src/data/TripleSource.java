package data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TripleSource implements WritableComparable<TripleSource> {

	public static final byte RDFS_DERIVED = 1;
	public static final byte OWL_DERIVED = 2;
	
	public static final byte TRANSITIVE_ENABLED = 3;
	public static final byte TRANSITIVE_DISABLED = 4;
	
	byte derivation = 0;
	int step = 0;

	@Override
	public void readFields(DataInput in) throws IOException {
		derivation = in.readByte();
		step = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.write(derivation);
		out.writeInt(step);
	}

	@Override
	//I know need this method
	public int compareTo(TripleSource arg0) {
		return 0;		
	}

	public boolean isTripleDerived() {
		return derivation != 0;
	}
	
	public int getStep() {
		return step;
	}
	
	public void setStep(int step) {
		this.step = step;
	}
	
	public void setDerivation(byte ruleset) {
		derivation = ruleset;
	}
	
	public byte getDerivation() {
		return derivation;
	}
}
