package writables;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class LongLongPair implements WritableComparable<LongLongPair>
{
	private long w2Count, w1w2Count;

	public LongLongPair()
	{
	}

	public LongLongPair(long w2Count, long w1w2Count)
	{
		this.w2Count = w2Count;
		this.w1w2Count = w1w2Count;
	}


	public long getw2Count()
	{
		return w2Count;
	}

	public long getw1w2Count()
	{
		return w1w2Count;
	}

	@Override
	public boolean equals(Object o)
	{
		if (this == o)
			return true;
		if (!(o instanceof LongLongPair))
			return false;
		LongLongPair that = (LongLongPair) o;
		return w2Count == that.w2Count &&
				w1w2Count == that.w1w2Count;
	}

	@Override
	public int hashCode()
	{
		return Objects.hash(w2Count, w1w2Count);
	}

	@Override
	public String toString()
	{
		return w1w2Count + " " + w1w2Count;
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeLong(w2Count);
		out.writeLong(w1w2Count);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		w2Count = in.readLong();
		w1w2Count = in.readLong();
	}

	@Override
	public int compareTo(LongLongPair o)
	{
		final int valueCompare = Long.compare(w1w2Count, o.w1w2Count);
		return valueCompare != 0 ? valueCompare : Long.compare(w2Count, o.w2Count);
	}
}
