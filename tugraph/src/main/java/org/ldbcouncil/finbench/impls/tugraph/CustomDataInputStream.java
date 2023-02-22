package org.ldbcouncil.finbench.impls.tugraph;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

public class CustomDataInputStream {

    private DataInputStream in;
    private byte word[];

    public CustomDataInputStream(InputStream in) {
        this(new DataInputStream(in));
    }

    public CustomDataInputStream(DataInputStream in) {
        this.in = in;
        this.word = new byte[32768];
    }

    public final byte readByte() throws IOException {
        in.readFully(word, 0, 1);
        return (byte) (word[0] & 0xff);
    }

    public final short readInt16() throws IOException {
        in.readFully(word, 0, 2);
        return (short) (((word[1] & 0xff) << 8) | (word[0] & 0xff));
    }

    public final int readInt32() throws IOException {
        in.readFully(word, 0, 4);
        return ((word[3] & 0xff) << 24) | ((word[2] & 0xff) << 16) | ((word[1] & 0xff) << 8) | (word[0] & 0xff);
    }

    public final long readInt64() throws IOException {
        in.readFully(word, 0, 8);
        return ((long) (word[7] & 0xff) << 56) | ((long) (word[6] & 0xff) << 48) | ((long) (word[5] & 0xff) << 40) | ((long) (word[4] & 0xff) << 32) |
                ((long) (word[3] & 0xff) << 24) | ((long) (word[2] & 0xff) << 16) | ((long) (word[1] & 0xff) << 8) | ((long) (word[0] & 0xff));
    }

    public final float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt32());
    }

    public final double readDouble() throws IOException {
        return Double.longBitsToDouble(readInt64());
    }

    public final String readString() throws IOException {
        final short stringLength = readInt16();
        in.readFully(word, 0, stringLength);
        return new String(word, 0, stringLength);
    }

    public final boolean readBoolean() throws IOException {
        in.readFully(word, 0, 1);
        return word[0] == 0 ? false : true;
    }

//	public void close() throws IOException {
//		in.close();
//	}

}
