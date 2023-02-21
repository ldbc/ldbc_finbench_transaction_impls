package org.ldbcouncil.finbench.impls.tugraph;

import java.io.ByteArrayOutputStream;

public class CustomDataOutputStream {
	
	private ByteArrayOutputStream out;
	
	public CustomDataOutputStream() {
		out = new ByteArrayOutputStream();
	}
	
	public void writeInt16(short i) {
		out.write((i     ) & 0xff);
		out.write((i >> 8) & 0xff);
	}

	public void writeInt32(int i) {
		out.write((i      ) & 0xff);
		out.write((i >>  8) & 0xff);
		out.write((i >> 16) & 0xff);
		out.write((i >> 24) & 0xff);
	}

	public void writeInt64(long i) {
		out.write((int)((i      ) & 0xff));
		out.write((int)((i >>  8) & 0xff));
		out.write((int)((i >> 16) & 0xff));
		out.write((int)((i >> 24) & 0xff));
		out.write((int)((i >> 32) & 0xff));
		out.write((int)((i >> 40) & 0xff));
		out.write((int)((i >> 48) & 0xff));
		out.write((int)((i >> 56) & 0xff));
	}

	public void writeString(String s) {
		int sLen = s.getBytes().length;
		writeInt16((short)sLen);
		out.write(s.getBytes(), 0, sLen);
	}
	
	public byte[] toByteArray() {
		return out.toByteArray();
	}

}
