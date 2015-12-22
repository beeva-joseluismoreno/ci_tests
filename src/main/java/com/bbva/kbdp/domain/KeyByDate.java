package com.bbva.kbdp.domain;

import java.util.Date;

public class KeyByDate implements Comparable<KeyByDate>{

	private Date fecLanz;
	private byte[] rowKey;
	
	
	public KeyByDate(Date fecLanz, byte[] rowKey) {
		super();
		this.fecLanz = fecLanz;
		this.rowKey = rowKey;
	}
	
	public Date getFecLanz() {
		return fecLanz;
	}
	public void setFecLanz(Date fecLanz) {
		this.fecLanz = fecLanz;
	}
	public byte[] getRowKey() {
		return rowKey;
	}
	public void setRowKey(byte[] rowKey) {
		this.rowKey = rowKey;
	}

	@Override
	public int compareTo(KeyByDate other) {
		if ( this.fecLanz.getTime() > other.fecLanz.getTime() )
	        return 1;
		else
			return -1;
	}
	
	
}
