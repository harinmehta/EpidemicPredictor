package com.prediction.model;

import java.util.List;

public class ObjectWrapper {

	private double southWestLongitude;
	private double southWestLatitude;
	private double northEastLongitude;
	private double northEastLatitude;
	private List<String> terms;
	public double getSouthWestLongitude() {
		return southWestLongitude;
	}
	public void setSouthWestLongitude(double southWestLongitude) {
		this.southWestLongitude = southWestLongitude;
	}
	public double getSouthWestLatitude() {
		return southWestLatitude;
	}
	public void setSouthWestLatitude(double southWestLatitude) {
		this.southWestLatitude = southWestLatitude;
	}
	public double getNorthEastLongitude() {
		return northEastLongitude;
	}
	public void setNorthEastLongitude(double northEastLongitude) {
		this.northEastLongitude = northEastLongitude;
	}
	public double getNorthEastLatitude() {
		return northEastLatitude;
	}
	public void setNorthEastLatitude(double northEastLatitude) {
		this.northEastLatitude = northEastLatitude;
	}
	public List<String> getTerms() {
		return terms;
	}
	public void setTerms(List<String> terms) {
		this.terms = terms;
	}
}
