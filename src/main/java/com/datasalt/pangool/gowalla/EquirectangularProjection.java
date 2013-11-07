package com.datasalt.pangool.gowalla;

// http://en.wikipedia.org/wiki/Equirectangular_projection
public class EquirectangularProjection {

	public final static double R = 6371;

	// http://www.movable-type.co.uk/scripts/latlong.html
	public static double distance(double lat, double lng, double lat2, double lng2) {
		return equirectangularProjection(radians(lat), radians(lng), radians(lat2), radians(lng2));
	}

	public static double radians(double degrees) {
		return degrees * Math.PI / 180.0;
	}

	private static double equirectangularProjection(double latRad, double lngRad, double lat2Rad,
	    double lng2Rad) {
		double x = (lng2Rad - lngRad) * Math.cos((latRad + lat2Rad) / 2.0);
		double y = (lat2Rad - latRad);
		return Math.sqrt(x * x + y * y) * R;
	}

	public static double euclideanDistance(double lat, double lng, double lat2, double lng2) {
		double x = (lat2 - lat);
		double y = (lng2 - lng);
		return Math.sqrt(x * x + y * y) * R;
	}
	
	public final static void main(String[] args) {
		
		// latitude has a minimum of -90 (south pole) and a maximum of 90
		// Longitude has a minimum of -180 (west of the prime meridian) and a maximum of 180 
		for(int i = 0; i < 10000; i++) {
			double pointLat = Math.random()*180 - 90;
			double pointLng = Math.random()*360 - 180;

			System.out.println(EquirectangularProjection.distance(pointLat, pointLng, pointLat + 0.01, pointLng + 0.01) + "\t" + euclideanDistance(radians(pointLat), radians(pointLng), radians(pointLat + 0.01), radians(pointLng + 0.01)));
		}
	}
}
