package cs.bigdata.Tutorial2;

/**
 * Question 2.8 : Displaying the content of a compact file
 * Isd represents each line of isd-history.txt 
 * @author Kpakpo Akouete, Belhaj Amine, Darnel Hossie
 * 
 **/

public class Isd {
	
	static String FIPS;
	static String STATION_NAME;
	static float ELEV;
	
	public static String getSTATION_NAME(String line) {
		STATION_NAME = line.substring(13,32);
		return STATION_NAME;
	}

	
	public static String getFIPS(String line) {
		FIPS = line.substring(43,45);
		return FIPS;
	}

	
	
	public static Float getELEV(String line) {
		try{
		String ELEV_String;
		ELEV_String = line.substring(74,81);
		return Float.parseFloat(ELEV_String);
		}catch(NumberFormatException e){
			return (float) 0.0;
		}
	}

	
	public static void main(String[] args) {
		String line = "USAF   WBAN  STATION NAME                  CTRY ST CALL  LAT     LON      ELEV(M) BEGIN    END";
		//String line2 = "007005 99999 CWOS 07005                                                           20120127 20120127";
		System.out.println(Isd.getSTATION_NAME(line));
	}

}
