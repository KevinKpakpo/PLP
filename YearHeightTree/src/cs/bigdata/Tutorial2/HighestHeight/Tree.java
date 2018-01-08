package cs.bigdata.Tutorial2.HighestHeight;

import java.util.Calendar;

public class Tree {
	String GEOPOINT;
	String ARRONDISSEMENT;
	static String GENRE;
	String ESPECE;
	String FAMILLE;
	static int ANNEE_PLANTATION;
	static int TREE_YEAR;
	static String HAUTEUR;
	int CIRCONFERENCE;
	String ADRESSE;
	static String NOM_COMMUN;
	String VARIETE;
	int OBJECTID;
	String NOM_EV;
	
	
	
	public static String getGENRE(String line) {
		String[] parts = line.split(";");
		GENRE = parts[3];
		return GENRE;
	}

	
	
	public static Float getHAUTEUR(String line) {
		try{
			String[] parts = line.split(";");
			HAUTEUR = parts[6];
			return Float.parseFloat(HAUTEUR);
		}catch(NumberFormatException e){
			return (float) 0.0;
		}
	}

	
	public static String getInfo(String line){
		String info;
		String year_info;
		String[] parts = line.split(";");
		try{
			// get year of the tree
			ANNEE_PLANTATION = Integer.parseInt(parts[5]);
			int SYS_YEAR = Calendar.getInstance().get(Calendar.YEAR);
			TREE_YEAR = SYS_YEAR - ANNEE_PLANTATION;
			year_info = " , YEAR: " + TREE_YEAR;
		}
		catch(NumberFormatException e){
			year_info = " , YEAR: " + "NA";
		}
		
		// get height of the tree
		HAUTEUR = parts[6];
		// get name of the tree
		NOM_COMMUN = parts[9];
		info = "NOM: " + NOM_COMMUN + " , HAUTEUR: " + HAUTEUR + year_info;
		
		return info;
	}
	
	public static void main(String[] args) {
		//String line = "(48.8302532096, 2.41400587444);12;Acer;opalus;Sapindaceae;1870;15.0;160.0;Ile de Bercy;Erable d'Italie;;91;Bois de Vincennes (Ile de Bercy)";
		String line2 = "(48.8302532096, 2.41400587444);12;Acer;opalus;Sapindaceae; ;15.0;160.0;Ile de Bercy;Erable d'Italie;;91;Bois de Vincennes (Ile de Bercy)";
		System.out.println(Tree.getHAUTEUR(line2));
	}

}
