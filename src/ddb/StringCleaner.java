package ddb;

import java.util.ArrayList;

public class StringCleaner {
	static public ArrayList<String> cleanList(ArrayList<String> words) {
		for (int i = 0; i < words.size(); ++i)
			words.set(i, clean(words.get(i)));		
		return words;
	}
	static public String clean(String str) {
		// TODO Auto-generated method stub
		int startPos = 0;
		while (str.charAt(startPos) == ' ') {
			startPos++;
		}
		return str.substring(startPos).trim();
	}
}
