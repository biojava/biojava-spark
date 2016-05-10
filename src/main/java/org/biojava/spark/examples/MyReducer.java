package org.biojava.spark.examples;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.Function2;

/**
 * Combine all the lists that have an intersection.
 * @author Anthony Bradley
 *
 */
public class MyReducer implements Function2<List<List<String>>,List<List<String>>, List<List<String>>> {

	/**
	 * The serial ID for this version of the class
	 */
	private static final long serialVersionUID = -6719890677753351769L;

	@Override
	public List<List<String>> call(List<List<String>> v1, List<List<String>> v2) throws Exception {

		List<List<String>> outList = new ArrayList<>();
		for(int i=0; i<v1.size(); i++){
			for(int j=i; j<v2.size(); j++) {
				List<String> listOne = v1.get(i);
				System.out.println(listOne);
				List<String> listTwo = v2.get(j);
				boolean intersection = listOne.retainAll(listTwo);
				// If they have an intersection - merge them
				if(intersection){
					listOne.addAll(listTwo);
				}
				// If not - then you could check to see if any of the pairs are in anyway similar....
				outList.add(listOne);
			}
		}
		return outList;
	}
}
