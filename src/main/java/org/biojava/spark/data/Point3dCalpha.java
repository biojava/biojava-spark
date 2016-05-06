package org.biojava.spark.data;

import java.util.ArrayList;
import java.util.List;

import javax.vecmath.Point3d;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

/**
 * A mapper from {@link StructureDataInterface} to the {@link Point3d}[] of the calpha coordinates.
 * @author Anthony Bradley
 *
 */
public class Point3dCalpha implements PairFlatMapFunction<Tuple2<String,StructureDataInterface>,String, Point3d[]> {

	/**
	 * The serial id for this version of the class.
	 */
	private static final long serialVersionUID = -1187474691802866518L;

	@Override
	public Iterable<Tuple2<String, Point3d[]>> call(Tuple2<String, StructureDataInterface> t) throws Exception {
		List<Tuple2<String, Point3d[]>> outList = new ArrayList<>();
		// Get the PDB id
		StructureDataInterface structureDataInterface = t._2;
		String pdbId = structureDataInterface.getStructureId();
		int atomCounter = 0;
		int groupCounter = 0;
		// Now loop through the entities
		for(int i=0; i<structureDataInterface.getNumChains(); i++){
			String chainId = pdbId+"."+structureDataInterface.getChainIds()[i];
			String entityType = SparkUtils.getType(structureDataInterface,i);
			if(entityType==null || entityType.equals("polymer")){
				// Loop over the group indices
				List<Point3d> fragList = new ArrayList<>();
				for(int groupId=0; groupId<structureDataInterface.getGroupsPerChain()[i]; groupId++){
					// Now get the CA coord
					int groupType = structureDataInterface.getGroupTypeIndices()[groupCounter];
					Point3d point3d = SparkUtils.getCalpha(structureDataInterface, groupType, atomCounter);
					atomCounter+=structureDataInterface.getNumAtomsInGroup(groupType);
					groupCounter++;
					if(point3d!=null){
						fragList.add(point3d);
					}
				}
				outList.add(new Tuple2<String, Point3d[]>(
						structureDataInterface.getStructureId()+"."+chainId, 
						fragList.toArray(new Point3d[0])));
			}
		}
		return outList;
	}


}
