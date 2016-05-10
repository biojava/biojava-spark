package org.biojava.spark.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.vecmath.Point3d;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

/**
 * A mapper from {@link StructureDataInterface} to the {@link Point3d}[] of the calpha coordinates.
 * @author Anthony Bradley
 *
 */
public class Point3dCalpha implements PairFlatMapFunction<Tuple2<String,StructureDataInterface>,String, Segment> {

	/** Define the length of a fragment. Null implies each chain is a fragment */
	private Integer fragmentLength;
	
	/**
	 * Constructor of the class. 
	 * @param fragmentLength the length of each fragment. Null means take each Chain as 
	 * as single fragment.
	 */
	public Point3dCalpha(Integer fragmentLength) {
		this.fragmentLength = fragmentLength;
	}
	
	/**
	 * The serial id for this version of the class.
	 */
	private static final long serialVersionUID = -1187474691802866518L;

	@Override
	public Iterable<Tuple2<String, Segment>> call(Tuple2<String, StructureDataInterface> t) throws Exception {
		StructureDataInterface structureDataInterface = t._2;
		List<Tuple2<String, Segment>> outList = new ArrayList<>();
		// Get the PDB id
		String pdbId = structureDataInterface.getStructureId();
		Map<Integer,String> chainIndexToEntityTypeMap = getChainEntity(structureDataInterface);
		int atomCounter = 0;
		int groupCounter = 0;
		// Now loop through the entities
		for(int i=0; i<structureDataInterface.getNumChains(); i++){
			String chainId = pdbId+"."+structureDataInterface.getChainIds()[i];
			if(chainIndexToEntityTypeMap.get(i)==null || chainIndexToEntityTypeMap.get(i).equals("polymer")){
				int fragCounter = 0;
				// Loop over the group indices
				List<Point3d> fragList = new ArrayList<>();
				String sequence = "";
				for(int groupId=0; groupId<structureDataInterface.getGroupsPerChain()[i]; groupId++){
					// Now get the CA coord
					int groupType = structureDataInterface.getGroupTypeIndices()[groupCounter];
					Point3d point3d = getCalpha(structureDataInterface, groupType, atomCounter);
					atomCounter+=structureDataInterface.getNumAtomsInGroup(groupType);
					groupCounter++;
					if(point3d!=null){
						fragList.add(point3d);
						sequence+=structureDataInterface.getGroupSingleLetterCode(groupType);
					}
					if (fragmentLength!=null && fragList.size()==fragmentLength) {
						outList.add(new Tuple2<String, Segment>(chainId+fragCounter, 
								new Segment(sequence, fragList.toArray(new Point3d[fragmentLength]))));
						fragList.remove(fragmentLength-1);
						sequence = sequence.substring(1, sequence.length());
						fragCounter++;
					}
				}
				if (fragmentLength==null && fragList.size()!=0) {
					outList.add(new Tuple2<String, Segment>(chainId, 
							new Segment(sequence, fragList.toArray(new Point3d[fragList.size()]))));
				}
				
			}
		}
		return outList;
	}


	/**
	 * Get a map of chain index to the entity type.
	 * @param structureDataInterface the input {@link StructureDataInterface}
	 * @return the map of chain indices to the entity type
	 */
	private Map<Integer,String> getChainEntity(StructureDataInterface structureDataInterface) {
		Map<Integer,String> outMap = new HashMap<>();
		for(int i=0; i<structureDataInterface.getNumEntities(); i++) {
			String type = structureDataInterface.getEntityType(i);
			for(int j : structureDataInterface.getEntityChainIndexList(i)) {
				outMap.put(j, type);
			}
		}
		return outMap;
	}


	/**
	 * Get the calpha as a {@link Point3d}.
	 * @param structureDataInterface the {@link StructureDataInterface} to read
	 * @param groupType the integer specifying the grouptype
	 * @param atomCounter the atom count at the start of this group
	 * @return the point3d object specifying the calpha of this point
	 */
	private Point3d getCalpha(StructureDataInterface structureDataInterface, int groupType, int atomCounter) {
		for(int i=0; i<structureDataInterface.getNumAtomsInGroup(groupType);i++){
			if(structureDataInterface.getGroupAtomNames(groupType)[i].equals("CA")){
				Point3d point3d = new Point3d();
				point3d.x = structureDataInterface.getxCoords()[atomCounter+i];
				point3d.y = structureDataInterface.getyCoords()[atomCounter+i]; 
				point3d.z = structureDataInterface.getzCoords()[atomCounter+i];
				return point3d;
			}
		}
		return null;
	}


}
