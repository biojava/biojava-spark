package org.biojava.spark.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.vecmath.Point3d;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.biojava.nbio.structure.Chain;
import org.biojava.nbio.structure.ChainImpl;
import org.biojava.nbio.structure.EntityInfo;
import org.biojava.nbio.structure.EntityType;
import org.biojava.nbio.structure.Structure;
import org.biojava.nbio.structure.StructureImpl;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

/**
 * Return the {@link StructureDataInterface} as a list of
 *  {@link Chain} objects.
 * @author Anthony Bradley
 *
 */
public class GetChains implements FlatMapFunction<Tuple2<String, StructureDataInterface>, Chain> {

	/**
	 * The serial id for this version of the class.
	 */
	private static final long serialVersionUID = -1524742762985460850L;

	@Override
	public Iterable<Chain> call(Tuple2<String, StructureDataInterface> t) throws Exception {
		StructureDataInterface structureDataInterface = t._2;
		Structure structure = new StructureImpl();
		structure.setPDBCode(structureDataInterface.getStructureId());
		// The list of chains to return
		List<Chain> outChains = new ArrayList<>();
		String pdbId = structureDataInterface.getStructureId();
		int atomCounter = 0;
		int groupCounter = 0;
		// Now loop through the entities
		for(int i=0; i<structureDataInterface.getNumChains(); i++){
			String chainId = structureDataInterface.getChainIds()[i];
			String entityType = SparkUtils.getType(structureDataInterface,i);
			EntityInfo entityInfo = new EntityInfo();
			entityInfo.setType(EntityType.entityTypeFromString(entityType));
			Chain chain = new ChainImpl();
			structure.addChain(chain);
			outChains.add(chain);
			chain.setId(chainId);
			chain.setEntityInfo(entityInfo);
			// Loop over the group indices
			for(int groupId=0; groupId<structureDataInterface.getGroupsPerChain()[i]; groupId++){
				int groupType = structureDataInterface.getGroupTypeIndices()[groupCounter];
				// Now add the group level information
			}
		}
		// Now return the chains
		return outChains;
	}


}
