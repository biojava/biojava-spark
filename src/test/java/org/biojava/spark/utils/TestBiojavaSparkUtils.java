package org.biojava.spark.utils;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;

import org.biojava.nbio.structure.Atom;
import org.junit.Test;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.decoder.GenericDecoder;
import org.rcsb.mmtf.decoder.ReaderUtils;
import org.rcsb.mmtf.spark.data.AtomSelectObject;

/**
 * Test the {@link BiojavaSparkUtils} class.
 * @author Anthony Bradley
 *
 */
public class TestBiojavaSparkUtils {

	/**
	 * Test that getAtoms gets a list of atoms.
	 * @throws IOException an error reading from the URL
	 */
	@Test
	public void testGetAndFilterAtoms() throws IOException {
		StructureDataInterface structureDataInterface = getData();
		List<Atom> atoms = BiojavaSparkUtils.getAtoms(structureDataInterface);
		assertEquals(atoms.size(),1107);
		List<Atom> calphas = BiojavaSparkUtils.getAtoms(structureDataInterface, new AtomSelectObject().atomNameList(new String[] {"CA"}));
		assertEquals(calphas.size(),116);
		List<Atom> prolines_lys = BiojavaSparkUtils.getAtoms(structureDataInterface, new AtomSelectObject().groupNameList(new String[] {"LYS","PRO"}));
		assertEquals(prolines_lys.size(),155);
		List<Atom> zybs = BiojavaSparkUtils.getAtoms(structureDataInterface, new AtomSelectObject().groupNameList(new String[] {"ZYB"}));
		assertEquals(zybs.size(),18);
		List<Atom> charged = BiojavaSparkUtils.getAtoms(structureDataInterface, new AtomSelectObject().charged(true));
		assertEquals(charged.size(),16);
		List<Atom> fluorines = BiojavaSparkUtils.getAtoms(structureDataInterface, new AtomSelectObject().elementNameList(new String[] {"F"}));
		assertEquals(fluorines.size(),1);
	}

	private StructureDataInterface getData() throws IOException {
		return new GenericDecoder(ReaderUtils.getDataFromUrl("4cup"));
	}
	
	
}
