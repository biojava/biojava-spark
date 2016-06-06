# BioJava-Spark
Algorithms that are built around BioJava and are running on Apache Spark

[![Build Status](https://travis-ci.org/biojava/biojava-spark.svg?branch=master)](https://travis-ci.org/biojava/biojava-spark)
[![License](http://img.shields.io/badge/license-LGPL_2.1-blue.svg?style=flat)](https://github.com/biojava/biojava/blob/master/LICENSE)
[![Status](http://img.shields.io/badge/status-experimental-red.svg?style=flat)](https://github.com/biojava/biojava-spark)

# Starting up

### Some initial instructions can be found on the mmtf-spark project
https://github.com/rcsb/mmtf-spark
## First download and untar a Hadoop sequence file of the PDB (~7 GB download) 
```
wget http://mmtf.rcsb.org/v0.2/hadoopfiles/full.tar
tar -xvf full.tar
```
Or you can get a C-alpha, phosphate, ligand only version (~800 Mb download)
```
wget http://mmtf.rcsb.org/v0.2/hadoopfiles/reduced.tar
tar -xvf reduced.tar
```
### Second add the mmtf-spark dependecy to your pom

```xml
		<dependency>
			<groupId>org.biojava</groupId>
			<artifactId>biojava-spark</artifactId>
			<version>0.1.0</version>
		</dependency>
```



## Extra Biojava examples

### Do some simple quality filtering

```
	new StructureDataRDD("/path/to/file")
				.filterResolution(maxResolution)
				.filterRfree(maxRfree)
				.size();
```

### Summarsing the elements in the PDB
```
	Map<String, Long> elementCountMap = BiojavaSparkUtils.findAtoms(new StructureDataRDD("/path/to/file")).countByElement();
```

### Finding inter-atomic contacts from the PDB

```
		Double mean = BiojavaSparkUtils.findContacts(new StructureDataRDD("/path/to/file"),
				new AtomSelectObject()
						.groupNameList(new String[] {"PRO","LYS"})
						.elementNameList(new String[] {"C"})
						.atomNameList(new String[] {"CA"}),
						cutoff)
				.getDistanceDistOfAtomInts("CA", "CA")
				.mean();
		System.out.println("\nMean PRO-LYS CA-CA distance: "+mean);
```

