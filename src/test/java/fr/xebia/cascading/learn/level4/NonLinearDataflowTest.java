package fr.xebia.cascading.learn.level4;

import org.junit.Before;
import org.junit.Test;

import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import fr.xebia.cascading.learn.Assert;

public class NonLinearDataflowTest {
	
	@Before
	public void doNotCareAboutOsStuff() {
		System.setProperty("line.separator", "\n");
	}
	
	@Test
	public void learnToCoGroup() throws Exception {
		// inputs of the job
		String presidentsPath = "src/test/resources/fifth-france-presidents.txt";
		Tap<?, ?, ?> presidentsSource = new FileTap(new TextDelimited(true, "\t"), presidentsPath);
		
		String partiesPath = "src/test/resources/fifth-france-parties.txt";
		Tap<?, ?, ?> partiesSource = new FileTap(new TextDelimited(true, "\t"), partiesPath);
		
		// actual output of the job
		String sinkPath = "target/level4/cogroup.txt";
		Tap<?, ?, ?> sink = new FileTap(new TextDelimited(true, "\t"), sinkPath, SinkMode.REPLACE);
		
		// create the job definition, and run it
		FlowDef flowDef = NonLinearDataflow.cogroup(presidentsSource, partiesSource, sink);
		new LocalFlowConnector().connect(flowDef).complete();
		
		// check that actual and expect outputs are the same
		Assert.sameContent(sinkPath, "src/test/resources/level4/cogroup/expectation.txt");
	}
	
	@Test
	public void learnToSplit() throws Exception {
		// input of the job
		String sourcePath = "src/test/resources/level4/cogroup/expectation.txt";
		Tap<?, ?, ?> source = new FileTap(new TextDelimited(true, "\t"), sourcePath);
		
		// actual outputs of the job
		String gaullistPath = "target/level4/split-gaullist.txt";
		Tap<?, ?, ?> gaullistSink = new FileTap(new TextDelimited(true, "\t"), gaullistPath, SinkMode.REPLACE);
		
		String republicanPath = "target/level4/split-republican.txt";
		Tap<?, ?, ?> republicanSink = new FileTap(new TextDelimited(true, "\t"), republicanPath, SinkMode.REPLACE);
		
		String socialistPath = "target/level4/split-socialist.txt";
		Tap<?, ?, ?> socialistSink = new FileTap(new TextDelimited(true, "\t"), socialistPath, SinkMode.REPLACE);
		
		// create the job definition, and run it
		FlowDef flowDef = NonLinearDataflow.split(source, gaullistSink, republicanSink, socialistSink);
		new LocalFlowConnector().connect(flowDef).complete();
		
		// check that actual and expect outputs are the same
		Assert.sameContent(gaullistPath, "src/test/resources/level4/split/expectation-gaullist.txt");
		Assert.sameContent(republicanPath, "src/test/resources/level4/split/expectation-republican.txt");
		Assert.sameContent(socialistPath, "src/test/resources/level4/split/expectation-socialist.txt");

	}

}
