package fr.xebia.cascading.learn.level3;

import org.junit.Before;
import org.junit.Test;

import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import fr.xebia.cascading.learn.Assert;

public class ReducingTest {
	
	@Before
	public void doNotCareAboutOsStuff() {
		System.setProperty("line.separator", "\n");
	}
	
	@Test
	public void learnToAggregate() throws Exception {
		// input of the job
		String sourcePath = "src/test/resources/level2/custom-transform/expectation.txt";
		Tap<?, ?, ?> source = new FileTap(new TextDelimited(true, "#"), sourcePath);
		
		// actual output of the job
		String sinkPath = "target/level3/aggregate.txt";
		Tap<?, ?, ?> sink = new FileTap(new TextDelimited(true, "\t"), sinkPath, SinkMode.REPLACE);
		
		// create the job definition, and run it
		FlowDef flowDef = Reducing.aggregate(source,sink);
		new LocalFlowConnector().connect(flowDef).complete();
		
		// check that actual and expect outputs are the same
		Assert.sameContent(sinkPath, "src/test/resources/level3/expectation.txt");
	}
	
	@Test
	public void learnToEfficientlyAggregate() throws Exception {
		// input of the job
		String sourcePath = "src/test/resources/level2/custom-transform/expectation.txt";
		Tap<?, ?, ?> source = new FileTap(new TextDelimited(true, "#"), sourcePath);
		
		// actual output of the job
		String sinkPath = "target/level3/aggregate.txt";
		Tap<?, ?, ?> sink = new FileTap(new TextDelimited(true, "\t"), sinkPath, SinkMode.REPLACE);
		
		// create the job definition, and run it
		FlowDef flowDef = Reducing.efficientlyAggregate(source,sink);
		new LocalFlowConnector().connect(flowDef).complete();
		
		// check that actual and expect outputs are the same
		Assert.sameContent(sinkPath, "src/test/resources/level3/expectation.txt");
	}

}
