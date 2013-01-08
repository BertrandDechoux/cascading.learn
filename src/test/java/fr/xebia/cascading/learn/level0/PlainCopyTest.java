package fr.xebia.cascading.learn.level0;

import org.junit.Before;
import org.junit.Test;

import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.scheme.local.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import fr.xebia.cascading.learn.Assert;

public class PlainCopyTest {
	
	@Before
	public void doNotCareAboutOsStuff() {
		System.setProperty("line.separator", "\n");
	}
	
	@Test
	public void knowTheBasics() throws Exception {
		// input of the job
		String sourcePath = "src/test/resources/hadoop-wiki-sample.txt";
		Tap<?, ?, ?> source = new FileTap(new TextLine(new Fields("line")), sourcePath);
		
		// actual output of the job
		String sinkPath = "target/level0/plain-copy.txt";
		Tap<?, ?, ?> sink = new FileTap(new TextLine(), sinkPath, SinkMode.REPLACE);
		
		// create the job definition, and run it
		FlowDef flowDef = PlainCopy.createFlowDefUsing(source,sink);
		new LocalFlowConnector().connect(flowDef).complete();
		
		// check that actual and expect outputs are the same
		Assert.sameContent(sinkPath, "src/test/resources/level0/expectation.txt");
	}

}
