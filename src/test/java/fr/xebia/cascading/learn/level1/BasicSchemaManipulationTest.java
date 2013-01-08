package fr.xebia.cascading.learn.level1;

import org.junit.Before;
import org.junit.Test;

import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.scheme.local.TextDelimited;
import cascading.scheme.local.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import fr.xebia.cascading.learn.Assert;

public class BasicSchemaManipulationTest {
	
	@Before
	public void doNotCareAboutOsStuff() {
		System.setProperty("line.separator", "\n");
	}
	
	@Test
	public void learnToDiscardField() throws Exception {
		// input of the job
		String sourcePath = "src/test/resources/hadoop-wiki-sample.txt";
		Tap<?, ?, ?> source = new FileTap(new TextLine(new Fields("discardme","line")), sourcePath);
		
		// actual output of the job
		String sinkPath = "target/level1/discard.txt";
		Tap<?, ?, ?> sink = new FileTap(new TextDelimited(true, "\t"), sinkPath, SinkMode.REPLACE);
		
		// create the job definition, and run it
		FlowDef flowDef = BasicSchemaManipulation.discardField(source,sink);
		new LocalFlowConnector().connect(flowDef).complete();
		
		// check that actual and expect outputs are the same
		Assert.sameContent(sinkPath, "src/test/resources/level1/expectation.txt");
	}
	
	@Test
	public void learnToRetainField() throws Exception {
		// input of the job
		String sourcePath = "src/test/resources/hadoop-wiki-sample.txt";
		Tap<?, ?, ?> source = new FileTap(new TextLine(new Fields("donotretainme","line")), sourcePath);
		
		// actual output of the job
		String sinkPath = "target/level1/retain.txt";
		Tap<?, ?, ?> sink = new FileTap(new TextDelimited(true, "\t"), sinkPath, SinkMode.REPLACE);
		
		// create the job definition, and run it
		FlowDef flowDef = BasicSchemaManipulation.retainField(source,sink);
		new LocalFlowConnector().connect(flowDef).complete();
		
		// check that actual and expect outputs are the same
		Assert.sameContent(sinkPath, "src/test/resources/level1/expectation.txt");
	}
	
	@Test
	public void learnToRenameField() throws Exception {
		// input of the job
		String sourcePath = "src/test/resources/hadoop-wiki-sample.txt";
		Tap<?, ?, ?> source = new FileTap(new TextLine(new Fields("renameme")), sourcePath);
		
		// actual output of the job
		String sinkPath = "target/level1/rename.txt";
		Tap<?, ?, ?> sink = new FileTap(new TextDelimited(true, "\t"), sinkPath, SinkMode.REPLACE);
		
		// create the job definition, and run it
		FlowDef flowDef = BasicSchemaManipulation.renameField(source,sink);
		new LocalFlowConnector().connect(flowDef).complete();
		
		// check that actual and expect outputs are the same
		Assert.sameContent(sinkPath, "src/test/resources/level1/expectation.txt");
	}

}
