package fr.xebia.cascading.learn.level2;

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

public class MappingTest {
	
	@Before
	public void doNotCareAboutOsStuff() {
		System.setProperty("line.separator", "\n");
	}
	
	@Test
	public void learnToFilterWithExpression() throws Exception {
		// input of the job
		String sourcePath = "src/test/resources/hadoop-wiki-sample.txt";
		Tap<?, ?, ?> source = new FileTap(new TextLine(new Fields("line")), sourcePath);
		
		// actual output of the job
		String sinkPath = "target/level2/filter-with-expression.txt";
		Tap<?, ?, ?> sink = new FileTap(new TextDelimited(true, "\t"), sinkPath, SinkMode.REPLACE);
		
		// create the job definition, and run it
		FlowDef flowDef = Mapping.filterWithExpression(source,sink);
		new LocalFlowConnector().connect(flowDef).complete();
		
		// check that actual and expect outputs are the same
		Assert.sameContent(sinkPath, "src/test/resources/level2/filter-with-expression/expectation.txt");
	}
	
	@Test
	public void learnToTransformWithExpression() throws Exception {
		// input of the job
		String sourcePath = "src/test/resources/hadoop-wiki-sample.txt";
		Tap<?, ?, ?> source = new FileTap(new TextLine(new Fields("line")), sourcePath);
		
		// actual output of the job
		String sinkPath = "target/level2/transform-with-expression.txt";
		Tap<?, ?, ?> sink = new FileTap(new TextDelimited(true, "\t"), sinkPath, SinkMode.REPLACE);
		
		// create the job definition, and run it
		FlowDef flowDef = Mapping.transformWithExpression(source,sink);
		new LocalFlowConnector().connect(flowDef).complete();
		
		// check that actual and expect outputs are the same
		Assert.sameContent(sinkPath, "src/test/resources/level2/transform-with-expression/expectation.txt");
	}
	
	@Test
	public void learnToTransformWithCustomExpression() throws Exception {
		// input of the job
		String sourcePath = "src/test/resources/level2/transform-with-expression/expectation.txt";
		Tap<?, ?, ?> source = new FileTap(new TextDelimited(true, "#"), sourcePath);
		
		// actual output of the job
		String sinkPath = "target/level2/custom-transform.txt";
		Tap<?, ?, ?> sink = new FileTap(new TextDelimited(true, "\t"), sinkPath, SinkMode.REPLACE);
		
		// create the job definition, and run it
		FlowDef flowDef = Mapping.transformWithACustomFunction(source,sink);
		new LocalFlowConnector().connect(flowDef).complete();
		
		// check that actual and expect outputs are the same
		Assert.sameContent(sinkPath, "src/test/resources/level2/custom-transform/expectation.txt");
	}

}
