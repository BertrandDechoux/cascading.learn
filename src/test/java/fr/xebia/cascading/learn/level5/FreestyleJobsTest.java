package fr.xebia.cascading.learn.level5;

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

public class FreestyleJobsTest {
	
	@Before
	public void doNotCareAboutOsStuff() {
		System.setProperty("line.separator", "\n");
	}
	
	@Test
	public void passCountWordOccurenceChallenge() throws Exception {
		// input of the job
		String sourcePath = "src/test/resources/hadoop-wiki-sample.txt";
		Tap<?, ?, ?> source = new FileTap(new TextLine(new Fields("line")), sourcePath);

		// actual output of the job
		String sinkPath = "target/level5/wordcount.txt";
		Tap<?, ?, ?> sink = new FileTap(new TextDelimited(true, "\t"), sinkPath, SinkMode.REPLACE);
		
		// create the job definition, and run it
		FlowDef flowDef = FreestyleJobs.countWordOccurences(source, sink);
		new LocalFlowConnector().connect(flowDef).complete();
		
		// check that actual and expect outputs are the same
		Assert.sameContent(sinkPath, "src/test/resources/level5/wordcount/expectation.txt");
	}
	
	@Test
	public void passTfIdfChallenge() throws Exception {
		// input of the job
		String sourcePath = "src/test/resources/hadoop-wiki-extract.txt";
		Tap<?, ?, ?> source = new FileTap(new TextDelimited(true, "\t"), sourcePath);

		// actual output of the job
		String sinkPath = "target/level5/tfidf.txt";
		Tap<?, ?, ?> sink = new FileTap(new TextDelimited(true, "\t"), sinkPath, SinkMode.REPLACE);
		
		// create the job definition, and run it
		FlowDef flowDef = FreestyleJobs.computeTfIdf(source, sink);
		new LocalFlowConnector().connect(flowDef).complete();
		
		// check that actual and expect outputs are the same
		Assert.sameContent(sinkPath, "src/test/resources/level5/tfidf/expectation.txt");
	}

}
