package fr.xebia.cascading.learn.level4;

import cascading.flow.FlowDef;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.pipe.joiner.InnerJoin;
import cascading.tap.Tap;
import cascading.tap.hadoop.TemplateTap;
import cascading.tuple.Fields;

/**
 * Up to now, operations were stacked one after the other. But the dataflow can
 * be non linear, with multiples sources, multiples sinks, forks and merges.
 */
public class NonLinearDataflow {
	
	/**
	 * Use {@link CoGroup} in order to know the party of each presidents.
	 * You will need to create (and bind) one Pipe per source.
	 * You might need to correct the schema in order to match the expected results.
	 * 
	 * @see http://docs.cascading.org/cascading/2.1/userguide/html/ch03s03.html#N20650
	 */
	public static FlowDef cogroup(Tap<?, ?, ?> presidentsSource, Tap<?, ?, ?> partiesSource,
			Tap<?, ?, ?> sink) {
		return null;
	}
	
	/**
	 * Split the input in order use a different sink for each party. There is no
	 * specific operator for that, use the same Pipe instance as the parent.
	 * You will need to create (and bind) one named Pipe per sink.
	 * 
	 * In a different context, one could use {@link TemplateTap} in order to arrive to a similar results.
	 * @see http://docs.cascading.org/cascading/2.1/userguide/htmlsingle/#N214FF
	 */
	public static FlowDef split(Tap<?, ?, ?> source,
			Tap<?, ?, ?> gaullistSink, Tap<?, ?, ?> republicanSink, Tap<?, ?, ?> socialistSink) {
		return null;
	}
	
}
