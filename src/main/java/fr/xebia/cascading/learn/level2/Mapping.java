package fr.xebia.cascading.learn.level2;

import cascading.flow.FlowDef;
import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.operation.expression.ExpressionFilter;
import cascading.operation.expression.ExpressionFunction;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * Now that we know how to maintain a clean schema, it is time to actually manipulate each input individually.
 * {@link Each} will allow you to execute a function for all inputs, a bit like a for loop.
 */
public class Mapping {
	
	/**
	 * Use a {@link Each} in order to {@link Filter} out "line"s which do not contains "Hadoop".
	 * {@link ExpressionFilter} makes it very easy for simple case.
	 * 
	 * source field(s) : "line"
	 * sink field(s) : "line"
	 * 
	 * @see http://docs.cascading.org/cascading/2.5/userguide/html/ch09s07.html
	 */
	public static FlowDef filterWithExpression(Tap<?, ?, ?> source, Tap<?, ?, ?> sink) {
		return null;
	}
	
	/**
	 * Use a {@link Each} in order to transform every "line" in lowercase.
	 * {@link Function} allows to change the input, and again 
	 * {@link ExpressionFunction} is really helpful for simple case.
	 * 
	 * source field(s) : "line"
	 * sink field(s) : "line"
	 *
	 * @see http://docs.cascading.org/cascading/2.5/userguide/html/ch09s07.html
	 */
	public static FlowDef transformWithExpression(Tap<?, ?, ?> source, Tap<?, ?, ?> sink) {
		return null;
	}
	
	/**
	 * Split each "line" into "word"s using a custom {@link Function}.
	 * The provided function do only the identity.
	 * 
	 * input field(s) : "line"
	 * output field(s) : "word"
	 * 
	 * @see http://docs.cascading.org/cascading/2.5/userguide/html/ch05s02.html
	 * 
	 * Of course, for a real use case, regular expressions could be used with existent functions
	 * @see http://docs.cascading.org/cascading/2.5/userguide/html/ch09s06.html
	 */
	public static FlowDef transformWithACustomFunction(Tap<?, ?, ?> source,
			Tap<?, ?, ?> sink) {
		Pipe pipe = new Each("split", new Fields("line"), new CustomSplitFunction<Object>(new Fields("word")), Fields.SWAP);
		return FlowDef.flowDef()//
				.addSource(pipe, source) //
				.addTail(pipe)//
				.addSink(pipe, sink);
	}
	
	

}
