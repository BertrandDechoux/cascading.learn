package fr.xebia.cascading.learn.level6;

import cascading.operation.Assertion;
import cascading.operation.Debug;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.Discard;

/**
 * That's the end. Hope you enjoyed it. But before leaving, 
 * here are a few tips that could help you in the future.
 */
public class CleanCode {

	/**
	 * The {@link Debug} is a pretty useful to understand the flow of data at a specific point.
	 * 
	 * @see http://docs.cascading.org/cascading/3.0/userguide/ch16-operations.html#debug-function
	 * @see http://docs.cascading.org/cascading/3.0/userguide/ch18-best-practices.html#_debugging_streams
	 */
	public void experimentWithTheDebugFunction() {
		throw new UnsupportedOperationException("Go back and try it on your own.");
	}
	
	/**
	 * {@link Assertion}s are also useful a concept, even in cascading.
	 * 
	 * @see http://docs.cascading.org/cascading/3.0/userguide/ch15-advanced.html#stream-assertions
	 * @see http://docs.cascading.org/cascading/3.0/userguide/ch16-operations.html#_assertions
	 */
	public void experimentWithTheAssertions() {
		throw new UnsupportedOperationException("Go back and try it on your own.");
	}
	
	/**
	 * Last but not least is the concept of {@link SubAssembly} which allows
	 * to package your common flow operations in a more concise way.
	 * You already have been using them, look at the class hierarchy and 
	 * the code of {@link Discard}.
	 * 
	 * @see http://docs.cascading.org/cascading/3.0/userguide/ch15-advanced.html#subassemblies
	 * @see http://docs.cascading.org/cascading/3.0/userguide/ch17-subassemblies.html#subassemblies
	 * @see http://docs.cascading.org/cascading/3.0/userguide/ch18-best-practices.html#_subassemblies_not_factories
	 */
	public void experimentWithSubAssemblies() {
		throw new UnsupportedOperationException("Go back and try it on your own.");
	}
	
}
