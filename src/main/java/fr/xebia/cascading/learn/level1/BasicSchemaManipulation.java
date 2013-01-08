package fr.xebia.cascading.learn.level1;

import cascading.flow.FlowDef;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Retain;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * {@link Tuple} have a soft schema defined by {@link Fields}.
 */
public class BasicSchemaManipulation {

	/**
	 * We want to replicate results of the plain copy (see level 0) but you are
	 * out of luck. The provided source has also a spam "discardme" field. Use
	 * {@link Discard} in order to get rid of it.
	 * 
	 * discardme, line -> line
	 * 
	 * The result file has the names of the fields in the first line.
	 * 
	 * @see http://docs.cascading.org/cascading/2.1/userguide/html/ch09s03.html
	 */
	public static FlowDef discardField(Tap<?, ?, ?> source, Tap<?, ?, ?> sink) {
		return null;
	}
	
	/**
	 * Same logic but we want to {@link Retain} only the "line" field.
	 * 
	 * donotretainme, line -> line
	 * 
	 * @see http://docs.cascading.org/cascading/2.1/userguide/html/ch09s05.html
	 */
	public static FlowDef retainField(Tap<?, ?, ?> source, Tap<?, ?, ?> sink) {
		return null;
	}
	
	/**
	 * Bad luck again, the field is badly named "renameme", {@link Rename} it into "line".
	 * 
	 * renameme -> line
	 * 
	 * @see http://docs.cascading.org/cascading/2.1/userguide/html/ch09s04.html
	 */
	public static FlowDef renameField(Tap<?, ?, ?> source, Tap<?, ?, ?> sink) {
		return null;
	}

}
