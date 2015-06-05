package fr.xebia.cascading.learn.level5;

import cascading.flow.FlowDef;
import cascading.operation.aggregator.First;
import cascading.operation.aggregator.Max;
import cascading.operation.expression.ExpressionFilter;
import cascading.operation.expression.ExpressionFunction;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexReplace;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.CountBy;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Retain;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import fr.xebia.cascading.learn.level2.CustomSplitFunction;

/**
 * You now know all the basics operators. Here you will have to compose them by yourself.
 */
public class FreestyleJobs {

	/**
	 * Word count is the Hadoop "Hello world" so it should be the first step.
	 * 
	 * source field(s) : "line"
	 * sink field(s) : "word","count"
	 */
	public static FlowDef countWordOccurences(Tap<?, ?, ?> source, Tap<?, ?, ?> sink) {
		return null;
	}
	
	/**
	 * Now, let's try a non trivial job : td-idf. Assume that each line is a
	 * document.
	 * 
	 * source field(s) : "id","content"
	 * sink field(s) : "docId","tfidf","word"
	 * 
	 * <pre>
	 * t being a term
	 * t' being any other term
	 * d being a document
	 * D being the set of documents
	 * Dt being the set of documents containing the term t
	 * 
	 * tf-idf(t,d,D) = tf(t,d) * idf(t, D)
	 * 
	 * where
	 * 
	 * tf(t,d) = f(t,d) / max (f(t',d))
	 * ie the frequency of the term divided by the highest term frequency for the same document
	 * 
	 * idf(t, D) = log( size(D) / size(Dt) )
	 * ie the logarithm of the number of documents divided by the number of documents containing the term t 
	 * </pre>
	 * 
	 * Wikipedia provides the full explanation
	 * @see http://en.wikipedia.org/wiki/tf-idf
	 * 
	 * If you are having issue applying functions, you might need to learn about field algebra
	 * @see http://docs.cascading.org/cascading/2.5/userguide/html/ch03s07.html
	 * 
	 * {@link First} or {@link Max} can be useful for isolating the maximum.
	 * 
	 * {@link HashJoin} can allow to do cross join.
	 * 
	 * PS : Do no think about efficiency, at least, not for a first try.
	 * PPS : You can remove results where tfidf < 0.1
	 */
	public static FlowDef computeTfIdf(Tap<?, ?, ?> source, Tap<?, ?, ?> sink) {
		return null;
	}
	
}
