package fr.xebia.cascading.learn.level3;

import cascading.flow.FlowDef;
import cascading.operation.Aggregator;
import cascading.operation.Function;
import cascading.operation.aggregator.Count;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.assembly.CountBy;
import cascading.tap.Tap;

/**
 * Once each input has been individually curated, it can be needed to aggregate
 * information.
 */
public class Reducing {

    /**
     * {@link GroupBy} "word" and then apply {@link Count}. It should be noted
     * that once grouped, the semantic is different. You will need to use a
     * {@link Every} instead of a {@link Each}. And {@link Count} is an
     * {@link Aggregator} instead of a {@link Function}.
     * 
     * source field(s) : "word" sink field(s) : "word","count"
     * 
     * @see http://docs.cascading.org/cascading/3.0/userguide/ch05-pipe-assemblies.html#_groupby
     */
    public static FlowDef aggregate(Tap<?, ?, ?> source, Tap<?, ?, ?> sink) {
        return null;
    }

    /**
     * Aggregation should be done as soon as possible and Cascading does have a
     * technique almost similar to map/reduce 'combiner'. Use {@link CountBy} in
     * order to do the same thing as above. It is shorter to write and more
     * efficient.
     * 
     * source field(s) : "word" sink field(s) : "word","count"
     * 
     * @see http://docs.cascading.org/cascading/3.0/userguide/ch17-subassemblies.html#CountBy
     */
    public static FlowDef efficientlyAggregate(Tap<?, ?, ?> source, Tap<?, ?, ?> sink) {
        return null;
    }
}
