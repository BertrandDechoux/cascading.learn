package fr.xebia.cascading.learn.level2;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class CustomSplitFunction<Context> extends BaseOperation<Context> implements Function<Context> {
	private static final long serialVersionUID = 1L;
	
	public CustomSplitFunction(Fields wordField) {
		super(1, wordField);
	}

	@Override
	public void operate(@SuppressWarnings("rawtypes") FlowProcess flowProcess,
			FunctionCall<Context> functionCall) {
		String line = functionCall.getArguments().getString(0);
		String word = line; // or not ?
		functionCall.getOutputCollector().add(new Tuple(word));
		functionCall.getOutputCollector().add(new Tuple(word));
	}
	
}