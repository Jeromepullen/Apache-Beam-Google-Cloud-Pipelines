package com.jeromepullen.googleapache;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;

public class WriteToLocalFile {
	
	public static void main(String[] args) {
		
		//Define the options for the pipeline
		PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
		
		//Create the pipeline
		Pipeline pipeline = Pipeline.create(pipelineOptions);
		
		//Create the ArrayList of Strings to be inserted into the csv file
		final List<String> input = Arrays.asList("This","is","a","test");
		
		//Write the ArrayList to a csv file in the specified local folder
		pipeline.apply(Create.of(input)).apply(TextIO.write().to("/Users/jeromepullenjr/Downloads/ApacheBeamOutput/1/Test").withNumShards(1).withSuffix(".txt"));
		
		//Run the pipeline
		pipeline.run().waitUntilFinish();
	}

}
