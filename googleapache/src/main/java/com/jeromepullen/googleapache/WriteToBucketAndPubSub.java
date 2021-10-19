package com.jeromepullen.googleapache;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;

public class WriteToBucketAndPubSub {
	public static void main(String[] args) {
		
		//Define the options for the pipeline
		DataflowPipelineOptions pipelineOptions = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
		
		//Set the Google Cloud project info
		pipelineOptions.setJobName("PubSubJobBestBuy");
		pipelineOptions.setProject("project-1-319500");
//		pipelineOptions.setRegion("us-central-1");
		pipelineOptions.setRunner(DataflowRunner.class);
//		pipelineOptions.setGcpTempLocation(null);
		
		
		//Create the pipeline
		Pipeline pipeline = Pipeline.create(pipelineOptions);
		
		//Make list of words to add to file and PubSub message
		final List<String> input = Arrays.asList("Best","Buy","is","GREAT!");
		
		//Where to write the pipeline to
		pipeline.apply(Create.of(input)).apply(TextIO.write().to("gs://jerome_apache_beam/input/BestBuy").withNumShards(1).withSuffix(".txt"));
		pipeline.apply(Create.of(input)).apply(PubsubIO.writeStrings().to("projects/project-1-319500/topics/PubSubTopic1"));
		
		//Run the pipeline
		pipeline.run().waitUntilFinish();
	}
}
