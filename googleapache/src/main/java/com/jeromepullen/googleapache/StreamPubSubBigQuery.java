package com.jeromepullen.googleapache;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import com.google.api.services.bigquery.model.TableRow;

public class StreamPubSubBigQuery {
	
	public static void main(String[] args) {
		
		//Define the options for the pipeline
		DataflowPipelineOptions dataflowPipelineOptions = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
		
		//Set the Google Cloud project info
		dataflowPipelineOptions.setJobName("StreamingIngestion");
		dataflowPipelineOptions.setProject("project-1-319500");
//		dataflowPipelineOptions.setRegion("us-central-1");
//		dataflowPipelineOptions.setGcpTempLocation(null);
		dataflowPipelineOptions.setRunner(DataflowRunner.class);

		//Create the pipeline
		Pipeline pipeline = Pipeline.create(dataflowPipelineOptions);
		
		//Read the message/strings from PubSub "PubSubTopic1"
		PCollection<String> pubsubmessage = pipeline.apply(PubsubIO.readStrings().fromTopic("projects/project-1-319500/topics/PubSubTopic1"));
		
		//Create a new row for each of the strings that are read in each message from PubSub
		PCollection<TableRow> bqrow = pubsubmessage.apply(ParDo.of(new ConvertorStringBq()));
	
		//Write the PubSub messages to BigQuery
		bqrow.apply(BigQueryIO.writeTableRows().to("project-1-319500:jerome_apache_beam.PubSubStream")
			.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
			.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

			//Run the pipeline
			pipeline.run();
			
	}
	
	public static class ConvertorStringBq extends DoFn<String, TableRow> {
		
		@ProcessElement
		public void processing(ProcessContext processContext)
		{
			//Assign the message, messageid, and messageprocessingtime to a field/row in the BigQuery table
			TableRow tableRow = new TableRow().set("message", processContext.element().toString())
					.set("messageid", processContext.element().toString()+":"+processContext.timestamp().toString())
					.set("messageprocessingtime", processContext.timestamp().toString());
			
			//Execute the processing
			processContext.output(tableRow);
		}
		
		

	}
}