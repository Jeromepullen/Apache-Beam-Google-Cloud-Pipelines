package com.jeromepullen.googleapache;


import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;


public class ReadFromBucketAndAddToBigQuery {
	
	  private static final Logger LOG = LoggerFactory.getLogger(ReadFromBucketAndAddToBigQuery.class);

	  public static void main(String[] args) {
		  
		  //Set up BigQuery pipeline
		  BigQuery bigQuery = PipelineOptionsFactory.fromArgs(args).withValidation().as(BigQuery.class);
		  
		  //Set the BigQuery project info
		  bigQuery.setTempLocation("gs://jerome_apache_beam/input");
		  bigQuery.setStagingLocation("gs://jerome_apache_beam/input");
		  bigQuery.setProject("project-1-319500");
		  
		//Create the BigQuery pipeline
	    Pipeline p = Pipeline.create(bigQuery);
	    
	    
	    //Create the Table Schema for BigQuery using an ArrayList
	    List<TableFieldSchema> columns = new ArrayList<TableFieldSchema>();
	    columns.add(new TableFieldSchema().setName("userId").setType("STRING"));
	    columns.add(new TableFieldSchema().setName("name").setType("STRING"));
	    columns.add(new TableFieldSchema().setName("country").setType("STRING"));
	    
	    TableSchema tblSchema = new TableSchema().setFields(columns);
	    
	    //Get the data from the Google Cloud Bucket
	    PCollection<String> pInput = p.apply(TextIO.read().from("gs://jerome_apache_beam/input/GoogleCloudTestNew.csv"));
	    
	    //Read the objects from the file
	    pInput.apply(ParDo.of(new DoFn<String, TableRow>(){
	    	
	    	@ProcessElement
	    	public void processElement(ProcessContext c) {
	    		
	    		String arr[] = c.element().split(",");
	    		
	    		//Set the objects from the file into separate rows within the BigQuery table
	    		TableRow row = new TableRow();
	    		
	    		row.set("userId", arr[0]);
	    		row.set("name", arr[1]);
	    		row.set("country", arr[2]);
	    		c.output(row);
	    		
//	    		System.out.println(c.element());

	      }
	    }))
	    
	    //Write the the objects from the file in the Google Bucket to BigQuery table "beam_table_new"
	    .apply(BigQueryIO.writeTableRows().to("jerome_apache_beam.beam_table_new")
	    		.withSchema(tblSchema)
	    		.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
	    		.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
	    		);
	    

	    //Run the pipeline "p"
	    p.run();
	  }
	}
 
