package com.datasalt.pangool.gowalla;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.tuplemr.IdentityTupleReducer;
import com.datasalt.pangool.tuplemr.OrderBy;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.mapred.lib.input.TupleTextInputFormat;
import com.datasalt.pangool.tuplemr.mapred.lib.output.TupleTextOutputFormat;

/**
 */
@SuppressWarnings("serial")
public class CoOcurrencesJob implements Tool, Configurable, Serializable {

	@Parameter(names = { "-i", "--input" }, description = "Gowalla checkins file to use as input.")
	private String input = "Gowalla_sample.txt";

	@Parameter(names = { "-o", "--output" }, description = "The output to use.")
	private String output = "out-gowalla";

	private transient Configuration conf;

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

  @Override
	public int run(String[] args) throws Exception {
		JCommander jComm = new JCommander(this);
		jComm.setProgramName("Gowalla-Pangool demo.");
		try {
			jComm.parse(args);
		} catch(Throwable t) {
			t.printStackTrace();
			jComm.usage();
			return -1;
		}

		FileSystem.get(conf).delete(new Path(output), true);

		TupleMRBuilder mr = new TupleMRBuilder(conf);

		List<Field> gowallaFields = Fields
		    .parse("userid:int, checkin:string, lat:double, lng:double, locationid:int");
		final Schema gowallaSchema = new Schema("gowalla", gowallaFields);
		gowallaFields.add(Field.create("geogroupid", Type.INT));
		final Schema intermediateSchema = new Schema("gowalla_intermediate", gowallaFields);

		InputFormat<?, ?> inputFormat = new TupleTextInputFormat(gowallaSchema, false, false, '\t',
		    TupleTextInputFormat.NO_QUOTE_CHARACTER, TupleTextInputFormat.NO_ESCAPE_CHARACTER, null, null);
		OutputFormat<?, ?> outputFormat = new TupleTextOutputFormat(intermediateSchema, false, '\t',
		    TupleTextInputFormat.NO_QUOTE_CHARACTER, TupleTextInputFormat.NO_ESCAPE_CHARACTER);

		mr.addInput(new Path(input), inputFormat, new TupleMapper<ITuple, NullWritable>() {

			Tuple augmented = new Tuple(intermediateSchema);
			
			@Override
			public void map(ITuple tuple, NullWritable ignore, TupleMRContext ctx, Collector collector)
			    throws IOException, InterruptedException {
				
				for(Field f: gowallaSchema.getFields()) {
					augmented.set(f.getName(), tuple.get(f.getName()));
				}
				augmented.set("geogroupid", 1);
				collector.write(augmented);
			}
		});
		mr.addIntermediateSchema(intermediateSchema);
		
		mr.setGroupByFields("geogroupid");
		mr.setOrderBy(OrderBy.parse("geogroupid:desc, lat:desc"));
		
		mr.setTupleReducer(new IdentityTupleReducer());
		mr.setOutput(new Path(output), outputFormat, ITuple.class, NullWritable.class);
		mr.createJob().waitForCompletion(true);
		return 0;
	}

	public static void main(String args[]) throws Exception {
		ToolRunner.run(new CoOcurrencesJob(), args);
	}
}
