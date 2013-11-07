package com.datasalt.pangool.gowalla;

import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

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
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.TupleReducer;
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
		gowallaFields.add(Field.create("latgroupid", Type.INT));
		gowallaFields.add(Field.create("lnggroupid", Type.INT));
		gowallaFields.add(Field.create("timegroupid", Type.INT));
		final Schema intermediateSchema = new Schema("gowalla_intermediate", gowallaFields);
		final Schema outputSchema = new Schema("output",
		    Fields.parse("locationid1:int, locationid2:int"));

		InputFormat<?, ?> inputFormat = new TupleTextInputFormat(gowallaSchema, false, false, '\t',
		    TupleTextInputFormat.NO_QUOTE_CHARACTER, TupleTextInputFormat.NO_ESCAPE_CHARACTER, null, null);
		OutputFormat<?, ?> outputFormat = new TupleTextOutputFormat(outputSchema, false, '\t',
		    TupleTextInputFormat.NO_QUOTE_CHARACTER, TupleTextInputFormat.NO_ESCAPE_CHARACTER);

		final double GRID_SIZE = 0.01; // degrees
		final double TIME_GRID_SIZE = 60; // seconds

		final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		format.setTimeZone(TimeZone.getTimeZone("UTC"));

		mr.addInput(new Path(input), inputFormat, new TupleMapper<ITuple, NullWritable>() {

			Tuple augmented = new Tuple(intermediateSchema);

			@Override
			public void map(ITuple tuple, NullWritable ignore, TupleMRContext ctx, Collector collector)
			    throws IOException, InterruptedException {

				for(Field f : gowallaSchema.getFields()) {
					augmented.set(f.getName(), tuple.get(f.getName()));
				}

				// Latitude has a minimum of -90 (south pole) and a maximum of 90
				// Longitude has a minimum of -180 (west of the prime meridian) and a maximum of 180
				double normLat = tuple.getDouble("lat") + 90;
				double normLng = tuple.getDouble("lng") + 180;

				int latId = (int) (normLat / GRID_SIZE);
				int lngId = (int) (normLng / GRID_SIZE);

				try {
					String checkin = tuple.getString("checkin");
					int UTCSeconds = (int) (format.parse(checkin).getTime() / 1000);
					int timeId = (int) (UTCSeconds / TIME_GRID_SIZE);
					
					augmented.set("latgroupid", latId);
					augmented.set("lnggroupid", lngId);
					augmented.set("timegroupid", timeId);
					
					collector.write(augmented);
				} catch(ParseException e) {
					throw new IOException(e);
				}
			}
		});
		mr.addIntermediateSchema(intermediateSchema);
		mr.setGroupByFields("latgroupid", "lnggroupid", "timegroupid");
		mr.setTupleReducer(new TupleReducer<ITuple, NullWritable>() {

			Tuple outTuple = new Tuple(outputSchema);

			public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context,
			    Collector collector) throws IOException, InterruptedException, TupleMRException {

				Set<Integer> locationIds = new HashSet<Integer>();
				for(ITuple tuple : tuples) {
					locationIds.add(tuple.getInteger("locationid"));
				}

				if(locationIds.size() > 0) {
					for(Integer locationId : locationIds) {
						for(Integer locationId2 : locationIds) {
							if(locationId != locationId2) {
								outTuple.set("locationid1", locationId);
								outTuple.set("locationid2", locationId2);
								collector.write(outTuple, NullWritable.get());
							}
						}
					}
				}
			};
		});
		mr.setOutput(new Path(output), outputFormat, ITuple.class, NullWritable.class);
		mr.createJob().waitForCompletion(true);

		return 0;
	}

	public static void main(String args[]) throws Exception {
		ToolRunner.run(new CoOcurrencesJob(), args);
	}
}
