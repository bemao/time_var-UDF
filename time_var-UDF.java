import org.apache.pig.EvalFunc;  //location: $PIG_HOME/pig-0.14.0-mapr-1504.jar
import org.apache.pig.data.Tuple; //location: $PIG_HOME/pig-0.14.0-mapr-1504.jar
import org.apache.pig.data.DataBag; //location: /opt/mapr/hadoop/hadoop-2.5.1/share/hadoop/common/hadoop-common-2.5.1-mapr-1501.jar
import org.apache.pig.data.TupleFactory; //used for building new Tuples

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class findTimeStats extends EvalFunc<Tuple>
	{
		public static Double date_dist(String d1, String d2){
				
				SimpleDateFormat date_parser = new SimpleDateFormat("yyyy-MM-dd");
				
				Date date1 = null;
				Date date2 = null;
				
				try {
					date1 = date_parser.parse(d1);
					date2 = date_parser.parse(d2);
				} catch (ParseException e) {
					return 0.0;
				}
				
				long diff = date2.getTime() - date1.getTime();
				long diff_in_days = TimeUnit.MILLISECONDS.toDays(diff);
				
				return 1.0*diff_in_days;
			}
		
		public static ArrayList<Double> time_stats(ArrayList<String> good_dates){
			//output: List (average, variance)
			
			ArrayList<Double> time_between_dates = new ArrayList<Double>();
			ArrayList<Double> time_stats_out = new ArrayList<Double>();
			
			int num_dates = good_dates.size();
			if (num_dates <= 1){                
				time_stats_out.add(0.0);
				time_stats_out.add(0.0);
			}
			
			// sort dates from oldest to most recent
			Collections.sort(good_dates, new Comparator<String>() {
				@Override
				public int compare(String s1, String s2){
					Integer s1_int = Integer.parseInt(s1.replace("-", ""));
					Integer s2_int = Integer.parseInt(s2.replace("-", ""));
					
					if (s1_int < s2_int){
						return -1;
					} else if (s1_int > s2_int) {
						return 1;
					} else {
						return 0;
					}
				}
			});
			
			// create ArrayList of time (in days) between transactions
			int prev = 0;
			for (int j = 1; j<num_dates; j++) {
				System.out.println(good_dates.get(prev));
				System.out.println(good_dates.get(j
						));
				time_between_dates.add(date_dist(good_dates.get(prev), good_dates.get(j)));
				prev = j;
			}
			
			// computes avg of time_between_dates
			double tot = 0.0;
			for (Double d: time_between_dates) {
				System.out.println(d);
				tot += d;
			}
			
			double avg = tot/(num_dates - 1);
			time_stats_out.add(avg);
			
			tot = 0.0;
			for (Double d: time_between_dates) {
				tot += (d-avg)*(d-avg);
			}
			time_stats_out.add(Math.sqrt(tot/(num_dates - 1)));
			
			return time_stats_out;
		}
		
		public Tuple exec(Tuple input) throws IOException {
			DataBag bag = (DataBag)input.get(0);
			Iterator it = bag.iterator();
			
			ArrayList<String> dates = new ArrayList<String>();
			
			while(it.hasNext()){
				Tuple t = (Tuple)it.next();   //The bag contains a set of tuples (YYYY-mm-dd)
				if(t != null && t.size() > 0 && t.get(0) != null){
				String date = t.get(0).toString();
					if (date.length() == 10){
						dates.add(date);
						}
				}
			}
			
			TupleFactory tf = TupleFactory.getInstance();
			Tuple out_tpl = tf.newTuple(time_stats(dates));
			
			return out_tpl;
	}
}
