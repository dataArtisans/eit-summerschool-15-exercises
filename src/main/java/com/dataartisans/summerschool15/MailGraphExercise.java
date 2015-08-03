package com.dataartisans.summerschool15;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 * Exercise 1. Implement all TODOs.
 *
 * Expected output looks like this:
 * (12,sewen@apache.org,35)
 */
public class MailGraphExercise {

	public static void main(String[] args) throws Exception {

		// get an ExecutionEnvironment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// (timestamp, sender)
		DataSet<Tuple2<String, String>> mails = getEmailDataSet(env);

		mails
				.map(new MailMonthEmailExtractor())
				.filter(new ExcludeJiraAndGit())
				.groupBy(0, 1)
				.reduceGroup(new MailCounter())
				.print();
	}

	public static class MailMonthEmailExtractor implements MapFunction<
			Tuple2<String, String>, Tuple2<String, String>> {

		@Override
		public Tuple2<String, String> map(Tuple2<String, String> value) throws Exception {
			// Input: (2015-03-02-21:52:27, NAME <email@adress.org>)
			return new Tuple2<String, String>(getMonth(value.f0), getEmailAddress(value.f1));
		}

		/**
		 * Returns the email address from strings like "NAME <email@address.org>".
		 */
		private String getEmailAddress(String senderString) {
			return senderString.substring(
					senderString.lastIndexOf("<") + 1,
					senderString.length() - 1).trim();
		}

		/** Returns the prefix from strings like "2015-03-02-21:52:27". */
		private String getMonth(String timestamp) {
			return timestamp.substring(0, 7);
		}
	}

	public static class ExcludeJiraAndGit implements FilterFunction<Tuple2<String, String>> {

		@Override
		public boolean filter(Tuple2<String, String> value) throws Exception {
			return !(value.f1.equals("jira@apache.org")
					|| value.f1.equals("git@git.apache.org"));
		}
	}

	public static class MailCounter implements GroupReduceFunction<
			Tuple2<String, String>, Tuple3<String, String, Integer>> {

		@Override
		public void reduce(
				Iterable<Tuple2<String, String>> values,
				Collector<Tuple3<String, String, Integer>> out) throws Exception {

			String timestamp = null;
			String sender = null;

			int count = 0;

			for (Tuple2<String, String> val : values) {
				timestamp = val.f0;
				sender = val.f1;
				count++;
			}

			out.collect(new Tuple3<String, String, Integer>(timestamp, sender, count));
		}
	}

	// -------------------------------------------------------------------------

	private static DataSet<Tuple2<String, String>> getEmailDataSet(ExecutionEnvironment env) {
		return env
				// format: (msg ID, timestamp, sender, subject, reply-to msg ID)
				.readCsvFile(
						ClassLoader.getSystemClassLoader()
								.getResource("dev@flink.apache.org.archive").getPath())
				.fieldDelimiter("|")
				.includeFields("011000") // we want (timestamp, sender)
				.types(String.class, String.class);
	}

}
