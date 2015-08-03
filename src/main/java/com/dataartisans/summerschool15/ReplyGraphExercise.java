/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.summerschool15;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class ReplyGraphExercise {

	// TODO adjust path
	private static String pathToArchive = "/path/to/dev-flink.apache.org.archive";

	public static void main(String[] args) throws Exception {

		// get an ExecutionEnvironment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// format: (msg ID, sender, reply-to msg ID)
		DataSet<Tuple3<String, String, String>> mails = getEmailDataSet(env, pathToArchive);

		DataSet<Tuple2<String, String>> replyConnections = mails
				.join(mails)
				.where(0)
				.equalTo(2)
				.projectFirst(1)
				.projectSecond(1);

		replyConnections
				.groupBy(0, 1)
				.reduceGroup(new ConnectionCounter())
				.print();
	}

	public static class ConnectionCounter implements GroupReduceFunction<
			Tuple2<String, String>, Tuple3<String, String, Integer>> {

		@Override
		public void reduce(
				Iterable<Tuple2<String, String>> values,
				Collector<Tuple3<String, String, Integer>> out) {

			String from = null;
			String to = null;
			int count = 0;

			for (Tuple2<String, String> val : values) {
				from = val.f0;
				to = val.f1;
				count++;
			}

			out.collect(new Tuple3<String, String, Integer>(from, to, count));
		}
	}

	// -------------------------------------------------------------------------

	private static DataSet<Tuple3<String, String, String>> getEmailDataSet(
			ExecutionEnvironment env,
			String pathToArchive) {

		return env
				// format: (msg ID, timestamp, sender, subject, reply-to msg ID)
				.readCsvFile(MailGraphExercise.class
						.getResource(pathToArchive).getPath())
				.fieldDelimiter("|")
				.includeFields("10101") // we want (msg ID, sender, reply-to msg ID)
				.types(String.class, String.class, String.class)
				.map(new MapFunction<Tuple3<String, String, String>, Tuple3<String, String, String>>() {
					@Override
					public Tuple3<String, String, String> map(
							Tuple3<String, String, String> value) throws Exception {

						value.f1 = value.f1.substring(
								value.f1.lastIndexOf("<") + 1,
								value.f1.length() - 1).trim();
						return value;
					}
				});
	}
}
