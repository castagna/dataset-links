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

package cmd;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import org.openjena.riot.RiotWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP;
import com.hp.hpl.jena.sparql.util.Timer;
import com.kasabi.labs.datasets.Utils;

public class Links {

	public static final String KASABI_API_KEY = System.getenv("KASABI_API_KEY_DEMO");
	private static final Map<String, Set<Query>> queries = new HashMap<String, Set<Query>>();
	private static final Logger log = LoggerFactory.getLogger(Links.class) ;
	private static final int OFFSET_INCREMENT = 100000 ;

	public static void main(String[] args) throws IOException {
		Dataset ds = DatasetFactory.createMem();
		File path = new File("src/main/resources");
		for (File file : path.listFiles()) {
			if ( file.isDirectory() ) {
				String dataset = file.getName();
				String uri = "http://data.kasabi.com/dataset/" + dataset;
				Model model = ds.getNamedModel(uri);
				log.info("Processing {} dataset...", dataset);
				for ( Query query : getQueries ( file ) ) {
					long results = -1;
					int i = 0;
					while ( results != 0 ) {
						try {
							Timer timer = new Timer();
							timer.startTimer();
							query.setLimit(OFFSET_INCREMENT);
							query.setOffset(i * OFFSET_INCREMENT);
							log.debug("Offset is {} and limit is {}", i * OFFSET_INCREMENT, OFFSET_INCREMENT);
							QueryExecution qexec = QueryExecutionFactory.sparqlService("http://api.kasabi.com/dataset/" + dataset + "/apis/sparql", query);
							((QueryEngineHTTP) qexec).addParam("apikey", KASABI_API_KEY);
							Model result = qexec.execConstruct();
							results = result.size();
				            model.add ( result );
				            log.info("Retrieved {} triples in {} seconds ...", results, (timer.endTimer() / 1000));
				            qexec.close();
				            i++;							
						} catch ( Exception e ) {
							log.error(e.getMessage(), e);
							results = 0;
						} catch ( Throwable t ) {							
							log.error(t.getMessage(), t);
							results = 0;
						}
					}
				}
			}
		}
		
		OutputStream out = new BufferedOutputStream( new GZIPOutputStream( new FileOutputStream ("links.nq.gz") ) );
		RiotWriter.writeNQuads(out, ds.asDatasetGraph());
		out.close();
	}

	private static Set<Query> getQueries(File path) {
		if ( !queries.containsKey(path.getName()) ) {
			File[] files = path.listFiles(new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					return name.endsWith(".rq");
				}
			});
			Arrays.sort(files);
			Set<Query> qs = new HashSet<Query>();
			for (File file : files) {
				Query query = QueryFactory.read(file.getAbsolutePath());
				qs.add(query);
			}
			queries.put(path.getName(), qs);
		}
		return queries.get(path.getName());
	}
	
}
