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
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Set;
import java.util.TreeSet;
import java.util.zip.GZIPOutputStream;

import org.openjena.riot.RiotWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Node_URI;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP;
import com.hp.hpl.jena.sparql.util.Timer;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.tdb.base.file.Location;

public class Links2 {

	public static final String KASABI_API_KEY = System.getenv("KASABI_API_KEY_DEMO");
	private static final Logger log = LoggerFactory.getLogger(Links2.class) ;
	private static final Set<String> datasets = load ( "datasets.properties" );
	private static final Set<String> properties = load ( "properties.properties" );
	private static final int OFFSET_INCREMENT = 50000 ;

	public static void main(String[] args) throws IOException {
		Location location = new Location ("target/tdb");
		Dataset ds = TDBFactory.createDataset(location);
		DatasetGraph dsg = ds.asDatasetGraph();
		for ( String dataset : datasets ) {
			String uri = "http://data.kasabi.com/dataset/" + dataset;		
			Node g = Node_URI.createURI(uri); 
			for ( String property : properties) {
				log.info ("Searching statements with {} property in the {} dataset...", property, dataset);
				int count = 0;
				Timer timer = new Timer();
				timer.startTimer();
				try {
					int i = 0;
					long results = OFFSET_INCREMENT;
					while ( results == OFFSET_INCREMENT ) {
						Query query = QueryFactory.create("SELECT ?s ?o { ?s <" + property + "> ?o }");
						query.setLimit(OFFSET_INCREMENT);
						query.setOffset(i * OFFSET_INCREMENT);
						log.debug("{}", query.toString().replaceAll("\n", " "));
						i++;
						QueryExecution qexec = QueryExecutionFactory.sparqlService("http://api.kasabi.com/dataset/" + dataset + "/apis/sparql", query);
						((QueryEngineHTTP) qexec).addParam("apikey", KASABI_API_KEY);
						ResultSet rs = qexec.execSelect();
						results = 0;
						while ( rs.hasNext() ) {
							QuerySolution solution = rs.next();
							results++;
							Node s = solution.getResource("s").asNode();
							Node o = solution.get("o").asNode();
							if ( o.isURI() ) {
								if ( o.getURI().startsWith("http://data.ordnancesurvey.co.uk/") )  {
									dsg.add(g, s, Node_URI.createURI(property), o);
									count++;								
								}
							} else {
								dsg.add(g, s, Node_URI.createURI(property), o);
								count++;							
							}
						}
					}
				} catch ( Exception e ) {
					log.error(e.getMessage());
				} catch ( Throwable t ) {							
					log.error(t.getMessage());
				}
				if ( count != 0 ) log.info("Found {} quads in {} seconds.", count, (timer.endTimer() / 1000));
			}
		}
		
		OutputStream out = new BufferedOutputStream( new GZIPOutputStream( new FileOutputStream ("links.nq.gz") ) );
		RiotWriter.writeNQuads(out, ds.asDatasetGraph());
		out.close();
	}
	
	private static Set<String> load ( String filename ) {
		Set<String> result = new TreeSet<String>();
		try {
			BufferedReader in = new BufferedReader( new FileReader(filename) );
			String line;
			while ( ( line = in.readLine() ) != null ) {
				result.add ( line.trim() );
			}
		} catch (IOException e) {
			log.error (e.getMessage(), e);
		}
		return result;
	}
	
}
