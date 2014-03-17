/***********************************************************************************************************************
 *
 * Copyright (C) 2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.tutorial.task2;

import java.util.Iterator;
import java.util.Map;

import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;
import eu.stratosphere.tutorial.util.Util;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.StringValue;

/**
 * This mapper computes the term frequency for each term in a document.
 * <p>
 * The term frequency of a term in a document is the number of times the term occurs in the respective document. If a
 * document contains a term three times, the term has a term frequency of 3 (for this document).
 * <p>
 * Example:
 * 
 * <pre>
 * Document 1: "Big Big Big Data"
 * Document 2: "Hello Big Data"
 * </pre>
 * 
 * The term frequency of "Big" in document 1 is 3 and 1 in document 2.
 * <p>
 * The map method will be called independently for each document.
 */
public class TermFrequencyMapper extends MapFunction {

	// ----------------------------------------------------------------------------------------------------------------

	/**
	 * Splits the document into terms and emits a PactRecord (docId, term, tf) for each term of the document.
	 * <p>
	 * Each input document has the format "docId, document contents".
	 */
	
	//Defining a Record with 3 parameters
	private final Record result = new Record(3);
	
	@Override
	public void map(Record record, Collector<Record> collector) {
		String document = record.getField(0, StringValue.class).toString();
		
		//Getting only the document contents
		String content[] = document.split(",");
		int docID = Integer.parseInt(content[0]);
		
		document = content[1];
		//Replacing all nonword characters with space
		document = document.replaceAll("\\W", "").toLowerCase();
		
		StringTokenizer token = new StringTokenizer(document);
		Map<String,Integer> map = new HashSet<String,Integer>();
		Set<String> stopwords = Util.STOP_WORDS;
		
		//Ensuring that duplicate words are not counted more than once in a document
		int counts = 1; 
		while (token.hasMoreElements()){
			String word = token.nextToken();
		
		//Stop should not be stored in map
			if(stopWords.contains(word.toString())){
				continue;
			}		
			
		//if map contains word
			if(map.containsKey(word))
			  {
				counts = map.get(word);
				map.put(word,count+1);
			  }
			
		//add a new word with a count of 1 to map
			else 
			{
				map.put(word,1);
			}
	}
	
	Iterator iterator = map.entrySet().iterator();
	while(iterator.hasNext()){
		Map.Entry pair = (Map.Entry)iterator.next();
		String word = pair.getKey().toString();
		int frequency = Integer.parseInt(pair.getValue().toString());
		
		//output
		result.setField(0,new IntValue(docID));
		result.setField(1,new StringValue(word));
		result.setField(2,new IntValue(frequency));
	  }
	}
 
}