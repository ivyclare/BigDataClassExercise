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
package eu.stratosphere.tutorial.task1;

import java.util.Set;
import java.util.StringTokenizer;

import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;


/**
 * This mapper is part of the document frequency computation.
 * <p>
 * The document frequency of a term is the number of documents it occurs in. If a document contains a term three times,
 * it is counted only once for this document. But if another document contains the word as well, the overall frequency
 * is two.
 * <p>
 * Example:
 * 
 * <pre>
 * Document 1: "Big Big Big Data"
 * Document 2: "Hello Big Data"
 * </pre>
 * 
 * The document frequency of "Big" is 2, because it appears in two documents (even though it appears four times in
 * total). "Hello" has a document frequency of 1, because it only appears in document 2.
 * <p>
 * The map method will be called independently for each document.
 */
public class DocumentFrequencyMapper extends MapFunction {

	// ----------------------------------------------------------------------------------------------------------------

	/**
	 * Splits the document into terms and emits a PactRecord (term, 1) for each term of the document.
	 * <p>
	 * Each input document has the format "docId, document contents".
	 * <p>
	 * Example:
	 * 
	 * <pre>
	 * 1,Gartner's definition (the 3Vs) is still widely used
	 * </pre>
	 * 
	 * The document ID of the document is 1 (start of line before the comma). The terms to be extracted are:
	 * <ul>
	 * <li>gartner</li>
	 * <li>s</li>
	 * <li>definition</li>
	 * <li>3vs</li>
	 * <li>still</li>
	 * <li>widely</li>
	 * </ul>
	 * Note that the stop words "the" and "is" have been removed and everything has been lower cased.
	 */
	@Override
	public void map(Record record, Collector<Record> collector) {
		// Document with format "docId, document contents"
		String document = record.getField(0, StringValue.class).toString();
		
		//Getting only the document contents
		String content[] = document.split(",");
		document = content[1];
		//Replacing all nonword characters with space
		document = document.replaceAll("\\W", "").toLowerCase();
		
		StringTokenizer token = new StringTokenizer(document);
		Set<String> duplicateWords = new HashSet<String>();
		Set<String> stopwords = Util.STOP_WORDS;
		
		//Ensuring that duplicate words are not counted more than once in a document
		while (token.hasMoreElements()){
			String word = token.nextToken();
			duplicateWords.add(word);
		}
		
		//Making sure stop words are not among the tokens
		for(String word : duplicateWords){
			if(stopWords.contains(word.toString()))
				continue;			
				collector.collect(new Record(new StringValue(word), new IntValue(1)));
		}
	  }
   }
		
		
		
		
		
		
		
		
	}
}