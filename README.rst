Input: 
a text string containing English words, whitespace (spaces and newlines) and punctuation 
like commas, periods, question marks and semicolons.

For example:

"""Hello, I like nuts. Do you like nuts? No? Are you sure? 
Why don't you like nuts? Are you nuts? I like you"""

Output:

Print a list of triplets. Each triplet is a pair of words and a count

For example the output for the sample input:

Are you: 2 
like nuts: 3 
you like: 3 
I like: 2

A pair of words should show up in the output if one of the words follows the other 
in the input and are separated only by whitespace. Every pair that shows up more than 
once should have an entry in the output with the correct number of occurrences. Note, 
that the order of the words in the pair doesn't matter: 'green bee' and 'bee green' are 
2 occurrences of the same pair. Ignore case. 'BlUe sKY' is the same pair as 'SKy bLUE'.

Your mission if you choose to accept it:

Write a function that accepts the input and produces the output


**This is my solution to the coding challenge**

I have designed this to be deployable easily as a production caliber solution.

The dependencies are in requirements.txt
pip install -r requirements.txt

To run unit tests:
python -m unittest discover

To run the job with sample input (you can use any file you would like)
python jobs/triplets_job.py sample_input.txt

We can discuss how this would be run on EMR or Hadoop / scheduling



