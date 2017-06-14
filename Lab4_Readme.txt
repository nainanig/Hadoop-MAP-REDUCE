---WordCount on Tweets----

- In this activity tweets are collected of a certain domain, in this case Politics: Topic - US Politics. Tweets have been collected in the same way as was done in Lab 1 using R programming.
- The collected tweets are written to a .txt file and then the code of "wordcount" is run over this input. The output of this is used to generate a "wordcloud" in Jupyter Notebook, namely WordCloud.


input folder->WordCount->input.txt (in input Folder)

output folder->WordCount->output_wcf - contains part-r-00000 which has the output word count

anciliary->WordCount->WordCount.ipynb

jar folder->WordCount-> wc.jar

Steps to run the code:
Run the following commands on terminal in ~/hadoop
1. start-hadoop.sh
2. hdfs dfs -mkdir -p ~/input
3. hdfs dfs -put ~/(path of folder contatining input.txt) ~/input
4. hadoop com.sun.tools.javac.Main WordCount.java
5. jar cf wc.jar WordCount*.class
6. hadoop jar wc.jar WordCount ~/input/(folder containing input.txt) ~/output_wcf

-----Word-Cooccurrence on Tweets-----

- In this activity two methods namely Pairs and Stripes were implemented on the tweets collected.
- Both of these methods give word cooccurence relationships.
- All files are contained in WordCooccur.

input folder->WordCooccur->input.txt

output folder->WordCooccur-> output_pair
Output folder->WordCooccur->output_stripe

jar folder-> WordCooccur->pairfinal.jar

jar folder-> WordCooccur->stripef.jar

----Featured Activity 1----

- In this activity lemmatization is performed over two Latin files. 
- All files are contained in FeaturedActivity1 folder.

input folder->FeaturedActivity1-> lucan.bellum_civile.part.1.tess, vergil.aeneid.tess. (cleaned files)

output folder->FeaturedActivity1->output_lemmaf

jar->FeaturedActivity1-> lemmafinal.jar

anciliary->FeaturedActivity1->new_lemmatizer.csv


----Featured Activity 2-----

-In this activity pair with lemmatization was implemented on n number of files to note the performance of mapreduce.
-Also Trigram is implemented in the same way as bigram is implemented.
-All the files are contained in the folder FeaturedActivity2 in respective folders.
-Trigram has been implemented without lemmatization.

Input- Latin files (choosen randomly) not in the folder

anciliary->FeaturedActivity2-> Bigram_Performance.ipynb.

jar->FeaturedActivity2->plemma.jar, trigram.jar

note-> All files and folders are also contained in Temp_all folder.

