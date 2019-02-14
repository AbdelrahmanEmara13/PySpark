# PySpark

Question One:

Implement distinct functionality provided by RDD.
Assume you have the following input
init_data = [(1,"elem1"),(2,"elem1"),(2,"elem2"),(2,"elem2")]
The output should be when we get the unique value per key
(1, ['elem1'])
(2, ['elem1', 'elem2'])
We also need to get the unique values globally

Question two:
Using the purplecow.txt file on the VM, write a Spark job that reads
the file and prints on the screen the line that is the longest.
Hints :
1- The path is : /home/training/training_materials/data/
purplecow.txt
2- Load purple cow.txt as local file not hfds file
