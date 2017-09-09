# h-index-spark-

# Big Data Systems 2017, First Spark Assignment

In this assignment, you will use Spark to calculate the h-index.
 https://en.wikipedia.org/wiki/H-index

# h-index

The h-index is an indicator of a researcher's productivity and impact. For a given researcher, we count all the citations to their work. Then we put them in descending order. The h-index is the position, in the order we created, of the last publication whose citation count is greater than the position.

A nice example is given in the Wikipedia article mentioned above. Say we have two researchers, R1R1 and R2R2. Both of them happen to have five publications each, to which we will refer with Ri,jRi,j. For researcher R1R1 we count the citations, order then, and we get:

R1,1=10,R1,2=8,R1,3=5,R1,4=4,R1,5=3R1,1=10,R1,2=8,R1,3=5,R1,4=4,R1,5=3

Similarly, for researcher R2R2 we get:

R2,1=25,R2,2=8,R2,3=5,R2,4=3,R2,5=3R2,1=25,R2,2=8,R2,3=5,R2,4=3,R2,5=3

Then R1R1 has an h-index of 4, because the fourth publication has 4 citations and the fifth has 3. In the same way, R2R2 has an h-index of 3, because the fourth publication has only 3 citations.

Your program will work with files of the form:

R2:A, R2:A, R1:A, R2:C, R2:B, R1:A, R2:C, R2:E, R2:D, R2:D, R1:A, R2:E, R1:D, R2:B
R1:B, R1:A, R1:B, R2:C, R2:B, R2:A, R1:D, R1:B, R1:D, R2:B, R1:A, R1:B
R2:A, R2:A, R1:B, R2:A, R1:C, R2:A, R2:A, R2:A, R2:C, R1:B, R2:A, R2:A, R2:D
R1:E, R2:C, R2:E, R1:A, R1:B, R1:A, R2:A, R2:B, R2:B, R2:A, R2:A, R1:C
R1:C, R2:A, R1:C, R2:A, R2:A, R2:A, R1:E, R2:A, R1:E, R2:B
R1:A, R2:A, R1:D, R2:A, R2:A, R2:A, R1:A, R1:B, R2:A, R1:A, R2:A, R2:B, R1:C

The lines have records separated by commas. Each record mention a researcher and a publication: that is, each record is a citation. Each publication is identifiable by researcher, so R2:A and R1:A are two different publications. Your purpose is to write a Spark program that takes such files and produces an output of the form:

R1 4
R2 3

That is, each line should contain a researcher and their h-index. The researchers should be sorted.

