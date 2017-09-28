import os
import re

from pyspark import SparkContext

blog_folder = "C:\\Users\\Bharath Krishna\\PycharmProjects\\BigData1\\blogs\\"
filenames = os.listdir(blog_folder)

sc = SparkContext(appName="BigData_Part2")

filename_rdd = sc.parallelize(filenames)
industries = filename_rdd.map(lambda filename: filename.split('.')[-3]).distinct().collect()
industry_names = sc.broadcast(industries)


#print(industry_names.value)

file_rdd = sc.wholeTextFiles(blog_folder)

# Regex to get the date and post
group_regex_string = '(?:<date>)([\s\S]*?)(?:<\/date>)[\s\S]*?(?:<post>)([\s\S]*?)(?:<\/post>)'
group_regex = re.compile(group_regex_string, re.IGNORECASE)

matches = file_rdd.flatMap(lambda file: group_regex.findall(file[1], re.IGNORECASE))

# match char which is not alphabet,digit or hyphen (ie. match all punctuations and word boundaries)
# with negative lookbehind to treat special case of industry name occuring as first word
match_non_chars_neg = '(?<![a-zA-Z0-9\-])'

# match char which is not alphabet,digit or hyphen (ie. match all punctuations and word boundaries)
match_non_chars = '[^a-zA-Z0-9\-]'

# Regex to get the industry names, which is either first word in post,
# or with previous and next characters as punctuations or word boundaries
# Example : (?<![a-zA-Z0-9\\-])(Internet|indUnk)[^a-zA-Z0-9\\-]
regex_string = match_non_chars_neg + '(' + '|'.join(
    industry_names.value) + ')' + match_non_chars

regex = re.compile(regex_string, re.IGNORECASE)


def match_regex(string):
    vals = regex.findall(string)
    #  Convert to lower for case-insensitive treatment and Remove duplicates using set
    return list(set([x.lower() for x in vals]))


# Create tuple of the form ((industry,date),1) to proceed similar to wordCount
ind_match = matches.flatMap(lambda t: [((s, t[0][t[0].find(',') + 1:]), 1) for s in match_regex(t[1])])

# Find the counts of occurrences of each (industry,date) pairs. Then convert it to the form (industry,(date,count))
# and run groupByKey and map the values to required output format
result = ind_match.reduceByKey(lambda t1, t2: t1 + t2).map(lambda t: (t[0][0], (t[0][1], t[1]))).groupByKey().map(
    lambda x: (x[0], tuple(x[1]))).collect()
print(result)


# Documentations reference
# Spark available operations - http://spark.apache.org/docs/2.1.1/programming-guide.html
# Python regex
# https://docs.python.org/3.5/howto/regex.html
# https://www.regular-expressions.info/lookaround.html
