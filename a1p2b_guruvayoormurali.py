from pyspark import SparkContext
import zipfile
import io
import os
import string
import re

blog_folder = "C:\\Users\\Bharath Krishna\\PycharmProjects\\BigData1\\blogs\\"
filenames = os.listdir(blog_folder)

sc = SparkContext(appName="BigData_Part2")

filename_rdd = sc.parallelize(filenames)
industries = filename_rdd.map(lambda filename: filename.split('.')[-3]).distinct().collect()
industry_names = sc.broadcast(industries)

print(industry_names.value)

file_rdd = sc.wholeTextFiles(blog_folder)

group_regex_string = '(?:<date>)([\s\S]*?)(?:<\/date>)[\s\S]*?(?:<post>)([\s\S]*?)(?:<\/post>)'
group_regex = re.compile(group_regex_string, re.IGNORECASE)

matches = file_rdd.flatMap(lambda file: group_regex.findall(file[1], re.IGNORECASE))

regex_string = '[' + string.punctuation + '\s]{1}' + '(' + '|'.join(
    industry_names.value) + ')[' + string.punctuation + '\s]{1}'
match_first_regex = '^(' + '|'.join(industry_names.value) + ')'
match_last_regex = '(' + '|'.join(industry_names.value) + ')$'

# full_regex_string = match_first_regex + '|' + regex_string + '|' + match_last_regex
# print(full_regex_string)
regex1 = re.compile(regex_string, re.IGNORECASE)
regex2 = re.compile(match_first_regex, re.IGNORECASE)
regex3 = re.compile(match_last_regex, re.IGNORECASE)

reg = [regex1,regex2,regex3]

def match_regex(string):
    vals = []
    for r in reg:
        val=r.findall(string)
        vals.extend(val)

    return [x.lower() for x in list(set(vals))]

ind_match = matches.flatMap(lambda t: [((s,t[0][t[0].find(',')+1:]),1) for s in match_regex(t[1])])

result = ind_match.reduceByKey(lambda t1,t2: t1 + t2).map(lambda t: (t[0][0],(t[0][1],t[1]))).groupByKey().map(lambda x:(x[0],tuple(x[1]))).collect()
print(result)
