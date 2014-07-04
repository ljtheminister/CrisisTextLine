import pandas as pd
import numpy as np
from collections import defaultdict
import matplotlib.pyplot as plt
import operator
import datetime as dt

data = pd.read_csv('conversation_level_analyst_data (1).csv')

fmt = '%m/%d/%Y %H:%M'
time_cols = ['queue_enter', 'queue_exit', 'conv_start', 'conv_end']

convert_to_datetime = lambda x: dt.datetime.strptime(x, fmt)
convert_to_datetime2 = lambda x: dt.datetime.strptime(x, fmt) if type(x)==str else x
ns_to_s = lambda x: float(x)/1e9
ns_to_min = lambda x: float(x)/(1e9*60)

data['queue_enter'] = data['queue_enter'].apply(convert_to_datetime, 1)
data['queue_exit'] = data['queue_exit'].apply(convert_to_datetime2, 1)
data['conv_start'] = data['conv_start'].apply(convert_to_datetime, 1)
data['conv_end'] = data['conv_end'].apply(convert_to_datetime, 1)

data['conv_time'] = data['conv_end'] - data['conv_start']
data['total_time'] = data['conv_end'] - data['queue_enter']
data['conv_time'] = data['conv_time'].apply(ns_to_min, 1)
data['total_time'] = data['total_time'].apply(ns_to_min, 1)



res_count = defaultdict(int)


rating_counselor_grp = data.groupby(['conv_rating'])
rating_visitor_grp = data.groupby(['Q36_visitor_feeling'])

bins = [15*x for x in xrange(24)]
for key, group in rating_counselor_grp:
    plt.figure()
    group['conv_time'].hist(bins=bins)
    plt.title(str(key))

plt.show()

    n, bins, patches = plt.hist(group['conv_time'], 20, histtype='bar')
    #group['conv_time'].plot(title=str(key))


for i, row in data.iterrows():
    try:
        resolutions = row['Q8_conv_resolution'].split(',')
        for resolution in resolutions:
            res_count[resolution] += 1
    except:
        pass




issues_count = defaultdict(int)

for i, row in data.iterrows():
    print i
    try:
        issues = row['Q13_issues'].split(',')
        for issue in issues:
            issues_count[issue] += 1
    except:
        pass

sorted_counts = sorted(issues_count.iteritems(), key=operator.itemgetter(1), reverse=True)

issues, counts = [], []
'''
for k, v in sorted_counts:
    issues.append(k)
    counts.append(v)
    plt.bar(v, label=k)

plt.legend(issues)
plt.show()



plt.bar(range(len(counts)), counts)
plt.legend(issues)
plt.show()
'''

conv_length = []
total_time = []
for i, row in data.iterrows():
    conv_start = dt.datetime.strptime(row['conv_start'], fmt)
    conv_end = dt.datetime.strptime(row['conv_end'], fmt)
    call_start = dt.datetime.strptime(row['queue_enter'], fmt)





dt.strptime(start, 
conv_length = [
