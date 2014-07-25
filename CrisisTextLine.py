import pandas as pd
import numpy as np
from collections import defaultdict
import matplotlib.pyplot as plt
import operator
import datetime as dt
import os
import csv

# Loading and processing the data

os.chdir('/home/lj/ML/CrisisTextLine/Data Challenge')

data = pd.read_csv('conversation_level_analyst_data (1).csv')

fmt = '%m/%d/%Y %H:%M'
time_cols = ['queue_enter', 'queue_exit', 'conv_start', 'conv_end']

convert_to_datetime = lambda x: dt.datetime.strptime(x, fmt)
convert_to_datetime2 = lambda x: dt.datetime.strptime(x, fmt) if type(x)==str else x
ns_to_s = lambda x: float(x)/1e9
ns_to_min = lambda x: float(x)/(1e9*60) if type(x)==np.timedelta64 else x

data['queue_enter'] = data['queue_enter'].apply(convert_to_datetime, 1)
data['queue_exit'] = data['queue_exit'].apply(convert_to_datetime2, 1)
data['conv_start'] = data['conv_start'].apply(convert_to_datetime, 1)
data['conv_end'] = data['conv_end'].apply(convert_to_datetime, 1)

data['conv_time'] = data['conv_end'] - data['conv_start']
data['conv_time'] = data['conv_time'].apply(ns_to_min, 1)

data['total_time'] = data['conv_end'] - data['queue_enter']
data['total_time'] = data['total_time'].apply(ns_to_min, 1)

data['queue_time'] = data['queue_exit'] - data['queue_enter']
data['queue_time'] = data['queue_time'].apply(ns_to_min, 1)

data.to_csv('data.csv')

data = pd.read_csv('data.csv')

# Counting the Issues
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

# Counting the Resolutions
res_count = defaultdict(int)

for i, row in data.iterrows():
    try:
        resolutions = row['Q8_conv_resolution'].split(',')
        for resolution in resolutions:
            res_count[resolution] += 1
    except:
        pass

with open('resolution_count.csv', 'wb') as f:
    csvwriter = csv.writer(f, delimiter=',')
    for key in res_count.keys():  
        csvwriter.writerow([key, res_count[key]])

# Counselor Rating
rating_counselor_grp = data.groupby(['conv_rating'])
bins = [15*x for x in xrange(25)]
bins = [7.5*x for x in xrange(50)]
bins = [3.75*x for x in xrange(100)]

results = ['Feeling better', 'Feeling the same', 'Feeling worse']
for key, group in rating_counselor_grp:
    plt.figure()
    group['conv_time'].hist(bins=bins)
    plt.title(results.pop() + ' (n=' + str(len(group)) +')')
    plt.xlabel('Length of conversation (minutes)')
    plt.ylabel('Frequency')
    plt.axvline(group['conv_time'].mean(), color='b', linestyle='dashed')
    print 'mean: ', group['conv_time'].mean()
    print 'median: ', group['conv_time'].median()

plt.show()



# Texter Rating
rating_visitor_grp = data.groupby(['Q36_visitor_feeling'])
results = ['Much Better', 'Improved', 'Worsened', 'Remained the same']
results = ['Remained the same', 'Worsened', 'Improved', 'Significantly better']

#plt.figure()
for key, group in rating_visitor_grp:
    plt.figure()
    group['conv_time'].hist(bins=bins, label=str(key))
    plt.xlabel('Length of conversation (minutes)')
    plt.ylabel('Frequency')
    print key, len(group)
    plt.title(results.pop() + ' (n=' + str(len(group)) +')')
    '''
    print 'mean: ', group['conv_time'].mean()
    print 'median: ', group['conv_time'].median()
    '''

plt.title('Overlayed Histogram of Texter Ratings vs. Conversation Length')
plt.legend(loc = 'upper right')
plt.show()


pd.crosstab(data.conv_rating, data.Q36_visitor_feeling, margins=True)






# Crisis Center

centers_grp = data.groupby('crisis_center_id')
plt.figure()
for key, group in centers_grp:
    print key, group['conv_time'].median()
    print key, group['queue_time'].median()
    group['conv_time'].hist(bins=bins)


# Counselors
counselors_grp = data.groupby('actor_id')
conv_time_med = {}
for key, group in counselors_grp:
    print key, group['conv_time'].median(), len(group)
    conv_time_med[key] = group['conv_time'].median()

# Flags
flag_grp = data.groupby('flag')

# Texters
texter_grp = data.groupby('texter_id')
repeats = texter_grp.filter(lambda x: len(x) > 1)
repeats_grp = repeats.groupby('texter_id')

repeats.to_csv('repeats.csv')
with open('repeats.csv', 'wb') as f:
    csvwriter = csv.writer(f, delimiter=',')
    for key, group in repeats_grp:
        csvwriter.writerow([key, len(group)])


first_time = pd.Series()
not_first = pd.Series()

for texter_id, texter_data in repeats_grp:
    print texter_id, len(texter_data)
    first_time = first_time.append(pd.Series(texter_data.index[0]))
    not_first = not_first.append(pd.Series(texter_data.index[1:]))
    #print texter_data.ix[1:, 'queue_enter']


first_time_data = data.ix[first_time]
not_first_data = data.ix[not_first]

first_time_data.to_csv('first_time.csv', index=False)
not_first_data.to_csv('not_first_time.csv', index=False)



def count_item(data, column_name, delimiter=','):
    counts = defaultdict(int) 
    for i, row in data.iterrows():
        try:
            items = row[column_name].split(delimiter)
            for item in items:
                counts[item] += 1
        except:
            pass
    return counts

first_time_resolutions = count_item(first_time_data, 'Q8_conv_resolution')
not_first_time_resolutions = count_item(not_first_data, 'Q8_conv_resolution')


def write_dict_to_csv(dictionary, filename):
    with open(filename + '.csv', 'wb') as f:
        csvwriter = csv.writer(f, delimiter=',')
        for key, value in dictionary.iteritems():
            csvwriter.writerow([key, value])



write_dict_to_csv(first_time_resolutions, 'first_time_resolutions')
write_dict_to_csv(not_first_time_resolutions, 'not_first_time_resolutions')



repeat_issues = defaultdict(int)

for i, row in repeats.iterrows():
    try:
        issues = row['Q13_issues'].split(',')
        for issue in issues:
            repeat_issues[issue] += 1
    except:
        pass


with open('repeats_issues.csv', 'wb') as f:
    csvwriter = csv.writer(f, delimiter=',')
    for issue, count in repeat_issues.iteritems():
        csvwriter.writerow([issue, count])

repeat_res = defaultdict(int)

for i, row in repeats.iterrows():
    try:
        resolutions = row['Q8_conv_resolution'].split(',')
        for resolution in resolutions:
            repeat_res[resolution] += 1
    except:
        pass

with open('repeat_resolution_count.csv', 'wb') as f:
    csvwriter = csv.writer(f, delimiter=',')
    for key in repeat_res.keys():  
        csvwriter.writerow([key, repeat_res[key]])






# Conversation Type

conv_type_grp = data.groupby('Q2_conv_type')

'''
plt.figure()
f, (ax1, ax2, ax3, ax4) = plt.subplots(3, sharex=True, sharey=True)

for key, group in rating_visitor_grp:
    group['conv_time'].hist(bins=bins)
    plt.title(key + ' (n=' + str(len(group)) +')')

plt.show()

f, (ax1, ax2, ax3) = plt.subplots(3, sharex=True, sharey=True)
g = rating_counselor_grp.get_group(-1.0)['conv_time']
ax1 = g.hist(bins=bins)
g = rating_counselor_grp.get_group(1.0)['conv_time']
ax2 = g.hist(bins=bins)
g = rating_counselor_grp.get_group(2.0)['conv_time']
ax3 = g.hist(bins=bins)
plt.show()


ax1.hist(g, bins=100, histtype='bar')
g = rating_counselor_grp.get_group(1.0)['conv_time']
ax2.hist(g, bins=100, histtype='bar')
g = rating_counselor_grp.get_group(2.0)['conv_time']
ax3.hist(g, bins=100, histtype='bar')
plt.show()



n, bins, patches = plt.hist(group['conv_time'], 20, histtype='bar')
#group['conv_time'].plot(title=str(key))
'''
