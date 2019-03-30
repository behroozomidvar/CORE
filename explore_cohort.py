import trajectories
import psycopg2
import operator
import datetime
import compare_group

# database connection
conn = psycopg2.connect("dbname='health_trajectory' user='omidvarb' host='localhost' password='212799'")
cur = conn.cursor()

class BufferItem:
    def __init__(self, cohort, similarity):
        self.cohort = cohort
        self.similarity = similarity
    def __repr__(self):
    	return repr((self.cohort, self.similarity))

def form_query_out_of_attribs(attribs):
	query = "select id_patient from patients where "
	for attrib in attribs:
		if attrib == "city":
			facet = attrib + " like '%" + attribs[attrib] + "%' and "
		else:
			facet = attrib + "='" + attribs[attrib] + "' and "
		query += facet
	return query[:-5]+";"

def calculate_contribution_ratios():
	contribution_ratio_table = {}
	attrib_lists = {}
	attrib_value_lists = {}
	attrib_cnt = 0
	for attrib in cohort_attribs:
		attrib_lists[attrib_cnt] = attrib
		attrib_value_lists[attrib_cnt] = cohort_attribs[attrib]
		attrib_cnt += 1
	for i in range(0,len(cohort_attribs)):
		cohort_attribs_copy = {}
		for j in range(0,len(cohort_attribs)):
			if i != j:
				key = attrib_lists[j]
				value = attrib_value_lists[j]
				cohort_attribs_copy[key] = value
		cohort_query_filtered = form_query_out_of_attribs(cohort_attribs_copy)
		cur.execute(cohort_query_filtered)
		cohort_filtered_rows = cur.fetchall()
		bigger_size = len(cohort_filtered_rows)
		contribution_ratio = (float(bigger_size) - float(cohort_size)) / float(bigger_size)
		contribution_ratio_table[attrib_lists[i]] = contribution_ratio
	return contribution_ratio_table

def get_attribute_values(attribute):
	values = []
	query = "select distinct "+attribute+" from patients;"
	cur.execute(query)
	rows = cur.fetchall()
	for row in rows:
		if str(row[0]) != cohort_attribs[attribute] and row[0]!=None:
			values.append(str(row[0]))
	return values

def get_cohort_domain(cohort_query):
	values = []
	query = "select distinct label from actions where type='pec' and id_patient in ("+cohort_query[:-1]+");"
	cur.execute(query)
	rows = cur.fetchall()
	for row in rows:
		if row[0]!=None:
			values.append(str(row[0]))
	return values

def sort_buffer(MyBuffer):
	for passnum in range(k-1,0,-1):
		for i in range(passnum):
			if MyBuffer[i].similarity < MyBuffer[i+1].similarity:
				temp = MyBuffer[i]
				MyBuffer[i] = MyBuffer[i+1]
				MyBuffer[i+1] = temp
	return MyBuffer

# cohort_attribs = {}
# cohort_attribs["gender"] = "F"
# cohort_attribs["department"] = "34"
# cohort_attribs["dead"] = "True"

cohort_attribs = {}
cohort_attribs["zipcode"] = "38000"
cohort_attribs["dead"] = "True"
cohort_attribs["gender"] = "F"

k = 3 # number of cohorts to explore -- It is also the buffer size
timelimit = 1000000000 # in microseconds -- 1 sec = 1,000,000 microsecond

Buffer = {}
for i in range(0,k):
	myBufferItem = BufferItem("",0)
	Buffer[i]= myBufferItem

cohort_query = form_query_out_of_attribs(cohort_attribs)
print "Input cohort:", cohort_query
cur.execute(cohort_query)
cohort_rows = cur.fetchall()
cohort_size = len(cohort_rows)
cohort_domain = get_cohort_domain(cohort_query)
contribution_ratio_table = calculate_contribution_ratios()
sorted_contribution_ratio_table = sorted(contribution_ratio_table.items(), key=operator.itemgetter(1), reverse=False)

total_time = 0
for facet in sorted_contribution_ratio_table:
	candidate_attribute = facet[0]
	print "starting making alrernative cohorts for attribute ", candidate_attribute
	attribute_domain_values = get_attribute_values(candidate_attribute)
	for attribute_value in attribute_domain_values:
		time_begin = datetime.datetime.now()
		cohort_attribs_copy = cohort_attribs
		cohort_attribs_copy[candidate_attribute] = attribute_value.replace("'","")
		alternative_query = form_query_out_of_attribs(cohort_attribs_copy)
		alternative_cohort_domain = get_cohort_domain(alternative_query)
		# print alternative_query
		domain_intersect = [val for val in cohort_domain if val in alternative_cohort_domain]
		min_intersect = 1 # if the size of intersection is less than this number, the alternative cohort will be ignored
		# print len(domain_intersect)
		if len(domain_intersect) < min_intersect:
			continue
		else:
			# print "yay! domains match! for query ", alternative_query
			# sim_cohorts = float(len(domain_intersect)) / float(cohort_size)
			sim_cohorts = trajectories.compare_cohort_pair(cohort_query, alternative_query, 10, "score")
			if sim_cohorts == -1:
				continue
			print "their similarity is", sim_cohorts
			if sim_cohorts > Buffer[k-1].similarity:
				print "we will put in the buffer"
				myBufferItem = BufferItem(alternative_query,sim_cohorts)
				Buffer[k-1] = myBufferItem
				Buffer = sort_buffer(Buffer)
				print "--- buffer"
				print Buffer
		end_time = datetime.datetime.now()
		diff_time = end_time - time_begin
		total_time += diff_time.microseconds
		if total_time > timelimit:
			print "timelimit eceeded! Bye!"
			print Buffer
			exit()
			
print "*** BUFFER"
for buffer_cell in Buffer:
	print buffer_cell, Buffer[buffer_cell]
