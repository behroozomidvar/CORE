import psycopg2
import datetime
import sys
import operator
import json
import math
import random

class Event:
    def __init__(self, type, name, month):
        self.type = type # "B" for Begin and "E" for End
        self.name = name # The name of the treatment, "OXY", "PPC", etc.
        self.month = month # How many months after the first activity of the patient
        self.visited = False
    def __repr__(self):
    	return repr((self.type, self.name, self.month))

class CohortEvent:
    def __init__(self, type, name, confidence):
        self.type = type # "B" for Begin and "E" for End
        self.name = name # The name of the treatment, "OXY", "PPC", etc.
        self.confidence = 1.0
        self.support = 0
    def __repr__(self):
    	return repr((self.type, self.name, self.confidence, self.support))
class GraphNode:
	def __init__(self,node_id,label,x,y,size,color):
		self.node_id = node_id
		self.label = label
		self.x = x
		self.y = y
		self.size = size
		self.color = color

class GraphEdge:
	def __init__(self,edge_id,source,target):
		self.edge_id = edge_id
		self.source = source
		self.target = target

class Graph:
	def __init__(self):
		self.nodes = []
		self.edges = []
	def add_node(node_id,label,x,y,size,color):
		my_node = GraphNode(node_id,label,x,y,size,color)
		self.nodes.append(my_node)
	def add_edge(edge_id,source,target):
		my_edge = GraphEdge(edge_id,source,target)
		self.edges.append(my_edge)	
	def to_file(filename):
		f = open(filename, 'w')
		json_data = {}
		json_data["nodes"] = self.nodes
		json_data["edges"] = self.edges
		json.dump(json_data, f)

conn = psycopg2.connect("dbname='health_trajectory' user='omidvarb' host='localhost' password='212799'")
cur = conn.cursor()

# ********** FUNCTIONS **********
def month_diff(first_date,current_date):
	# I've hard-coded the granularity of month here
	# 2016-09-05 00:00:00
	first_year = int(first_date[0:4])
	first_month = int(first_date[5:7])
	current_year = int(current_date[0:4])
	current_month = int(current_date[5:7])
	return abs(current_year-first_year)*12 + (first_month - current_month)

def get_sorted_events(patient_id):
	query_activity = "select from_date,to_date,value from activities where id_patient = '"+str(patient_id)+"' and entity = 'treatment' and type = 'pec' order by to_date"
	cur.execute(query_activity)
	rows = cur.fetchall()
	event_list = []
	first_date_ever = ""
	event_cnt = 0
	for row in rows:
		if event_cnt == 0:
			first_date_ever = str(row[0])
		event_list.append(Event("B", row[2], month_diff(str(row[0]),first_date_ever)))
		event_list.append(Event("E", row[2], month_diff(str(row[1]),first_date_ever)))
		event_cnt = event_cnt +1
		sorted_events = sorted(event_list, key=operator.attrgetter('month'))
	return sorted_events

def sim_users(patient_id_1, patient_id_2):
	targets= []
	sorted_events_p1 = get_sorted_events(patient_id_1)
	sorted_events_p2 = get_sorted_events(patient_id_2)
	event_cnt = 0
	last_month = sorted_events_p1[-1].month
	if last_month < sorted_events_p2[-1].month:
		last_month = sorted_events_p2[-1].month
	sum_costs = 0
	main_event_cnt = 0
	for main_event in sorted_events_p1:
		main_event_cnt += 1
		sub_event_cnt = 0
		for sub_event in sorted_events_p2:
			sub_event_cnt += 1
			if sub_event.visited == True:
					continue
			if sub_event.type == main_event.type and sub_event.name == main_event.name:
				# print main_event.name, main_event.type, sub_event.name, sub_event.type, main_event.month, sub_event.month, float(abs(last_month - (main_event.month-sub_event.month))) / last_month
				targets.append(sub_event.month)
				sub_event.visited = True
				if last_month == 0:
					last_month = 0.00001
				cost = float(last_month - abs(main_event.month-sub_event.month)) / last_month
				# print "***",cost 
				sum_costs += cost
				break
	# print crossings, "crossings"
	return float(sum_costs) / float(len(sorted_events_p1))
	# return float(sum_costs) / float(len(sorted_events_p1)) * math.exp(crossings*-1)
def activity_sequence(patient_id):
	out = ""
	sorted_p = get_sorted_events(patient_id)
	for event in sorted_p:
		if event.type == "B":
			out+=event.name+" "
	return out


# ********** FUNCTIONS **********

# cohort_query = "select id_patient from patients where gender = 'F' and zipcode = '38000' and dead=FALSE and birth_year > 1970"
# cohort_query = "select id_patient from patients where id_patient in ('96095','14892','66813','66210')"
# cohort_query = "select id_patient from patients where gender = 'F' and zipcode = '38000' and dead=FALSE and birth_year > 1970 and id_patient not in ('96095','14892','66813','66210', '43050','18916','10658','87498', '97110', '30037', '68651', '94890', '37219', '17682', '68049', '97599', '14996', '87037', '58620', '28440')"

# cohort_query = "select id_patient from patients where id_patient in ('47665','53580','79833','15974','33651','92437','72694','85561','58524','62591','83814','90131')"
cohort_query = "select id_patient from patients where gender = 'F' and zipcode = '38000' and birth_year > 1970 and dead=False;"
# compute_group_member_sim(cohort_query)

# p = sim_users(20620,17569)

cur.execute(cohort_query)
rows = cur.fetchall()
print "Number of subjects in the cohort:", len(rows)
all_sorted_events = {}
all_members = []
all_event_names = []
last_month = 0
cohort_behavior = {}
for row in rows:
	all_members.append(row[0])

max_s = 0
the_member = 0
for member1 in all_members:
	sum_s = 0
	for member2 in all_members:
		if member1 == member2:
			continue
		s = sim_users(member1,member2)
		sum_s += s
	print member1, sum_s
	if sum_s > max_s:
		max_s = sum_s
		the_member = member1
print "**", the_member, max_s





		