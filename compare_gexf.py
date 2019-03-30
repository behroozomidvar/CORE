import psycopg2
import datetime
import sys
import operator
import json
import math
import random
import elementtree.ElementTree as ET


class Event:
    def __init__(self, type, name, month):
        self.type = type # "B" for Begin and "E" for End
        self.name = name # The name of the treatment, "OXY", "PPC", etc.
        self.month = month # How many months after the first activity of the patient
        self.visited = False
    def __repr__(self):
    	return repr((self.type, self.name, self.month, self.confidence))

class CohortEvent:
    def __init__(self, type, name, confidence):
        self.type = type # "B" for Begin and "E" for End
        self.name = name # The name of the treatment, "OXY", "PPC", etc.
        self.confidence = 1.0
        self.support = 0
    def __repr__(self):
    	return repr((self.type, self.name, self.confidence, self.support))


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
	json_data = {}
	json_data["nodes"] = []
	json_data["edges"] = []
	sorted_events_p1 = get_sorted_events(patient_id_1)
	sorted_events_p2 = get_sorted_events(patient_id_2)
	event_cnt = 0
	for sub_event in sorted_events_p1:
		event_cnt += 1
		name = "p1_"+str(event_cnt)
		the_color = '#0F0'
		the_label = sub_event.name+"_"+sub_event.type
		if sub_event.type == "E":
			the_color = '#F00'
			the_label = ""
		json_data["nodes"].append({
			'id': name,
      		'label': the_label,
      		'x': sub_event.month,
      		'y': 0,
      		'size': 4,
      		'color': the_color
			})
	event_cnt = 0	
	for sub_event in sorted_events_p2:
		event_cnt += 1
		name = "p2_"+str(event_cnt)
		the_color = '#0F0'
		the_label = sub_event.name+"_"+sub_event.type
		if sub_event.type == "E":
			the_color = '#F00'
			the_label = ""
		json_data["nodes"].append({
			'id': name,
      		'label': the_label,
      		'x': sub_event.month,
      		'y': 10,
      		'size': 4,
      		'color': the_color
			})
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
				json_data["edges"].append({
					'id': "e_"+str(main_event_cnt)+"_"+str(sub_event_cnt),
      				'source': "p1_"+str(main_event_cnt),
		    		'target': "p2_"+str(sub_event_cnt),
      				'color': '#ccc',
    				'hover_color': '#000'
					})
				if last_month == 0:
					last_month = 0.00001
				cost = float(last_month - abs(main_event.month-sub_event.month)) / last_month
				sum_costs += cost
				# update cohort behavior - begin
				# which_cell = abs(main_event.month+sub_event.month)/2
				# new_behavior = CohortEvent(main_event.type,main_event.name,float(cost) / float(len(all_members)))
				# cohort_behavior[which_cell].append(new_behavior)
				# all_event_names.append(main_event.name)
				# update cohort behavior - end
				break
	with open('viz/data.json', 'w') as outfile:
		json.dump(json_data, outfile)
	crossings = 0
	prev_target = -1
	for target in targets:
		if target < prev_target:
			crossings += 1
		prev_target = target
	# print crossings, "crossings"
	# print float(sum_costs) / float(len(sorted_events_p1))
	return float(sum_costs) / float(len(sorted_events_p1)) * math.exp(crossings*-1)
def activity_sequence(patient_id):
	out = ""
	sorted_p = get_sorted_events(patient_id)
	for event in sorted_p:
		if event.type == "B":
			out+=event.name+" "
	return out
def make_json(patient_ids):
	json_data = {}
	json_data["nodes"] = []
	json_data["edges"] = []
	cnt_patient = 0
	for patient_id in patient_ids:
		cnt_patient += 1
		sorted_p = get_sorted_events(patient_id)
		cnt_event = 0
		previous_id = -1
		for event in sorted_p:
			current_id = "p"+str(patient_id)+"_"+str(cnt_event)
			json_data["nodes"].append({
				'id': current_id,
      			'label': event.name+"_"+event.type,
      		    'x': event.month,
      			'y': cnt_patient*10,
      			'size': 2,
      			'color': '#666'
				})
			if previous_id != -1:
				json_data["edges"].append({
				'id': "e_"+previous_id+"_"+current_id,
      			'source': previous_id,
      			'target': current_id,
      			'color': '#ccc',
    			'hover_color': '#000'
				})
			previous_id = current_id
			cnt_event += 1
	with open('viz/data.json', 'w') as outfile:
		json.dump(json_data, outfile)
# q = "select * from patients where gender = 'F' and zipcode = '38000' and dead=TRUE and birth_year > 1953"

# <?xml version="1.0" encoding="UTF-8"?>
# <gexf xmlns="http://www.gexf.net/1.2draft" version="1.2">
#     <graph mode="static" defaultedgetype="directed">
#         <nodes>
#             <node id="0" label="Hello" />
#             <node id="1" label="Word" />
#         </nodes>
#         <edges>
#             <edge id="0" source="0" target="1" />
#         </edges>
#     </graph>
# </gexf>

def compute_group_member_sim(query):
	all_users = []
	q_allusers = query
	cur.execute(q_allusers)
	rows = cur.fetchall()
	print "Number of subjects in the cohort:", len(rows)
	for row in rows:
		all_users.append(row[0])
	max_sim = 0
	min_sim = 1
	max_sim_u = ""
	min_sim_u = ""
	
	# build a tree structure
	root = ET.Element("gexf")
	root.set("xmlns","http://www.gexf.net/1.2draft")
	root.set("version","1.2")
	graph = ET.SubElement(root, "graph")
	graph.set("mode","static")
	graph.set("defaultedgetype","directed")
	nodes = ET.SubElement(graph, "nodes")
	edges = ET.SubElement(graph, "edges")

	node_as_users={}
	for user in all_users:
		node_as_users[user] = ET.SubElement(nodes,"node")
		node_as_users[user].set("id",str(user))
		node_as_users[user].set("label",str(user))
	edge_as_users={}
	cnt_edge = 0
	for user1 in all_users:
		for user2 in all_users:
			if user1 == user2:
				continue
			s = sim_users(user1,user2)
			# print user1, user2, s
			if s > 0:
				cnt_edge += 1
				edge_as_users[cnt_edge] = ET.SubElement(edges,"edge")
				edge_as_users[cnt_edge].set("id",str(cnt_edge))
				edge_as_users[cnt_edge].set("source",str(user1))
				edge_as_users[cnt_edge].set("target",str(user2))
				edge_as_users[cnt_edge].set("weight",str(s))
			if s < min_sim:
				min_sim = s
				min_sim_u = str(user1)+"-"+str(user2)
			if s > max_sim:
				max_sim = s
				max_sim_u = str(user1)+"-"+str(user2)
	# wrap it in an ElementTree instance, and save as XML
	tree = ET.ElementTree(root)
	tree.write("viz/group.gexf")
	# print min_sim, min_sim_u, max_sim, max_sim_u
# ********** FUNCTIONS **********

# cohort_query = "select id_patient from patients where gender = 'F' and zipcode = '38000' and dead=FALSE and birth_year > 1970"
# cohort_query = "select id_patient from patients where region = 'Ile-de-France ';"
cohort_query = "select id_patient from patients where city like '%TOULOUSE%' and dead=True;"
compute_group_member_sim(cohort_query)

# cur.execute(cohort_query)
# rows = cur.fetchall()
# print "Number of subjects in the cohort:", len(rows)
# all_sorted_events = {}
# all_members = []


# all_event_names = []
# last_month = 0
# cohort_behavior = {}
# for row in rows:
# 	id_patient = row[0]
# 	sorted_event = get_sorted_events(id_patient)
# 	if last_month < sorted_event[-1].month:
# 		last_month = sorted_event[-1].month
# 	all_sorted_events[id_patient]=sorted_event
# 	all_members.append(id_patient)

# print "Lenght of the period in month:", last_month
# branching_count = {}
# for user in all_members:
# 	branching_count[user]=0
# for i in range(0,last_month):
# 	cohort_behavior[i]=[]
# for user1 in all_members:
# 	print "doing branching for user", user1
# 	for user2 in all_members:
# 		if user1 == user2:
# 			continue
# 		if sim_users(user1,user2) == 0:
# 			branching_count[user1] += 1
# 			branching_count[user2] += 1

# print "branching done"
# print "branching counts"
# for user in all_members:
# 	print user, branching_count[user]

# all_event_names = set(all_event_names)

# # print "cohort behavior"
# behavior_matrix = {}
# for i in range(0,last_month):
# 	behavior_matrix[i]={}
# 	for event in all_event_names:
# 		behavior_matrix[i][event+"_B"]=0
# 		behavior_matrix[i][event+"_E"]=0

# print "behavior matrix done"

# max_support = 0
# for i in range(0,last_month):
# 	print "generate behavior on time", i
# 	distinct_segment_set = []
# 	for event1 in cohort_behavior[i]:
# 		found = False
# 		for event2 in distinct_segment_set:
# 			if (event1.type == event2.type and event1.name == event2.name):
# 				found = True
# 				event2.support += 1
# 				if event2.support > max_support:
# 					max_support = event2.support
# 		if found == False:
# 			distinct_segment_set.append(event1)
# 	if len(distinct_segment_set) != 0:
# 		# print "T"+str(i)+":",
# 		for event in distinct_segment_set:
# 			confidence = event.confidence * event.support / max_support
# 			behavior_matrix[i][str(event.name)+"_"+str(event.type)]=round(confidence,2)
# 		# print

# threshold = 0.1
# print "time",
# for event in all_event_names:
# 	print ","+str(event+"_B"),
# 	print ","+str(event+"_E"),
# print
# for i in range(0,last_month):
# 	found = False
# 	text = ""
# 	for event in all_event_names:
# 		if behavior_matrix[i][event+"_B"] < threshold:
# 			text += "0, "
# 		else:
# 			text += str(behavior_matrix[i][event+"_B"])+", "
# 			found = True
# 		if behavior_matrix[i][event+"_E"] < threshold:
# 			text += "0, "
# 		else:
# 			text += str(behavior_matrix[i][event+"_E"])+", "
# 			founs = True

# 	if found == True:
# 		print "T"+str(i)+","+text


