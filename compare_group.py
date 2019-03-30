import psycopg2
import datetime
import sys
import operator
import json
import math
import random
import time

class Event:
    def __init__(self, type, name, month):
        self.type = type # "B" for Begin and "E" for End
        self.name = name # The name of the treatment, "OXY", "PPC", etc.
        self.month = month # How many months after the first activity of the patient
        self.visited = False
    def __repr__(self):
    	return repr((self.type, self.name, self.month))
class CohortEvent:
    def __init__(self, type, name, confidence, month):
        self.type = type # "B" for Begin and "E" for End
        self.name = name # The name of the treatment, "OXY", "PPC", etc.
        self.confidence = confidence
        self.support = 0
        self.month = month
        self.visited = False
    def __repr__(self):
    	return repr((self.type, self.name, self.confidence, self.month))

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
def retrieve_trajectories(patient_id):
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
def activity_sequence(patient_id):
	out = ""
	sorted_p = retrieve_trajectories(patient_id)
	for event in sorted_p:
		if event.type == "B":
			out+=event.name+" "
	return out
def compute_group_member_sim(query):
	all_users = []
	q_allusers = query
	cur.execute(q_allusers)
	rows = cur.fetchall()
	for row in rows:
		all_users.append(row[0])
	max_sim = 0
	min_sim = 1
	max_sim_u = ""
	min_sim_u = ""
	json_data = {}
	json_data["nodes"] = []
	json_data["edges"] = []
	for user in all_users:
		json_data["nodes"].append({
			'id': "p"+str(user),
      		'label': str(user),
      		'x': random.randint(1,10),
      		'y': random.randint(1,10),
      		'size': 2,
      		'color': '#666'
			})
	for user1 in all_users:
		for user2 in all_users:
			if user1 == user2:
				continue
			s = sim_users(user1,user2)
			print user1, user2, s
			if s > 0:
				json_data["edges"].append({
					'id': "e_"+str(user1)+"_"+str(user2),
      				'label': str(s),
      				'source': "p"+str(user1),
      				'target': "p"+str(user2),
      				'size': s*10000,
      				'color': '#666'
					})
			if s < min_sim:
				min_sim = s
				min_sim_u = str(user1)+"-"+str(user2)
			if s > max_sim:
				max_sim = s
				max_sim_u = str(user1)+"-"+str(user2)
	with open('viz/data.json', 'w') as outfile:
		json.dump(json_data, outfile)
	print min_sim, min_sim_u, max_sim, max_sim_u
def get_group_events(query,threshold):
	cur.execute(query)
	rows = cur.fetchall()
	cnt_warp= 0
	# print "Number of subjects in the cohort:", len(rows)
	all_sorted_events = {} # stores all sorted events for each patient
	all_members = []
	all_event_names = []
	last_month = 0 # longest period in the cohort
	cohort_behavior = []

	start = time.time()
	for row in rows:
		id_patient = row[0]
		sorted_event = retrieve_trajectories(id_patient)
		if last_month < sorted_event[-1].month:
			last_month = sorted_event[-1].month
		all_sorted_events[id_patient]=sorted_event
		all_members.append(id_patient)
	end = time.time()
	print "computing local similarities", (end - start), "seconds"

	# threshold = 0.005
	start = time.time()
	for user1 in all_members:
		for user2 in all_members:
			if user1 == user2:
				continue
			sorted_events_p1 = all_sorted_events[user1]
			sorted_events_p2 = all_sorted_events[user2]
			local_last_month = sorted_events_p1[-1].month
			if local_last_month < sorted_events_p2[-1].month:
				local_last_month = sorted_events_p2[-1].month
			sum_costs = 0
			for main_event in sorted_events_p1:
				for sub_event in sorted_events_p2:
					if sub_event.visited == True:
							continue
					if sub_event.type == main_event.type and sub_event.name == main_event.name:
						sub_event.visited = True
						if local_last_month == 0:
							local_last_month = 0.00001
						cost = float(last_month - abs(main_event.month-sub_event.month)) / last_month
						final_cost = float(cost) / float(len(all_members))
						if final_cost >= threshold:
							already_exists = False
							which_cell = abs(main_event.month+sub_event.month)/2
							for cell in cohort_behavior:
								if cell.name == main_event.name and cell.type==main_event.type and cell.month==which_cell:
									already_exists = True
									break
							if already_exists == False:
								cnt_warp += 1
								new_behavior = CohortEvent(main_event.type,main_event.name,final_cost,which_cell)
								cohort_behavior.append(new_behavior)
								break
	cohort_behavior_sorted = sorted(cohort_behavior, key=operator.attrgetter('month'))
	# print "nb warps", cnt_warp
	end = time.time()
	print "computing warps", (end - start), "seconds"
	return cohort_behavior_sorted
def identify_treatment_color(treatment):
	# print "**"+treatment+"**"
	if treatment == "PPC":
		return "'rgb(141,160,203)'"
	if treatment == "OXY":
		return "'rgb(141,160,203)'"

def sim_cohorts(cohort1_events, cohort2_events):
	targets = []
	json_data = {}
	json_data["nodes"] = []
	json_data["edges"] = []
	last_month = cohort1_events[-1].month
	print "***", len(cohort2_events)
	if last_month < cohort2_events[-1].month:
		last_month = cohort2_events[-1].month
	# make JSON nodes - begin
	x_print_cnt = {}
	for i in range(0,last_month+1):
		x_print_cnt[i] = -1
	event_cnt = 0
	for sub_event in cohort1_events:
		event_cnt += 1
		name = "p1_"+str(event_cnt)
		the_color = '#0F0'
		the_label = sub_event.name+"_"+sub_event.type+" @ T"+str(sub_event.month)
		x_print_cnt[sub_event.month] += 1
		the_x = sub_event.month
		the_y= x_print_cnt[sub_event.month]
		if sub_event.type == "E":
			the_color = '#F00'
			the_x += 10
			# the_label = ""
		
		json_data["nodes"].append({
			'id': name,
      		'label': the_label,
      		'x': the_x+20,
      		'y': 0,
      		'size': 4,
      		'color': the_color
			})
		prev_month = sub_event.month

	for i in range(0,last_month+1):
		x_print_cnt[i] = -1
	event_cnt = 0	
	for sub_event in cohort2_events:
		event_cnt += 1
		name = "p2_"+str(event_cnt)
		the_color =  '#0F0'
		the_label = sub_event.name+"_"+sub_event.type+" @ T"+str(sub_event.month)
		the_x = sub_event.month
		x_print_cnt[sub_event.month] += 1
		the_y= x_print_cnt[sub_event.month]
		# print "decision:", last_x, the_x
		if sub_event.type == "E":
			the_color = '#F00'
			# the_label = ""
			the_x += 10
		# print "position", the_label, the_y, the_x
		json_data["nodes"].append({
			'id': name,
      		'label': the_label,
      		'x': the_x+20,
      		'y': 20,
      		'size': 4,
      		'color': the_color
			})
		
	# make JSON nodes - end
	sum_costs = 0
	sth_cnt = 0
	main_event_cnt = 0
	for main_event in cohort1_events:
		# print "checking", main_event
		main_event_cnt += 1
		sub_event_cnt = 0
		for sub_event in cohort2_events:
			sub_event_cnt += 1
			if sub_event.visited == True:
				continue
			if sub_event.type == main_event.type and sub_event.name == main_event.name:
				json_data["edges"].append({
					'id': "e_"+str(main_event_cnt)+"_"+str(sub_event_cnt),
      				'source': "p1_"+str(main_event_cnt),
		    		'target': "p2_"+str(sub_event_cnt),
      				'color': '#ccc',
    				'hover_color': '#000',
    				})
				sth_cnt += 1
				sub_event.visited = True
				if last_month == 0:
					last_month = 0.00001
				cost = float(last_month - abs(main_event.month-sub_event.month)) / last_month
				targets.append(sub_event.month)
				# print "map", main_event.name, main_event.type, main_event.month, main_event.visited, "to", sub_event.name, sub_event.type, sub_event.month, sub_event.visited, "with value", cost, "max month", last_month
				# print main_event.type, main_event.name, cost
				sum_costs += cost
				break
	with open('viz/data.json', 'w') as outfile:
		json.dump(json_data, outfile)
	
	crossings = 0
	prev_target = -1
	for target in targets:
		if target < prev_target:
			crossings += 1
		prev_target = target
	# print "crossings", crossings
	return float(sum_costs) / float(sth_cnt) * math.exp(crossings*-1)
# ********** FUNCTIONS **********

# cohort1_query = "select id_patient from patients where gender = 'F' and zipcode = '38000' and dead=FALSE and birth_year > 1950"
# cohort2_query = "select id_patient from patients where gender = 'F' and zipcode = '38000' and dead=TRUE and birth_year > 1950"
# cohort3_query = "select id_patient from patients where gender = 'F' and zipcode = '5100' and dead=TRUE;"
# cohort4_query = "select id_patient from patients where region = 'Ile-de-France ';" # dirty region
# cohort4_query = "select id_patient from patients where region = 'Ile-de-France ' and id_patient not in ('47665','53580','79833','15974','33651','92437','72694','85561','58524','62591','83814','90131')"
# cohort6_query = "select id_patient from patients where region = 'Auvergne ';"		# clean region
# cohort6_query = "select id_patient from patients where city like '%MARSEILLE%';" # other dirtr region
# cohort6_query = "select id_patient from patients where city like '%TOULOUSE%' and dead=True;"
# random1_query = "select id_patient from patients order by random() limit 50;"
# random2_query = "select id_patient from patients order by random() limit 50;"

# start = time.time()
# cohort1_events = get_group_events(random1_query,0.001)
# print

# cohort2_events = get_group_events(random2_query,0.001)
# end = time.time()
# # print "cohort construction time", (end - start), "seconds"

# start = time.time()
# print sim_cohorts(cohort1_events,cohort2_events)
# end = time.time()
# print "comparison time", (end - start), "seconds"


