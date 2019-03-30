import psycopg2
import buffering
import events

conn = psycopg2.connect("dbname='health_trajectory' user='omidvarb' host='localhost' password='212799'")
cur = conn.cursor()

class DemographicAttribute:
	def __init__(self, attribute, value):
		self.attribute = attribute
		self.value = value
	def __repr__(self):
		return repr((self.attribute, self.value))

class Cohort:
	def __init__(self, members, demogs, actions):
		self.members = members
		self.demogs = demogs
		self.actions = actions
	def __repr__(self):
		return repr((self.members, self.demogs, self.actions))

def form_cohort_query(demogs,actions):
	# basic version -- should become more sohpisticated to accept markers as well.
	demogs_for_query = demogs.replace(","," and ")
	cohort_query = "select patients.id_patient from patients, actions where patients.id_patient = actions.id_patient and "+demogs_for_query+" and label in (" +actions+ ");"
	return cohort_query

def get_cohort_members(cohort_query):
	cohort_membres = []
	cur.execute(cohort_query)
	rows = cur.fetchall()
	for row in rows:
		cohort_membres.append(row[0])
	return cohort_membres

def represent_cohort(c, significance_threshold): # c is the cohort
	this_buffer = []
	trajectories = get_patient_trajectories(c.members)
	for p1 in c.members:
		for p2 in c.members:
			if p1 == p2:
				continue
			E = compare_trajectories(trajectories[p1], trajectories[p2])
			for e in E:
				this_buffer.append(buffering.BufferCell(e,0))
	buffer_merged = buffering.buffer_merge(this_buffer)
	cohort_representation = buffering.buffer_filter(buffer_merged,significance_threshold)
	return cohort_representation
	