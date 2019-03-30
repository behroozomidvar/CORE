class Event:
	def __init__(self, patient, action, time):
		self.patient = patient
		self.action = action # one unique action here
		self.time = time
	def __repr__(self):
		return repr((self.patient, self.action, self.time))

class AggregatedEvent:
	def __init__(self, members, action, psi):
		self.members = members
		self.action = action # one unique action here
		self.psi = psi # list of timestamps, note that it is not a set
	def __repr__(self):
		return repr((self.members, self.action, self.psi))

def convert_to_aggregated_event(e):
	members = []
	psi = []
	members.append(e.patient)
	psi.append(e.time)
	action = e.action
	return AggregatedEvent(members,action,psi)