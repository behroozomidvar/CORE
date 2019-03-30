f_patients = open("patients.dat")
f_medoid = open("out_center_100.dat")
f_cluster = open("out_label_100.dat")

patient_map = {}
line_cnt = 0
for line in f_patients:
	line = line.strip()
	patient_map[line_cnt] = int(line)
	line_cnt += 1

medoid = {}
line_cnt = 0
for line in f_medoid:
	line = line.strip()
	medoid[line_cnt] = int(line)
	line_cnt += 1

patient_cluster = {}
line_cnt = 0
for line in f_cluster:
	line = line.strip()
	parts = line.split(" ")
	patient_cluster[int(parts[0])] = int(parts[1])

for patient in patient_map:
	cluster = patient_cluster[patient]
	the_medoid = medoid[cluster]
	print str(patient_map[patient])+","+str(patient_map[the_medoid])+",100"