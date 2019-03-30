import trajectories
import psycopg2

conn = psycopg2.connect("dbname='health_trajectory' user='omidvarb' host='localhost' password='212799'")
cur = conn.cursor()

patient_id = "33417"

extended_query = "select from_date,to_date,value,category,entity,id_patient from activities where entity = 'treatment' and type = 'pec' and id_patient = "+patient_id
cur.execute(extended_query)
rows = cur.fetchall()

print trajectories.retrieve_trajectories("33417",rows)