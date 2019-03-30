import tornado.ioloop
import tornado.web
import os.path
from tornado import template
import trajectories
import sequence_matching

class MainHandler(tornado.web.RequestHandler) :
	def get(self):
		self.redirect('static/index.html')

class RepresentHandler(tornado.web.RequestHandler) :
	def get(self):
		example_input = self.get_argument("q")
		cohort_query = "SELECT ID_PATIENT FROM PATIENTS WHERE " + example_input + ";"
		representation = trajectories.represent_cohort(cohort_query, 5)
		self.render('represent.html', representation=representation.split("\n"))

settings = dict(
	template_path = os.path.join(os.path.dirname(__file__), "templates"),
	static_path = "static",
	debug = True
)

	
application = tornado.web.Application([
	(r"/", MainHandler),
	# (r"/prepare", PrepareHandler),
	(r"/represent", RepresentHandler),
	], **settings)



if __name__ == "__main__":
	print "Server running ..."
	application.listen(8888)
	tornado.ioloop.IOLoop.instance().start()