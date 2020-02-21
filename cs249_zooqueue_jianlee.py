from kazoo.client import KazooClient
import time, sys, re

_WORKPATH = "/workitems"
_RESULTPATH= "/results"

class Zooqueue():

	def __init__(self, host, queue_path, agent_name):
		self.host = host
		self.queue_path = queue_path
		self.agent_name = agent_name
		self.zk = KazooClient(hosts=self.host)
		#1. Connect to Server
		self.zk.start()
		print("\n1. Connected to ZooKeeper server as : "+self.agent_name)

		#2. Enter Queue
		self.en_queue()

		try:
			while True:
				time.sleep(3)
		## ***RUBRIC : Zookeeper disconnection
		except KeyboardInterrupt:
			print("\n!!Disconnecting agent from server.")
			self.zk.stop()
			self.zk.close()
			sys.exit(0)
	
	def en_queue(self):
		# ***RUBRIC : Agent's znode created under /queue with EPHEMERAL, SEQUENCE option enabled
		self.agent_abs_path = self.zk.create(path=self.queue_path+"/"+self.agent_name, ephemeral=True, sequence=True, makepath=True)
		self.agent_name_num = self.agent_abs_path[7:]
		print("\n2. Entered Queue as : "+self.agent_abs_path)
		
		#3. Check Queue Status
		self.check_queue()

	def check_queue(self,event=None):
		print("\n3. Checking queue status")
		sorted_queue_list = self.getchildren(self.queue_path)
		agent_index = sorted_queue_list.index(self.agent_name_num)

		#There is an agent ahead of queue
		if agent_index != 0 :
			#4-0. Set watch to wait for turn (RUBRIC : WAITING FOR THE HEAD OF QUEUE (watch))
			self.zk.exists(self.queue_path+"/"+sorted_queue_list[agent_index-1], watch=self.check_queue)
			print(">>Queue Status: ",sorted_queue_list)
			print(">>...Waiting for TURN...")
		
		elif agent_index == 0:
			print(">>First in line!")		
			#4-1. Take a work item to process
			self.processwork()

	def processwork(self,event=None):
		print("\n4. Processing Work Item")
		sorted_work_items = self.getchildren(_WORKPATH)
		
		#Empty work item list
		if not sorted_work_items:
			# ***RUBRIC : WAITING FOR WORK ITEM (watch)
			self.zk.get_children(_WORKPATH, watch=self.processwork)
			print(">>...Waiting for WORK...")

		else:
			#5. Process a work item with the lowest sequence number
			self.zk.delete(_WORKPATH+"/"+sorted_work_items[0])
			print(">>Processed item : "+_WORKPATH+"/"+sorted_work_items[0])

			self.addresult(sorted_work_items[0])
	
	def addresult(self,workitem):
		message = self.agent_name_num+" processed "+workitem
		bmessage= str.encode(message)

		#6. Update Result of processed work item (RUBRIC : result znode created under /results with SEQUENCE option enabled)
		result_path = self.zk.create(path=_RESULTPATH+"/"+self.agent_name, value=bmessage, sequence=True, makepath=True)
		print("\n5. Result added to : "+result_path)
		
		#7. Move to the back of the queue
		self.de_queue()
		
	def de_queue(self):
		print("\n6. Dequeue agent "+self.agent_name)
		self.zk.delete(path=self.agent_abs_path)
		print("---------------Round Completed------------------")
		self.en_queue()

	def get_last10(self, elem):
		return elem[-10:]

	#Wrapper for zk.get_children, provides sorted list of children in given path
	def getchildren(self, check_path):
		children = self.zk.get_children(check_path)
		return sorted(children, key=self.get_last10)

zq = Zooqueue(host=sys.argv[1], queue_path=sys.argv[2], agent_name=sys.argv[3])
