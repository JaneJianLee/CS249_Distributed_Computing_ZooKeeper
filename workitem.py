#Generate Workitem for ZooQueue

from kazoo.client import KazooClient
import time, sys, re

_WORKPATH = "/workitems"

class Workitem():

	def __init__(self, host):
		self.host = host
		self.zk = KazooClient(hosts=self.host)
		
        #1. Connect to Server
		self.zk.start()
		print("\n Generating New Work Items for ZooQueue Agents)

		try:
			while True:
				self.createnew()
                time.sleep(5)

		except KeyboardInterrupt:
			print("\n Ending Creating New Work Items ")
			self.zk.stop()
			self.zk.close()
			sys.exit(0)
        
    def createnew(self):
        itempath = self.zk.create(path=_WORKPATH+"/"+"item-", sequence=True, makepath=True)
        print("Created new item : "+itempath)

zq = Workitem(host=sys.argv[1])