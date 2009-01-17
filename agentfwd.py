#!/usr/bin/env python

from twisted.internet import reactor, protocol, error
from twisted.python import log
from optparse import OptionParser
import sys
import logging

oparser = OptionParser()
oparser.add_option('-d', '--debug', action='store_true',
                   help='Output debug messages to stderr')
oparser.set_defaults(
	debug=False
)

debug = log.msg

class EndMarker: pass

class TwistQueue:
	def __init__(self):
		self._q = []
		self.output_cb = None
		self.end_cb = None

	def setGetCallback(self, cb):
		self.output_cb = cb
		self.push_data()

	def push_data(self):
		while self.output_cb is not None and self._q:
			val = self.get()
			if val is EndMarker:
				if self.end_cb is not None:
					self.end_cb()
				self.end_cb = None
				self.get = None
				break
			self.output_cb(val)

	def put(self, val):
		self._q.append(val)
		self.push_data()

	def get(self):
		return self._q.pop(0)

	def close(self):
		if not self._q and self.output_cb is None:
			# nothing will notice this EndMarker unless another output_cb
			# gets attached. so, a dummy...
			self.output_cb = 1
		self.put(EndMarker)
		self.put = None

def drop_on_floor(data, endname):
	debug("%s gone; data dropped on floor: %r" % (endname, data))

class PipeEnd(protocol.Protocol):
	def connectionMade(self):
		debug("%s: connection made!" % self)
		debug("%s peer: %s" % (self, self.transport.getPeer()))
		self.my_inq.setGetCallback(self.transport.write)
		self.my_inq.end_cb = self.otherEndClosed

	def dataReceived(self, data):
		debug('%s: data received: %r' % (self, data))
		self.my_outq.put(data)

	def connectionLost(self, reason):
		# clear circular and external references
		if reason.check(error.ConnectionDone):
			debug("%s: connection closed cleanly." % self)
		else:
			debug("%s: connection lost: %s" % (self, reason))
		self.my_outq.close()
		self.my_inq.setGetCallback(lambda d: drop_on_floor(d, str(self)))
		self.my_inq.end_cb = None

	def otherEndClosed(self):
		debug("%s: other end closed." % self)
		self.transport.loseConnection()

class ClientEnd(PipeEnd):
	def connectionMade(self):
		self.my_outq = self.factory.my_outq
		self.my_inq = self.factory.my_inq
		PipeEnd.connectionMade(self)

class ServingEnd(PipeEnd):
	def connectionMade(self):
		self.my_outq = TwistQueue()
		self.my_inq = TwistQueue()
		PipeEnd.connectionMade(self)
		self.factory.connectClientEnd(self.getClientEndFactory())

	def getClientEndFactory(self):
		f = protocol.ClientFactory()
		f.protocol = ClientEnd
		f.my_inq = self.my_outq
		f.my_outq = self.my_inq
		return f

def ServePipes(connect_serving_end, connect_client_end):
	f = protocol.ServerFactory()
	f.protocol = ServingEnd
	f.connectClientEnd = connect_client_end
	return connect_serving_end(f)

if __name__ == "__main__":
	opts, args = oparser.parse_args(sys.argv[1:])
	if opts.debug:
		log.startLogging(sys.stderr)
	if len(args) != 2:
		sys.stderr.write('Need two arguments; existing and new socket paths\n')
		sys.exit(1)
	existing, newsock = args
	ServePipes(
		lambda f: reactor.listenUNIX(newsock, f),
		lambda f: reactor.connectUNIX(existing, f)
	)
	reactor.run()
