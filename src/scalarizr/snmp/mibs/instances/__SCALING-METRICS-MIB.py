'''
Created on Sep 27, 2010

@author: marat
'''
from scalarizr.bus import bus
from scalarizr.queryenv import ScalingMetric
from scalarizr.util import read_shebang
import signal
import os, time, logging
from subprocess import Popen, PIPE
from scalarizr.util import system, kill_childs
from threading import Thread
from Queue import Queue, Empty

# SNMP imports
from pysnmp.smi.builder import MibBuilder
from scalarizr.config import ScalarizrState
(MibScalarInstance, MibTableRow, MibTable, MibTableColumn, Integer32, ModuleIdentity) = mibBuilder.importSymbols(
		'SNMPv2-SMI',
		'MibScalarInstance', 'MibTableRow' , 'MibTable', 'MibTableColumn', 'Integer32', 'ModuleIdentity')
(DisplayString, ) = mibBuilder.importSymbols('SNMPv2-TC', 'DisplayString')
#(mtxTable, mtxEntry, mtxIndex, mtxId, mtxName, mtxValue, mtxError) = mibBuilder.importSymbols(
#		'SCALING-METRICS-MIB', 
#		'mtxTable', 'mtxEntry', 'mtxIndex', 'mtxId', 'mtxName', 'mtxValue', 'mtxError')


scalr = ModuleIdentity((1, 3, 6, 1, 4, 1, 40000))
mtxTable = MibTable((1, 3, 6, 1, 4, 1, 40000, 5))
mtxEntry = MibTableRow((1, 3, 6, 1, 4, 1, 40000, 5, 1)).setIndexNames((0, "SCALING-METRICS-MIB", "mtxIndex"))
mtxIndex = MibTableColumn((1, 3, 6, 1, 4, 1, 40000, 5, 1, 1), Integer32()).setMaxAccess("readonly")
mtxId = MibTableColumn((1, 3, 6, 1, 4, 1, 40000, 5, 1, 2), Integer32()).setMaxAccess("readonly")
mtxName = MibTableColumn((1, 3, 6, 1, 4, 1, 40000, 5, 1, 3), DisplayString()).setMaxAccess("readonly")
mtxValue = MibTableColumn((1, 3, 6, 1, 4, 1, 40000, 5, 1, 4), DisplayString()).setMaxAccess("readonly")
mtxError = MibTableColumn((1, 3, 6, 1, 4, 1, 40000, 5, 1, 5), DisplayString()).setMaxAccess("readonly")

_metrics = None
_metrics_timestamp = None

logger = logging.getLogger(__name__)

class MtxTableImpl(MibTable):

	EXEC_TIMEOUT = 3
	'''
	Executing timeout for script when obtain metric with 'execute' method 
	'''

	_metrics = None
	_metrics_timestamp = None
	
	def getNextNode(self, name, idx):
		print 'Metric getnextnode'
		mibBuilder.lastBuildId += 1
		
		# Clean old values
		for k in mibBuilder.mibSymbols['__SCALING-METRICS-MIB'].keys():
			if k.startswith('mtxIndex') or k.startswith('mtxId') or k.startswith('mtxName') or k.startswith('mtxValue') or k.startswith('mtxEntry') or k.startswith('mtxError'):
				del mibBuilder.mibSymbols['__SCALING-METRICS-MIB'][k]
				
		# Update with new values
		mibBuilder.mibSymbols['__SCALING-METRICS-MIB'].update(values())
		return MibTable.getNextNode(self, name, idx)


MtxTableInst = MtxTableImpl(mtxTable.getName())

def values():
	global _metrics
	global _metrics_timestamp
	
	logger.info('metric values')
	queryenv = bus.queryenv_service
	cnf = bus.cnf
	
	# Obtain scaling metrics from Scalr. Cache result for 30 minutes
	now = time.time()
	if not _metrics or now - _metrics_timestamp > 1800:
		if cnf.state != ScalarizrState.IMPORTING: 
			_metrics = queryenv.get_scaling_metrics()
			_metrics_timestamp = now
		else:
			return dict()

	# TODO: investigate how efficiently will be do calculations in parallel
	
	ret = dict()
	
	ret.update({
		'mtxTable' : MtxTableInst,
		'scalr'    : scalr,
		'mtxIndex' : mtxIndex,
		'mtxId'    : mtxId,
		'mtxName'  : mtxName,
		'mtxValue' : mtxValue,
		'mtxError' : mtxError,
		'mtxEntry' : mtxEntry
		})
	
	index = 0
	queue = Queue()
	for metric in _metrics:
		queue.put(metric)
	
	workers = []
	for i in range(len(_metrics)):
		index += 1
		worker = Thread(target = update_metric, name = 'Worker-%s' % i, args = (queue, index, ret))
		worker.start()
		workers.append(worker)
		
	for worker in workers:
		worker.join()
				
	return ret

def _get_execute( metric):
	logger.info('metric get execute')
	if not os.access(metric.path, os.X_OK):
		raise BaseException("File is not executable: '%s'" % metric.path)
	
	proc = Popen(metric.path, stdout=PIPE, stderr=PIPE, close_fds=True)
	start_time = time.time()
	while time.time() - start_time < MtxTableImpl.EXEC_TIMEOUT:
		if proc.poll() is None:
			time.sleep(0.1)
		else:
			break
	else:
		print 'TimeOUT!!!', time.time() - start_time,
		if hasattr(proc, 'kill'):
			# python >= 2.6
			
			print 'Killing 2.6'
			kill_childs(proc.pid)
			proc.terminate()
		else:
			print 'Killing %s' % proc.pid
			kill_childs(proc.pid)
			os.kill(proc.pid, signal.SIGTERM)
		raise BaseException('Timeouted')
							
	print 'Communication',

	stdout, stderr = proc.communicate()
	
	if proc.returncode > 0:
		raise BaseException(stderr if stderr else 'exitcode: %d' % proc.returncode)
	
	logger.info('returning %s', stdout)
	return stdout

def _get_read( metric):
	print 'metric get read'
	if not os.access(metric.path, os.R_OK):
		raise BaseException("File is not readable: '%s'" % metric.path)
	
	file = None
	try:
		file = open(metric.path, 'r')
		value = file.readline()
	finally:
		if file:
			file.close()
		del file
	
	return value

def update_metric(queue, index, ret):
	error = ''
	value = 0.0
	try:
		metric = queue.get(False)
	except Empty:
		return
			
	try:
		# Retrieve metric value
		if ScalingMetric.RetriveMethod.EXECUTE == metric.retrieve_method:
			value = _get_execute(metric)
		elif ScalingMetric.RetriveMethod.READ  == metric.retrieve_method:
			value = _get_read(metric)
		else:
			raise BaseException('Unknown retrieve method %s' % metric.retrieve_method)
		
		# Convert value to float
		try:
			value = float(value)
		except ValueError, e:
			raise ValueError("Cannot convert value '%s' to float" % value)
			
	except (BaseException, Exception), e:
		value = 0.0
		error = str(e)[0:255]

		
	# Export MibTableRow

	ret.update({
		'mtxIndex%s' % index : MibScalarInstance(mtxIndex.getName(), (index,), mtxIndex.getSyntax().clone(
			index
		)),
		'mtxId%s' % index : MibScalarInstance(mtxId.getName(), (index,), mtxId.getSyntax().clone(
			int(metric.id)
		)),
		'mtxName%s' % index : MibScalarInstance(mtxName.getName(), (index,), mtxName.getSyntax().clone(
			metric.name
		)),
		'mtxValue%s' % index : MibScalarInstance(mtxValue.getName(), (index,), mtxValue.getSyntax().clone(
			'%.7f' % value
		)),
		'mtxError%s' % index : MibScalarInstance(mtxError.getName(), (index,), mtxError.getSyntax().clone(
			error
		))
	})
	

mibBuilder.mibSymbols["__SCALING-METRICS-MIB"] = values()